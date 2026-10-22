package armazenamento

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/luuanvieira/monitor-processo/internal/alarmes"
	"github.com/luuanvieira/monitor-processo/internal/processo"
)

// Escritor grava em lote, fora do caminho crítico: o motor de alarmes nunca
// espera o banco. Se o banco ficar fora, as leituras se acumulam até
// MaxPendentes; a partir daí as mais antigas são descartadas (e contadas).
// Eventos de alarme nunca são descartados, porque são o registro de auditoria.
type Escritor struct {
	store        Store
	Intervalo    time.Duration
	TamanhoLote  int
	MaxPendentes int
	log          *slog.Logger

	mu       sync.Mutex
	leituras []processo.Leitura
	eventos  []alarmes.Evento
	sinal    chan struct{}

	Gravadas    atomic.Uint64
	Descartadas atomic.Uint64
	Erros       atomic.Uint64
}

func NovoEscritor(store Store, log *slog.Logger) *Escritor {
	return &Escritor{
		store:        store,
		Intervalo:    time.Second,
		TamanhoLote:  1000,
		MaxPendentes: 100_000,
		log:          log,
		sinal:        make(chan struct{}, 1),
	}
}

// Enfileirar nunca bloqueia o chamador por I/O.
func (e *Escritor) Enfileirar(ls []processo.Leitura, ev []alarmes.Evento) {
	e.mu.Lock()
	e.leituras = append(e.leituras, ls...)
	e.eventos = append(e.eventos, ev...)
	e.limitar()
	cheio := len(e.leituras) >= e.TamanhoLote
	e.mu.Unlock()
	if cheio {
		select {
		case e.sinal <- struct{}{}:
		default:
		}
	}
}

// Pendentes retorna quantas leituras e eventos aguardam gravação.
func (e *Escritor) Pendentes() (leituras, eventos int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.leituras), len(e.eventos)
}

// limitar descarta as leituras mais antigas acima de MaxPendentes. Chamar com mu travado.
func (e *Escritor) limitar() {
	if excesso := len(e.leituras) - e.MaxPendentes; excesso > 0 {
		e.Descartadas.Add(uint64(excesso))
		e.leituras = append(e.leituras[:0:0], e.leituras[excesso:]...)
	}
}

// Executar grava periodicamente até o contexto ser cancelado; então faz uma
// última gravação com prazo próprio para não perder o que estava pendente.
func (e *Escritor) Executar(ctx context.Context) {
	ticker := time.NewTicker(e.Intervalo)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.gravar(ctx)
		case <-e.sinal:
			e.gravar(ctx)
		case <-ctx.Done():
			final, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			e.gravar(final)
			cancel()
			if l, ev := e.Pendentes(); l > 0 || ev > 0 {
				e.log.Error("encerrando com dados não gravados", "leituras", l, "eventos", ev)
			}
			return
		}
	}
}

func (e *Escritor) gravar(ctx context.Context) {
	e.mu.Lock()
	leituras, eventos := e.leituras, e.eventos
	e.leituras, e.eventos = nil, nil
	e.mu.Unlock()

	devolver := func(ls []processo.Leitura, ev []alarmes.Evento) {
		e.mu.Lock()
		e.leituras = append(ls, e.leituras...)
		e.eventos = append(ev, e.eventos...)
		e.limitar()
		e.mu.Unlock()
	}

	if len(eventos) > 0 {
		if err := e.store.SalvarEventos(ctx, eventos); err != nil {
			e.Erros.Add(1)
			e.log.Warn("falha ao gravar eventos; nova tentativa no próximo ciclo", "erro", err, "pendentes", len(eventos))
			devolver(leituras, eventos)
			return
		}
	}
	for len(leituras) > 0 {
		n := min(len(leituras), e.TamanhoLote)
		if err := e.store.SalvarLeituras(ctx, leituras[:n]); err != nil {
			e.Erros.Add(1)
			e.log.Warn("falha ao gravar leituras; nova tentativa no próximo ciclo", "erro", err, "pendentes", len(leituras))
			devolver(leituras, nil)
			return
		}
		e.Gravadas.Add(uint64(n))
		leituras = leituras[n:]
	}
}
