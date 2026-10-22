// Package app liga as peças: fonte de leituras (simulador ou API) -> motor de
// alarmes -> gravação em lote + transmissão em tempo real.
//
//	simulador ─┐
//	           ├─▶ fila ─▶ processador ─┬─▶ motor de alarmes
//	API POST ──┘                        ├─▶ escritor em lote ─▶ Store
//	                                    └─▶ broker SSE ─▶ dashboard
//
// Só existe um processador, então as leituras de um tag são avaliadas sempre
// na ordem em que chegaram.
package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/luuanvieira/monitor-processo/internal/alarmes"
	"github.com/luuanvieira/monitor-processo/internal/armazenamento"
	"github.com/luuanvieira/monitor-processo/internal/processo"
	"github.com/luuanvieira/monitor-processo/internal/simulador"
	"github.com/luuanvieira/monitor-processo/internal/transmissao"
)

var ErrFilaCheia = errors.New("fila de processamento cheia, tente novamente")

type Config struct {
	Tags       []processo.Tag
	Store      armazenamento.Store
	Simulador  bool
	Semente    uint64
	Velocidade int           // segundos simulados por passo real
	Passo      time.Duration // intervalo real entre passos do simulador
	Log        *slog.Logger
}

type App struct {
	Tags      []processo.Tag
	TagsPorID map[string]processo.Tag
	Motor     *alarmes.Motor
	Planta    *simulador.Planta // nil quando o simulador está desligado
	Store     armazenamento.Store
	Escritor  *armazenamento.Escritor
	Broker    *transmissao.Broker

	velocidade int
	passo      time.Duration
	entrada    chan []processo.Leitura
	log        *slog.Logger

	Processadas atomic.Uint64
	Rejeitadas  atomic.Uint64
}

func Nova(cfg Config) *App {
	if cfg.Velocidade < 1 {
		cfg.Velocidade = 1
	}
	if cfg.Passo <= 0 {
		cfg.Passo = time.Second
	}
	a := &App{
		Tags:       cfg.Tags,
		TagsPorID:  map[string]processo.Tag{},
		Motor:      alarmes.NovoMotor(cfg.Tags),
		Store:      cfg.Store,
		Escritor:   armazenamento.NovoEscritor(cfg.Store, cfg.Log),
		Broker:     transmissao.NovoBroker(),
		velocidade: cfg.Velocidade,
		passo:      cfg.Passo,
		entrada:    make(chan []processo.Leitura, 256),
		log:        cfg.Log,
	}
	for _, t := range cfg.Tags {
		a.TagsPorID[t.ID] = t
	}
	if cfg.Simulador {
		a.Planta = simulador.Nova(cfg.Tags, cfg.Semente, time.Now().UTC().Truncate(time.Second))
	}
	return a
}

// Agora é o relógio da aplicação: o tempo simulado quando o simulador está
// ligado (que pode andar mais rápido que o real), senão o relógio do sistema.
func (a *App) Agora() time.Time {
	if a.Planta != nil {
		return a.Planta.Agora()
	}
	return time.Now().UTC()
}

// Receber coloca leituras na fila de processamento, esperando no máximo `espera`.
func (a *App) Receber(ctx context.Context, ls []processo.Leitura, espera time.Duration) error {
	t := time.NewTimer(espera)
	defer t.Stop()
	select {
	case a.entrada <- ls:
		return nil
	case <-t.C:
		a.Rejeitadas.Add(uint64(len(ls)))
		return ErrFilaCheia
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Executar roda simulador, processador e escritor até o contexto ser
// cancelado. O encerramento é em ordem: a fonte para, o processador esvazia a
// fila e o escritor grava o que restou.
func (a *App) Executar(ctx context.Context) {
	ctxEscritor, pararEscritor := context.WithCancel(context.Background())
	var wgEscritor sync.WaitGroup
	wgEscritor.Add(1)
	go func() { defer wgEscritor.Done(); a.Escritor.Executar(ctxEscritor) }()

	var wgSim sync.WaitGroup
	if a.Planta != nil {
		wgSim.Add(1)
		go func() { defer wgSim.Done(); a.simular(ctx) }()
	}

	processado := make(chan struct{})
	go func() { a.processar(ctx); close(processado) }()

	<-ctx.Done()
	wgSim.Wait()
	<-processado
	pararEscritor()
	wgEscritor.Wait()
	a.log.Info("pipeline encerrado",
		"processadas", a.Processadas.Load(), "gravadas", a.Escritor.Gravadas.Load(),
		"descartadas", a.Escritor.Descartadas.Load())
}

func (a *App) simular(ctx context.Context) {
	ticker := time.NewTicker(a.passo)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var lote []processo.Leitura
			for i := 0; i < a.velocidade; i++ {
				lote = append(lote, a.Planta.Passo(time.Second)...)
			}
			// Bloqueia se a fila estiver cheia: o simulador desacelera junto
			// com o processamento em vez de perder dados.
			select {
			case a.entrada <- lote:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (a *App) processar(ctx context.Context) {
	for {
		select {
		case lote := <-a.entrada:
			a.processarLote(lote)
		case <-ctx.Done():
			for {
				select {
				case lote := <-a.entrada:
					a.processarLote(lote)
				default:
					return
				}
			}
		}
	}
}

func (a *App) processarLote(lote []processo.Leitura) {
	var eventos []alarmes.Evento
	for _, l := range lote {
		eventos = append(eventos, a.Motor.Processar(l)...)
	}
	a.Processadas.Add(uint64(len(lote)))
	a.Escritor.Enfileirar(lote, eventos)
	a.publicar("leituras", ultimasPorTag(lote))
	if len(eventos) > 0 {
		a.publicar("alarmes", eventos)
		for _, e := range eventos {
			if e.Estado == alarmes.AtivoNaoReconhecido {
				a.log.Info("alarme", "tag", e.Tag, "tipo", e.Tipo, "prioridade", e.Prioridade, "mensagem", e.Mensagem)
			}
		}
	}
}

// RegistrarEventos grava e transmite eventos gerados fora do processador (reconhecimentos).
func (a *App) RegistrarEventos(ev []alarmes.Evento) {
	if len(ev) == 0 {
		return
	}
	a.Escritor.Enfileirar(nil, ev)
	a.publicar("alarmes", ev)
}

func (a *App) publicar(evento string, dados any) {
	if err := a.Broker.Publicar(evento, dados); err != nil {
		a.log.Error("falha ao transmitir", "evento", evento, "erro", err)
	}
}

// ultimasPorTag reduz um lote à leitura mais recente de cada tag (o dashboard
// não precisa de 10 pontos por segundo quando a simulação está acelerada).
func ultimasPorTag(lote []processo.Leitura) []processo.Leitura {
	idx := map[string]int{}
	var out []processo.Leitura
	for _, l := range lote {
		if i, ok := idx[l.Tag]; ok {
			if l.Momento.After(out[i].Momento) {
				out[i] = l
			}
			continue
		}
		idx[l.Tag] = len(out)
		out = append(out, l)
	}
	return out
}
