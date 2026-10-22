package armazenamento

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/luuanvieira/monitor-processo/internal/alarmes"
	"github.com/luuanvieira/monitor-processo/internal/processo"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func l(tag string, seg int, v float64) processo.Leitura {
	return processo.Leitura{Tag: tag, Momento: t0.Add(time.Duration(seg) * time.Second), Valor: v}
}

func TestMemoriaConsultaPorIntervaloELimite(t *testing.T) {
	m := NovaMemoria(100, 10)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_ = m.SalvarLeituras(ctx, []processo.Leitura{l("A", i, float64(i))})
	}
	got, _ := m.Leituras(ctx, "A", t0.Add(2*time.Second), t0.Add(7*time.Second), 3)
	if len(got) != 3 || got[0].Valor != 5 || got[2].Valor != 7 {
		t.Fatalf("esperava as 3 mais recentes do intervalo (5,6,7), veio %+v", got)
	}
}

func TestMemoriaOrdenaEDeduplica(t *testing.T) {
	m := NovaMemoria(100, 10)
	ctx := context.Background()
	_ = m.SalvarLeituras(ctx, []processo.Leitura{l("A", 0, 0), l("A", 2, 2), l("A", 1, 1), l("A", 1, 99), l("A", 2, 99)})
	got, _ := m.Leituras(ctx, "A", t0, t0.Add(time.Hour), 0)
	if len(got) != 3 || got[0].Valor != 0 || got[1].Valor != 1 || got[2].Valor != 2 {
		t.Fatalf("esperava 0,1,2 em ordem e sem duplicatas, veio %+v", got)
	}
}

func TestMemoriaRespeitaCapacidade(t *testing.T) {
	m := NovaMemoria(5, 2)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		_ = m.SalvarLeituras(ctx, []processo.Leitura{l("A", i, float64(i))})
		_ = m.SalvarEventos(ctx, []alarmes.Evento{{Seq: uint64(i), Tag: "A"}})
	}
	got, _ := m.Leituras(ctx, "A", t0, t0.Add(time.Hour), 0)
	if len(got) != 5 || got[0].Valor != 15 {
		t.Fatalf("esperava as 5 últimas, veio %+v", got)
	}
	ev, _ := m.Eventos(ctx, "", 0)
	if len(ev) != 2 || ev[0].Seq != 19 {
		t.Fatalf("esperava os 2 últimos eventos, mais recente primeiro: %+v", ev)
	}
}

// storeInstavel falha enquanto `fora` for true.
type storeInstavel struct {
	*Memoria
	mu   sync.Mutex
	fora bool
}

func (s *storeInstavel) setFora(v bool) { s.mu.Lock(); s.fora = v; s.mu.Unlock() }

func (s *storeInstavel) SalvarLeituras(ctx context.Context, ls []processo.Leitura) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fora {
		return errors.New("banco fora do ar")
	}
	return s.Memoria.SalvarLeituras(ctx, ls)
}

func (s *storeInstavel) SalvarEventos(ctx context.Context, ev []alarmes.Evento) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fora {
		return errors.New("banco fora do ar")
	}
	return s.Memoria.SalvarEventos(ctx, ev)
}

func TestEscritorSobreviveAoBancoForaDoAr(t *testing.T) {
	store := &storeInstavel{Memoria: NovaMemoria(1000, 1000), fora: true}
	e := NovoEscritor(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	e.Intervalo = 10 * time.Millisecond
	e.MaxPendentes = 50

	ctx, cancel := context.WithCancel(context.Background())
	feito := make(chan struct{})
	go func() { e.Executar(ctx); close(feito) }()

	for i := 0; i < 80; i++ {
		e.Enfileirar([]processo.Leitura{l("A", i, float64(i))}, nil)
	}
	e.Enfileirar(nil, []alarmes.Evento{{Seq: 1, Tag: "A"}})
	time.Sleep(50 * time.Millisecond)
	if e.Erros.Load() == 0 {
		t.Fatal("deveria ter registrado erros com o banco fora")
	}

	store.setFora(false)
	cancel()
	<-feito

	got, _ := store.Leituras(context.Background(), "A", t0, t0.Add(time.Hour), 0)
	if len(got) != 50 || got[0].Valor != 30 {
		t.Fatalf("esperava as 50 leituras mais recentes gravadas, veio %d (primeira %v)", len(got), got[0].Valor)
	}
	if e.Descartadas.Load() != 30 {
		t.Fatalf("esperava 30 descartadas, veio %d", e.Descartadas.Load())
	}
	if ev, _ := store.Eventos(context.Background(), "", 0); len(ev) != 1 {
		t.Fatal("o evento de alarme não pode ser perdido")
	}
}
