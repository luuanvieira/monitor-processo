//go:build integracao

// Testes contra um PostgreSQL real:
//
//	docker compose up -d postgres
//	TEST_DATABASE_URL=postgres://monitor:monitor@localhost:5432/monitor?sslmode=disable \
//	  go test -tags integracao ./internal/armazenamento/postgres/
package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/luuanvieira/monitor-processo/internal/alarmes"
	"github.com/luuanvieira/monitor-processo/internal/processo"
)

func conectar(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL não definido")
	}
	ctx := context.Background()
	s, err := Conectar(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, "truncate leituras, eventos_alarme"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestLeiturasIdaEVolta(t *testing.T) {
	s := conectar(t)
	ctx := context.Background()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	lote := []processo.Leitura{
		{Tag: "TT-101", Momento: t0, Valor: 80.1},
		{Tag: "TT-101", Momento: t0.Add(time.Second), Valor: 80.2, Qualidade: processo.QualidadeRuim},
		{Tag: "PT-101", Momento: t0, Valor: 2.0},
	}
	if err := s.SalvarLeituras(ctx, lote); err != nil {
		t.Fatal(err)
	}
	// Reenviar o mesmo lote não pode duplicar nem falhar.
	if err := s.SalvarLeituras(ctx, lote); err != nil {
		t.Fatal(err)
	}

	got, err := s.Leituras(ctx, "TT-101", t0, t0.Add(time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != lote[0] || got[1] != lote[1] {
		t.Fatalf("leituras diferentes do gravado: %+v", got)
	}
	ultima, _ := s.Leituras(ctx, "TT-101", t0, t0.Add(time.Minute), 1)
	if len(ultima) != 1 || ultima[0].Valor != 80.2 {
		t.Fatalf("limite deveria trazer a mais recente: %+v", ultima)
	}
}

func TestEventosIdaEVolta(t *testing.T) {
	s := conectar(t)
	ctx := context.Background()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ev := []alarmes.Evento{
		{Seq: 1, Momento: t0, Tag: "TT-101", Tipo: alarmes.H, Prioridade: alarmes.Media, Estado: alarmes.AtivoNaoReconhecido, Valor: 86, Mensagem: "alto"},
		{Seq: 2, Momento: t0.Add(time.Second), Tag: "FT-201", Tipo: alarmes.LL, Prioridade: alarmes.Alta, Estado: alarmes.AtivoNaoReconhecido, Valor: 1, Mensagem: "baixo"},
	}
	if err := s.SalvarEventos(ctx, ev); err != nil {
		t.Fatal(err)
	}
	todos, _ := s.Eventos(ctx, "", 10)
	if len(todos) != 2 || todos[0] != ev[1] {
		t.Fatalf("esperava os 2 eventos, mais recente primeiro: %+v", todos)
	}
	so, _ := s.Eventos(ctx, "TT-101", 10)
	if len(so) != 1 || so[0] != ev[0] {
		t.Fatalf("filtro por tag falhou: %+v", so)
	}
}
