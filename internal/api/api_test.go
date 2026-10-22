package api_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/luuanvieira/monitor-processo/config"
	"github.com/luuanvieira/monitor-processo/internal/alarmes"
	"github.com/luuanvieira/monitor-processo/internal/api"
	"github.com/luuanvieira/monitor-processo/internal/app"
	"github.com/luuanvieira/monitor-processo/internal/armazenamento"
	"github.com/luuanvieira/monitor-processo/internal/processo"
)

type ambiente struct {
	app *app.App
	srv *httptest.Server
}

func novoAmbiente(t *testing.T, simulador bool) *ambiente {
	t.Helper()
	tags, err := processo.CarregarTags(bytes.NewReader(config.TagsPadrao))
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	a := app.Nova(app.Config{
		Tags: tags, Store: armazenamento.NovaMemoria(10_000, 1000), Simulador: simulador,
		Semente: 1, Velocidade: 30, Passo: 5 * time.Millisecond, Log: log,
	})
	a.Escritor.Intervalo = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	feito := make(chan struct{})
	go func() { a.Executar(ctx); close(feito) }()

	encerrando := make(chan struct{})
	srv := httptest.NewServer(api.Novo(a, log, encerrando))
	t.Cleanup(func() {
		close(encerrando)
		srv.Close()
		cancel()
		<-feito
	})
	return &ambiente{app: a, srv: srv}
}

func (e *ambiente) req(t *testing.T, metodo, caminho, corpo string) (int, []byte) {
	t.Helper()
	r, err := http.NewRequest(metodo, e.srv.URL+caminho, strings.NewReader(corpo))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func esperar(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	limite := time.Now().Add(10 * time.Second)
	for time.Now().Before(limite) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("tempo esgotado esperando: " + msg)
}

func TestTagsEHistorico(t *testing.T) {
	e := novoAmbiente(t, true)
	esperar(t, func() bool { return e.app.Escritor.Gravadas.Load() > 100 }, "leituras gravadas")

	status, corpo := e.req(t, "GET", "/api/tags", "")
	var tags []struct {
		ID     string            `json:"id"`
		Ultima *processo.Leitura `json:"ultima"`
	}
	if status != 200 || json.Unmarshal(corpo, &tags) != nil || len(tags) != 5 || tags[0].Ultima == nil {
		t.Fatalf("GET /api/tags: %d %s", status, corpo)
	}

	status, corpo = e.req(t, "GET", "/api/tags/TT-101/leituras?minutos=60&limite=10", "")
	var ls []processo.Leitura
	if status != 200 || json.Unmarshal(corpo, &ls) != nil || len(ls) != 10 {
		t.Fatalf("histórico: %d %s", status, corpo)
	}
	if status, _ = e.req(t, "GET", "/api/tags/XX-1/leituras", ""); status != 404 {
		t.Fatalf("tag desconhecido deveria dar 404, deu %d", status)
	}
	if status, _ = e.req(t, "GET", "/api/tags/TT-101/leituras?limite=abc", ""); status != 400 {
		t.Fatalf("limite inválido deveria dar 400, deu %d", status)
	}
}

func TestFalhaGeraAlarmeEReconhecimento(t *testing.T) {
	e := novoAmbiente(t, true)

	status, corpo := e.req(t, "POST", "/api/simulador/falhas", `{"tipo":"falha_sinal","tag":"FT-201"}`)
	if status != 201 {
		t.Fatalf("injetar falha: %d %s", status, corpo)
	}
	if status, _ := e.req(t, "POST", "/api/simulador/falhas", `{"tipo":"falha_sinal"}`); status != 422 {
		t.Fatalf("falha de instrumento sem tag deveria dar 422, deu %d", status)
	}

	esperar(t, func() bool {
		for _, a := range e.app.Motor.Ativos() {
			if a.Tag == "FT-201" && a.Tipo == alarmes.Qualidade {
				return true
			}
		}
		return false
	}, "alarme de qualidade do FT-201")

	if status, corpo := e.req(t, "POST", "/api/alarmes/FT-201/QUALIDADE/reconhecer", ""); status != 200 {
		t.Fatalf("reconhecer: %d %s", status, corpo)
	}
	if status, _ := e.req(t, "POST", "/api/alarmes/FT-201/QUALIDADE/reconhecer", ""); status != 409 {
		t.Fatalf("reconhecer de novo deveria dar 409, deu %d", status)
	}
	if status, _ := e.req(t, "POST", "/api/alarmes/FT-201/NADA/reconhecer", ""); status != 404 {
		t.Fatalf("alarme inexistente deveria dar 404, deu %d", status)
	}

	esperar(t, func() bool {
		_, corpo := e.req(t, "GET", "/api/alarmes/historico?tag=FT-201", "")
		return bytes.Contains(corpo, []byte(`"ativo_reconhecido"`))
	}, "reconhecimento gravado no histórico")

	if status, _ := e.req(t, "DELETE", "/api/simulador/falhas/falha_sinal?tag=FT-201", ""); status != 204 {
		t.Fatalf("remover falha deveria dar 204, deu %d", status)
	}
	if status, _ := e.req(t, "DELETE", "/api/simulador/falhas/falha_sinal?tag=FT-201", ""); status != 404 {
		t.Fatalf("remover falha inativa deveria dar 404, deu %d", status)
	}
}

func TestIngestaoValidaAsLeituras(t *testing.T) {
	e := novoAmbiente(t, false)
	agora := time.Now().UTC().Format(time.RFC3339)

	casos := []struct {
		corpo  string
		status int
	}{
		{`[{"tag":"TT-101","momento":"` + agora + `","valor":95}]`, 202},
		{`[{"tag":"XX-1","momento":"` + agora + `","valor":1}]`, 422},
		{`[{"tag":"TT-101","valor":1}]`, 422},
		{`{"tag":"TT-101"}`, 400},
		{`[]`, 400},
		{`[{"tag":"TT-101","momento":"` + agora + `","valor":1,"extra":1}]`, 400},
	}
	for _, c := range casos {
		if status, corpo := e.req(t, "POST", "/api/leituras", c.corpo); status != c.status {
			t.Errorf("POST %s: esperava %d, veio %d %s", c.corpo, c.status, status, corpo)
		}
	}
	esperar(t, func() bool { return e.app.Processadas.Load() == 1 }, "leitura processada")
}

func TestSimuladorDesligadoResponde404(t *testing.T) {
	e := novoAmbiente(t, false)
	if status, _ := e.req(t, "GET", "/api/simulador/falhas", ""); status != 404 {
		t.Fatalf("esperava 404, veio %d", status)
	}
}

func TestStreamSSE(t *testing.T) {
	e := novoAmbiente(t, true)
	resp, err := http.Get(e.srv.URL + "/api/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type: %s", ct)
	}
	leitor := bufio.NewReader(resp.Body)
	limite := time.Now().Add(5 * time.Second)
	for time.Now().Before(limite) {
		linha, err := leitor.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(linha, "event: leituras") {
			return
		}
	}
	t.Fatal("nenhum evento de leituras recebido")
}

func TestMetricasEDashboard(t *testing.T) {
	e := novoAmbiente(t, true)
	esperar(t, func() bool { return e.app.Processadas.Load() > 0 }, "processamento")

	status, corpo := e.req(t, "GET", "/metrics", "")
	for _, nome := range []string{"monitor_leituras_processadas_total", `monitor_alarmes_ativos{prioridade="alta"}`, "monitor_gravacao_pendentes"} {
		if !bytes.Contains(corpo, []byte(nome)) {
			t.Errorf("métrica %s ausente", nome)
		}
	}
	if status != 200 {
		t.Fatalf("/metrics: %d", status)
	}
	status, corpo = e.req(t, "GET", "/", "")
	if status != 200 || !bytes.Contains(corpo, []byte("<title>Monitor de Processo</title>")) {
		t.Fatalf("dashboard: %d", status)
	}
	if status, _ := e.req(t, "GET", "/readyz", ""); status != 200 {
		t.Fatalf("/readyz: %d", status)
	}
}
