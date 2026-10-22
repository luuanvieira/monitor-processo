// Package api expõe a aplicação por HTTP: consultas, reconhecimento de
// alarmes, ingestão de leituras, injeção de falhas no simulador, stream SSE,
// métricas no formato Prometheus e o dashboard.
package api

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/luuanvieira/monitor-processo/internal/alarmes"
	"github.com/luuanvieira/monitor-processo/internal/app"
	"github.com/luuanvieira/monitor-processo/internal/processo"
	"github.com/luuanvieira/monitor-processo/internal/simulador"
)

//go:embed web
var web embed.FS

const (
	maxCorpo         = 1 << 20 // 1 MiB
	maxLeiturasPost  = 5000
	esperaFila       = 2 * time.Second
	janelaInundacao  = 10 * time.Minute
	limiteInundacao  = 10 // EEMUA 191: mais de 10 alarmes em 10 min por operador
	intervaloPulsoSE = 15 * time.Second
)

type Servidor struct {
	app        *app.App
	log        *slog.Logger
	encerrando <-chan struct{} // fechado no shutdown para liberar as conexões SSE
}

// Novo monta o roteador. `encerrando` deve ser fechado quando o servidor HTTP
// começar a desligar (as conexões SSE ficam abertas indefinidamente).
func Novo(a *app.App, log *slog.Logger, encerrando <-chan struct{}) http.Handler {
	s := &Servidor{app: a, log: log, encerrando: encerrando}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/tags", s.listarTags)
	mux.HandleFunc("GET /api/tags/{id}/leituras", s.leiturasDoTag)
	mux.HandleFunc("GET /api/alarmes", s.alarmesAtivos)
	mux.HandleFunc("GET /api/alarmes/historico", s.historicoAlarmes)
	mux.HandleFunc("POST /api/alarmes/{tag}/{tipo}/reconhecer", s.reconhecer)
	mux.HandleFunc("POST /api/alarmes/reconhecer-todos", s.reconhecerTodos)
	mux.HandleFunc("POST /api/leituras", s.receberLeituras)
	mux.HandleFunc("GET /api/simulador/falhas", s.listarFalhas)
	mux.HandleFunc("POST /api/simulador/falhas", s.ativarFalha)
	mux.HandleFunc("DELETE /api/simulador/falhas/{tipo}", s.removerFalha)
	mux.HandleFunc("POST /api/simulador/manutencao", s.manutencao)
	mux.HandleFunc("GET /api/stream", s.stream)
	mux.HandleFunc("GET /metrics", s.metricas)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", s.pronto)

	estatico, _ := fs.Sub(web, "web")
	mux.Handle("GET /", http.FileServerFS(estatico))

	return s.registrar(mux)
}

// --- utilitários -------------------------------------------------------------

func responderJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func responderErro(w http.ResponseWriter, status int, msg string) {
	responderJSON(w, status, map[string]string{"erro": msg})
}

func inteiro(r *http.Request, nome string, padrao, min, max int) (int, error) {
	v := r.URL.Query().Get(nome)
	if v == "" {
		return padrao, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < min || n > max {
		return 0, errors.New("parâmetro " + nome + " inválido (entre " + strconv.Itoa(min) + " e " + strconv.Itoa(max) + ")")
	}
	return n, nil
}

type gravadorStatus struct {
	http.ResponseWriter
	status int
}

func (g *gravadorStatus) WriteHeader(s int) { g.status = s; g.ResponseWriter.WriteHeader(s) }
func (g *gravadorStatus) Flush() {
	if f, ok := g.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (s *Servidor) registrar(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inicio := time.Now()
		g := &gravadorStatus{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(g, r)
		if r.URL.Path == "/metrics" || r.URL.Path == "/healthz" {
			return
		}
		s.log.Debug("http", "metodo", r.Method, "caminho", r.URL.Path, "status", g.status, "duracao", time.Since(inicio))
	})
}

// --- tags e leituras ------------------------------------------------------------

type tagResposta struct {
	processo.Tag
	Ultima  *processo.Leitura `json:"ultima"`
	Alarmes []alarmes.Alarme  `json:"alarmes"`
}

func (s *Servidor) listarTags(w http.ResponseWriter, _ *http.Request) {
	ultimas := s.app.Motor.Ultimas()
	porTag := map[string][]alarmes.Alarme{}
	for _, a := range s.app.Motor.Ativos() {
		porTag[a.Tag] = append(porTag[a.Tag], a)
	}
	out := make([]tagResposta, 0, len(s.app.Tags))
	for _, t := range s.app.Tags {
		al := porTag[t.ID]
		if al == nil {
			al = []alarmes.Alarme{}
		}
		out = append(out, tagResposta{Tag: t, Ultima: ultimas[t.ID], Alarmes: al})
	}
	responderJSON(w, http.StatusOK, out)
}

func (s *Servidor) leiturasDoTag(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.app.TagsPorID[id]; !ok {
		responderErro(w, http.StatusNotFound, "tag desconhecido")
		return
	}
	minutos, err := inteiro(r, "minutos", 30, 1, 7*24*60)
	if err != nil {
		responderErro(w, http.StatusBadRequest, err.Error())
		return
	}
	limite, err := inteiro(r, "limite", 2000, 1, 50_000)
	if err != nil {
		responderErro(w, http.StatusBadRequest, err.Error())
		return
	}
	ate := s.app.Agora()
	ls, err := s.app.Store.Leituras(r.Context(), id, ate.Add(-time.Duration(minutos)*time.Minute), ate, limite)
	if err != nil {
		s.log.Error("consulta de leituras", "erro", err)
		responderErro(w, http.StatusServiceUnavailable, "histórico indisponível no momento")
		return
	}
	responderJSON(w, http.StatusOK, ls)
}

func (s *Servidor) receberLeituras(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxCorpo)
	var ls []processo.Leitura
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ls); err != nil {
		responderErro(w, http.StatusBadRequest, "JSON inválido: esperado uma lista de leituras: "+err.Error())
		return
	}
	if len(ls) == 0 || len(ls) > maxLeiturasPost {
		responderErro(w, http.StatusBadRequest, "envie entre 1 e "+strconv.Itoa(maxLeiturasPost)+" leituras")
		return
	}
	for i := range ls {
		if err := processo.ValidarLeitura(ls[i], s.app.TagsPorID); err != nil {
			responderErro(w, http.StatusUnprocessableEntity, "leitura "+strconv.Itoa(i)+": "+err.Error())
			return
		}
		ls[i].Momento = ls[i].Momento.UTC()
	}
	if err := s.app.Receber(r.Context(), ls, esperaFila); err != nil {
		w.Header().Set("Retry-After", "1")
		responderErro(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	responderJSON(w, http.StatusAccepted, map[string]int{"aceitas": len(ls)})
}

// --- alarmes ----------------------------------------------------------------------

func (s *Servidor) alarmesAtivos(w http.ResponseWriter, _ *http.Request) {
	ativos := s.app.Motor.Ativos()
	if ativos == nil {
		ativos = []alarmes.Alarme{}
	}
	recentes := s.app.Motor.AtivacoesNaJanela(s.app.Agora(), janelaInundacao)
	responderJSON(w, http.StatusOK, map[string]any{
		"ativos":           ativos,
		"ativacoes_10min":  recentes,
		"inundacao":        recentes > limiteInundacao,
		"limite_inundacao": limiteInundacao,
		"nao_reconhecidos": contarNaoReconhecidos(ativos),
	})
}

func contarNaoReconhecidos(as []alarmes.Alarme) int {
	n := 0
	for _, a := range as {
		if a.Estado == alarmes.AtivoNaoReconhecido || a.Estado == alarmes.RetornouNaoReconhecido {
			n++
		}
	}
	return n
}

func (s *Servidor) historicoAlarmes(w http.ResponseWriter, r *http.Request) {
	limite, err := inteiro(r, "limite", 100, 1, 5000)
	if err != nil {
		responderErro(w, http.StatusBadRequest, err.Error())
		return
	}
	ev, err := s.app.Store.Eventos(r.Context(), r.URL.Query().Get("tag"), limite)
	if err != nil {
		s.log.Error("consulta de eventos", "erro", err)
		responderErro(w, http.StatusServiceUnavailable, "histórico indisponível no momento")
		return
	}
	responderJSON(w, http.StatusOK, ev)
}

func (s *Servidor) reconhecer(w http.ResponseWriter, r *http.Request) {
	ev, err := s.app.Motor.Reconhecer(r.PathValue("tag"), alarmes.Tipo(r.PathValue("tipo")), s.app.Agora())
	switch {
	case errors.Is(err, alarmes.ErrAlarmeInexistente):
		responderErro(w, http.StatusNotFound, err.Error())
	case errors.Is(err, alarmes.ErrNadaAReconhecer):
		responderErro(w, http.StatusConflict, err.Error())
	case err != nil:
		responderErro(w, http.StatusInternalServerError, err.Error())
	default:
		s.app.RegistrarEventos([]alarmes.Evento{ev})
		responderJSON(w, http.StatusOK, ev)
	}
}

func (s *Servidor) reconhecerTodos(w http.ResponseWriter, _ *http.Request) {
	ev := s.app.Motor.ReconhecerTodos(s.app.Agora())
	s.app.RegistrarEventos(ev)
	responderJSON(w, http.StatusOK, map[string]int{"reconhecidos": len(ev)})
}

// --- simulador ----------------------------------------------------------------------

type falhaDisponivel struct {
	Tipo      simulador.TipoFalha `json:"tipo"`
	Descricao string              `json:"descricao"`
	ExigeTag  bool                `json:"exige_tag"`
}

func (s *Servidor) semSimulador(w http.ResponseWriter) bool {
	if s.app.Planta == nil {
		responderErro(w, http.StatusNotFound, "simulador desligado (SIMULADOR=false)")
		return true
	}
	return false
}

func (s *Servidor) listarFalhas(w http.ResponseWriter, _ *http.Request) {
	if s.semSimulador(w) {
		return
	}
	var disp []falhaDisponivel
	for _, t := range simulador.TiposFalha() {
		exige := t == simulador.SensorTravado || t == simulador.DerivaSensor || t == simulador.FalhaSinal
		disp = append(disp, falhaDisponivel{Tipo: t, Descricao: t.Descricao(), ExigeTag: exige})
	}
	responderJSON(w, http.StatusOK, map[string]any{"ativas": s.app.Planta.Falhas(), "disponiveis": disp})
}

func (s *Servidor) ativarFalha(w http.ResponseWriter, r *http.Request) {
	if s.semSimulador(w) {
		return
	}
	var req struct {
		Tipo simulador.TipoFalha `json:"tipo"`
		Tag  string              `json:"tag"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		responderErro(w, http.StatusBadRequest, "JSON inválido")
		return
	}
	f, err := s.app.Planta.AtivarFalha(req.Tipo, req.Tag)
	if err != nil {
		responderErro(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	s.log.Info("falha injetada", "tipo", f.Tipo, "tag", f.Tag)
	responderJSON(w, http.StatusCreated, f)
}

func (s *Servidor) removerFalha(w http.ResponseWriter, r *http.Request) {
	if s.semSimulador(w) {
		return
	}
	if !s.app.Planta.RemoverFalha(simulador.TipoFalha(r.PathValue("tipo")), r.URL.Query().Get("tag")) {
		responderErro(w, http.StatusNotFound, "falha não está ativa")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Servidor) manutencao(w http.ResponseWriter, _ *http.Request) {
	if s.semSimulador(w) {
		return
	}
	s.app.Planta.Manutencao()
	w.WriteHeader(http.StatusNoContent)
}

// --- tempo real -----------------------------------------------------------------------

func (s *Servidor) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		responderErro(w, http.StatusInternalServerError, "streaming não suportado")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ch, cancelar := s.app.Broker.Assinar()
	defer cancelar()

	_, _ = w.Write([]byte("retry: 3000\n: conectado\n\n"))
	flusher.Flush()

	pulso := time.NewTicker(intervaloPulsoSE)
	defer pulso.Stop()
	for {
		select {
		case msg := <-ch:
			if _, err := w.Write(msg); err != nil {
				return
			}
			flusher.Flush()
		case <-pulso.C:
			// Comentário SSE: mantém proxies e balanceadores sem fechar a conexão.
			if _, err := w.Write([]byte(": pulso\n\n")); err != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		case <-s.encerrando:
			return
		}
	}
}

func (s *Servidor) pronto(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.app.Store.Ping(ctx); err != nil {
		responderErro(w, http.StatusServiceUnavailable, "armazenamento indisponível")
		return
	}
	w.WriteHeader(http.StatusOK)
}
