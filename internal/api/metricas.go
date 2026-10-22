package api

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/luuanvieira/monitor-processo/internal/alarmes"
)

// metricas expõe contadores no formato texto do Prometheus, sem dependências.
func (s *Servidor) metricas(w http.ResponseWriter, _ *http.Request) {
	var b strings.Builder
	escrever := func(nome, tipo, ajuda string, linhas ...string) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", nome, ajuda, nome, tipo)
		for _, l := range linhas {
			b.WriteString(l)
			b.WriteByte('\n')
		}
	}
	a := s.app

	escrever("monitor_leituras_processadas_total", "counter", "Leituras avaliadas pelo motor de alarmes.",
		fmt.Sprintf("monitor_leituras_processadas_total %d", a.Processadas.Load()))
	escrever("monitor_leituras_rejeitadas_total", "counter", "Leituras recusadas por fila cheia.",
		fmt.Sprintf("monitor_leituras_rejeitadas_total %d", a.Rejeitadas.Load()))

	porPrioridade := map[alarmes.Prioridade]int{alarmes.Alta: 0, alarmes.Media: 0, alarmes.Baixa: 0}
	for _, al := range a.Motor.Ativos() {
		porPrioridade[al.Prioridade]++
	}
	var ativos []string
	for _, p := range []alarmes.Prioridade{alarmes.Alta, alarmes.Media, alarmes.Baixa} {
		ativos = append(ativos, fmt.Sprintf(`monitor_alarmes_ativos{prioridade="%s"} %d`, p, porPrioridade[p]))
	}
	escrever("monitor_alarmes_ativos", "gauge", "Alarmes fora do estado normal, por prioridade.", ativos...)

	totais := a.Motor.TotalAtivacoes()
	tipos := make([]string, 0, len(totais))
	for t := range totais {
		tipos = append(tipos, string(t))
	}
	sort.Strings(tipos)
	var ativacoes []string
	for _, t := range tipos {
		ativacoes = append(ativacoes, fmt.Sprintf(`monitor_alarmes_ativacoes_total{tipo="%s"} %d`, t, totais[alarmes.Tipo(t)]))
	}
	escrever("monitor_alarmes_ativacoes_total", "counter", "Ativações de alarme desde o início, por tipo.", ativacoes...)
	escrever("monitor_alarmes_ativacoes_10min", "gauge", "Ativações nos últimos 10 minutos (inundação acima de 10).",
		fmt.Sprintf("monitor_alarmes_ativacoes_10min %d", a.Motor.AtivacoesNaJanela(a.Agora(), janelaInundacao)))

	pl, pe := a.Escritor.Pendentes()
	escrever("monitor_gravacao_pendentes", "gauge", "Itens aguardando gravação no armazenamento.",
		fmt.Sprintf(`monitor_gravacao_pendentes{tipo="leituras"} %d`, pl),
		fmt.Sprintf(`monitor_gravacao_pendentes{tipo="eventos"} %d`, pe))
	escrever("monitor_gravacao_leituras_total", "counter", "Leituras gravadas no armazenamento.",
		fmt.Sprintf("monitor_gravacao_leituras_total %d", a.Escritor.Gravadas.Load()))
	escrever("monitor_gravacao_descartadas_total", "counter", "Leituras descartadas por excesso de pendências.",
		fmt.Sprintf("monitor_gravacao_descartadas_total %d", a.Escritor.Descartadas.Load()))
	escrever("monitor_gravacao_erros_total", "counter", "Falhas ao gravar no armazenamento.",
		fmt.Sprintf("monitor_gravacao_erros_total %d", a.Escritor.Erros.Load()))

	escrever("monitor_stream_clientes", "gauge", "Clientes conectados ao stream SSE.",
		fmt.Sprintf("monitor_stream_clientes %d", a.Broker.Clientes()))
	escrever("monitor_stream_descartadas_total", "counter", "Mensagens SSE descartadas por cliente lento.",
		fmt.Sprintf("monitor_stream_descartadas_total %d", a.Broker.Descartadas.Load()))

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(b.String()))
}
