package alarmes

import (
	"errors"
	"testing"
	"time"

	"github.com/luuanvieira/monitor-processo/internal/processo"
)

var t0 = time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)

func f(v float64) *float64 { return &v }

func tagTemperatura() processo.Tag {
	return processo.Tag{
		ID: "TT-1", Unidade: "°C", FaixaMin: 0, FaixaMax: 150,
		Limites:        processo.Limites{L: f(70), H: f(85), HH: f(90)},
		BandaMorta:     1,
		AtrasoAtivacao: processo.Duracao(5 * time.Second),
		JanelaTaxa:     processo.Duracao(time.Minute),
	}
}

func leitura(seg int, v float64) processo.Leitura {
	return processo.Leitura{Tag: "TT-1", Momento: t0.Add(time.Duration(seg) * time.Second), Valor: v}
}

// alimentar envia valores com 1 s de intervalo a partir do segundo `inicio`.
func alimentar(m *Motor, inicio int, valores ...float64) []Evento {
	var ev []Evento
	for i, v := range valores {
		ev = append(ev, m.Processar(leitura(inicio+i, v))...)
	}
	return ev
}

func repetir(v float64, n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = v
	}
	return out
}

func estado(t *testing.T, m *Motor, tipo Tipo) Estado {
	t.Helper()
	return m.tags["TT-1"].alarmes[tipo].Estado
}

func TestAtrasoDeAtivacaoIgnoraPicoCurto(t *testing.T) {
	m := NovoMotor([]processo.Tag{tagTemperatura()})
	// 3 s acima do limite com atraso de 5 s: não pode alarmar.
	ev := alimentar(m, 0, 80, 86, 86, 86, 80, 80)
	if len(ev) != 0 {
		t.Fatalf("pico de 3 s não deveria alarmar, eventos: %+v", ev)
	}
	// 6 s seguidos acima do limite: alarma.
	ev = alimentar(m, 10, repetir(86, 7)...)
	if len(ev) != 1 || ev[0].Tipo != H || ev[0].Estado != AtivoNaoReconhecido {
		t.Fatalf("esperava um H ativo, veio %+v", ev)
	}
	if ev[0].Momento != t0.Add(15*time.Second) {
		t.Fatalf("deveria ativar após 5 s de condição, ativou em %v", ev[0].Momento.Sub(t0))
	}
}

func TestBandaMortaEvitaAlarmeOscilando(t *testing.T) {
	m := NovoMotor([]processo.Tag{tagTemperatura()})
	alimentar(m, 0, repetir(86, 10)...)
	if estado(t, m, H) != AtivoNaoReconhecido {
		t.Fatal("H deveria estar ativo")
	}
	// Oscilando entre 84,5 e 85,5 (dentro da banda morta de 1 °C): continua ativo, sem eventos.
	if ev := alimentar(m, 10, 84.5, 85.5, 84.5, 85.5, 84.2); len(ev) != 0 {
		t.Fatalf("oscilação dentro da banda morta não deveria gerar eventos: %+v", ev)
	}
	// Abaixo de 84 (H - banda morta): normaliza.
	ev := alimentar(m, 20, 83.9)
	if len(ev) != 1 || ev[0].Estado != RetornouNaoReconhecido {
		t.Fatalf("esperava retorno não reconhecido, veio %+v", ev)
	}
}

func TestCicloDeReconhecimento(t *testing.T) {
	m := NovoMotor([]processo.Tag{tagTemperatura()})
	alimentar(m, 0, repetir(86, 10)...)

	ev, err := m.Reconhecer("TT-1", H, t0.Add(11*time.Second))
	if err != nil || ev.Estado != AtivoReconhecido {
		t.Fatalf("reconhecer ativo: %v %+v", err, ev)
	}
	if _, err := m.Reconhecer("TT-1", H, t0.Add(12*time.Second)); !errors.Is(err, ErrNadaAReconhecer) {
		t.Fatalf("reconhecer duas vezes deveria falhar, veio %v", err)
	}

	// Ativo reconhecido que normaliza vai direto para normal.
	ev2 := alimentar(m, 20, 80)
	if len(ev2) != 1 || ev2[0].Estado != Normal {
		t.Fatalf("esperava normal, veio %+v", ev2)
	}
	if len(m.Ativos()) != 0 {
		t.Fatalf("não deveria haver alarmes ativos: %+v", m.Ativos())
	}
}

func TestRetornoNaoReconhecidoPrecisaDeReconhecimento(t *testing.T) {
	m := NovoMotor([]processo.Tag{tagTemperatura()})
	alimentar(m, 0, repetir(86, 10)...)
	alimentar(m, 10, 80)
	if estado(t, m, H) != RetornouNaoReconhecido {
		t.Fatal("esperava retornou_nao_reconhecido")
	}
	if len(m.Ativos()) != 1 {
		t.Fatal("alarme retornado mas não reconhecido deve continuar na lista do operador")
	}
	evs := m.ReconhecerTodos(t0.Add(15 * time.Second))
	if len(evs) != 1 || evs[0].Estado != Normal {
		t.Fatalf("esperava normal após reconhecer, veio %+v", evs)
	}
}

func TestHHeHSaoAlarmesIndependentes(t *testing.T) {
	m := NovoMotor([]processo.Tag{tagTemperatura()})
	ev := alimentar(m, 0, repetir(91, 10)...)
	tipos := map[Tipo]bool{}
	for _, e := range ev {
		tipos[e.Tipo] = true
	}
	if !tipos[H] || !tipos[HH] {
		t.Fatalf("valor acima de HH deve ativar H e HH, veio %+v", ev)
	}
	if ativos := m.Ativos(); ativos[0].Tipo != HH {
		t.Fatalf("HH (prioridade alta) deve vir primeiro, veio %s", ativos[0].Tipo)
	}
}

func TestQualidadeRuimSuprimeAlarmesDeProcesso(t *testing.T) {
	m := NovoMotor([]processo.Tag{tagTemperatura()})
	alimentar(m, 0, repetir(86, 10)...) // H ativo

	ruim := leitura(10, -7.5)
	ruim.Qualidade = processo.QualidadeRuim
	ev := m.Processar(ruim)
	if len(ev) != 1 || ev[0].Tipo != Qualidade {
		t.Fatalf("esperava só o alarme de qualidade, veio %+v", ev)
	}
	if estado(t, m, H) != AtivoNaoReconhecido {
		t.Fatal("leitura ruim não pode normalizar o H")
	}
	if estado(t, m, L) != Normal {
		t.Fatal("leitura ruim (-7,5) não pode disparar o L")
	}
}

func TestValorForaDaFaixaDoInstrumentoEhQualidadeRuim(t *testing.T) {
	m := NovoMotor([]processo.Tag{tagTemperatura()})
	ev := m.Processar(leitura(0, 200))
	if len(ev) != 1 || ev[0].Tipo != Qualidade {
		t.Fatalf("200 °C está fora da faixa 0-150, esperava alarme de qualidade: %+v", ev)
	}
}

func TestTaxaDeVariacao(t *testing.T) {
	cfg := tagTemperatura()
	cfg.TaxaMaxPorMin = f(3)
	m := NovoMotor([]processo.Tag{cfg})

	// Sobe 0,1 °C/s = 6 °C/min, de 75 até 81 (sem cruzar limites).
	var ev []Evento
	for i := 0; i <= 60; i++ {
		ev = append(ev, m.Processar(leitura(i, 75+0.1*float64(i)))...)
	}
	if len(ev) != 1 || ev[0].Tipo != Taxa {
		t.Fatalf("esperava alarme de taxa, veio %+v", ev)
	}
	// O alarme não pode disparar antes de meia janela de histórico.
	if ev[0].Momento.Before(t0.Add(30 * time.Second)) {
		t.Fatalf("taxa calculada com pouca história: %v", ev[0].Momento.Sub(t0))
	}
}

func TestSensorCongelado(t *testing.T) {
	cfg := tagTemperatura()
	cfg.CongeladoApos = processo.Duracao(2 * time.Minute)
	m := NovoMotor([]processo.Tag{cfg})

	ev := alimentar(m, 0, repetir(80, 119)...)
	if len(ev) != 0 {
		t.Fatalf("1 min 59 s parado ainda não é congelado: %+v", ev)
	}
	ev = alimentar(m, 119, 80, 80)
	if len(ev) != 1 || ev[0].Tipo != Congelado {
		t.Fatalf("esperava alarme de congelado, veio %+v", ev)
	}
	ev = alimentar(m, 121, 80.1)
	if len(ev) != 1 || ev[0].Estado != RetornouNaoReconhecido {
		t.Fatalf("valor voltou a variar, esperava normalizar: %+v", ev)
	}
}

func TestEWMADetectaDerivaLenta(t *testing.T) {
	cfg := tagTemperatura()
	cfg.EWMA = &processo.ConfigEWMA{Lambda: 0.1, L: 3, Aquecimento: 50, DesvioMin: 0.01}
	m := NovoMotor([]processo.Tag{cfg})

	// Aquecimento: valores alternando 79,9 / 80,1 (média 80, desvio ~0,1).
	for i := 0; i < 50; i++ {
		m.Processar(leitura(i, 80+0.1*float64(1-2*(i%2))))
	}
	// Desvio de +0,3 °C: longe dos limites H/L, mas estatisticamente significativo.
	var ev []Evento
	for i := 50; i < 100; i++ {
		ev = append(ev, m.Processar(leitura(i, 80.3))...)
	}
	if len(ev) != 1 || ev[0].Tipo != Deriva || ev[0].Prioridade != Baixa {
		t.Fatalf("esperava um alarme de deriva, veio %+v", ev)
	}
}

func TestConfirmacaoDaEWMAIgnoraExcursaoCurta(t *testing.T) {
	cfg := tagTemperatura()
	cfg.EWMA = &processo.ConfigEWMA{Lambda: 0.5, L: 3, Aquecimento: 50, DesvioMin: 0.01,
		Confirmacao: processo.Duracao(10 * time.Second)}
	m := NovoMotor([]processo.Tag{cfg})
	for i := 0; i < 50; i++ {
		m.Processar(leitura(i, 80+0.1*float64(1-2*(i%2))))
	}
	// 5 s fora e depois volta: não pode alarmar.
	ev := alimentar(m, 50, 80.5, 80.5, 80.5, 80.5, 80.5, 80, 80, 80, 80, 80)
	if len(ev) != 0 {
		t.Fatalf("excursão curta não deveria gerar alarme de deriva: %+v", ev)
	}
	// Fora por mais de 10 s: alarma.
	ev = alimentar(m, 60, repetir(80.5, 15)...)
	if len(ev) != 1 || ev[0].Tipo != Deriva {
		t.Fatalf("esperava alarme de deriva após a confirmação, veio %+v", ev)
	}
}

func TestAtivacoesNaJanela(t *testing.T) {
	m := NovoMotor([]processo.Tag{tagTemperatura()})
	alimentar(m, 0, repetir(91, 10)...) // H e HH
	if n := m.AtivacoesNaJanela(t0.Add(time.Minute), 10*time.Minute); n != 2 {
		t.Fatalf("esperava 2 ativações, veio %d", n)
	}
	if n := m.AtivacoesNaJanela(t0.Add(20*time.Minute), 10*time.Minute); n != 0 {
		t.Fatalf("ativações antigas não contam na janela, veio %d", n)
	}
}

func TestTagDesconhecidoEhIgnorado(t *testing.T) {
	m := NovoMotor([]processo.Tag{tagTemperatura()})
	if ev := m.Processar(processo.Leitura{Tag: "XX-9", Momento: t0, Valor: 1}); ev != nil {
		t.Fatalf("tag desconhecido não deveria gerar eventos: %+v", ev)
	}
	if _, err := m.Reconhecer("XX-9", H, t0); !errors.Is(err, ErrAlarmeInexistente) {
		t.Fatalf("esperava ErrAlarmeInexistente, veio %v", err)
	}
}
