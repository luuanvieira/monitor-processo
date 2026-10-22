// Package alarmes avalia as leituras e mantém o estado dos alarmes de cada tag.
//
// O ciclo de vida segue o modelo da norma ISA-18.2 (simplificado):
//
//	normal                    --condição ativa-->      ativo não reconhecido
//	ativo não reconhecido     --operador reconhece-->  ativo reconhecido
//	ativo não reconhecido     --condição normaliza-->  retornou não reconhecido
//	ativo reconhecido         --condição normaliza-->  normal
//	retornou não reconhecido  --operador reconhece-->  normal
//	retornou não reconhecido  --condição ativa-->      ativo não reconhecido
//
// Para reduzir alarmes falsos (o "alarme que fica piscando"), os limites usam
// banda morta para normalizar e atraso de ativação para disparar. Leituras
// com qualidade ruim só geram o alarme de qualidade e não são usadas pelas
// outras regras.
package alarmes

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/luuanvieira/monitor-processo/internal/processo"
)

type Tipo string

const (
	LL        Tipo = "LL"
	L         Tipo = "L"
	H         Tipo = "H"
	HH        Tipo = "HH"
	Taxa      Tipo = "TAXA"
	Congelado Tipo = "CONGELADO"
	Deriva    Tipo = "DERIVA"
	Qualidade Tipo = "QUALIDADE"
)

type Prioridade string

const (
	Alta  Prioridade = "alta"
	Media Prioridade = "media"
	Baixa Prioridade = "baixa"
)

var pesoPrioridade = map[Prioridade]int{Alta: 0, Media: 1, Baixa: 2}

func prioridadeDe(t Tipo) Prioridade {
	switch t {
	case HH, LL:
		return Alta
	case H, L, Taxa, Qualidade:
		return Media
	default:
		return Baixa
	}
}

type Estado string

const (
	Normal                 Estado = "normal"
	AtivoNaoReconhecido    Estado = "ativo_nao_reconhecido"
	AtivoReconhecido       Estado = "ativo_reconhecido"
	RetornouNaoReconhecido Estado = "retornou_nao_reconhecido"
)

func (e Estado) ativo() bool { return e == AtivoNaoReconhecido || e == AtivoReconhecido }

// Alarme é a fotografia do estado atual de um alarme.
type Alarme struct {
	Tag           string     `json:"tag"`
	Tipo          Tipo       `json:"tipo"`
	Prioridade    Prioridade `json:"prioridade"`
	Estado        Estado     `json:"estado"`
	Valor         float64    `json:"valor"`
	Mensagem      string     `json:"mensagem"`
	AtivadoEm     time.Time  `json:"ativado_em"`
	MudouEm       time.Time  `json:"mudou_em"`
	ReconhecidoEm *time.Time `json:"reconhecido_em,omitempty"`
}

// Evento registra uma transição de estado (é o que vai para o histórico).
type Evento struct {
	Seq        uint64     `json:"seq"`
	Momento    time.Time  `json:"momento"`
	Tag        string     `json:"tag"`
	Tipo       Tipo       `json:"tipo"`
	Prioridade Prioridade `json:"prioridade"`
	Estado     Estado     `json:"estado"`
	Valor      float64    `json:"valor"`
	Mensagem   string     `json:"mensagem"`
}

var (
	ErrAlarmeInexistente = errors.New("alarme não existe")
	ErrNadaAReconhecer   = errors.New("alarme não está aguardando reconhecimento")
)

type alarme struct {
	Alarme
	pendenteDesde time.Time // início da condição durante o atraso de ativação
}

type ponto struct {
	t time.Time
	v float64
}

type estadoEWMA struct {
	n           int
	media, m2   float64 // acumuladores de Welford durante o aquecimento
	ref, desvio float64
	z           float64
	pronto      bool
}

type estadoTag struct {
	cfg     processo.Tag
	alarmes map[Tipo]*alarme
	ordem   []Tipo

	ultima   *processo.Leitura
	historia []ponto // janela para a taxa de variação

	refCongelado  float64
	mudouEm       time.Time
	temReferencia bool

	ewma estadoEWMA
}

// Motor é seguro para uso concorrente.
type Motor struct {
	mu        sync.Mutex
	tags      map[string]*estadoTag
	ordemTags []string
	seq       uint64
	ativacoes []time.Time // instantes de ativação, para medir inundação de alarmes
	porTipo   map[Tipo]uint64
}

// NovoMotor prepara os alarmes previstos na configuração de cada tag.
func NovoMotor(tags []processo.Tag) *Motor {
	m := &Motor{tags: map[string]*estadoTag{}, porTipo: map[Tipo]uint64{}}
	for _, cfg := range tags {
		st := &estadoTag{cfg: cfg, alarmes: map[Tipo]*alarme{}}
		add := func(t Tipo) {
			st.alarmes[t] = &alarme{Alarme: Alarme{Tag: cfg.ID, Tipo: t, Prioridade: prioridadeDe(t), Estado: Normal}}
			st.ordem = append(st.ordem, t)
		}
		for t, lim := range map[Tipo]*float64{LL: cfg.Limites.LL, L: cfg.Limites.L, H: cfg.Limites.H, HH: cfg.Limites.HH} {
			if lim != nil {
				add(t)
			}
		}
		if cfg.TaxaMaxPorMin != nil {
			add(Taxa)
		}
		if cfg.CongeladoApos > 0 {
			add(Congelado)
		}
		if cfg.EWMA != nil {
			add(Deriva)
		}
		add(Qualidade)
		sort.Slice(st.ordem, func(i, j int) bool { return st.ordem[i] < st.ordem[j] })
		m.tags[cfg.ID] = st
		m.ordemTags = append(m.ordemTags, cfg.ID)
	}
	return m
}

// Processar avalia uma leitura e retorna as transições de estado que ela causou.
func (m *Motor) Processar(l processo.Leitura) []Evento {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, ok := m.tags[l.Tag]
	if !ok {
		return nil
	}
	leitura := l
	st.ultima = &leitura
	var ev []Evento
	cfg := st.cfg
	u := cfg.Unidade

	ruim := l.Qualidade != processo.QualidadeBoa || l.Valor < cfg.FaixaMin || l.Valor > cfg.FaixaMax
	ev = m.avaliar(ev, st, Qualidade, ruim, l, 0, fmt.Sprintf("%s com qualidade ruim ou fora da faixa do instrumento", cfg.ID))
	if ruim {
		// Uma leitura inválida não pode disparar nem normalizar alarmes de processo.
		st.historia = st.historia[:0]
		return ev
	}

	v := l.Valor
	atraso := cfg.AtrasoAtivacao.D()
	bm := cfg.BandaMorta
	lim := cfg.Limites

	if lim.HH != nil {
		cond := v >= *lim.HH || (st.alarmes[HH].Estado.ativo() && v > *lim.HH-bm)
		ev = m.avaliar(ev, st, HH, cond, l, atraso, fmt.Sprintf("%s muito alto: %.2f %s (limite HH %.2f)", cfg.ID, v, u, *lim.HH))
	}
	if lim.H != nil {
		cond := v >= *lim.H || (st.alarmes[H].Estado.ativo() && v > *lim.H-bm)
		ev = m.avaliar(ev, st, H, cond, l, atraso, fmt.Sprintf("%s alto: %.2f %s (limite H %.2f)", cfg.ID, v, u, *lim.H))
	}
	if lim.L != nil {
		cond := v <= *lim.L || (st.alarmes[L].Estado.ativo() && v < *lim.L+bm)
		ev = m.avaliar(ev, st, L, cond, l, atraso, fmt.Sprintf("%s baixo: %.2f %s (limite L %.2f)", cfg.ID, v, u, *lim.L))
	}
	if lim.LL != nil {
		cond := v <= *lim.LL || (st.alarmes[LL].Estado.ativo() && v < *lim.LL+bm)
		ev = m.avaliar(ev, st, LL, cond, l, atraso, fmt.Sprintf("%s muito baixo: %.2f %s (limite LL %.2f)", cfg.ID, v, u, *lim.LL))
	}

	if cfg.TaxaMaxPorMin != nil {
		ev = m.avaliarTaxa(ev, st, l)
	}
	if cfg.CongeladoApos > 0 {
		ev = m.avaliarCongelado(ev, st, l)
	}
	if cfg.EWMA != nil {
		ev = m.avaliarEWMA(ev, st, l)
	}
	return ev
}

func (m *Motor) avaliarTaxa(ev []Evento, st *estadoTag, l processo.Leitura) []Evento {
	cfg := st.cfg
	janela := cfg.JanelaTaxa.D()
	st.historia = append(st.historia, ponto{l.Momento, l.Valor})
	corte := l.Momento.Add(-janela)
	i := 0
	for i < len(st.historia)-1 && st.historia[i+1].t.Before(corte) {
		i++
	}
	st.historia = st.historia[i:]

	primeiro := st.historia[0]
	duracao := l.Momento.Sub(primeiro.t)
	if duracao < janela/2 {
		return ev // pouca história para uma taxa confiável
	}
	taxa := math.Abs(l.Valor-primeiro.v) / duracao.Minutes()
	max := *cfg.TaxaMaxPorMin
	cond := taxa > max || (st.alarmes[Taxa].Estado.ativo() && taxa > 0.8*max)
	return m.avaliar(ev, st, Taxa, cond, l, 0,
		fmt.Sprintf("%s variando rápido: %.2f %s/min (máximo %.2f)", cfg.ID, taxa, cfg.Unidade, max))
}

func (m *Motor) avaliarCongelado(ev []Evento, st *estadoTag, l processo.Leitura) []Evento {
	if !st.temReferencia || math.Abs(l.Valor-st.refCongelado) > 1e-9 {
		st.refCongelado = l.Valor
		st.mudouEm = l.Momento
		st.temReferencia = true
	}
	parado := l.Momento.Sub(st.mudouEm)
	cond := parado >= st.cfg.CongeladoApos.D()
	return m.avaliar(ev, st, Congelado, cond, l, 0,
		fmt.Sprintf("%s sem variar há %s: possível transmissor travado", st.cfg.ID, parado.Round(time.Second)))
}

// avaliarEWMA implementa a carta de controle EWMA. A referência (média e desvio)
// é estimada nas primeiras amostras; depois, a média móvel exponencial z é
// comparada com limites de ±L·σ·√(λ/(2−λ)).
func (m *Motor) avaliarEWMA(ev []Evento, st *estadoTag, l processo.Leitura) []Evento {
	c := st.cfg.EWMA
	e := &st.ewma
	if !e.pronto {
		e.n++
		delta := l.Valor - e.media
		e.media += delta / float64(e.n)
		e.m2 += delta * (l.Valor - e.media)
		if e.n >= c.Aquecimento {
			e.ref = e.media
			e.desvio = math.Max(math.Sqrt(e.m2/float64(e.n-1)), c.DesvioMin)
			e.z = e.ref
			e.pronto = true
		}
		return ev
	}
	e.z = c.Lambda*l.Valor + (1-c.Lambda)*e.z
	limite := c.L * e.desvio * math.Sqrt(c.Lambda/(2-c.Lambda))
	desvio := e.z - e.ref
	cond := math.Abs(desvio) > limite || (st.alarmes[Deriva].Estado.ativo() && math.Abs(desvio) > 0.8*limite)

	sentido := "acima"
	if desvio < 0 {
		sentido = "abaixo"
	}
	lz := l
	lz.Valor = e.z
	return m.avaliar(ev, st, Deriva, cond, lz, c.Confirmacao.D(),
		fmt.Sprintf("%s com tendência %s da referência: média móvel %.3f vs %.3f %s",
			st.cfg.ID, sentido, e.z, e.ref, st.cfg.Unidade))
}

// avaliar aplica a máquina de estados para um alarme.
func (m *Motor) avaliar(ev []Evento, st *estadoTag, tipo Tipo, cond bool, l processo.Leitura, atraso time.Duration, msg string) []Evento {
	a := st.alarmes[tipo]
	if a == nil {
		return ev
	}
	ativo := a.Estado.ativo()

	switch {
	case cond && ativo:
		a.Valor, a.Mensagem = l.Valor, msg
		return ev

	case cond && !ativo:
		if atraso > 0 {
			if a.pendenteDesde.IsZero() {
				a.pendenteDesde = l.Momento
			}
			if l.Momento.Sub(a.pendenteDesde) < atraso {
				return ev
			}
		}
		a.pendenteDesde = time.Time{}
		a.AtivadoEm = l.Momento
		a.ReconhecidoEm = nil
		m.ativacoes = append(m.ativacoes, l.Momento)
		m.porTipo[tipo]++
		return append(ev, m.transicao(a, AtivoNaoReconhecido, l, msg))

	case !cond && ativo:
		a.pendenteDesde = time.Time{}
		novo := Normal
		if a.Estado == AtivoNaoReconhecido {
			novo = RetornouNaoReconhecido
		}
		return append(ev, m.transicao(a, novo, l, msg))

	default:
		a.pendenteDesde = time.Time{}
		return ev
	}
}

func (m *Motor) transicao(a *alarme, novo Estado, l processo.Leitura, msg string) Evento {
	m.seq++
	a.Estado = novo
	a.Valor = l.Valor
	a.Mensagem = msg
	a.MudouEm = l.Momento
	return Evento{
		Seq: m.seq, Momento: l.Momento, Tag: a.Tag, Tipo: a.Tipo, Prioridade: a.Prioridade,
		Estado: novo, Valor: l.Valor, Mensagem: msg,
	}
}

// Reconhecer registra que o operador viu o alarme.
func (m *Motor) Reconhecer(tag string, tipo Tipo, momento time.Time) (Evento, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.tags[tag]
	if !ok {
		return Evento{}, ErrAlarmeInexistente
	}
	a, ok := st.alarmes[tipo]
	if !ok {
		return Evento{}, ErrAlarmeInexistente
	}
	return m.reconhecer(a, momento)
}

func (m *Motor) reconhecer(a *alarme, momento time.Time) (Evento, error) {
	var novo Estado
	switch a.Estado {
	case AtivoNaoReconhecido:
		novo = AtivoReconhecido
	case RetornouNaoReconhecido:
		novo = Normal
	default:
		return Evento{}, ErrNadaAReconhecer
	}
	t := momento
	a.ReconhecidoEm = &t
	l := processo.Leitura{Tag: a.Tag, Momento: momento, Valor: a.Valor}
	return m.transicao(a, novo, l, "reconhecido pelo operador: "+a.Mensagem), nil
}

// ReconhecerTodos reconhece todos os alarmes pendentes.
func (m *Motor) ReconhecerTodos(momento time.Time) []Evento {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ev []Evento
	for _, id := range m.ordemTags {
		st := m.tags[id]
		for _, t := range st.ordem {
			if e, err := m.reconhecer(st.alarmes[t], momento); err == nil {
				ev = append(ev, e)
			}
		}
	}
	return ev
}

// Ativos retorna os alarmes que não estão normais, do mais para o menos prioritário.
func (m *Motor) Ativos() []Alarme {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Alarme
	for _, st := range m.tags {
		for _, a := range st.alarmes {
			if a.Estado != Normal {
				out = append(out, a.Alarme)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if pi, pj := pesoPrioridade[out[i].Prioridade], pesoPrioridade[out[j].Prioridade]; pi != pj {
			return pi < pj
		}
		if !out[i].AtivadoEm.Equal(out[j].AtivadoEm) {
			return out[i].AtivadoEm.Before(out[j].AtivadoEm)
		}
		return out[i].Tag+string(out[i].Tipo) < out[j].Tag+string(out[j].Tipo)
	})
	return out
}

// Ultimas retorna a leitura mais recente de cada tag (nil se ainda não houve).
func (m *Motor) Ultimas() map[string]*processo.Leitura {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]*processo.Leitura, len(m.tags))
	for id, st := range m.tags {
		if st.ultima != nil {
			l := *st.ultima
			out[id] = &l
		} else {
			out[id] = nil
		}
	}
	return out
}

// AtivacoesNaJanela conta quantos alarmes dispararam no intervalo (agora-janela, agora].
// A EEMUA 191 considera "inundação de alarmes" mais de 10 alarmes em 10 minutos
// para um operador.
func (m *Motor) AtivacoesNaJanela(agora time.Time, janela time.Duration) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	corte := agora.Add(-janela)
	// descarta o que tem mais de 1 hora para a lista não crescer sem limite
	limpo := m.ativacoes[:0]
	for _, t := range m.ativacoes {
		if t.After(agora.Add(-time.Hour)) {
			limpo = append(limpo, t)
		}
	}
	m.ativacoes = limpo
	n := 0
	for _, t := range m.ativacoes {
		if t.After(corte) && !t.After(agora) {
			n++
		}
	}
	return n
}

// TotalAtivacoes retorna o contador acumulado de ativações por tipo.
func (m *Motor) TotalAtivacoes() map[Tipo]uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[Tipo]uint64, len(m.porTipo))
	for k, v := range m.porTipo {
		out[k] = v
	}
	return out
}
