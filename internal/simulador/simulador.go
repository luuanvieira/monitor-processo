// Package simulador gera leituras de uma pequena unidade de processo:
// um tanque pulmão (TQ-301) alimenta o reator R-101 pela bomba P-201.
//
//	TQ-301 (LT-301) ──P-201 (FT-201, VT-501)──▶ R-101 (TT-101, PT-101)
//
// O modelo é simples de propósito, mas fisicamente coerente: uma falha de
// resfriamento eleva a temperatura e, por consequência, a pressão; o desarme
// da bomba zera a vazão e faz o nível do tanque subir. Assim uma única causa
// gera uma cascata de alarmes, como numa planta de verdade.
package simulador

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/luuanvieira/monitor-processo/internal/processo"
)

// TipoFalha identifica uma falha que pode ser injetada na planta.
type TipoFalha string

const (
	// Falhas de processo (afetam a planta inteira).
	PerdaResfriamento TipoFalha = "perda_resfriamento"
	DegradacaoBomba   TipoFalha = "degradacao_bomba"
	DesarmeBomba      TipoFalha = "desarme_bomba"

	// Falhas de instrumento (exigem um tag).
	SensorTravado TipoFalha = "sensor_travado"
	DerivaSensor  TipoFalha = "deriva_sensor"
	FalhaSinal    TipoFalha = "falha_sinal"
)

var descricoes = map[TipoFalha]string{
	PerdaResfriamento: "Perda de água de resfriamento do reator: temperatura e pressão sobem",
	DegradacaoBomba:   "Desgaste do rolamento da P-201: vibração aumenta lentamente",
	DesarmeBomba:      "Desarme da P-201: vazão cai a zero e o nível do tanque sobe",
	SensorTravado:     "Transmissor travado: o valor para de variar",
	DerivaSensor:      "Transmissor descalibrando: o valor medido se afasta do real",
	FalhaSinal:        "Sinal aberto/curto (ex. 4-20 mA fora da faixa): qualidade ruim",
}

// Descricao retorna o texto explicativo da falha.
func (t TipoFalha) Descricao() string { return descricoes[t] }

func (t TipoFalha) deInstrumento() bool {
	return t == SensorTravado || t == DerivaSensor || t == FalhaSinal
}

// TiposFalha lista todas as falhas disponíveis.
func TiposFalha() []TipoFalha {
	return []TipoFalha{PerdaResfriamento, DegradacaoBomba, DesarmeBomba, SensorTravado, DerivaSensor, FalhaSinal}
}

// Falha ativa na planta.
type Falha struct {
	Tipo      TipoFalha `json:"tipo"`
	Tag       string    `json:"tag,omitempty"`
	Descricao string    `json:"descricao"`
	Desde     time.Time `json:"desde"`
}

func chave(tipo TipoFalha, tag string) string { return string(tipo) + "|" + tag }

var ErrFalhaInvalida = errors.New("falha inválida")

// Parâmetros do processo em regime normal.
const (
	setpointTemp   = 80.0 // °C
	tempPerda      = 105.0
	tauTemp        = 300.0 // s
	pressaoBase    = 2.0   // bar a 80 °C
	ganhoPressao   = 0.05  // bar/°C
	vazaoNominal   = 12.0  // m³/h
	tauVazao       = 5.0   // s
	nivelInicial   = 60.0  // %
	capacidadeTQ   = 10.0  // m³
	vibracaoBase   = 2.5   // mm/s
	vibracaoParada = 0.2
	desgastePorMin = 0.03  // mm/s por minuto de degradação
	derivaPorMin   = 0.005 // fração da faixa por minuto
)

// Ruído de medição (desvio-padrão) de cada transmissor.
var ruido = map[string]float64{"TT-101": 0.15, "PT-101": 0.01, "FT-201": 0.2, "LT-301": 0.3, "VT-501": 0.1}

// Planta é segura para uso concorrente (o ticker chama Passo e a API injeta falhas).
type Planta struct {
	mu    sync.Mutex
	rng   *rand.Rand
	tags  map[string]processo.Tag
	agora time.Time

	// Estado "real" do processo (o que os instrumentos tentam medir).
	temperatura, vazao, nivel, desgaste float64

	falhas   map[string]Falha
	travados map[string]float64 // último valor medido de cada sensor travado
}

// Nova cria a planta em regime permanente. A semente torna a simulação reproduzível.
func Nova(tags []processo.Tag, semente uint64, inicio time.Time) *Planta {
	m := make(map[string]processo.Tag, len(tags))
	for _, t := range tags {
		m[t.ID] = t
	}
	return &Planta{
		rng:         rand.New(rand.NewPCG(semente, semente^0x9e3779b97f4a7c15)),
		tags:        m,
		agora:       inicio,
		temperatura: setpointTemp,
		vazao:       vazaoNominal,
		nivel:       nivelInicial,
		falhas:      map[string]Falha{},
		travados:    map[string]float64{},
	}
}

func (p *Planta) ativa(tipo TipoFalha, tag string) (Falha, bool) {
	f, ok := p.falhas[chave(tipo, tag)]
	return f, ok
}

// Passo avança a simulação em dt e retorna uma leitura por instrumento.
func (p *Planta) Passo(dt time.Duration) []processo.Leitura {
	p.mu.Lock()
	defer p.mu.Unlock()

	s := dt.Seconds()
	p.agora = p.agora.Add(dt)

	// Reator: temperatura segue o setpoint (ou o novo equilíbrio, sem resfriamento).
	alvo := setpointTemp
	if _, ok := p.ativa(PerdaResfriamento, ""); ok {
		alvo = tempPerda
	}
	p.temperatura += (alvo - p.temperatura) * (1 - math.Exp(-s/tauTemp))
	pressao := pressaoBase + ganhoPressao*(p.temperatura-setpointTemp)

	// Bomba e tanque pulmão: a entrada do tanque é fixa; a saída é a vazão da bomba.
	_, desarmada := p.ativa(DesarmeBomba, "")
	alvoVazao := vazaoNominal
	if desarmada {
		alvoVazao = 0
	}
	p.vazao += (alvoVazao - p.vazao) * (1 - math.Exp(-s/tauVazao))
	p.nivel += (vazaoNominal - p.vazao) / 3600 * s / capacidadeTQ * 100
	p.nivel = math.Max(0, math.Min(100, p.nivel))

	if _, ok := p.ativa(DegradacaoBomba, ""); ok {
		p.desgaste += desgastePorMin * s / 60
	}
	vibracao := vibracaoBase + p.desgaste
	if desarmada {
		vibracao = vibracaoParada
	}

	reais := map[string]float64{
		"TT-101": p.temperatura,
		"PT-101": pressao,
		"FT-201": p.vazao,
		"LT-301": p.nivel,
		"VT-501": vibracao,
	}

	ids := make([]string, 0, len(reais))
	for id := range reais {
		if _, ok := p.tags[id]; ok {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)

	leituras := make([]processo.Leitura, 0, len(ids))
	for _, id := range ids {
		leituras = append(leituras, p.medir(id, reais[id]))
	}
	return leituras
}

// medir aplica ruído e as falhas de instrumento sobre o valor real.
func (p *Planta) medir(id string, real float64) processo.Leitura {
	tag := p.tags[id]
	l := processo.Leitura{Tag: id, Momento: p.agora, Qualidade: processo.QualidadeBoa}

	if f, ok := p.ativa(DerivaSensor, id); ok {
		minutos := p.agora.Sub(f.Desde).Minutes()
		real += derivaPorMin * minutos * (tag.FaixaMax - tag.FaixaMin)
	}
	valor := real + p.rng.NormFloat64()*ruido[id]
	if v, ok := p.travados[id]; ok {
		valor = v
	} else if _, ok := p.ativa(SensorTravado, id); ok {
		p.travados[id] = valor
	}

	if _, ok := p.ativa(FalhaSinal, id); ok {
		// Transmissor 4-20 mA com laço aberto: sinal abaixo de 3,8 mA.
		l.Valor = tag.FaixaMin - 0.05*(tag.FaixaMax-tag.FaixaMin)
		l.Qualidade = processo.QualidadeRuim
		return l
	}

	l.Valor = math.Round(math.Max(tag.FaixaMin, math.Min(tag.FaixaMax, valor))*1000) / 1000
	return l
}

// AtivarFalha injeta uma falha a partir do instante atual da simulação.
func (p *Planta) AtivarFalha(tipo TipoFalha, tag string) (Falha, error) {
	if _, ok := descricoes[tipo]; !ok {
		return Falha{}, fmt.Errorf("%w: tipo desconhecido %q", ErrFalhaInvalida, tipo)
	}
	if tipo.deInstrumento() {
		if _, ok := p.tags[tag]; !ok {
			return Falha{}, fmt.Errorf("%w: %s exige um tag válido", ErrFalhaInvalida, tipo)
		}
	} else if tag != "" {
		return Falha{}, fmt.Errorf("%w: %s é uma falha de processo e não aceita tag", ErrFalhaInvalida, tipo)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	k := chave(tipo, tag)
	if f, ok := p.falhas[k]; ok {
		return f, nil // idempotente
	}
	f := Falha{Tipo: tipo, Tag: tag, Descricao: tipo.Descricao(), Desde: p.agora}
	p.falhas[k] = f
	return f, nil
}

// RemoverFalha encerra uma falha. O desgaste da bomba é mantido (o rolamento
// não se conserta sozinho); use Manutencao para zerá-lo.
func (p *Planta) RemoverFalha(tipo TipoFalha, tag string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := chave(tipo, tag)
	if _, ok := p.falhas[k]; !ok {
		return false
	}
	delete(p.falhas, k)
	if tipo == SensorTravado {
		delete(p.travados, tag)
	}
	return true
}

// Manutencao troca o rolamento da bomba (zera o desgaste acumulado).
func (p *Planta) Manutencao() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.desgaste = 0
	delete(p.falhas, chave(DegradacaoBomba, ""))
}

// Falhas lista as falhas ativas em ordem de início.
func (p *Planta) Falhas() []Falha {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Falha, 0, len(p.falhas))
	for _, f := range p.falhas {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Desde.Equal(out[j].Desde) {
			return chave(out[i].Tipo, out[i].Tag) < chave(out[j].Tipo, out[j].Tag)
		}
		return out[i].Desde.Before(out[j].Desde)
	})
	return out
}

// Agora retorna o instante atual da simulação.
func (p *Planta) Agora() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.agora
}
