// Package processo define os tipos do domínio: tags (instrumentos), leituras e
// a configuração de alarmes de cada tag.
package processo

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

// Qualidade indica se a leitura pode ser usada para decisões. Leituras ruins
// não disparam alarmes de processo (só o alarme de qualidade do instrumento).
type Qualidade int16

const (
	QualidadeBoa Qualidade = iota
	QualidadeRuim
)

func (q Qualidade) String() string {
	if q == QualidadeBoa {
		return "boa"
	}
	return "ruim"
}

func (q Qualidade) MarshalJSON() ([]byte, error) { return json.Marshal(q.String()) }

func (q *Qualidade) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	switch strings.ToLower(s) {
	case "boa", "":
		*q = QualidadeBoa
	case "ruim":
		*q = QualidadeRuim
	default:
		return fmt.Errorf("qualidade inválida %q (use boa ou ruim)", s)
	}
	return nil
}

// Leitura é um valor medido por um instrumento em um instante.
type Leitura struct {
	Tag       string    `json:"tag"`
	Momento   time.Time `json:"momento"`
	Valor     float64   `json:"valor"`
	Qualidade Qualidade `json:"qualidade"`
}

// Duracao aceita "5s", "2m" etc. no JSON de configuração.
type Duracao time.Duration

func (d Duracao) D() time.Duration { return time.Duration(d) }

func (d Duracao) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (d *Duracao) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duracao(v)
	return nil
}

// Limites de alarme. Ponteiros nulos significam "sem esse alarme".
type Limites struct {
	LL *float64 `json:"ll,omitempty"`
	L  *float64 `json:"l,omitempty"`
	H  *float64 `json:"h,omitempty"`
	HH *float64 `json:"hh,omitempty"`
}

// ConfigEWMA configura a carta de controle EWMA, que detecta deriva lenta
// muito antes de o valor chegar aos limites de alarme.
type ConfigEWMA struct {
	Lambda      float64 `json:"lambda"`        // peso da amostra nova (0 < λ <= 1)
	L           float64 `json:"l"`             // largura dos limites em desvios-padrão
	Aquecimento int     `json:"aquecimento"`   // amostras usadas para estimar média e desvio
	DesvioMin   float64 `json:"desvio_minimo"` // piso para o desvio estimado
	// Confirmacao: a média móvel precisa ficar fora dos limites por esse tempo.
	// Flutuações aleatórias saem e voltam rápido; uma deriva real permanece.
	Confirmacao Duracao `json:"confirmacao"`
}

// Tag descreve um instrumento e as regras de alarme aplicadas a ele.
type Tag struct {
	ID        string  `json:"id"`
	Descricao string  `json:"descricao"`
	Unidade   string  `json:"unidade"`
	FaixaMin  float64 `json:"faixa_min"` // faixa calibrada do instrumento
	FaixaMax  float64 `json:"faixa_max"`

	Limites        Limites `json:"limites"`
	BandaMorta     float64 `json:"banda_morta"`     // histerese para normalizar o alarme
	AtrasoAtivacao Duracao `json:"atraso_ativacao"` // condição precisa durar isso para alarmar

	TaxaMaxPorMin *float64 `json:"taxa_max_por_min,omitempty"`
	JanelaTaxa    Duracao  `json:"janela_taxa,omitempty"`

	CongeladoApos Duracao `json:"congelado_apos,omitempty"`

	EWMA *ConfigEWMA `json:"ewma,omitempty"`
}

// CarregarTags lê e valida a configuração de tags em JSON.
func CarregarTags(r io.Reader) ([]Tag, error) {
	var tags []Tag
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&tags); err != nil {
		return nil, fmt.Errorf("configuração de tags: %w", err)
	}
	vistos := map[string]bool{}
	var erros []error
	for i := range tags {
		t := &tags[i]
		if t.JanelaTaxa == 0 {
			t.JanelaTaxa = Duracao(time.Minute)
		}
		if err := t.validar(); err != nil {
			erros = append(erros, err)
		}
		if vistos[t.ID] {
			erros = append(erros, fmt.Errorf("tag %s duplicado", t.ID))
		}
		vistos[t.ID] = true
	}
	if len(tags) == 0 {
		erros = append(erros, errors.New("nenhum tag configurado"))
	}
	return tags, errors.Join(erros...)
}

func (t Tag) validar() error {
	if t.ID == "" {
		return errors.New("tag sem id")
	}
	prefixo := "tag " + t.ID + ": "
	if t.FaixaMax <= t.FaixaMin {
		return errors.New(prefixo + "faixa_max deve ser maior que faixa_min")
	}
	if t.BandaMorta < 0 {
		return errors.New(prefixo + "banda_morta não pode ser negativa")
	}
	// Os limites precisam estar em ordem: LL <= L < H <= HH.
	ordem := []*float64{t.Limites.LL, t.Limites.L, t.Limites.H, t.Limites.HH}
	anterior := math.Inf(-1)
	for _, v := range ordem {
		if v == nil {
			continue
		}
		if *v < anterior {
			return errors.New(prefixo + "limites fora de ordem (esperado LL <= L <= H <= HH)")
		}
		anterior = *v
	}
	if t.TaxaMaxPorMin != nil && *t.TaxaMaxPorMin <= 0 {
		return errors.New(prefixo + "taxa_max_por_min deve ser positiva")
	}
	if e := t.EWMA; e != nil {
		if e.Lambda <= 0 || e.Lambda > 1 || e.L <= 0 || e.Aquecimento < 10 {
			return errors.New(prefixo + "ewma inválido (0 < lambda <= 1, l > 0, aquecimento >= 10)")
		}
	}
	return nil
}

// ValidarLeitura confere uma leitura recebida de fora (API de ingestão).
func ValidarLeitura(l Leitura, tags map[string]Tag) error {
	if _, ok := tags[l.Tag]; !ok {
		return fmt.Errorf("tag desconhecido %q", l.Tag)
	}
	if l.Momento.IsZero() {
		return fmt.Errorf("tag %s: momento ausente", l.Tag)
	}
	if math.IsNaN(l.Valor) || math.IsInf(l.Valor, 0) {
		return fmt.Errorf("tag %s: valor não é um número finito", l.Tag)
	}
	return nil
}
