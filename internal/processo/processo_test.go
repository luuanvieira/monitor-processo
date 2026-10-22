package processo

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/luuanvieira/monitor-processo/config"
)

func TestConfiguracaoPadraoEhValida(t *testing.T) {
	tags, err := CarregarTags(bytes.NewReader(config.TagsPadrao))
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) != 5 {
		t.Fatalf("esperava 5 tags, veio %d", len(tags))
	}
}

func TestCarregarTagsRejeitaConfiguracaoInvalida(t *testing.T) {
	casos := map[string]string{
		"limites fora de ordem": `[{"id":"A","faixa_min":0,"faixa_max":10,"limites":{"h":5,"l":6}}]`,
		"faixa invertida":       `[{"id":"A","faixa_min":10,"faixa_max":0}]`,
		"tag duplicado":         `[{"id":"A","faixa_min":0,"faixa_max":1},{"id":"A","faixa_min":0,"faixa_max":1}]`,
		"campo desconhecido":    `[{"id":"A","faixa_min":0,"faixa_max":1,"limite_alto":3}]`,
		"ewma inválido":         `[{"id":"A","faixa_min":0,"faixa_max":1,"ewma":{"lambda":2,"l":3,"aquecimento":100}}]`,
		"lista vazia":           `[]`,
	}
	for nome, js := range casos {
		if _, err := CarregarTags(strings.NewReader(js)); err == nil {
			t.Errorf("%s: deveria falhar", nome)
		}
	}
}

func TestJanelaTaxaPadrao(t *testing.T) {
	tags, err := CarregarTags(strings.NewReader(`[{"id":"A","faixa_min":0,"faixa_max":1,"taxa_max_por_min":1}]`))
	if err != nil {
		t.Fatal(err)
	}
	if tags[0].JanelaTaxa.D() != time.Minute {
		t.Fatalf("janela padrão deveria ser 1 min, veio %v", tags[0].JanelaTaxa.D())
	}
}

func TestLeituraJSON(t *testing.T) {
	var l Leitura
	err := json.Unmarshal([]byte(`{"tag":"A","momento":"2026-01-01T00:00:00Z","valor":1.5,"qualidade":"ruim"}`), &l)
	if err != nil || l.Qualidade != QualidadeRuim {
		t.Fatalf("decodificação falhou: %v %+v", err, l)
	}
	if err := json.Unmarshal([]byte(`{"tag":"A","qualidade":"talvez"}`), &l); err == nil {
		t.Fatal("qualidade inválida deveria falhar")
	}
}

func TestValidarLeitura(t *testing.T) {
	tags := map[string]Tag{"A": {ID: "A"}}
	agora := time.Now()
	if err := ValidarLeitura(Leitura{Tag: "A", Momento: agora, Valor: 1}, tags); err != nil {
		t.Fatal(err)
	}
	for _, l := range []Leitura{
		{Tag: "B", Momento: agora},
		{Tag: "A"},
		{Tag: "A", Momento: agora, Valor: math.NaN()},
	} {
		if ValidarLeitura(l, tags) == nil {
			t.Errorf("deveria rejeitar %+v", l)
		}
	}
}
