package simulador_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/luuanvieira/monitor-processo/config"
	"github.com/luuanvieira/monitor-processo/internal/alarmes"
	"github.com/luuanvieira/monitor-processo/internal/processo"
	"github.com/luuanvieira/monitor-processo/internal/simulador"
)

var inicio = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func tagsPadrao(t *testing.T) []processo.Tag {
	t.Helper()
	tags, err := processo.CarregarTags(bytes.NewReader(config.TagsPadrao))
	if err != nil {
		t.Fatal(err)
	}
	return tags
}

type cenario struct {
	planta *simulador.Planta
	motor  *alarmes.Motor
	ativos map[string]time.Duration // "TAG/TIPO" -> instante (desde o início) da 1ª ativação
}

func novoCenario(t *testing.T) *cenario {
	tags := tagsPadrao(t)
	return &cenario{
		planta: simulador.Nova(tags, 42, inicio),
		motor:  alarmes.NovoMotor(tags),
		ativos: map[string]time.Duration{},
	}
}

func (c *cenario) rodar(d time.Duration) {
	for i := 0; i < int(d/time.Second); i++ {
		for _, l := range c.planta.Passo(time.Second) {
			for _, e := range c.motor.Processar(l) {
				k := e.Tag + "/" + string(e.Tipo)
				if _, ok := c.ativos[k]; !ok && e.Estado == alarmes.AtivoNaoReconhecido {
					c.ativos[k] = e.Momento.Sub(inicio)
				}
			}
		}
	}
}

func (c *cenario) exigir(t *testing.T, chaves ...string) {
	t.Helper()
	for _, k := range chaves {
		if _, ok := c.ativos[k]; !ok {
			t.Errorf("esperava o alarme %s; disparados: %v", k, c.ativos)
		}
	}
}

func TestOperacaoNormalNaoGeraAlarmes(t *testing.T) {
	// Um sistema de alarmes bom não incomoda o operador quando está tudo bem.
	c := novoCenario(t)
	c.rodar(6 * time.Hour)
	if len(c.ativos) != 0 {
		t.Fatalf("6 h de operação normal não deveriam gerar alarmes: %v", c.ativos)
	}
}

func TestPerdaDeResfriamentoGeraCascata(t *testing.T) {
	c := novoCenario(t)
	c.rodar(10 * time.Minute)
	if _, err := c.planta.AtivarFalha(simulador.PerdaResfriamento, ""); err != nil {
		t.Fatal(err)
	}
	c.rodar(15 * time.Minute)
	c.exigir(t, "TT-101/TAXA", "TT-101/H", "TT-101/HH", "PT-101/H", "PT-101/HH")

	// Temperatura reage antes da pressão (a pressão é consequência).
	if c.ativos["TT-101/H"] >= c.ativos["PT-101/H"] {
		t.Errorf("TT-101/H (%v) deveria vir antes de PT-101/H (%v)", c.ativos["TT-101/H"], c.ativos["PT-101/H"])
	}
}

func TestDesarmeDaBomba(t *testing.T) {
	c := novoCenario(t)
	c.rodar(10 * time.Minute)
	if _, err := c.planta.AtivarFalha(simulador.DesarmeBomba, ""); err != nil {
		t.Fatal(err)
	}
	c.rodar(20 * time.Minute)
	c.exigir(t, "FT-201/TAXA", "FT-201/L", "FT-201/LL", "LT-301/H")
}

func TestEWMADetectaDesgasteDaBombaMuitoAntesDoLimite(t *testing.T) {
	c := novoCenario(t)
	c.rodar(35 * time.Minute) // a EWMA precisa de 30 min de aquecimento para aprender a referência
	if _, err := c.planta.AtivarFalha(simulador.DegradacaoBomba, ""); err != nil {
		t.Fatal(err)
	}
	c.rodar(90 * time.Minute)
	c.exigir(t, "VT-501/DERIVA", "VT-501/H")

	antecedencia := c.ativos["VT-501/H"] - c.ativos["VT-501/DERIVA"]
	if antecedencia < 45*time.Minute {
		t.Fatalf("a deriva deveria avisar com bastante antecedência, avisou só %v antes do H", antecedencia)
	}
	t.Logf("EWMA detectou o desgaste %v antes do alarme H", antecedencia.Round(time.Second))
}

func TestFalhasDeInstrumento(t *testing.T) {
	c := novoCenario(t)
	c.rodar(10 * time.Minute)
	for _, f := range []struct {
		tipo simulador.TipoFalha
		tag  string
	}{{simulador.SensorTravado, "LT-301"}, {simulador.FalhaSinal, "FT-201"}} {
		if _, err := c.planta.AtivarFalha(f.tipo, f.tag); err != nil {
			t.Fatal(err)
		}
	}
	c.rodar(6 * time.Minute)
	c.exigir(t, "LT-301/CONGELADO", "FT-201/QUALIDADE")
	if _, ok := c.ativos["FT-201/LL"]; ok {
		t.Error("sinal com qualidade ruim não pode disparar LL de vazão")
	}
}

func TestDerivaDeSensor(t *testing.T) {
	c := novoCenario(t)
	c.rodar(35 * time.Minute)
	if _, err := c.planta.AtivarFalha(simulador.DerivaSensor, "TT-101"); err != nil {
		t.Fatal(err)
	}
	c.rodar(10 * time.Minute)
	c.exigir(t, "TT-101/DERIVA")
}

func TestValidacaoDeFalhas(t *testing.T) {
	p := simulador.Nova(tagsPadrao(t), 1, inicio)
	if _, err := p.AtivarFalha("explosao", ""); err == nil {
		t.Error("tipo desconhecido deveria falhar")
	}
	if _, err := p.AtivarFalha(simulador.SensorTravado, ""); err == nil {
		t.Error("falha de instrumento sem tag deveria falhar")
	}
	if _, err := p.AtivarFalha(simulador.PerdaResfriamento, "TT-101"); err == nil {
		t.Error("falha de processo com tag deveria falhar")
	}
	a, _ := p.AtivarFalha(simulador.PerdaResfriamento, "")
	b, _ := p.AtivarFalha(simulador.PerdaResfriamento, "")
	if a != b || len(p.Falhas()) != 1 {
		t.Error("ativar a mesma falha duas vezes deve ser idempotente")
	}
	if !p.RemoverFalha(simulador.PerdaResfriamento, "") || p.RemoverFalha(simulador.PerdaResfriamento, "") {
		t.Error("remover deve funcionar uma vez só")
	}
}

func TestSimulacaoEhReproduzivel(t *testing.T) {
	a := simulador.Nova(tagsPadrao(t), 7, inicio)
	b := simulador.Nova(tagsPadrao(t), 7, inicio)
	for i := 0; i < 100; i++ {
		la, lb := a.Passo(time.Second), b.Passo(time.Second)
		for j := range la {
			if la[j] != lb[j] {
				t.Fatalf("mesma semente deveria gerar os mesmos valores: %+v vs %+v", la[j], lb[j])
			}
		}
	}
}
