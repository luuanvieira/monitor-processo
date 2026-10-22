// Package armazenamento define onde o histórico (leituras e eventos de alarme)
// é guardado. Há duas implementações: memória (padrão, útil para demonstração e
// testes) e PostgreSQL (subpacote postgres).
package armazenamento

import (
	"context"
	"time"

	"github.com/luuanvieira/monitor-processo/internal/alarmes"
	"github.com/luuanvieira/monitor-processo/internal/processo"
)

// Store é o contrato de persistência usado pela aplicação.
type Store interface {
	SalvarLeituras(ctx context.Context, leituras []processo.Leitura) error
	SalvarEventos(ctx context.Context, eventos []alarmes.Evento) error

	// Leituras retorna as leituras do tag em [desde, ate], em ordem cronológica,
	// limitadas às `limite` mais recentes.
	Leituras(ctx context.Context, tag string, desde, ate time.Time, limite int) ([]processo.Leitura, error)
	// Eventos retorna os eventos mais recentes primeiro (tag vazio = todos).
	Eventos(ctx context.Context, tag string, limite int) ([]alarmes.Evento, error)

	Ping(ctx context.Context) error
	Close() error
}
