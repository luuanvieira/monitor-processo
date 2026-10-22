package armazenamento

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/luuanvieira/monitor-processo/internal/alarmes"
	"github.com/luuanvieira/monitor-processo/internal/processo"
)

// Memoria guarda as últimas N leituras por tag e os últimos M eventos.
type Memoria struct {
	mu          sync.RWMutex
	porTag      map[string][]processo.Leitura
	eventos     []alarmes.Evento
	maxLeituras int
	maxEventos  int
}

func NovaMemoria(maxLeiturasPorTag, maxEventos int) *Memoria {
	return &Memoria{porTag: map[string][]processo.Leitura{}, maxLeituras: maxLeiturasPorTag, maxEventos: maxEventos}
}

func (m *Memoria) SalvarLeituras(_ context.Context, ls []processo.Leitura) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, l := range ls {
		s := m.porTag[l.Tag]
		// Mantém a ordem cronológica mesmo se chegar algo atrasado pela API.
		if n := len(s); n > 0 && l.Momento.Before(s[n-1].Momento) {
			i := sort.Search(n, func(i int) bool { return !s[i].Momento.Before(l.Momento) })
			if i < n && s[i].Momento.Equal(l.Momento) {
				continue // mesma chave (tag, momento): ignora, como o ON CONFLICT do Postgres
			}
			s = append(s, processo.Leitura{})
			copy(s[i+1:], s[i:])
			s[i] = l
		} else if n > 0 && l.Momento.Equal(s[n-1].Momento) {
			continue
		} else {
			s = append(s, l)
		}
		if len(s) > m.maxLeituras {
			s = append(s[:0:0], s[len(s)-m.maxLeituras:]...)
		}
		m.porTag[l.Tag] = s
	}
	return nil
}

func (m *Memoria) SalvarEventos(_ context.Context, ev []alarmes.Evento) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventos = append(m.eventos, ev...)
	if len(m.eventos) > m.maxEventos {
		m.eventos = append(m.eventos[:0:0], m.eventos[len(m.eventos)-m.maxEventos:]...)
	}
	return nil
}

func (m *Memoria) Leituras(_ context.Context, tag string, desde, ate time.Time, limite int) ([]processo.Leitura, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s := m.porTag[tag]
	ini := sort.Search(len(s), func(i int) bool { return !s[i].Momento.Before(desde) })
	fim := sort.Search(len(s), func(i int) bool { return s[i].Momento.After(ate) })
	if ini >= fim {
		return []processo.Leitura{}, nil
	}
	if limite > 0 && fim-ini > limite {
		ini = fim - limite
	}
	return append([]processo.Leitura(nil), s[ini:fim]...), nil
}

func (m *Memoria) Eventos(_ context.Context, tag string, limite int) ([]alarmes.Evento, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []alarmes.Evento{}
	for i := len(m.eventos) - 1; i >= 0 && (limite <= 0 || len(out) < limite); i-- {
		if tag == "" || m.eventos[i].Tag == tag {
			out = append(out, m.eventos[i])
		}
	}
	return out, nil
}

func (m *Memoria) Ping(context.Context) error { return nil }
func (m *Memoria) Close() error               { return nil }
