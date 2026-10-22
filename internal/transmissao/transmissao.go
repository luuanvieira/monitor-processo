// Package transmissao distribui eventos em tempo real para os clientes
// conectados por Server-Sent Events (SSE).
package transmissao

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
)

// Broker é seguro para uso concorrente. Um cliente lento nunca trava os
// outros nem o processamento: se o buffer dele encher, a mensagem é descartada
// para ele (e contada). O dashboard se recupera na próxima atualização.
type Broker struct {
	mu       sync.Mutex
	clientes map[chan []byte]struct{}

	Descartadas atomic.Uint64
}

func NovoBroker() *Broker {
	return &Broker{clientes: map[chan []byte]struct{}{}}
}

// Assinar retorna o canal do cliente e a função para cancelar a assinatura.
func (b *Broker) Assinar() (<-chan []byte, func()) {
	ch := make(chan []byte, 64)
	b.mu.Lock()
	b.clientes[ch] = struct{}{}
	b.mu.Unlock()
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.clientes, ch)
			b.mu.Unlock()
		})
	}
}

// Publicar serializa uma vez e entrega a todos os clientes no formato SSE.
func (b *Broker) Publicar(evento string, dados any) error {
	js, err := json.Marshal(dados)
	if err != nil {
		return err
	}
	msg := []byte(fmt.Sprintf("event: %s\ndata: %s\n\n", evento, js))

	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.clientes {
		select {
		case ch <- msg:
		default:
			b.Descartadas.Add(1)
		}
	}
	return nil
}

// Clientes retorna quantos clientes estão conectados.
func (b *Broker) Clientes() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.clientes)
}
