package main

// https://jon.chrt.dev/2026/03/20/adding-live-reload-to-a-static-site-generator-written-in-go.html
import (
	"log/slog"
	"sync"
)

type Broker[T any] struct {
	mu      sync.Mutex
	clients map[chan T]struct{}
}

func NewBroker[T any]() *Broker[T] {
	return &Broker[T]{
		clients: make(map[chan T]struct{}),
	}
}

func (b *Broker[T]) Subscribe() chan T {
	ch := make(chan T, 10)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	slog.Debug("ssebroker client subscribed", "client_qty", len(b.clients))
	return ch
}

func (b *Broker[T]) Unsubscribe(ch chan T) {
	b.mu.Lock()
	delete(b.clients, ch)
	close(ch)
	b.mu.Unlock()
	slog.Debug("ssebroker client unsubscribed", "client_qty", len(b.clients))
}

func (b *Broker[T]) Broadcast(data T) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for ch := range b.clients {
		select {
		case ch <- data:
		default: // Buffer is full, drop the message
			slog.Info("dropped message for slow client")
		}
	}
}
