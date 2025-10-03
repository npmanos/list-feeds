package jetstream

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	baseBackoff = 1 * time.Second
	maxBackoff  = 60 * time.Second
)

type BackoffManager struct {
	delays map[string]time.Duration
	mu     sync.Mutex
}

func NewBackoffManager() *BackoffManager {
	return &BackoffManager{
		delays: make(map[string]time.Duration),
	}
}

func (b *BackoffManager) Wait(host string) {
	b.mu.Lock()
	delay, ok := b.delays[host]
	b.mu.Unlock()

	if !ok || delay == 0 {
		return
	}

	log.Printf("Backoff for %s: waiting for %v before retry", host, delay)
	time.Sleep(delay)
}

func (b *BackoffManager) Backoff(host string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delay, ok := b.delays[host]
	if !ok {
		b.delays[host] = baseBackoff
		return
	}

	newDelay := min(delay*2, maxBackoff)
	b.delays[host] = newDelay
}

func (b *BackoffManager) Reset(host string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.delays[host] = 0
}

func StartConsumer(ctx context.Context, hosts []string) {
	backoff := NewBackoffManager()

	for {
		for _, host := range hosts {
			url := fmt.Sprintf("wss://%s/subscribe", host)

			backoff.Wait(host)

			log.Printf("Connecting to Jetstream instance: %s", host)

			conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, http.Header{})
			if err != nil {
				log.Printf("Failed to connect to %s: %v", host, err)
				backoff.Backoff(host)
				continue
			}

			backoff.Reset(host)
			log.Printf("Succesfully connected to %s", host)

			for {
				messageType, p, err := conn.ReadMessage()
				if err != nil {
					log.Printf("Connection to %s lost: %v", host, err)
					conn.Close()
					backoff.Backoff(host)
					break
				}

				if messageType == websocket.TextMessage {
					// TODO: Process the message (p)
					log.Printf("Received message: %s", string(p))
				}
			}
		}
	}
}
