package jetstream

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	baseBackoff = 1 * time.Second
	maxBackoff  = 60 * time.Second
)

var POST_COLLECTIONS = []string{"app.bsky.feed.post", "app.bsky.feed.repost", "app.bsky.feed.like"}
var LIST_MEMBER_COLLECTIONS = []string{"app.bsky.graph.listitem"}

type backoffManager struct {
	delays map[string]time.Duration
	mu     sync.Mutex
}

func newBackoffManager() *backoffManager {
	return &backoffManager{
		delays: make(map[string]time.Duration),
	}
}

func (b *backoffManager) Wait(host string) {
	b.mu.Lock()
	delay, ok := b.delays[host]
	b.mu.Unlock()

	if !ok || delay == 0 {
		return
	}

	log.Printf("Backoff for %s: waiting for %v before retry", host, delay)
	time.Sleep(delay)
}

func (b *backoffManager) Backoff(host string) {
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

func (b *backoffManager) Reset(host string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.delays[host] = 0
}

type JetstreamConfig struct {
	Name              string
	Hosts             []string
	Cursor            int64
	WantedDids        []string
	WantedCollections []string
	MaxSize           uint32
	ExtraHeaders      http.Header
	EventsChannel     chan<- *Event
}

type JetstreamConsumer struct {
	config  *JetstreamConfig
	backoff *backoffManager
}

type socketMessage struct {
	messageType int
	p           []byte
	err         error
}

func NewJetstreamConsumer(config *JetstreamConfig) *JetstreamConsumer {
	_, ok := config.ExtraHeaders["User-Agent"]
	if !ok {
		config.ExtraHeaders.Add("User-Agent", "list-feeds/v0.0.1")
	}

	return &JetstreamConsumer{
		config:  config,
		backoff: newBackoffManager(),
	}
}

func (c *JetstreamConsumer) buildURL(host string) (string, error) {
	jetstreamURL, err := url.Parse(fmt.Sprintf("wss://%s/subscribe", host))
	if err != nil {
		return "", err
	}

	params := jetstreamURL.Query()

	if c.config.Cursor > 0 {
		var adjustedCursor = (time.Duration(c.config.Cursor) * time.Microsecond) - (5 * time.Second)
		params.Set("cursor", fmt.Sprintf("%d", adjustedCursor.Microseconds()))
	}

	for _, did := range c.config.WantedDids {
		params.Add("wantedDids", did)
	}

	for _, collection := range c.config.WantedCollections {
		params.Add("wantedCollections", collection)
	}

	if c.config.MaxSize > 0 {
		params.Set("maxMessageSizeBytes", fmt.Sprintf("%d", c.config.MaxSize))
	}

	jetstreamURL.RawQuery = params.Encode()

	return jetstreamURL.String(), nil
}

func (c *JetstreamConsumer) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		for _, host := range c.config.Hosts {
			url, err := c.buildURL(host)
			if err != nil {
				log.Fatalf("%s: Malformed Jetstream host: %s", c.config.Name, host)
			}

			c.backoff.Wait(host)

			log.Printf("%s: Connecting to Jetstream instance: %s", c.config.Name, host)

			conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, c.config.ExtraHeaders)
			if err != nil {
				log.Printf("%s: Failed to connect to %s: %v", c.config.Name, host, err)
				c.backoff.Backoff(host)
				continue
			}

			c.backoff.Reset(host)
			log.Printf("%s: Succesfully connected to %s", c.config.Name, host)

			readChan := make(chan socketMessage)
			go func() {
				for {
					messageType, p, err := conn.ReadMessage()
					readChan <- socketMessage{messageType, p, err}
					if err != nil {
						return
					}
				}
			}()

		dispatchLoop:
			for {
				select {
				case <-ctx.Done():
					log.Printf("%s: Disconnecting from Jetstream instance: %s", c.config.Name, host)
					conn.Close()
					return
				case msg := <-readChan:
					if msg.err != nil {
						log.Printf("%s: Connection to %s lost: %v", c.config.Name, host, err)
						conn.Close()
						c.backoff.Backoff(host)
						break dispatchLoop
					}
					if msg.messageType == websocket.TextMessage {
						event, err := UnmarshalEvent(msg.p)
						if err != nil {
							log.Printf("%s: Error unmarshaling jetstream event: %v", c.config.Name, err)
						}
						c.config.Cursor = event.Cursor
						c.config.EventsChannel <- event
					}
				}
			}
		}
	}
}
