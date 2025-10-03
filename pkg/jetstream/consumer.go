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

type JetstreamConsumer struct {
	name              string
	hosts             []string
	cursor            int64
	wantedDids        []string
	wantedCollections []string
	maxSize           uint32
	extraHeaders      map[string]string
	backoff           *backoffManager
}

func NewJetstreamConsumer(
	name string,
	hosts []string,
	cursor int64,
	wantedDids []string,
	wantedCollections []string,
	maxSize uint32,
	extraHeaders map[string]string,
) *JetstreamConsumer {
	_, ok := extraHeaders["User-Agent"]
	if !ok {
		extraHeaders["User-Agent"] = "list-feeds/v0.0.1"
	}

	return &JetstreamConsumer{
		name:              name,
		hosts:             hosts,
		cursor:            cursor,
		wantedDids:        wantedDids,
		wantedCollections: wantedCollections,
		maxSize:           maxSize,
		extraHeaders:      extraHeaders,
		backoff:           newBackoffManager(),
	}
}

func (c *JetstreamConsumer) buildURL(host string) (string, error) {
	jetstreamURL, err := url.Parse(fmt.Sprintf("wss://%s/subscribe", host))
	if err != nil {
		return "", err
	}

	params := jetstreamURL.Query()

	if c.cursor > 0 {
		params.Set("cursor", fmt.Sprintf("%d", c.cursor))
	}

	for _, did := range c.wantedDids {
		params.Add("wantedDids", did)
	}

	for _, collection := range c.wantedCollections {
		params.Add("wantedCollections", collection)
	}

	if c.maxSize > 0 {
		params.Set("maxMessageSizeBytes", fmt.Sprintf("%d", c.maxSize))
	}

	jetstreamURL.RawQuery = params.Encode()

	return jetstreamURL.String(), nil
}

func (c *JetstreamConsumer) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		for _, host := range c.hosts {
			url, err := c.buildURL(host)
			if err != nil {
				log.Fatalf("%s: Malformed Jetstream host: %s", c.name, host)
			}

			c.backoff.Wait(host)

			log.Printf("%s: Connecting to Jetstream instance: %s", c.name, host)

			conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, http.Header{})
			if err != nil {
				log.Printf("%s: Failed to connect to %s: %v", c.name, host, err)
				c.backoff.Backoff(host)
				continue
			}

			c.backoff.Reset(host)
			log.Printf("%s: Succesfully connected to %s", c.name, host)

			for {
				select {
				case <-ctx.Done():
					log.Printf("%s: Disconnecting from Jetstream instance: %s", c.name, host)
					conn.Close()
					return
				default:
					messageType, p, err := conn.ReadMessage()
					if err != nil {
						log.Printf("%s: Connection to %s lost: %v", c.name, host, err)
						conn.Close()
						c.backoff.Backoff(host)
						break
					}

					if messageType == websocket.TextMessage {
						// TODO: Process the message (p)
						// log.Printf("%s: Received message: %s", c.name, string(p))
						if p != nil {
						}
					}
				}
			}
		}
	}
}
