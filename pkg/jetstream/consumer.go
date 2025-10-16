package jetstream

import (
	"context"
	"fmt"
	"iter"
	"log"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/npmanos/list-feeds/pkg/utils"
)

const (
	backoffMultiplier = 2 * time.Second
	maxBackoff        = 60 * time.Second
)

var POST_COLLECTIONS = []string{"app.bsky.feed.post", "app.bsky.feed.repost", "app.bsky.feed.like"}
var LIST_MEMBER_COLLECTIONS = []string{"app.bsky.graph.listitem"}

type backoffManager struct {
	hostQueue      *utils.PriorityQueue[string]
	currentHost    string
	currentBackoff int
}

func newBackoffManager(hosts []string) *backoffManager {
	priorityMap := make(map[string]int)
	for i, host := range hosts {
		priorityMap[host] = i
	}

	hostQueue := utils.NewPriorityQueue(priorityMap)

	return &backoffManager{
		hostQueue: hostQueue,
	}
}

func (b *backoffManager) Hosts() iter.Seq[string] {
	return func(yield func(string) bool) {
		for {
			b.currentHost, b.currentBackoff = b.hostQueue.PopTyped()

			if b.currentBackoff > 0 {
				var delay = time.Duration(b.currentBackoff) * backoffMultiplier
				log.Printf("Backoff for %s: waiting for %v before retry", b.currentHost, delay)
				time.Sleep(delay)
			}

			b.currentBackoff++

			if !yield(b.currentHost) {
				b.hostQueue.PushTyped(b.currentHost, b.currentBackoff)
				return
			}

			b.hostQueue.PushTyped(b.currentHost, b.currentBackoff)
		}
	}
}

func (b *backoffManager) ResetCurrentHost() {
	b.currentBackoff = 0
}

type ListMemberUpdate struct {
	AddMember    string
	RemoveMember string
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
	WantedDidsUpdates <-chan *ListMemberUpdate
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

	slices.Sort(config.WantedDids)

	return &JetstreamConsumer{
		config:  config,
		backoff: newBackoffManager(config.Hosts),
	}
}

func (c *JetstreamConsumer) buildURL(host string) (string, error) {
	jetstreamURL, err := url.Parse(fmt.Sprintf("wss://%s/subscribe", host))
	if err != nil {
		return "", err
	}

	params := jetstreamURL.Query()

	if c.config.Cursor > 1 {
		var adjustedCursor = (time.Duration(c.config.Cursor) * time.Microsecond) - (5 * time.Second)
		params.Set("cursor", fmt.Sprintf("%d", adjustedCursor.Microseconds()))
	} else if c.config.Cursor == 1 {
		params.Set("cursor", fmt.Sprintf("%d", c.config.Cursor))
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

	for host := range c.backoff.Hosts() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		url, err := c.buildURL(host)
		if err != nil {
			log.Fatalf("%s: Malformed Jetstream host: %s", c.config.Name, host)
		}

		log.Printf("%s: Connecting to Jetstream instance: %s", c.config.Name, host)

		conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, c.config.ExtraHeaders)
		if err != nil {
			log.Printf("%s: Failed to connect to %s: %v", c.config.Name, host, err)
			continue
		}

		c.backoff.ResetCurrentHost()
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
			if c.config.WantedDidsUpdates != nil {
				select {
				case didUpdate := <-c.config.WantedDidsUpdates:
					if didUpdate.AddMember != "" {
						if _, found := slices.BinarySearch(c.config.WantedDids, didUpdate.AddMember); !found {
							continue
						}
						c.config.WantedDids = append(c.config.WantedDids, didUpdate.AddMember)
						slices.Sort(c.config.WantedDids)
					}
					if didUpdate.RemoveMember != "" {
						if idx, found := slices.BinarySearch(c.config.WantedDids, didUpdate.RemoveMember); found {
							copy(c.config.WantedDids[idx:], c.config.WantedDids[idx+1:])
							c.config.WantedDids[len(c.config.WantedDids)-1] = ""
							c.config.WantedDids = c.config.WantedDids[:len(c.config.WantedDids)-1]
						}
					}
					break dispatchLoop
				default:
				}
			}

			select {
			case <-ctx.Done():
				log.Printf("%s: Disconnecting from Jetstream instance: %s", c.config.Name, host)
				conn.Close()
				return
			case msg := <-readChan:
				if msg.err != nil {
					log.Printf("%s: Connection to %s lost: %v", c.config.Name, host, err)
					conn.Close()
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
