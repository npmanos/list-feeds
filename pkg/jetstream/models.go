package jetstream

import (
	"reflect"
	"time"

	"github.com/goccy/go-json"

	"github.com/go-viper/mapstructure/v2"
)

type PostRef struct {
	CID string `json:"cid"`
	URI string `json:"uri"`
}

type PostReply struct {
	Parent *PostRef `json:"parent"`
	Root   *PostRef `json:"root"`
}

type PostRecord struct {
	Type      string     `json:"$type"`
	CreatedAt time.Time  `json:"createdAt"`
	Reply     *PostReply `json:"reply,omitempty"`
}

type RepostRecord struct {
	Type      string    `json:"$type"`
	CreatedAt time.Time `json:"createdAt"`
	Subject   *PostRef  `json:"subject,omitempty"`
}

type LikeRecord struct {
	Type      string    `json:"$type"`
	CreatedAt time.Time `json:"createdAt"`
	Subject   *PostRef  `json:"subject,omitempty"`
}

type ListItemRecord struct {
	Type      string    `json:"$type"`
	CreatedAt time.Time `json:"createdAt"`
	List      string    `json:"list"`
	Subject   string    `json:"subject"`
}

type CommitOp string

const (
	CommitCreate CommitOp = "create"
	CommitUpdate CommitOp = "update"
	CommitDelete CommitOp = "delete"
)

type Commit struct {
	Rev        string   `json:"rev"`
	Operation  CommitOp `json:"operation"`
	Collection string   `json:"collection"`
	RKey       string   `json:"rkey"`
	Record     any      `json:"record,omitempty"`
	CID        string   `json:"cid"`
}

type EventKind string

const (
	CommitEvent    EventKind = "commit"
	IdentityEvent  EventKind = "identity"
	AccountEvent   EventKind = "account"
	HeartbeatEvent EventKind = "heartbeat"
)

type Event struct {
	DID      string         `json:"did"`
	Cursor   int64          `json:"time_us"`
	Kind     EventKind      `json:"kind"`
	Commit   Commit         `json:"commit,omitzero"`
	Identity map[string]any `json:"identity,omitempty"`
	Account  map[string]any `json:"account,omitempty"`
	ShardID  string         `json:"-"`
}

func recordDecodeHook() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		if f.Kind() != reflect.Map {
			return data, nil
		}

		stringMap, ok := data.(map[string]interface{})
		if !ok {
			return data, nil
		}

		cursorVal, ok := stringMap["time_us"].(float64)
		if ok {
			stringMap["time_us"] = int64(cursorVal)
		}

		createdAtVal, ok := stringMap["createdAt"]
		if ok {
			createdAt, err := time.Parse(time.RFC3339, createdAtVal.(string))
			if err != nil {
				return nil, err
			}

			stringMap["createdAt"] = &createdAt
		}

		typeVal, ok := stringMap["$type"]
		if !ok {
			return data, nil
		}

		switch typeVal {
		case "app.bsky.feed.post":
			var record PostRecord
			err := mapstructure.Decode(stringMap, &record)
			return record, err
		case "app.bsky.feed.repost":
			var record RepostRecord
			err := mapstructure.Decode(stringMap, &record)
			return record, err
		case "app.bsky.feed.like":
			var record LikeRecord
			err := mapstructure.Decode(stringMap, &record)
			return record, err
		case "app.bsky.graph.listitem":
			var record ListItemRecord
			err := mapstructure.Decode(stringMap, &record)
			return record, err
		}

		return data, nil
	}
}

func UnmarshalEvent(data []byte) (*Event, error) {
	var rawEvent map[string]interface{}
	if err := json.Unmarshal(data, &rawEvent); err != nil {
		return nil, err
	}

	var event Event
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:     &event,
		DecodeHook: recordDecodeHook(),
		TagName:    "json",
	})

	if err != nil {
		return nil, err
	}

	if err := decoder.Decode(rawEvent); err != nil {
		return nil, err
	}

	return &event, nil
}
