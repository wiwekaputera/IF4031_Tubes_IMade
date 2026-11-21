// Package jetstream implements the poller's Publisher and State on NATS
// JetStream: alerts and ingest messages are published with a message ID and
// acknowledged by the broker; the cursor and status live in a KV bucket.
package jetstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/sensorpantai"
	"github.com/wiwekaputera/if4031-tubes-imade/observability"
	"github.com/wiwekaputera/if4031-tubes-imade/sensor-pantai-poller/internal/poller"
)

const service = "sensor-pantai-poller"

// Client wraps one JetStream context.
type Client struct {
	js nats.JetStreamContext

	mu sync.Mutex
	kv nats.KeyValue
}

// New wraps js.
func New(js nats.JetStreamContext) *Client { return &Client{js: js} }

// AlertStreamConfig is the priority alert stream.
func AlertStreamConfig() *nats.StreamConfig {
	return &nats.StreamConfig{
		Name:       event.AlertStreamName,
		Subjects:   []string{event.AlertSubjectFilter},
		Storage:    nats.FileStorage,
		Retention:  nats.LimitsPolicy,
		MaxAge:     event.AlertMaxAge,
		Duplicates: event.AlertDuplicateWindow,
		Replicas:   1,
	}
}

// EnsureAlertStream creates BNPB_ALERTS if it does not exist.
func (c *Client) EnsureAlertStream() error {
	if _, err := c.js.StreamInfo(event.AlertStreamName); err == nil {
		return nil
	} else if !errors.Is(err, nats.ErrStreamNotFound) {
		return err
	}
	_, err := c.js.AddStream(AlertStreamConfig())
	if errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		return nil
	}
	return err
}

func (c *Client) publish(ctx context.Context, subject, msgID, stream string, v any) (bool, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return false, err
	}
	ctx, op := observability.StartPublish(ctx, service, subject)
	msg := nats.NewMsg(subject)
	msg.Data = data
	msg.Header.Set(nats.MsgIdHdr, msgID)
	observability.Inject(ctx, msg.Header)
	ack, err := c.js.PublishMsg(msg, nats.Context(ctx), nats.ExpectStream(stream))
	if err != nil {
		op.End("error", err)
		return false, err
	}
	if ack.Duplicate {
		op.End("duplicate", nil)
		return true, nil
	}
	op.End("ok", nil)
	return false, nil
}

// PublishAlert publishes a critical alert and waits for the ack.
func (c *Client) PublishAlert(ctx context.Context, env event.Envelope) (bool, error) {
	return c.publish(ctx, event.CoastalAlertSubject, fmt.Sprintf("%s:%d", env.EventID, env.Version), event.AlertStreamName, env)
}

// PublishIngest publishes to hazard.ingest.SENSOR_PANTAI and waits for the ack.
func (c *Client) PublishIngest(ctx context.Context, m event.IngestMessage) error {
	_, err := c.publish(ctx, event.IngestSubject(m.Source), m.MessageID, event.IngestStreamName, m)
	return err
}

func (c *Client) bucket() (nats.KeyValue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.kv != nil {
		return c.kv, nil
	}
	kv, err := c.js.KeyValue(sensorpantai.StateBucket)
	if errors.Is(err, nats.ErrBucketNotFound) {
		kv, err = c.js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:  sensorpantai.StateBucket,
			History: 1,
			Storage: nats.FileStorage,
		})
	}
	if err != nil {
		return nil, err
	}
	c.kv = kv
	return kv, nil
}

// LoadCursor returns the saved cursor (zero value when none).
func (c *Client) LoadCursor(context.Context) (poller.Cursor, error) {
	var cur poller.Cursor
	kv, err := c.bucket()
	if err != nil {
		return cur, err
	}
	e, err := kv.Get(sensorpantai.KeyCursor)
	if errors.Is(err, nats.ErrKeyNotFound) {
		return cur, nil
	}
	if err != nil {
		return cur, err
	}
	if err := json.Unmarshal(e.Value(), &cur); err != nil {
		// Unreadable position: start from the sensor's beginning; keys dedup.
		return poller.Cursor{}, nil
	}
	return cur, nil
}

// SaveCursor stores the cursor and the sensor time it was read at.
func (c *Client) SaveCursor(_ context.Context, cur poller.Cursor) error {
	kv, err := c.bucket()
	if err != nil {
		return err
	}
	b, err := json.Marshal(cur)
	if err != nil {
		return err
	}
	_, err = kv.Put(sensorpantai.KeyCursor, b)
	return err
}

// PutStatus stores the status for client-api.
func (c *Client) PutStatus(_ context.Context, st sensorpantai.Status) error {
	kv, err := c.bucket()
	if err != nil {
		return err
	}
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	_, err = kv.Put(sensorpantai.KeyStatus, b)
	return err
}
