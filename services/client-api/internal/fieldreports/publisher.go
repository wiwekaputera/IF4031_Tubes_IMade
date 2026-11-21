// Package fieldreports stores accepted field reports in JetStream.
//
// client-api answers 202 only after JetStream has acknowledged the message,
// i.e. after the report is persisted by the broker. Every later step
// (field-report-consumer, the receiver) may fail or restart without losing it.
package fieldreports

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/fieldreport"
	"github.com/wiwekaputera/if4031-tubes-imade/observability"
)

// ErrUnavailable means the report was not stored and the client should retry.
var ErrUnavailable = errors.New("field report store unavailable")

// Publisher stores one report. duplicate is true when JetStream already holds
// a report with the same message ID (a client retry); the report is then not
// stored a second time.
type Publisher interface {
	Publish(ctx context.Context, r fieldreport.Report) (duplicate bool, err error)
}

// StreamConfig is the JetStream stream holding field reports, on file
// storage so a broker restart keeps them. field-report-consumer creates the
// same stream when it starts first.
func StreamConfig() *nats.StreamConfig {
	return &nats.StreamConfig{
		Name:       fieldreport.StreamName,
		Subjects:   []string{fieldreport.SubjectFilter},
		Storage:    nats.FileStorage,
		Retention:  nats.LimitsPolicy,
		MaxAge:     fieldreport.StreamMaxAge,
		Duplicates: fieldreport.DuplicateWindow,
		Replicas:   1,
	}
}

// EnsureStream creates the stream if it does not exist yet. Errors are
// returned to the caller; Publish retries it on the next report.
func (p *NATSPublisher) EnsureStream() error {
	return p.ensure()
}

// NATSPublisher publishes to JetStream. It connects in the background so
// client-api starts (and keeps serving reads) while the broker is down.
type NATSPublisher struct {
	conn *nats.Conn
	js   nats.JetStreamContext

	mu       sync.Mutex
	ensured  bool
	ensureFn func() error
}

// NewNATSPublisher connects to url with unlimited reconnects.
func NewNATSPublisher(url string) (*NATSPublisher, error) {
	conn, err := nats.Connect(url,
		nats.Name("BNPB client-api field reports"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create JetStream context: %w", err)
	}
	p := &NATSPublisher{conn: conn, js: js}
	p.ensureFn = p.ensureStream
	return p, nil
}

// Close drains the connection.
func (p *NATSPublisher) Close() {
	_ = p.conn.Drain()
}

// Connected reports whether the broker connection is up.
func (p *NATSPublisher) Connected() bool {
	return p.conn.IsConnected()
}

func (p *NATSPublisher) ensureStream() error {
	if _, err := p.js.StreamInfo(fieldreport.StreamName); err == nil {
		return nil
	} else if !errors.Is(err, nats.ErrStreamNotFound) {
		return err
	}
	_, err := p.js.AddStream(StreamConfig())
	if errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		return nil
	}
	return err
}

func (p *NATSPublisher) ensure() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ensured {
		return nil
	}
	if err := p.ensureFn(); err != nil {
		return err
	}
	p.ensured = true
	return nil
}

// Publish stores r and waits for the JetStream acknowledgement.
func (p *NATSPublisher) Publish(ctx context.Context, r fieldreport.Report) (bool, error) {
	if !p.conn.IsConnected() {
		return false, fmt.Errorf("%w: not connected to NATS", ErrUnavailable)
	}
	if err := p.ensure(); err != nil {
		return false, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	data, err := json.Marshal(r)
	if err != nil {
		return false, fmt.Errorf("encode report: %w", err)
	}

	ctx, op := observability.StartPublish(ctx, "client-api", fieldreport.Subject)
	msg := nats.NewMsg(fieldreport.Subject)
	msg.Data = data
	msg.Header.Set(nats.MsgIdHdr, r.MessageID())
	observability.Inject(ctx, msg.Header)

	ack, err := p.js.PublishMsg(msg, nats.Context(ctx), nats.ExpectStream(fieldreport.StreamName))
	if err != nil {
		op.End("error", err)
		if errors.Is(err, nats.ErrNoStreamResponse) || errors.Is(err, nats.ErrStreamNotFound) {
			// The stream may have been deleted (broker data wiped): create it again next time.
			p.mu.Lock()
			p.ensured = false
			p.mu.Unlock()
		}
		return false, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	if ack.Duplicate {
		op.End("duplicate", nil)
		return true, nil
	}
	op.End("ok", nil)
	return false, nil
}
