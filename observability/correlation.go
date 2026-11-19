package observability

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"
)

// CorrelationHeader is the header that carries the request's correlation ID.
const CorrelationHeader = "X-Correlation-ID"

type correlationKey struct{}

// WithCorrelationID returns ctx carrying id.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

// CorrelationID returns the correlation ID in ctx, or "".
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)
	return id
}

// NewCorrelationID returns a random UUIDv4-shaped ID.
func NewCorrelationID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("corr-%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
