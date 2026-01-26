package rtds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const DefaultURL = "wss://ws-live-data.polymarket.com"

const DefaultPingInterval = 5 * time.Second

type Subscription struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`

	// Filters is an optional JSON string (not an object).
	Filters string `json:"filters,omitempty"`

	ClobAuth  any `json:"clob_auth,omitempty"`
	GammaAuth any `json:"gamma_auth,omitempty"`
}

type subscribeRequest struct {
	Action        string         `json:"action"`
	Subscriptions []Subscription `json:"subscriptions"`
}

// Message matches the RTDS message envelope.
// payload is kept as RawMessage so callers can unmarshal based on topic/type.
type Message struct {
	Topic        string          `json:"topic"`
	Type         string          `json:"type"`
	Timestamp    int64           `json:"timestamp"`
	Payload      json.RawMessage `json:"payload"`
	ConnectionID string          `json:"connection_id,omitempty"`
}

type Options struct {
	PingInterval time.Duration

	BackoffMin time.Duration
	BackoffMax time.Duration

	OutBuffer int
}

func (o Options) withDefaults() Options {
	if o.PingInterval <= 0 {
		o.PingInterval = DefaultPingInterval
	}
	if o.BackoffMin <= 0 {
		o.BackoffMin = 500 * time.Millisecond
	}
	if o.BackoffMax <= 0 {
		o.BackoffMax = 15 * time.Second
	}
	if o.OutBuffer <= 0 {
		o.OutBuffer = 256
	}
	return o
}

// Start connects to the RTDS WebSocket and emits decoded messages.
func Start(ctx context.Context, url string, subs []Subscription, opts Options) (<-chan Message, <-chan error) {
	opts = opts.withDefaults()
	if url == "" {
		url = DefaultURL
	}

	out := make(chan Message, opts.OutBuffer)
	errs := make(chan error, 16)

	go func() {
		defer close(out)
		defer close(errs)

		backoff := opts.BackoffMin
		for {
			if ctx.Err() != nil {
				return
			}

			conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
			if err != nil {
				emitErrNonBlocking(errs, fmt.Errorf("rtds dial: %w", err))
				sleepWithJitter(ctx, backoff)
				backoff = nextBackoff(backoff, opts.BackoffMax)
				continue
			}

			backoff = opts.BackoffMin

			if err := runSession(ctx, conn, subs, opts.PingInterval, out, errs); err != nil && ctx.Err() == nil {
				emitErrNonBlocking(errs, err)
			}

			_ = conn.Close()
			if ctx.Err() != nil {
				return
			}
			sleepWithJitter(ctx, backoff)
			backoff = nextBackoff(backoff, opts.BackoffMax)
		}
	}()

	return out, errs
}

func runSession(
	ctx context.Context,
	conn *websocket.Conn,
	subs []Subscription,
	pingInterval time.Duration,
	out chan<- Message,
	errs chan<- error,
) error {
	if conn == nil {
		return fmt.Errorf("rtds session: nil conn")
	}

	req := subscribeRequest{Action: "subscribe", Subscriptions: subs}
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("rtds subscribe marshal: %w", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, reqBytes); err != nil {
		return fmt.Errorf("rtds subscribe write: %w", err)
	}

	var writeMu sync.Mutex
	stop := make(chan struct{})
	var stopOnce sync.Once
	stopAll := func() { stopOnce.Do(func() { close(stop) }) }

	go func() {
		defer stopAll()
		t := time.NewTicker(pingInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-t.C:
				writeMu.Lock()
				_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
				werr := conn.WriteMessage(websocket.TextMessage, []byte("ping"))
				writeMu.Unlock()
				if werr != nil {
					emitErrNonBlocking(errs, fmt.Errorf("rtds ping: %w", werr))
					_ = conn.Close()
					return
				}
			}
		}
	}()

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	for {
		typ, msg, err := conn.ReadMessage()
		if err != nil {
			stopAll()
			// If we're shutting down, this is expected.
			if errors.Is(err, websocket.ErrCloseSent) || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("rtds read: %w", err)
		}

		if typ != websocket.TextMessage && typ != websocket.BinaryMessage {
			continue
		}

		if len(msg) == 0 {
			continue
		}
		if string(msg) == "pong" || string(msg) == "ping" {
			continue
		}

		var m Message
		if err := json.Unmarshal(msg, &m); err != nil {
			emitErrNonBlocking(errs, fmt.Errorf("rtds json decode: %w", err))
			continue
		}

		select {
		case out <- m:
		default:
		}
	}
}

func emitErrNonBlocking(ch chan<- error, err error) {
	if err == nil {
		return
	}
	select {
	case ch <- err:
	default:
	}
}

func nextBackoff(cur, max time.Duration) time.Duration {
	next := cur * 2
	if next > max {
		return max
	}
	return next
}

func sleepWithJitter(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	j := int64(d) / 7
	if j > 0 {
		d = time.Duration(int64(d) + rand.Int64N(2*j+1) - j)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
