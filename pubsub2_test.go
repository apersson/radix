package radix

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tilinna/clock"
)

type pubSubTestHarness struct {
	t     *testing.T
	ctx   context.Context
	conn  PubSubConn2
	pubCh chan<- PubSubMessage

	eventCh chan string
	clock   *clock.Mock

	nextMsg    PubSubMessage
	nextErr    error
	nextDoneCh chan struct{}
}

func newPubSubTestHarness(t *testing.T) *pubSubTestHarness {
	h := &pubSubTestHarness{
		t:       t,
		eventCh: make(chan string),
		clock:   clock.NewMock(time.Now().Truncate(time.Hour)),
	}

	var cancelFn context.CancelFunc
	h.ctx, cancelFn = context.WithTimeout(context.Background(), 2*time.Second)
	h.t.Cleanup(cancelFn)

	var stub Conn
	stub, h.pubCh = NewPubSubStubConn("", "", nil)
	h.conn = pubSubConfig{
		clock:       h.clock,
		testEventCh: h.eventCh,
	}.new(stub)

	return h
}

func (h *pubSubTestHarness) assertEvent(expEvent string, fn func()) bool {
	h.t.Helper()
	select {
	case gotEvent := <-h.eventCh:
		if !assert.Equal(h.t, expEvent, gotEvent) {
			return false
		}
	case <-h.ctx.Done():
		h.t.Fatalf("expected event %q", expEvent)
		return false
	}

	if fn != nil {
		fn()
	}

	h.eventCh <- ""
	return true
}

func (h *pubSubTestHarness) startNext(ctx context.Context) {
	h.t.Helper()
	h.nextDoneCh = make(chan struct{})
	go func() {
		h.nextMsg, h.nextErr = h.conn.Next(ctx)
		close(h.nextDoneCh)
	}()
}

func (h *pubSubTestHarness) assertNext(expMsg PubSubMessage, expErr error) {
	h.t.Helper()
	select {
	case <-h.nextDoneCh:
	case <-h.ctx.Done():
		h.t.Fatal("expected call to Next to have completed, but it hasn't")
	}

	if expErr != nil {
		assert.Equal(h.t, expErr, h.nextErr)
	} else {
		assert.Equal(h.t, expMsg, h.nextMsg)
	}
}

func stdPubSubTests(t *testing.T, mkHarness func(t *testing.T) *pubSubTestHarness) {
	t.Run("ping", func(t *testing.T) {
		h := mkHarness(t)
		ctx, cancel := context.WithCancel(h.ctx)
		h.startNext(ctx)
		h.assertEvent("next-top", nil)
		h.assertEvent("wrapped-ctx", func() {
			h.clock.Add(pubSubDefaultPingInterval)
		})

		h.assertEvent("decode-returned", nil)

		h.assertEvent("next-top", nil)
		h.assertEvent("pinged", nil)

		// do it again just to be sure
		h.assertEvent("wrapped-ctx", func() {
			h.clock.Add(pubSubDefaultPingInterval)
		})

		h.assertEvent("decode-returned", nil)

		h.assertEvent("next-top", nil)
		h.assertEvent("pinged", nil)

		// now return
		cancel()
		h.assertEvent("wrapped-ctx", nil)
		h.assertEvent("decode-returned", nil)

		h.assertNext(PubSubMessage{}, context.Canceled)
	})

	t.Run("sub", func(t *testing.T) {
		h := mkHarness(t)
		assert.NoError(h.t, h.conn.Subscribe(h.ctx, "foo"))

		// first pull in the response from the SUBSCRIBE call
		h.startNext(h.ctx)
		h.assertEvent("next-top", nil)
		h.assertEvent("wrapped-ctx", nil)
		h.assertEvent("decode-returned", nil)
		h.assertEvent("next-top", nil)

		// loop once for fun
		h.assertEvent("wrapped-ctx", func() {
			h.clock.Add(pubSubCtxWrapTimeout)
		})
		h.assertEvent("decode-returned", nil)
		h.assertEvent("next-top", nil)

		// now publish and read that
		msg := PubSubMessage{Type: "message", Channel: "foo", Message: []byte("bar")}
		h.pubCh <- msg
		h.assertEvent("wrapped-ctx", nil)
		h.assertEvent("decode-returned", nil)
		h.assertNext(msg, nil)
	})
}

func TestPubSub2(t *testing.T) {
	stdPubSubTests(t, newPubSubTestHarness)
}
