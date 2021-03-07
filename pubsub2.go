package radix

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mediocregopher/radix/v4/resp"
	"github.com/tilinna/clock"
)

// PubSubConn wraps an existing Conn to support redis' pubsub system. Unlike
// Conn, a PubSubConn's methods are _not_ thread-safe.
type PubSubConn2 interface {

	// Subscribe subscribes the PubSubConn to the given set of channels.
	Subscribe(ctx context.Context, channels ...string) error

	// Unsubscribe unsubscribes the PubSubConn from the given set of channels.
	Unsubscribe(ctx context.Context, channels ...string) error

	// PSubscribe is like Subscribe, but it subscribes to a set of patterns and
	// not individual channels.
	PSubscribe(ctx context.Context, patterns ...string) error

	// PUnsubscribe is like Unsubscribe, but it unsubscribes from a set of
	// patterns and not individual channels.
	PUnsubscribe(ctx context.Context, patterns ...string) error

	// Ping performs a simple Ping command on the PubSubConn, returning an error
	// if it failed for some reason.
	//
	// Ping will be periodically called by Next in the default PubSubConn
	// implementation.
	Ping(ctx context.Context) error

	// Next blocks until a message is published to the PubSubConn or an error is
	// encountered. If the context is canceled then the resulting error is
	// returned immediately.
	Next(ctx context.Context) (PubSubMessage, error)

	// Close closes the PubSubConn and cleans up all resources it holds.
	Close() error
}

type pubSubConfig struct {
	PubSubConfig
	clock clock.Clock

	testEventCh chan string
}

const pubSubDefaultPingInterval = 5 * time.Second

func (cfg pubSubConfig) withDefaults() pubSubConfig {
	if cfg.PingInterval == -1 {
		cfg.PingInterval = 0
	} else if cfg.PingInterval == 0 {
		cfg.PingInterval = pubSubDefaultPingInterval
	}

	if cfg.clock == nil {
		cfg.clock = clock.Realtime()
	}

	return cfg
}

type pubSubConn2 struct {
	cfg  pubSubConfig
	conn Conn

	subs, psubs map[string]bool
	pingTicker  *clock.Ticker
}

func (cfg pubSubConfig) new(conn Conn) PubSubConn2 {
	c := &pubSubConn2{
		cfg:   cfg.withDefaults(),
		conn:  conn,
		subs:  map[string]bool{},
		psubs: map[string]bool{},
	}

	if c.cfg.PingInterval > 0 {
		c.pingTicker = c.cfg.clock.NewTicker(c.cfg.PingInterval)
	}

	return c
}

func (c *pubSubConn2) Close() error {
	if c.pingTicker != nil {
		c.pingTicker.Stop()
	}

	return c.conn.Close()
}

func (c *pubSubConn2) cmd(cmd string, args ...string) resp.Marshaler {
	return Cmd(nil, cmd, args...).(resp.Marshaler)
}

func (c *pubSubConn2) Subscribe(ctx context.Context, channels ...string) error {
	if err := c.conn.EncodeDecode(ctx, c.cmd("SUBSCRIBE", channels...), nil); err != nil {
		return err
	}

	for _, ch := range channels {
		c.subs[ch] = true
	}
	return nil
}

func (c *pubSubConn2) Unsubscribe(ctx context.Context, channels ...string) error {
	if err := c.conn.EncodeDecode(ctx, c.cmd("UNSUBSCRIBE", channels...), nil); err != nil {
		return err
	}

	for _, ch := range channels {
		delete(c.subs, ch)
	}
	return nil
}

func (c *pubSubConn2) PSubscribe(ctx context.Context, patterns ...string) error {
	if err := c.conn.EncodeDecode(ctx, c.cmd("PSUBSCRIBE", patterns...), nil); err != nil {
		return err
	}

	for _, p := range patterns {
		c.psubs[p] = true
	}
	return nil
}

func (c *pubSubConn2) PUnsubscribe(ctx context.Context, patterns ...string) error {
	if err := c.conn.EncodeDecode(ctx, c.cmd("PUNSUBSCRIBE", patterns...), nil); err != nil {
		return err
	}

	for _, p := range patterns {
		delete(c.psubs, p)
	}
	return nil
}

func (c *pubSubConn2) Ping(ctx context.Context) error {
	return c.conn.EncodeDecode(ctx, c.cmd("PING"), nil)
}

func (c *pubSubConn2) testEvent(event string) {
	if c.cfg.testEventCh != nil {
		c.cfg.testEventCh <- event
		<-c.cfg.testEventCh
	}
}

var nullCtxCancel = context.CancelFunc(func() {})

const pubSubCtxWrapTimeout = 1 * time.Second

func (c *pubSubConn2) wrapNextCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.pingTicker == nil {
		return ctx, nullCtxCancel
	}

	return c.cfg.clock.TimeoutContext(ctx, pubSubCtxWrapTimeout)
}

func (c *pubSubConn2) Next(ctx context.Context) (PubSubMessage, error) {
	for {
		c.testEvent("next-top")

		if c.pingTicker != nil {
			select {
			case <-c.pingTicker.C:
				if err := c.Ping(ctx); err != nil {
					return PubSubMessage{}, fmt.Errorf("calling PING internally: %w", err)
				}
				c.testEvent("pinged")
			default:
			}
		}

		// ctx has the potential to be wrapped so that it will have a 1 second
		// deadline, so that we can loop back up to check the pingTicker now and
		// then.
		innerCtx, cancel := c.wrapNextCtx(ctx)
		c.testEvent("wrapped-ctx")

		var msg PubSubMessage
		err := c.conn.EncodeDecode(innerCtx, nil, &msg)
		cancel()
		c.testEvent("decode-returned")

		if errors.Is(err, errNotPubSubMessage) {
			continue
		} else if ctxErr := ctx.Err(); ctxErr != nil {
			return msg, ctxErr
		} else if errors.Is(err, context.DeadlineExceeded) {
			continue
		} else if err != nil {
			return msg, err
		}

		if msg.Pattern != "" && !c.psubs[msg.Pattern] {
			c.testEvent("skipped-pattern")
			continue
		} else if !c.subs[msg.Channel] {
			c.testEvent("skipped-channel")
			continue
		}

		return msg, nil
	}
}
