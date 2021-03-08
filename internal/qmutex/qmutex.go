// Package qmutex implements a queued, 2-stage mutex
package qmutex

import (
	"context"
)

type unlockBMsg struct {
	canceledCh      <-chan struct{}
	startCh, doneCh chan struct{}
}

// QMutex implements a queued, 2-stage mutex, ie one where a process exclusively
// takes action A, puts itself in a queue, and when it's its turn it exclusively
// takes action B. The process order in which action A is taken is the same as
// the order in which action B is taken.
type QMutex struct {
	unlockACh chan struct{}
	unlockBCh chan unlockBMsg
	stopCh    chan struct{}
}

// New initializes a new QMutex instance. queueSize is used to determine how
// many processes can be blocked waiting for their B action before A actions
// start blocking as well.
func New(queueSize int) *QMutex {
	q := &QMutex{
		unlockACh: make(chan struct{}, 1),
		unlockBCh: make(chan unlockBMsg, queueSize),
		stopCh:    make(chan struct{}),
	}
	q.unlockACh <- struct{}{}

	go q.spin()
	return q
}

// Close cleans up all resources held by QMutex. This should not be called in
// parallel with any other method.
func (q *QMutex) Close() {
	close(q.stopCh)
}

func (q *QMutex) spin() {
	for unlockBMsg := range q.unlockBCh {
		select {
		case <-q.stopCh:
			return
		case unlockBMsg.startCh <- struct{}{}:
		case <-unlockBMsg.canceledCh:
			continue
		}

		<-unlockBMsg.doneCh
	}
}

// Do performs first action A then action B, both exclusively. After performing
// A the process will block while it waits in a queue to perform action B. The
// order of processes in the queue will be the same as the order in which they
// executed their A.
//
// If the A callback returns an error then that is returned and B is not
// performed. If the B callback returns an error then that is returned.
//
// If the context is canceled before A is performed then A will not be
// performed. If it is canceled before B is performed then B will not be
// performed. In either case the context's Err() will be returned.
//
// If the context is canceled _while_ A is being performed then A will finish,
// and if it returns an error that error will be returned. Otherwise the
// context's Err() will be returned and B will not be performed.
//
// If the context is canceled _while_ B is being performed then B will finish
// and its return will be returned from Do.
//
func (q *QMutex) Do(ctx context.Context, a, b func() error) error {
	canceledCh := ctx.Done()
	select {
	case <-q.unlockACh:
	case <-canceledCh:
		return ctx.Err()
	}

	if err := a(); err != nil {
		q.unlockACh <- struct{}{}
		return err
	}

	unlockBMsg := unlockBMsg{
		canceledCh: canceledCh,
		startCh:    make(chan struct{}),
		doneCh:     make(chan struct{}),
	}

	select {
	case q.unlockBCh <- unlockBMsg:
	case <-canceledCh:
		return ctx.Err()
	}

	q.unlockACh <- struct{}{}

	select {
	case <-unlockBMsg.startCh:
	case <-canceledCh:
		return ctx.Err()
	}

	err := b()
	close(unlockBMsg.doneCh)

	return err
}
