package qmutex

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQMutex(t *testing.T) {
	const n = 100
	q := New(n)
	ctx := context.Background()

	aCh, bCh := make(chan int, n), make(chan int, n)
	writeCh := func(ch chan int, i int, label string) func() error {
		return func() error {
			select {
			case ch <- i:
			default:
				return fmt.Errorf("couldn't write %d (label:%q)", i, label)
			}

			time.Sleep(time.Duration(rand.Intn(10000)) * time.Microsecond)
			return nil
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(n)

	exp := map[int]struct{}{}

	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		exp[i] = struct{}{}
		go func(i int) {
			errCh <- q.Do(ctx, writeCh(aCh, i, "a"), writeCh(bCh, i, "b"))
			wg.Done()
		}(i)
	}
	wg.Wait()

	assert.Equal(t, n, len(aCh), "len(aCh)")
	assert.Equal(t, n, len(bCh), "len(bCh)")
	for len(aCh) != 0 && len(bCh) != 0 {
		a, b := <-aCh, <-bCh
		assert.Equal(t, a, b)
		assert.Contains(t, exp, a)
		delete(exp, a)
	}

	assert.Equal(t, map[int]struct{}{}, exp)

	for len(errCh) > 0 {
		assert.NoError(t, <-errCh)
	}
}
