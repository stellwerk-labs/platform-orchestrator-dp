package completionhooks

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNotifyDoesNotWaitForReceiver(t *testing.T) {
	hooks := new(CompletionHooks[string, int])
	waiter, remove := hooks.AddWaiter("deployment")
	defer remove()

	notified := make(chan int, 1)
	go func() {
		notified <- hooks.Notify("deployment", 42)
	}()

	select {
	case count := <-notified:
		require.Equal(t, 1, count)
	case <-time.After(time.Second):
		t.Fatal("Notify blocked while its waiter had no active receiver")
	}

	value, ok := <-waiter
	require.True(t, ok)
	require.Equal(t, 42, value)
	_, ok = <-waiter
	require.False(t, ok)
}

func TestRemoveWaiterOverlappingNotifyIsSafe(t *testing.T) {
	for range 1_000 {
		hooks := new(CompletionHooks[string, int])
		waiter, remove := hooks.AddWaiter("deployment")
		start := make(chan struct{})
		var group sync.WaitGroup
		group.Add(2)
		var notified int
		go func() {
			defer group.Done()
			<-start
			remove()
		}()
		go func() {
			defer group.Done()
			<-start
			notified = hooks.Notify("deployment", 42)
		}()
		close(start)
		group.Wait()

		switch notified {
		case 0:
			select {
			case <-waiter:
				t.Fatal("removed waiter was notified")
			default:
			}
		case 1:
			require.Equal(t, 42, <-waiter)
			_, ok := <-waiter
			require.False(t, ok)
		default:
			t.Fatalf("unexpected notification count %d", notified)
		}
		remove() // Cleanup remains idempotent after either race outcome.
	}
}

func TestWaiterLimitAndNotifyHaveSingleChannelOwner(t *testing.T) {
	hooks := &CompletionHooks[string, int]{MaximumWaitersPerHandle: 1}
	old, removeOld := hooks.AddWaiter("deployment")
	current, removeCurrent := hooks.AddWaiter("deployment")
	defer removeCurrent()

	_, oldOpen := <-old
	require.False(t, oldOpen)
	removeOld() // The limit already detached and closed this waiter.

	require.Equal(t, 1, hooks.Notify("deployment", 42))
	require.Equal(t, 42, <-current)
	_, currentOpen := <-current
	require.False(t, currentOpen)
	removeCurrent()
}
