package completionhooks

import (
	"slices"
	"sync"
)

// CompletionHooks is a mechanism of allowing API handlers to register interest in a particular event, and have that event trigger them when it arrives.
// This is primarily used to implement a long-poll API handler which waits for deployments to complete.
type CompletionHooks[h comparable, T any] struct {

	// MaximumWaitersPerHandle defines the maximum number of old waiters to retain per handle before closing excess waiters. This is used to prevent socket and connection leak or denial of service.
	// Defaults to 0 - disabled.
	MaximumWaitersPerHandle int

	mux     sync.Mutex
	waiters map[h][]chan T
}

// AddWaiter registers a new handler for the event and returns the channel that will be closed when it is triggered. The remove function MUST be called in a defer or other finalizer to make
// sure the handler is cleaned up.
func (h *CompletionHooks[ht, T]) AddWaiter(handle ht) (ch chan T, remove func()) {
	ch = make(chan T)

	h.mux.Lock()
	defer h.mux.Unlock()
	if h.waiters == nil {
		// no waiters! create the map from scratch
		h.waiters = map[ht][]chan T{
			handle: {ch},
		}
	} else if oldWaiters, ok := h.waiters[handle]; ok {
		// waiters already exist, lets make sure close and reject any old ones we no longer need.
		if numOldWaitersToClose := len(oldWaiters) - h.MaximumWaitersPerHandle + 1; h.MaximumWaitersPerHandle > 0 && numOldWaitersToClose > 0 {
			for _, w := range oldWaiters[:numOldWaitersToClose] {
				close(w)
			}
			oldWaiters = slices.Clone(oldWaiters[numOldWaitersToClose:])
		}
		h.waiters[handle] = append(oldWaiters, ch)
	} else {
		h.waiters[handle] = []chan T{ch}
	}

	return ch, func() { h.removeWaiter(handle, ch) }
}

func (h *CompletionHooks[ht, T]) removeWaiter(handle ht, ch chan T) {
	h.mux.Lock()
	defer h.mux.Unlock()
	if waiters, ok := h.waiters[handle]; ok {
		for i, w := range waiters {
			if w == ch {
				h.waiters[handle] = append(waiters[:i], waiters[i+1:]...)
				break
			}
		}
	}
}

// Notify notifies any handlers that may exist for the handle.
func (h *CompletionHooks[ht, T]) Notify(handle ht, msg T) int {
	h.mux.Lock()
	defer h.mux.Unlock()
	if waiters, ok := h.waiters[handle]; ok {
		delete(h.waiters, handle)
		for _, ch := range waiters {
			ch <- msg
			close(ch)
		}
		return len(waiters)
	}
	return 0
}

type DeploymentOrgAndId struct {
	OrgId        string
	DeploymentId string
}

const MaximumWaitersPerDeploymentHandler = 3
