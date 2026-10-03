package agent

import "sync/atomic"

// RunEffectFence stops new side effects after forced cancellation.
type RunEffectFence struct{ closed atomic.Bool }

func (f *RunEffectFence) Close() {
	if f != nil {
		f.closed.Store(true)
	}
}

func (f *RunEffectFence) Closed() bool { return f != nil && f.closed.Load() }
