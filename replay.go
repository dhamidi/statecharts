package statecharts

import (
	"context"
	"fmt"
	"sync/atomic"
)

// replayGate wraps a real IOProcessor, suppressing dispatch until told to
// go live -- used by Rehydrate so replaying historical log entries never
// repeats real-world side effects. Because delayed-send timer bookkeeping
// lives in the interpreter core rather than inside whatever IOProcessor is
// plugged in, gating IOProcessor.Send/Cancel alone is sufficient here: no
// separate Clock swap is needed on Instance.
type replayGate struct {
	io   IOProcessor
	live atomic.Bool
}

func (g *replayGate) Attach(d Dispatcher) { g.io.Attach(d) }

func (g *replayGate) Send(ctx context.Context, req SendRequest) error {
	if !g.live.Load() {
		return nil
	}
	return g.io.Send(ctx, req)
}

func (g *replayGate) Cancel(ctx context.Context, sendID Identifier) error {
	if !g.live.Load() {
		return nil
	}
	return g.io.Cancel(ctx, sendID)
}

func (g *replayGate) goLive() { g.live.Store(true) }

// Rehydrate reconstructs a running Instance for sessionID: it loads the
// latest Checkpoint if one exists (skipping replay from sequence 0),
// Restores from it, then replays every subsequent Log entry through the
// exact same ingress call any live caller would use, Instance.Send -- there
// is no special replay code path. Real IOProcessor dispatch is suppressed
// until replay catches up; the returned Instance is then left fully live
// (further Sends dispatch for real).
func Rehydrate(ctx context.Context, chart *Chart, datamodel any, log Log, snapshots SnapshotStore, sessionID string, realIO IOProcessor, opts ...Option) (*Instance, error) {
	gate := &replayGate{io: realIO}
	// The gate is appended last so it always wins over any conflicting
	// WithIOProcessor a caller might mistakenly also pass in opts.
	allOpts := append(append([]Option{}, opts...), WithIOProcessor(gate))

	from := uint64(1)
	var in *Instance

	cp, ok, err := snapshots.Load(ctx, sessionID)
	if err != nil {
		return nil, fmt.Errorf("statecharts: Rehydrate: load checkpoint: %w", err)
	}
	if ok {
		in, err = Restore(chart, datamodel, cp.Snapshot, allOpts...)
		if err != nil {
			return nil, fmt.Errorf("statecharts: Rehydrate: restore checkpoint: %w", err)
		}
		from = cp.Seq + 1
	} else {
		in = New(chart, datamodel, allOpts...)
	}

	if err := in.Start(ctx); err != nil {
		return nil, fmt.Errorf("statecharts: Rehydrate: start: %w", err)
	}

	for entry, err := range log.Read(ctx, sessionID, from) {
		if err != nil {
			return nil, fmt.Errorf("statecharts: Rehydrate: read log: %w", err)
		}
		if err := in.Send(ctx, entry.Event); err != nil {
			return nil, fmt.Errorf("statecharts: Rehydrate: replay seq %d: %w", entry.Seq, err)
		}
	}

	gate.goLive()
	return in, nil
}
