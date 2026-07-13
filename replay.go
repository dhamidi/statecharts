package statecharts

import (
	"context"
	"fmt"
	"sync/atomic"
)

// replayGate wraps a real IOProcessor and a real Logger, suppressing both
// until told to go live -- used by Rehydrate so replaying historical log
// entries never repeats real-world side effects, whether that's genuinely
// external dispatch or a diagnostic Logger write. Because delayed-send
// timer bookkeeping lives in the interpreter core rather than inside
// whatever IOProcessor is plugged in, gating IOProcessor.Send/Cancel alone
// is sufficient here: no separate Clock swap is needed on Instance.
type replayGate struct {
	io     IOProcessor
	logger Logger
	live   atomic.Bool
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

// Log implements Logger, suppressing every call until goLive, the same way
// Send and Cancel suppress real dispatch. A nil wrapped Logger -- from
// Rehydrate being called with WithLogger(nil) -- makes Log a permanent
// no-op instead of a nil dereference, matching doLog's own nil-safe
// handling of an unconfigured Logger.
func (g *replayGate) Log(label string, data any) {
	if g.logger == nil || !g.live.Load() {
		return
	}
	g.logger.Log(label, data)
}

func (g *replayGate) goLive() { g.live.Store(true) }

// Rehydrate reconstructs a running Instance for sessionID: it loads the
// latest Checkpoint if one exists (skipping replay from sequence 0),
// Restores from it, then replays every subsequent Log entry through the
// exact same ingress call any live caller would use, Instance.Send -- there
// is no special replay code path. Real IOProcessor dispatch and real Logger
// calls are both suppressed until replay catches up; the returned Instance
// is then left fully live (further Sends dispatch, and Log calls write, for
// real).
func Rehydrate(ctx context.Context, chart *Chart, datamodel any, log Log, snapshots SnapshotStore, sessionID string, realIO IOProcessor, opts ...Option) (*Instance, error) {
	// Logger, unlike IOProcessor, has no explicit Rehydrate parameter -- it
	// only ever arrives via a WithLogger call inside opts, defaulting to
	// NoopLogger otherwise. Apply opts to a throwaway config just to read
	// off the Logger it configures; opts themselves are still passed to
	// New/Restore below exactly once, unmodified.
	probe := defaultInstanceConfig()
	for _, opt := range opts {
		opt(&probe)
	}

	gate := &replayGate{io: realIO, logger: probe.logger}
	// The gate is appended last so it always wins over any conflicting
	// WithIOProcessor/WithLogger a caller might mistakenly also pass in
	// opts. WithSessionID(sessionID) is appended last for a different
	// reason: this call's own sessionID parameter, not whatever a caller
	// might have passed in opts, is the authoritative identity for the
	// resulting Instance.
	allOpts := append(append([]Option{}, opts...), WithIOProcessor(gate), WithLogger(gate), WithSessionID(sessionID))

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
