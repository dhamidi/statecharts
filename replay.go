package statecharts

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"
	"sync/atomic"
	"time"
)

// replayGate wraps a real IOProcessor and a real Logger, suppressing both
// until told to go live -- used by Rehydrate so replaying historical log
// entries never repeats real-world side effects, whether that's genuinely
// external dispatch or a diagnostic Logger write. Delayed-send timers use a
// separate non-firing replayClock until the gate goes live.
type replayGate struct {
	io          IOProcessor
	logger      Logger
	ingressHook func(Event) error
	live        atomic.Bool
}

func (g *replayGate) Attach(d Dispatcher) { g.io.Attach(d) }

func (g *replayGate) Send(ctx context.Context, req SendRequest) error {
	if !g.live.Load() {
		if replay, ok := g.io.(ReplayAwareIOProcessor); ok {
			return replay.ReplaySend(ctx, req)
		}
		return nil
	}
	return g.io.Send(ctx, req)
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

func (g *replayGate) ingress(ev Event) error {
	if g.ingressHook == nil || !g.live.Load() {
		return nil
	}
	return g.ingressHook(ev)
}

// IOProcessors implements IOProcessorDescriber by forwarding straight
// through to the wrapped io, if it implements the interface itself.
// Unlike Send/Cancel/Log, reading an already-advertised address has no
// real-world side effect to suppress during replay, so this is never gated
// behind g.live.
func (g *replayGate) IOProcessors() []IOProcessorInfo {
	d, ok := g.io.(IOProcessorDescriber)
	if !ok {
		return nil
	}
	return d.IOProcessors()
}

func (g *replayGate) goLive() { g.live.Store(true) }

func (g *replayGate) recover(ctx context.Context) error {
	if replay, ok := g.io.(ReplayAwareIOProcessor); ok {
		return replay.Recover(ctx)
	}
	return nil
}

// Rehydrate reconstructs a running Instance for sessionID: it loads the
// latest Checkpoint if one exists, then replays every subsequent Log entry.
// Real I/O, Logger calls, invocation starts, and live timers are suppressed
// until replay catches up. Each entry's Timestamp supplies logical time for
// delayed sends created while applying it, and a KindTimerFired entry consumes
// the corresponding pending send by SendID without logging the fire again.
// Finally, remaining timers are activated against the configured Clock --
// including synchronously applying overdue sends -- and active invocations
// are reconciled before the returned Instance is considered live.
func Rehydrate(ctx context.Context, chart *Chart, datamodel any, log Log, snapshots SnapshotStore, sessionID SessionID, realIO IOProcessor, opts ...Option) (*Instance, error) {
	// Logger and Clock, unlike IOProcessor, have no explicit Rehydrate
	// parameters -- they only arrive via opts. Apply opts to a throwaway
	// config so replay can gate the real Logger and defer the real Clock;
	// opts themselves are still passed to New/Restore below unmodified.
	probe := defaultInstanceConfig()
	for _, opt := range opts {
		opt(&probe)
	}

	from := uint64(1)
	var cp Checkpoint
	hasCheckpoint := false
	cacheMiss := false
	if chart.version != "" {
		var err error
		cp, hasCheckpoint, err = snapshots.Load(ctx, sessionID)
		if errors.Is(err, ErrInvalidSnapshot) {
			hasCheckpoint = false
			cacheMiss = true
		} else if err != nil {
			return nil, fmt.Errorf("statecharts: Rehydrate: load checkpoint: %w", err)
		}
		cacheMiss = !hasCheckpoint
	}
	// Checkpoints are a disposable replay optimization. If their format is
	// from another version, ignore them and rebuild from the authoritative
	// log rather than either interpreting an incompatible snapshot or making
	// an otherwise recoverable session unloadable.
	lastSeq, err := log.LastSeq(ctx, sessionID)
	if err != nil {
		return nil, fmt.Errorf("statecharts: Rehydrate: last sequence: %w", err)
	}
	if hasCheckpoint && (cp.Snapshot.Version != snapshotVersion || cp.Snapshot.ChartVersion != chart.version || cp.Seq > lastSeq) {
		hasCheckpoint = false
		cacheMiss = true
	}
	if hasCheckpoint {
		from = cp.Seq + 1
	}

	// Pull the first entry before Start so initial-entry executable content
	// (when there is no checkpoint) gets the oldest recorded logical time
	// available, rather than hydration's wall-clock time. Every entry resets
	// this clock again immediately before it is applied below.
	next, stop := iter.Pull2(log.Read(ctx, sessionID, from))
	defer stop()
	first, readErr, hasFirst := next()
	if readErr != nil {
		return nil, fmt.Errorf("statecharts: Rehydrate: read log: %w", readErr)
	}

	logicalNow := probe.clock.Now()
	if hasFirst && !first.Timestamp.IsZero() {
		logicalNow = first.Timestamp
	}
	replayTime := newReplayClock(logicalNow)
	// The explicit realIO parameter remains authoritative for SCXML; custom
	// registrations come from opts. Gate every type, not just SCXML.
	probe.processors[0].io = realIO
	var gates []*replayGate
	var gateOpts []Option
	for _, registered := range probe.processors {
		gate := &replayGate{io: registered.io, logger: probe.logger, ingressHook: probe.ingressHook}
		gates = append(gates, gate)
		gateOpts = append(gateOpts, withIOProcessorReplacement(registered.typ, gate))
	}
	gate := gates[0]

	// Appending these options last makes them authoritative during replay,
	// even if opts contained conflicting values.
	allOpts := append(append([]Option{}, opts...), gateOpts...)
	allOpts = append(allOpts, WithLogger(gate), WithClock(replayTime),
		WithIngressHook(gate.ingress), WithSessionID(sessionID))

	var in *Instance
	if hasCheckpoint {
		in, err = Restore(chart, datamodel, cp.Snapshot, allOpts...)
		if err != nil {
			if !errors.Is(err, ErrInvalidSnapshot) {
				return nil, fmt.Errorf("statecharts: Rehydrate: restore checkpoint: %w", err)
			}
			hasCheckpoint = false
			cacheMiss = true
			stop()
			from = 1
			next, stop = iter.Pull2(log.Read(ctx, sessionID, from))
			defer stop()
			first, readErr, hasFirst = next()
			if readErr != nil {
				return nil, fmt.Errorf("statecharts: Rehydrate: read log: %w", readErr)
			}
			if hasFirst && !first.Timestamp.IsZero() {
				replayTime.Set(first.Timestamp)
			}
		}
	}
	if !hasCheckpoint {
		in = New(chart, datamodel, allOpts...)
	}

	// Reconstruct deterministic bookkeeping during bootstrap and replay, but
	// start neither invocations nor timers that could repeat real-world work.
	in.suppressInvoke.Store(true)
	in.deferTimerActivation.Store(true)
	keepInstance := false
	defer func() {
		if !keepInstance {
			_ = in.Stop(ctx)
		}
	}()
	if err := in.Start(ctx); err != nil {
		return nil, fmt.Errorf("statecharts: Rehydrate: start: %w", err)
	}

	replayEntry := func(entry LogEntry) error {
		replayTime.Set(entry.Timestamp)
		switch entry.Kind {
		case KindSessionStarted:
			return nil
		case KindExternalEvent:
			return in.Send(ctx, entry.Event)
		case KindTimerFired:
			req := actorRequest{kind: reqReplayTimerFired, entry: entry, reply: make(chan error, 1)}
			if err := in.submit(ctx, req); err != nil {
				return err
			}
			return in.awaitReply(ctx, req.reply)
		default:
			return fmt.Errorf("unknown log entry kind %q", entry.Kind)
		}
	}

	if hasFirst {
		if err := replayEntry(first); err != nil {
			return nil, fmt.Errorf("statecharts: Rehydrate: replay seq %d: %w", first.Seq, err)
		}
	}
	for {
		entry, readErr, more := next()
		if !more {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("statecharts: Rehydrate: read log: %w", readErr)
		}
		if err := replayEntry(entry); err != nil {
			return nil, fmt.Errorf("statecharts: Rehydrate: replay seq %d: %w", entry.Seq, err)
		}
	}

	// A cache is disposable: refresh a rejected or absent one best-effort at
	// the exact final log boundary, while replay side effects remain gated.
	if cacheMiss && chart.version != "" {
		if snap, snapErr := in.Snapshot(ctx); snapErr == nil {
			_ = snapshots.Save(ctx, sessionID, Checkpoint{Snapshot: snap, Seq: lastSeq})
		}
	}

	in.suppressInvoke.Store(false)
	for _, g := range gates {
		g.goLive()
	}
	for _, g := range gates {
		if err := g.recover(ctx); err != nil {
			return nil, fmt.Errorf("statecharts: Rehydrate: recover outbox: %w", err)
		}
	}

	// Timer activation and invocation reconciliation run together on the
	// Instance's actor goroutine. An overdue timer may exit an invoking state
	// or finish the chart, so timers must settle before invocation resumption
	// and there must be no follow-up request racing a clean actor shutdown.
	finishReq := actorRequest{kind: reqFinishReplay, clock: probe.clock, reply: make(chan error, 1)}
	if err := in.submit(ctx, finishReq); err != nil {
		if errors.Is(err, ErrInstanceStopped) && in.Err() == nil {
			keepInstance = true
			return in, nil
		}
		return nil, fmt.Errorf("statecharts: Rehydrate: finish replay: %w", err)
	}
	if err := in.awaitReply(ctx, finishReq.reply); err != nil {
		if errors.Is(err, ErrInstanceStopped) && in.Err() == nil {
			keepInstance = true
			return in, nil
		}
		return nil, fmt.Errorf("statecharts: Rehydrate: finish replay: %w", err)
	}

	keepInstance = true
	return in, nil
}

// replayClock supplies the historical "now" associated with the LogEntry
// currently being replayed. AfterFunc deliberately never calls f: timer
// elapse is represented by KindTimerFired entries during replay, and any
// still-pending timers are armed against the real configured Clock only once
// replay catches up.
type replayClock struct {
	mu  sync.RWMutex
	now time.Time
}

func newReplayClock(now time.Time) *replayClock {
	return &replayClock{now: now}
}

func (c *replayClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

func (c *replayClock) Set(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

func (*replayClock) AfterFunc(time.Duration, func()) func() bool {
	return func() bool { return false }
}
