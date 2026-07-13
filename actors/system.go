package actors

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dhamidi/statecharts"
)

// Option configures a System built by NewSystem.
type Option func(*systemConfig)

type systemConfig struct {
	nodeName       string
	log            statecharts.Log
	snapshots      statecharts.SnapshotStore
	idleTimeout    time.Duration
	residencyLimit func(resident int) bool
	clock          statecharts.Clock
	onSweepError   func(name statecharts.Identifier, err error)
	fallback       statecharts.IOProcessor
}

// WithNodeName labels a System for diagnostics -- which process currently
// has which actors resident shows up in error messages, for logs and
// metrics, not for addressing. An actor's name means the same thing
// regardless of which node's System happens to have it loaded right now.
func WithNodeName(name string) Option {
	return func(c *systemConfig) { c.nodeName = name }
}

// WithLog supplies the Log every Durable actor's messages are appended to
// before they are applied. A System with no Log configured still works --
// Spawn without Durable never touches it -- but Spawn with Durable returns
// an error if either WithLog or WithSnapshotStore is missing.
func WithLog(log statecharts.Log) Option {
	return func(c *systemConfig) { c.log = log }
}

// WithSnapshotStore supplies where a Durable actor's checkpoints are saved
// and loaded from. It is commonly the same value passed to WithLog, since
// *sqllog.Log satisfies both.
func WithSnapshotStore(store statecharts.SnapshotStore) Option {
	return func(c *systemConfig) { c.snapshots = store }
}

// WithIdleTimeout sets how long a durable actor may sit resident without
// receiving a message before the system checkpoints and pages it out. Zero
// disables idle-based paging. A residency limit, if configured, can still
// force eviction regardless of idle time. Defaults to five minutes.
func WithIdleTimeout(d time.Duration) Option {
	return func(c *systemConfig) { c.idleTimeout = d }
}

// WithResidencyLimit gives the system a predicate to consult, with the
// current resident-actor count, before admitting a new activation (a Spawn
// or a page-in triggered by routing). fn returning true means make room
// first: the system evicts durable resident actors, least-recently-active
// first, until fn returns false or nothing evictable is left. If nothing
// evictable is left and fn still returns true, the activation fails rather
// than silently exceeding budget.
func WithResidencyLimit(fn func(resident int) bool) Option {
	return func(c *systemConfig) { c.residencyLimit = fn }
}

// WithMaxResident caps the number of simultaneously resident actors at n.
// It is sugar for WithResidencyLimit(func(resident int) bool { return
// resident >= n }).
func WithMaxResident(n int) Option {
	return WithResidencyLimit(func(resident int) bool { return resident >= n })
}

// WithClock sets the Clock a System uses for idle-timeout bookkeeping and
// for every Instance it spawns. Defaults to statecharts.NewRealClock().
// Tests can supply a statecharts.ManualClock so idle-timeout eviction is
// triggered deterministically by Advance instead of by sleeping on a real
// timer.
func WithClock(clk statecharts.Clock) Option {
	return func(c *systemConfig) { c.clock = clk }
}

// WithOnSweepError registers a callback invoked whenever an idle-timeout
// sweep fails to evict a candidate actor -- e.g. Snapshot, LastSeq, or
// SnapshotStore.Save returning an error. A sweep runs on its own timer
// goroutine with no caller to return an error to, so without this option
// such a failure is silent: paging simply does not happen for that actor
// this round, with nothing logged. fn is called with the actor's name and
// the error that aborted its eviction. A single failing actor's checkpoint
// error never halts sweeping of the others. nil, the default, leaves
// sweep failures silent.
func WithOnSweepError(fn func(name statecharts.Identifier, err error)) Option {
	return func(c *systemConfig) { c.onSweepError = fn }
}

// WithFallback gives a System an IOProcessor to try for a Send target that
// isn't a name the System itself has spawned. A name the System already
// knows -- resident or not -- is always resolved locally first; the
// fallback is only consulted once that lookup misses, and only for that one
// Send call. Without WithFallback, a Send to an unrecognized name is an
// ordinary "unknown actor" error, exactly as if no fallback existed.
//
// This is what lets two independent Systems address each other: an actor in
// one addresses a name that belongs to the other, the local System's own
// routing doesn't recognize it, and the fallback is the one other place left
// to try before giving up. Bridge is a ready-made fallback for exactly this
// case.
func WithFallback(io statecharts.IOProcessor) Option {
	return func(c *systemConfig) { c.fallback = io }
}

func defaultSystemConfig() systemConfig {
	return systemConfig{
		idleTimeout: 5 * time.Minute,
		clock:       statecharts.NewRealClock(),
	}
}

// System is a group of named, addressable actors: instances of registered
// Charts, spawned under a name, optionally durable. See the package doc
// comment for how System, Chart, and Instance correspond to actor system,
// actor definition, and actor.
type System struct {
	cfg systemConfig

	chartsMu sync.Mutex
	charts   map[statecharts.Identifier]*statecharts.Chart

	tableMu sync.Mutex
	table   map[statecharts.Identifier]*actorEntry

	stopped     atomic.Bool
	sweepMu     sync.Mutex
	sweepCancel func() bool

	// asyncWG tracks in-flight asynchronous peer deliveries (see
	// router.go), so Stop can wait for them to finish touching an Instance
	// before returning.
	asyncWG sync.WaitGroup
}

// actorEntry is the system's record of one named actor, whether or not it
// is currently resident. Only (name, kind, durable) survive a page-out;
// instance and lastActive are meaningful only while resident.
type actorEntry struct {
	name    statecharts.Identifier
	kind    statecharts.Identifier
	durable bool

	// mu serializes activation (page-in) and eviction (page-out) for this
	// one name, so at most one live Instance for it ever exists at a time
	// -- Log.Append's gapless-Seq promise assumes exactly one writer per
	// session.
	mu sync.Mutex

	instance   atomic.Pointer[statecharts.Instance]
	lastActive atomic.Int64 // UnixNano
}

// NewSystem builds a System configured by opts. It is ready to Register
// charts and Spawn actors immediately. Idle-timeout paging, if enabled,
// starts running right away.
func NewSystem(opts ...Option) *System {
	cfg := defaultSystemConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	s := &System{
		cfg:    cfg,
		charts: map[statecharts.Identifier]*statecharts.Chart{},
		table:  map[statecharts.Identifier]*actorEntry{},
	}
	s.armSweep()
	return s
}

// Register makes chart's kind (chart.ID()) available to Spawn. Every chart
// a System will ever spawn must be registered before the first Spawn that
// names it: paging an actor back in reconstructs its Instance from the
// registered Chart, since the Go value itself is never persisted.
//
// Register fails if chart has no datamodel factory (see
// statecharts.WithNewDatamodel) -- it could never be paged in without a
// caller present to supply one -- or if a chart with the same ID is already
// registered.
func (s *System) Register(chart *statecharts.Chart) error {
	if _, ok := chart.NewDatamodel(); !ok {
		return fmt.Errorf("actors: Register: chart %q has no datamodel factory (statecharts.WithNewDatamodel)", chart.ID())
	}
	s.chartsMu.Lock()
	defer s.chartsMu.Unlock()
	if _, exists := s.charts[chart.ID()]; exists {
		return fmt.Errorf("actors: Register: chart %q is already registered", chart.ID())
	}
	s.charts[chart.ID()] = chart
	return nil
}

func (s *System) chartFor(kind statecharts.Identifier) (*statecharts.Chart, bool) {
	s.chartsMu.Lock()
	defer s.chartsMu.Unlock()
	c, ok := s.charts[kind]
	return c, ok
}

// SpawnOption configures a Spawn call.
type SpawnOption func(*spawnConfig)

type spawnConfig struct {
	durable bool
}

// Durable makes a spawned actor's messages persist to the system's Log
// before they are applied, so it can be paged out and later paged back in
// -- even in a different process, against the same Log -- resuming exactly
// where it left off. A name's durability is fixed at its first Spawn: a
// name spawned without Durable cannot later be spawned durable, and vice
// versa.
func Durable() SpawnOption {
	return func(c *spawnConfig) { c.durable = true }
}

// Spawn gives an actor a name -- its address within the system -- and
// starts it running under the Chart registered for kind. Spawn is
// idempotent for a name that is already resident: calling it again for the
// same name, kind, and durability is a no-op.
//
// Without Durable, Spawn behaves like statecharts.New plus Start: the actor
// begins in kind's initial configuration and keeps no record of what it
// does. With Durable, Spawn also resumes an actor that already has Log
// history under name, loading its latest checkpoint and replaying whatever
// came after -- one call handles both "start fresh" and "resume", since a
// name with no prior history simply starts fresh.
func (s *System) Spawn(ctx context.Context, name, kind statecharts.Identifier, opts ...SpawnOption) error {
	// Fast, unsynchronized fail-fast path only -- avoids the chart lookup
	// below for the common case of calling Spawn well after Stop. The
	// authoritative check that actually prevents a Spawn/Stop race
	// (entryFor, under tableMu) runs regardless of what this observes.
	if s.stopped.Load() {
		return fmt.Errorf("actors: Spawn: system is stopped")
	}

	var cfg spawnConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	if _, ok := s.chartFor(kind); !ok {
		return fmt.Errorf("actors: Spawn: kind %q was never Registered", kind)
	}
	if cfg.durable && (s.cfg.log == nil || s.cfg.snapshots == nil) {
		return fmt.Errorf("actors: Spawn: Durable() requires WithLog and WithSnapshotStore")
	}

	entry, err := s.entryFor(name, kind, cfg.durable)
	if err != nil {
		return err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()
	return s.activateLocked(ctx, entry)
}

// entryFor returns the table entry for name, creating one on first use. A
// name's kind and durability are fixed by whichever call creates its entry;
// a later Spawn naming a different kind or durability is an error.
//
// The stopped check here, under the same tableMu Stop takes to snapshot the
// table, is what closes the Spawn/Stop TOCTOU: Spawn's "is the system
// stopped, then insert into the table" and Stop's "mark stopped, then
// snapshot the table" are each a single critical section on tableMu, so
// there is no window in which entryFor inserts a name that Stop's snapshot
// has already missed.
func (s *System) entryFor(name, kind statecharts.Identifier, durable bool) (*actorEntry, error) {
	s.tableMu.Lock()
	defer s.tableMu.Unlock()
	if s.stopped.Load() {
		return nil, fmt.Errorf("actors: Spawn: system is stopped")
	}
	if e, ok := s.table[name]; ok {
		if e.kind != kind {
			return nil, fmt.Errorf("actors: %q was spawned as kind %q, not %q", name, e.kind, kind)
		}
		if e.durable != durable {
			return nil, fmt.Errorf("actors: %q durability is fixed at its first Spawn (durable=%v)", name, e.durable)
		}
		return e, nil
	}
	e := &actorEntry{name: name, kind: kind, durable: durable}
	s.table[name] = e
	return e, nil
}

// resolve reports whether name is known to s -- spawned at some point,
// resident or not -- without paging anything in. This is the cheap,
// synchronous check routingProcessor.Send performs to decide whether Send
// itself should fail.
func (s *System) resolve(name statecharts.Identifier) (*actorEntry, bool) {
	s.tableMu.Lock()
	defer s.tableMu.Unlock()
	e, ok := s.table[name]
	return e, ok
}

// activateLocked makes entry resident, paging it in (durable actors, via
// statecharts.Rehydrate) or starting it fresh (non-durable, via
// statecharts.New plus Start), admitting it under the residency limit
// first. Callers must hold entry.mu.
//
// Refusing to activate once s.stopped is set closes the second half of the
// Spawn/Stop TOCTOU (see entryFor's doc comment for the first half): entry
// existing in the table and Stop capturing it in its snapshot is not
// enough on its own, because activation itself (this method) runs after
// entryFor returns, under entry.mu, which Stop's own per-entry loop also
// takes. Without this check, a Spawn or page-in that starts before Stop's
// CompareAndSwap but loses the race for entry.mu to Stop's per-entry pass
// (Stop finds the entry not yet resident, does nothing, moves on) would
// still go on to create a live Instance goroutine after Stop has already
// returned, with nothing left to ever stop it. Since s.stopped only ever
// transitions false->true and never back, and Stop's per-entry loop takes
// the same entry.mu this method's caller already holds, this check and
// Stop's own pass over the same entry can never both miss a
// still-in-flight activation: either this check observes stopped and
// refuses, or it doesn't and Stop's per-entry lock acquisition blocks
// until activation finishes and then finds (and stops) the result.
func (s *System) activateLocked(ctx context.Context, entry *actorEntry) error {
	if entry.instance.Load() != nil {
		return nil
	}
	if s.stopped.Load() {
		return fmt.Errorf("actors: activate %q: system is stopped", entry.name)
	}
	if err := s.admit(ctx, entry); err != nil {
		return err
	}

	chart, ok := s.chartFor(entry.kind)
	if !ok {
		return fmt.Errorf("actors: activate %q: kind %q is not registered", entry.name, entry.kind)
	}
	dm, _ := chart.NewDatamodel()
	proc := newRoutingProcessor(s, entry.name)

	var inst *statecharts.Instance
	var err error
	if entry.durable {
		// WithTimerFiredHook write-ahead-logs a chart's own internally
		// delayed <send>s the moment their timer fires (LoggingTimerFiredHook,
		// log.go) -- the counterpart, for timer-originated messages, to the
		// explicit Log.Append System.deliver performs before every
		// externally-originated message (Tell, peer Send). Without this, a
		// durable actor's self-scheduled sends would never be durable.
		inst, err = statecharts.Rehydrate(ctx, chart, dm, s.cfg.log, s.cfg.snapshots, string(entry.name), proc,
			statecharts.WithClock(s.cfg.clock),
			statecharts.WithTimerFiredHook(statecharts.LoggingTimerFiredHook(s.cfg.log, string(entry.name))),
		)
	} else {
		inst = statecharts.New(chart, dm, statecharts.WithIOProcessor(proc), statecharts.WithClock(s.cfg.clock))
		err = inst.Start(ctx)
	}
	if err != nil {
		return fmt.Errorf("actors: activate %q: %w", entry.name, err)
	}

	entry.instance.Store(inst)
	entry.lastActive.Store(s.cfg.clock.Now().UnixNano())
	return nil
}

// admit makes room for one more resident actor, if the configured residency
// limit says to, by evicting durable resident actors least-recently-active
// first. exclude is the entry being activated -- never itself a candidate,
// since it is not yet resident.
func (s *System) admit(ctx context.Context, exclude *actorEntry) error {
	if s.cfg.residencyLimit == nil {
		return nil
	}
	for s.cfg.residencyLimit(s.residentCount()) {
		victim := s.pickEvictionVictim(exclude)
		if victim == nil {
			return fmt.Errorf("actors: residency limit reached (resident=%d) and no evictable actor available", s.residentCount())
		}
		err := s.evictLocked(ctx, victim)
		victim.mu.Unlock()
		if err != nil {
			return fmt.Errorf("actors: evict %q to make room: %w", victim.name, err)
		}
	}
	return nil
}

func (s *System) residentCount() int {
	s.tableMu.Lock()
	defer s.tableMu.Unlock()
	n := 0
	for _, e := range s.table {
		if e.instance.Load() != nil {
			n++
		}
	}
	return n
}

// pickEvictionVictim returns the least-recently-active durable resident
// actor other than exclude, already locked -- callers must unlock it,
// whether or not they go on to call evictLocked with it. Non-durable actors
// are never returned: they have no Log to rebuild themselves from, so
// evicting one would destroy it rather than hibernate it. An entry whose mu
// is already held elsewhere (itself mid activation or eviction) is skipped
// rather than waited on, so eviction never blocks on unrelated in-flight
// work.
func (s *System) pickEvictionVictim(exclude *actorEntry) *actorEntry {
	s.tableMu.Lock()
	candidates := make([]*actorEntry, 0, len(s.table))
	for _, e := range s.table {
		if e == exclude || !e.durable || e.instance.Load() == nil {
			continue
		}
		candidates = append(candidates, e)
	}
	s.tableMu.Unlock()

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].lastActive.Load() < candidates[j].lastActive.Load()
	})
	for _, e := range candidates {
		if !e.mu.TryLock() {
			continue
		}
		if e.instance.Load() != nil {
			return e
		}
		e.mu.Unlock()
	}
	return nil
}

// evictLocked checkpoints and stops entry's live Instance: Snapshot, pair
// with the Log's current LastSeq into a Checkpoint, SnapshotStore.Save,
// then Instance.Stop -- in that order, so a crash between any two steps
// still leaves a durable, replayable record, and so Snapshot is never
// called on an already-stopped Instance (which would return
// statecharts.ErrInstanceStopped). Callers must hold entry.mu and know
// entry.durable is true.
func (s *System) evictLocked(ctx context.Context, entry *actorEntry) error {
	inst := entry.instance.Load()
	if inst == nil {
		return nil
	}
	snap, err := inst.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("actors: snapshot %q: %w", entry.name, err)
	}
	seq, err := s.cfg.log.LastSeq(ctx, string(entry.name))
	if err != nil {
		return fmt.Errorf("actors: last seq %q: %w", entry.name, err)
	}
	if err := s.cfg.snapshots.Save(ctx, string(entry.name), statecharts.Checkpoint{Snapshot: snap, Seq: seq}); err != nil {
		return fmt.Errorf("actors: save checkpoint %q: %w", entry.name, err)
	}
	if err := inst.Stop(ctx); err != nil {
		return fmt.Errorf("actors: stop %q: %w", entry.name, err)
	}
	entry.instance.Store(nil)
	return nil
}

// deliver is the single choke point every message into the system passes
// through, whether from Tell or from asynchronous peer delivery
// (deliverAsync). It acquires name's live Instance -- paging it in first if
// it names a known but not currently resident durable actor -- write-ahead
// logs ev if name is durable, and only then calls Deliver, all while
// holding entry.mu for name.
//
// Holding entry.mu across this entire sequence, not just the pointer read,
// is what makes delivery and eviction for the same name mutually
// exclusive: an idle-timeout or residency-limit eviction (evictLocked,
// runSweep, admit) cannot Snapshot-and-stop this same entry while a
// delivery to it is in progress, and cannot run between this method's
// Log.Append and its Deliver call either, since both happen under the same
// lock acquisition. That in turn is what keeps Log.Append calls for a
// given name strictly ordered the same as the Deliver calls they precede:
// two concurrent callers targeting the same durable name are serialized
// here, so the write-ahead log's order always matches application order,
// never a race between them.
//
// This blocks its caller's goroutine for an entire macrostep (activation,
// possibly a full replay, plus one Deliver), but never the goroutine of an
// actor's own interpreter: Tell runs on whatever goroutine an application
// called it from, and deliverAsync runs on the system's own per-Send
// goroutine (see router.go) -- neither is the target's own interpreter
// goroutine, so this holds up only the system's bookkeeping for name, not
// any chart's own microstep execution. That is the same non-blocking
// contract IOProcessor.Send itself relies on (see routingProcessor.Send,
// which hands off to deliverAsync precisely so its own return is
// immediate).
func (s *System) deliver(ctx context.Context, name statecharts.Identifier, ev statecharts.Event) error {
	if s.stopped.Load() {
		return fmt.Errorf("actors: %q: system is stopped", name)
	}
	entry, ok := s.resolve(name)
	if !ok {
		return fmt.Errorf("actors: unknown actor %q", name)
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	inst := entry.instance.Load()
	if inst == nil {
		if !entry.durable {
			return fmt.Errorf("actors: %q is not resident (non-durable actors are never paged back in)", name)
		}
		if err := s.activateLocked(ctx, entry); err != nil {
			return err
		}
		inst = entry.instance.Load()
	}

	if entry.durable {
		// Write-ahead: durability's entire premise (log.go's own contract)
		// is that Append happens, and succeeds, before a message's effects
		// are applied -- a crash between the two just means replay
		// reprocesses this entry, which is safe since Instance.Deliver is
		// deterministic given the event. Appending here, rather than only at
		// the next checkpoint, is what keeps a crash from losing every
		// message since the last checkpoint instead of just the one
		// in-flight macrostep.
		if _, err := s.cfg.log.Append(ctx, statecharts.LogEntry{
			SessionID: string(name),
			Kind:      statecharts.KindExternalEvent,
			Timestamp: time.Now().UTC(),
			Event:     ev,
		}); err != nil {
			return fmt.Errorf("actors: append %q: %w", name, err)
		}
	}

	entry.lastActive.Store(s.cfg.clock.Now().UnixNano())
	return inst.Deliver(ctx, ev)
}

// Tell delivers ev to the actor named name, paging it in first if it names
// a durable actor currently paged out. Tell and a chart's own ec.Send
// resolve names identically -- an actor cannot tell whether a message came
// from Tell or from another actor in the system.
func (s *System) Tell(ctx context.Context, name statecharts.Identifier, ev statecharts.Event) error {
	return s.deliver(ctx, name, ev)
}

// deliverAsync acquires target (paging it in if necessary), write-ahead
// logs ev if target is durable, and delivers ev to it, off the sending
// actor's own goroutine (see deliver's own doc comment for why holding
// target's entry.mu across all of that here is safe). A failure discovered
// here -- the target's activation or rehydrate failing, the log append
// failing, or the system having stopped in the meantime -- is reported
// back to origin, the sending actor's own Dispatcher, since there is no
// other route back into that session once its own dispatchNow call has
// already returned.
func (s *System) deliverAsync(ctx context.Context, target statecharts.Identifier, ev statecharts.Event, origin statecharts.Dispatcher) {
	if err := s.deliver(ctx, target, ev); err != nil {
		s.reportDeliveryFailure(ctx, origin, target, err)
	}
}

// reportDeliveryFailure delivers a synthetic error.communication event to
// origin, mirroring the shape the interpreter core itself gives a
// synchronously-discovered dispatch failure (see interpreter.go,
// reportCommError), so a chart handles both the same way regardless of
// whether the failure was discovered during Send or afterward.
func (s *System) reportDeliveryFailure(ctx context.Context, origin statecharts.Dispatcher, target statecharts.Identifier, cause error) {
	if origin == nil {
		return
	}
	ev := statecharts.Event{
		Name:   statecharts.ErrEventCommunication,
		Type:   statecharts.EventExternal,
		Data:   fmt.Errorf("actors: deliver to %q failed: %w", target, cause),
		Origin: target,
	}
	_ = origin.Deliver(ctx, ev)
}

// armSweep schedules the next idle-timeout check, if idle-timeout paging is
// enabled and the system has not been stopped. It uses the system's own
// Clock (see WithClock), so a ManualClock in tests fires this
// deterministically on Advance rather than on a real timer.
func (s *System) armSweep() {
	if s.stopped.Load() || s.cfg.idleTimeout <= 0 {
		return
	}
	cancel := s.cfg.clock.AfterFunc(s.cfg.idleTimeout, s.runSweep)
	s.sweepMu.Lock()
	s.sweepCancel = cancel
	s.sweepMu.Unlock()
}

// runSweep evicts every durable resident actor idle for at least
// idleTimeout, then reschedules itself.
func (s *System) runSweep() {
	if s.stopped.Load() {
		return
	}
	now := s.cfg.clock.Now()

	s.tableMu.Lock()
	var idle []*actorEntry
	for _, e := range s.table {
		if !e.durable || e.instance.Load() == nil {
			continue
		}
		last := time.Unix(0, e.lastActive.Load())
		if now.Sub(last) >= s.cfg.idleTimeout {
			idle = append(idle, e)
		}
	}
	s.tableMu.Unlock()

	for _, e := range idle {
		if !e.mu.TryLock() {
			continue
		}
		if e.instance.Load() != nil {
			if err := s.evictLocked(context.Background(), e); err != nil && s.cfg.onSweepError != nil {
				s.cfg.onSweepError(e.name, err)
			}
		}
		e.mu.Unlock()
	}

	s.armSweep()
}

// Stop stops the entire system. Every durable resident actor is
// checkpointed and stopped (Instance.Snapshot, SnapshotStore.Save,
// Instance.Stop, exactly as idle-timeout eviction does it). Every
// non-durable resident actor is simply stopped, its state lost, since it
// has no Log to rebuild itself from. The idle-timeout sweep is cancelled.
// Stop is idempotent: calling it again is a no-op.
//
// Marking the system stopped and snapshotting the table to iterate over
// happen inside the same tableMu critical section (see entryFor's own doc
// comment) so a Spawn racing this call either completes entirely first --
// its new entry is included in the snapshot below -- or observes stopped
// and never inserts an entry at all; there is no window in which a
// freshly spawned actor's goroutine survives Stop unwatched.
//
// Every entry is stopped from its own goroutine, and the wait for all of
// them is bounded by ctx (see awaitWithContext), not a plain sequential
// loop taking entry.mu directly: System.deliver holds entry.mu across an
// entire, potentially long-running Deliver call, and entry.mu is a plain
// sync.Mutex with no context awareness of its own. A sequential loop
// blocked in e.mu.Lock() for one wedged entry (a target actor's action
// that never returns) would therefore hang Stop(ctx) regardless of ctx's
// deadline, before even reaching the final wait for async deliveries.
// Running each entry's stop attempt on its own goroutine means a wedged
// entry only blocks that one goroutine; Stop itself only waits on all of
// them together, bounded by ctx, and returns ctx.Err() if it fires first
// while those goroutines keep trying in the background -- the same "still
// make a best effort" contract as the final asyncWG wait.
func (s *System) Stop(ctx context.Context) error {
	s.tableMu.Lock()
	if !s.stopped.CompareAndSwap(false, true) {
		s.tableMu.Unlock()
		return nil
	}
	entries := make([]*actorEntry, 0, len(s.table))
	for _, e := range s.table {
		entries = append(entries, e)
	}
	s.tableMu.Unlock()

	s.sweepMu.Lock()
	if s.sweepCancel != nil {
		s.sweepCancel()
	}
	s.sweepMu.Unlock()

	var errMu sync.Mutex
	var firstErr error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	var wg sync.WaitGroup
	for _, e := range entries {
		wg.Add(1)
		go func(e *actorEntry) {
			defer wg.Done()
			e.mu.Lock()
			defer e.mu.Unlock()
			inst := e.instance.Load()
			if inst == nil {
				return
			}
			var err error
			if e.durable {
				err = s.evictLocked(ctx, e)
			} else {
				err = inst.Stop(ctx)
				// Only mark this entry gone on confirmed success: inst.Stop
				// already treats redundant/idempotent stops as success
				// (ErrInstanceStopped -> nil), so a non-nil err here is a
				// real failure -- e.g. ctx expiring -- and the Instance's
				// goroutine may still be alive. Clearing the pointer
				// anyway would make the table lie that this actor is
				// gone, and nothing would ever try to stop it again.
				if err == nil {
					e.instance.Store(nil)
				}
			}
			recordErr(err)
		}(e)
	}
	recordErr(awaitWithContext(ctx, &wg))
	recordErr(awaitWithContext(ctx, &s.asyncWG))

	return firstErr
}

// awaitWithContext blocks until wg.Wait() would return or ctx fires,
// whichever comes first, returning ctx.Err() in the latter case. If ctx
// fires first, wg's underlying work is not cancelled -- there is no
// general way to cancel an arbitrary sync.WaitGroup's callers -- it
// simply stops being waited on here, so whatever is still in flight (a
// per-entry stop attempt wedged behind a target actor's own never-
// returning action, an in-flight asynchronous delivery) keeps running to
// completion in the background, best effort, exactly as Stop's own doc
// comment promises.
func awaitWithContext(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
