package actors

import (
	"context"
	"errors"
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
	dispatchLimit  int
	clock          statecharts.Clock
	logger         statecharts.Logger
	onSweepError   func(name statecharts.Identifier, err error)
	onResidency    func(ResidencyChange)
	fallback       statecharts.IOProcessor
}

// ActorID is a stable actor identity within a System. It is an alias of
// statecharts.Identifier, so IDs may be hierarchical (for example,
// "accounts.invoice-42") and use the same validation and comparison APIs.
// A node name is not part of an ActorID; routing appends it with "@".
type ActorID = statecharts.Identifier

// ResidencyState describes whether an actor is loaded, being reconstructed,
// or available only from durable storage.
type ResidencyState string

const (
	ResidencyPagedOut  ResidencyState = "paged out"
	ResidencyHydrating ResidencyState = "hydrating"
	ResidencyResident  ResidencyState = "resident"
)

// ResidencyChange is emitted when an actor crosses a residency lifecycle
// boundary. ActorID is the stable identity and never includes a node suffix.
type ResidencyChange struct {
	ActorID ActorID
	State   ResidencyState
}

// WithNodeName sets this System's routing location. An actor with ID
// "accounts.invoice-42" on node "host-a" has routing key
// "accounts.invoice-42@host-a". The node does not affect Instance session
// IDs, Log keys, or SnapshotStore keys, so a System can retain its isolated
// durable history when it moves to another host.
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

// WithResidencyObserver registers a synchronous observer for actor residency
// transitions. A paged-in actor reports hydrating before replay begins and
// resident after its Instance is ready; eviction reports paged out. The
// callback must return promptly and must not call methods that activate,
// evict, or stop actors in this System.
func WithResidencyObserver(fn func(ResidencyChange)) Option {
	return func(c *systemConfig) { c.onResidency = fn }
}

// WithDispatchLimit bounds the number of accepted peer deliveries waiting
// for the System's ordered dispatcher. IOProcessor.Send never blocks; once
// the queue is full it returns an error.communication to the sender instead
// of allocating an unbounded goroutine per message. Defaults to 1024.
func WithDispatchLimit(n int) Option {
	return func(c *systemConfig) { c.dispatchLimit = n }
}

// WithClock sets the Clock a System uses for idle-timeout bookkeeping and
// for every Instance it spawns. Defaults to statecharts.NewRealClock().
// Tests can supply a statecharts.ManualClock so idle-timeout eviction is
// triggered deterministically by Advance instead of by sleeping on a real
// timer.
func WithClock(clk statecharts.Clock) Option {
	return func(c *systemConfig) { c.clock = clk }
}

// WithLogger sets the Logger every Instance a System spawns is configured
// with (see statecharts.WithLogger). Defaults to statecharts.NoopLogger.
func WithLogger(l statecharts.Logger) Option {
	return func(c *systemConfig) { c.logger = l }
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

// WithFallback adds an IOProcessor behind the System's actor router. A Send
// with a custom processor type is routed directly to it. For the SCXML and
// "actors" types, a name the System already knows -- resident or not -- is
// resolved locally first and the fallback is consulted only when lookup
// misses. Without WithFallback, a custom type is unsupported and an
// unrecognized actor name is an ordinary "unknown actor" error.
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
		idleTimeout:   5 * time.Minute,
		dispatchLimit: 1024,
		clock:         statecharts.NewRealClock(),
		logger:        statecharts.NoopLogger,
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

	// admissionMu makes the residency check, any required eviction, and the
	// publication of the newly started Instance one atomic admission. Entry
	// locks still serialize lifecycle changes for an individual actor.
	admissionMu sync.Mutex

	stopped     atomic.Bool
	sweepMu     sync.Mutex
	sweepCancel func() bool

	// dispatchMu/dispatchCond protect one bounded FIFO of asynchronous peer
	// work. A single lazy worker preserves send order without allocating a
	// goroutine per message. Bridge uses the source System's same queue, so
	// Stop also waits for accepted cross-System deliveries.
	dispatchMu      sync.Mutex
	dispatchCond    *sync.Cond
	dispatchQueue   []func()
	dispatchRunning bool
	dispatchClosed  bool
	dispatchDone    chan struct{}
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
	s.dispatchCond = sync.NewCond(&s.dispatchMu)
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

// ErrSystemStopped is returned by Spawn and by deliver (and so by Tell and
// deliverAsync, which both call it) once Stop has been called on the
// System, or is running concurrently and wins the race. s.stopped only
// ever transitions false->true, never back, so once a caller observes this
// error no later call for any name will succeed either. Callers that only
// care whether the system is gone, not which specific call observed it,
// can test for this with errors.Is.
var ErrSystemStopped = errors.New("actors: system is stopped")

// ErrKindNotRegistered is returned by Spawn, and by any activation
// (including a page-in triggered by routing) that reaches a kind with no
// matching Register call, when kind names a chart the System has never
// seen. Register it before spawning or delivering to a name of that kind;
// retrying without registering the chart fails the same way every time.
var ErrKindNotRegistered = errors.New("actors: kind is not registered")

// ErrDurabilityUnsupported is returned by Spawn when called with Durable
// against a System missing WithLog, WithSnapshotStore, or both. Configure
// both options when constructing the System, or drop Durable from this
// Spawn call; the error does not depend on anything about the call itself
// that a retry could change.
var ErrDurabilityUnsupported = errors.New("actors: durable spawn requires WithLog and WithSnapshotStore")

// ErrKindMismatch is returned by Spawn when name was already spawned under
// a different kind. A name's kind is fixed by whichever Spawn call first
// creates its entry; every later Spawn for that name must agree. The
// error's message (via Error) reports the name along with the original and
// attempted kind; a caller that only needs to know that a mismatch
// occurred, not the specific values, can test for it with errors.Is.
var ErrKindMismatch = errors.New("actors: name was spawned under a different kind")

// ErrDurabilityMismatch is returned by Spawn when name was already spawned
// with a different durability setting than this call requests. A name's
// durability, like its kind, is fixed by its first Spawn and cannot change
// on a later call for the same name.
var ErrDurabilityMismatch = errors.New("actors: name's durability is fixed by its first Spawn")

// ErrResidencyExhausted is returned by Spawn, and by any activation
// (including a page-in triggered by routing) that finds the residency
// limit (see WithResidencyLimit) still exceeded after evicting every
// durable resident actor eligible for eviction. Unlike ErrKindNotRegistered
// or ErrDurabilityMismatch, this failure is not a property of the call that
// triggered it -- it depends on which other actors happen to be resident at
// the moment, so a caller may reasonably retry it, e.g. after backing off
// or after other actors have gone idle and been paged out on their own.
var ErrResidencyExhausted = errors.New("actors: residency limit reached and no evictable actor available")

// ErrUnknownActor is returned by Tell (via deliver) when name has never been
// Spawned in this System. It is distinct from the error.communication event
// a chart's own Send raises for the same condition (see routingProcessor.Send
// in router.go): that path is internal to the interpreter and never reaches
// Go code as an error value, while ErrUnknownActor is what a direct caller
// of Tell observes and can test for with errors.Is.
var ErrUnknownActor = errors.New("actors: unknown actor")

// ErrInvalidActorID is returned by Spawn when its actor ID is not a valid
// statecharts.Identifier. In particular, "@" belongs only to routing keys;
// pass the stable actor ID to Spawn and use "<actor-id>@<node>" with Tell or
// SendOptions.Target.
var ErrInvalidActorID = errors.New("actors: invalid actor ID")

// Spawn gives an actor its stable ID within the system and
// starts it running under the Chart registered for kind. Spawn is
// idempotent for an ID that is already resident: calling it again for the
// same ID, kind, and durability is a no-op. IDs are Identifiers and may be
// hierarchical; routing locations such as "invoice-42@host-a" belong in
// Tell or SendOptions.Target, not Spawn.
//
// Without Durable, Spawn behaves like statecharts.New plus Start: the actor
// begins in kind's initial configuration and keeps no record of what it
// does. With Durable, Spawn also resumes an actor that already has Log
// history under name, loading its latest checkpoint and replaying whatever
// came after -- one call handles both "start fresh" and "resume", since a
// name with no prior history simply starts fresh.
func (s *System) Spawn(ctx context.Context, name ActorID, kind statecharts.Identifier, opts ...SpawnOption) error {
	// Fast, unsynchronized fail-fast path only -- avoids the chart lookup
	// below for the common case of calling Spawn well after Stop. The
	// authoritative check that actually prevents a Spawn/Stop race
	// (entryFor, under tableMu) runs regardless of what this observes.
	if s.stopped.Load() {
		return fmt.Errorf("actors: Spawn: %w", ErrSystemStopped)
	}
	if _, err := statecharts.NewIdentifier(string(name)); err != nil {
		return fmt.Errorf("actors: Spawn: %q: %v: %w", name, err, ErrInvalidActorID)
	}

	var cfg spawnConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	if _, ok := s.chartFor(kind); !ok {
		return fmt.Errorf("actors: Spawn: kind %q was never Registered: %w", kind, ErrKindNotRegistered)
	}
	if cfg.durable && (s.cfg.log == nil || s.cfg.snapshots == nil) {
		return fmt.Errorf("actors: Spawn: %w", ErrDurabilityUnsupported)
	}

	entry, err := s.entryFor(name, kind, cfg.durable)
	if err != nil {
		return err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()
	return s.activateLocked(ctx, entry)
}

// entryFor returns the table entry for an actor ID, creating one on first
// use. An ID's kind and durability are fixed by whichever call creates its
// entry; a later Spawn naming a different kind or durability is an error.
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
		return nil, fmt.Errorf("actors: Spawn: %w", ErrSystemStopped)
	}
	if e, ok := s.table[name]; ok {
		if e.kind != kind {
			return nil, fmt.Errorf("actors: %q was spawned as kind %q, not %q: %w", name, e.kind, kind, ErrKindMismatch)
		}
		if e.durable != durable {
			return nil, fmt.Errorf("actors: %q durability is fixed at its first Spawn (durable=%v): %w", name, e.durable, ErrDurabilityMismatch)
		}
		return e, nil
	}
	e := &actorEntry{name: name, kind: kind, durable: durable}
	s.table[name] = e
	return e, nil
}

// resolve reports whether an actor ID is known to s -- spawned at some point,
// resident or not -- without paging anything in. This is the cheap,
// synchronous check routingProcessor.Send performs to decide whether Send
// itself should fail.
func (s *System) resolve(name statecharts.Identifier) (*actorEntry, bool) {
	s.tableMu.Lock()
	defer s.tableMu.Unlock()
	e, ok := s.table[name]
	return e, ok
}

func (s *System) address(name statecharts.Identifier) statecharts.Identifier {
	return routingKey(name, s.cfg.nodeName)
}

// resolveTarget accepts both a local actor ID and an ID@node routing key for
// this System, returning the table entry and stable actor ID.
func (s *System) resolveTarget(target statecharts.Identifier) (*actorEntry, statecharts.Identifier, bool) {
	if entry, ok := s.resolve(target); ok {
		return entry, target, true
	}
	actorID, node, addressed := splitRoutingKey(target)
	if !addressed || node != s.cfg.nodeName {
		return nil, "", false
	}
	entry, ok := s.resolve(actorID)
	return entry, actorID, ok
}

// IsResident reports whether target names a known actor that is currently
// loaded in memory. target may be a local actor ID or this System's qualified
// ID@node routing key. Unknown and remotely addressed targets return false.
// IsResident is observational: it never activates or pages in an actor.
func (s *System) IsResident(target statecharts.Identifier) bool {
	entry, _, ok := s.resolveTarget(target)
	return ok && entry.instance.Load() != nil
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
func (s *System) activateLocked(ctx context.Context, entry *actorEntry) (err error) {
	if entry.instance.Load() != nil {
		return nil
	}
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	// Another caller can only have activated this entry before we acquired
	// entry.mu, but retain this check beside the admission boundary so future
	// lifecycle callers cannot accidentally reserve twice.
	if entry.instance.Load() != nil {
		return nil
	}
	if s.stopped.Load() {
		return fmt.Errorf("actors: activate %q: %w", entry.name, ErrSystemStopped)
	}
	s.notifyResidency(entry, ResidencyHydrating)
	defer func() {
		if err != nil {
			s.notifyResidency(entry, ResidencyPagedOut)
		}
	}()
	if err := s.admit(ctx, entry); err != nil {
		return err
	}

	chart, ok := s.chartFor(entry.kind)
	if !ok {
		return fmt.Errorf("actors: activate %q: kind %q is not registered: %w", entry.name, entry.kind, ErrKindNotRegistered)
	}
	dm, _ := chart.NewDatamodel()
	address := s.address(entry.name)
	sessionID := statecharts.SessionID(entry.name)
	proc := newRoutingProcessor(s, address)
	ingressHook := func(ev statecharts.Event) error {
		entry.lastActive.Store(s.cfg.clock.Now().UnixNano())
		if !entry.durable {
			return nil
		}
		if _, err := s.cfg.log.Append(context.Background(), statecharts.LogEntry{
			SessionID: sessionID,
			Kind:      statecharts.KindExternalEvent,
			Timestamp: s.cfg.clock.Now().UTC(),
			Event:     ev,
		}); err != nil {
			return fmt.Errorf("actors: append %q: %w", entry.name, err)
		}
		return nil
	}

	var inst *statecharts.Instance
	if entry.durable {
		// WithTimerFiredDetailsHook write-ahead-logs a chart's own internally
		// delayed <send>s the moment their timer fires
		// (LoggingTimerFiredDetailsHook, log.go) -- the timer-originated
		// counterpart to System.deliver's explicit Log.Append before each
		// externally-originated message (Tell, peer Send). Without this, a
		// durable actor's self-scheduled sends would never be durable.
		instanceOpts := []statecharts.Option{
			statecharts.WithClock(s.cfg.clock),
			statecharts.WithLogger(s.cfg.logger),
			statecharts.WithIngressHook(ingressHook),
			statecharts.WithTimerFiredDetailsHook(statecharts.LoggingTimerFiredDetailsHook(s.cfg.log, sessionID, s.cfg.clock)),
		}
		hasLogEntries := false
		for _, readErr := range s.cfg.log.Read(ctx, sessionID, 1) {
			if readErr != nil {
				return fmt.Errorf("actors: inspect log for %q: %w", entry.name, readErr)
			}
			hasLogEntries = true
			break
		}
		_, hasCheckpoint, loadErr := s.cfg.snapshots.Load(ctx, sessionID)
		if loadErr != nil {
			return fmt.Errorf("actors: inspect checkpoint for %q: %w", entry.name, loadErr)
		}
		if !hasLogEntries && !hasCheckpoint {
			// This is a genuinely new actor, not a reconstruction. Starting it
			// live is what lets initial <invoke> content run. Persist the start
			// boundary first so a crash before the first ordinary message cannot
			// make a later process mistake the actor for new and invoke twice.
			if _, err := s.cfg.log.Append(ctx, statecharts.LogEntry{
				SessionID: sessionID,
				Kind:      statecharts.KindSessionStarted,
				Timestamp: s.cfg.clock.Now().UTC(),
			}); err != nil {
				return fmt.Errorf("actors: record session start for %q: %w", entry.name, err)
			}
			liveOpts := append(instanceOpts,
				statecharts.WithIOProcessor(proc),
				statecharts.WithSessionID(sessionID),
			)
			inst = statecharts.New(chart, dm, liveOpts...)
			err = inst.Start(ctx)
		} else {
			inst, err = statecharts.Rehydrate(ctx, chart, dm, s.cfg.log, s.cfg.snapshots, sessionID, proc, instanceOpts...)
		}
	} else {
		inst = statecharts.New(chart, dm,
			statecharts.WithIOProcessor(proc), statecharts.WithClock(s.cfg.clock),
			statecharts.WithLogger(s.cfg.logger), statecharts.WithIngressHook(ingressHook),
			statecharts.WithSessionID(sessionID))
		err = inst.Start(ctx)
	}
	if err != nil {
		return fmt.Errorf("actors: activate %q: %w", entry.name, err)
	}

	entry.instance.Store(inst)
	entry.lastActive.Store(s.cfg.clock.Now().UnixNano())
	s.notifyResidency(entry, ResidencyResident)
	return nil
}

func (s *System) notifyResidency(entry *actorEntry, state ResidencyState) {
	if s.cfg.onResidency != nil {
		s.cfg.onResidency(ResidencyChange{ActorID: entry.name, State: state})
	}
}

// admit makes room for one more resident actor, if the configured residency
// limit says to, by evicting durable resident actors least-recently-active
// first. exclude is the entry being activated -- never itself a candidate,
// since it is not yet resident.
func (s *System) admit(ctx context.Context, exclude *actorEntry) error {
	// Free, no-selection-heuristic-needed room: an actor that has already
	// reached a top-level final configuration (or terminated with an
	// error) needs no residency-limit pressure to justify evicting it --
	// see reapFinished's own doc comment.
	s.reapFinished()

	if s.cfg.residencyLimit == nil {
		return nil
	}
	for s.cfg.residencyLimit(s.residentCount()) {
		victim := s.pickEvictionVictim(ctx, exclude)
		if victim == nil {
			return fmt.Errorf("actors: residency limit reached (resident=%d): %w", s.residentCount(), ErrResidencyExhausted)
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

// instanceFinished reports, without blocking, whether inst's actor
// goroutine has already exited on its own -- it reached a top-level final
// configuration, was Stopped, or terminated with a fatal error. A chart
// that has finished this way will never do anything else: there is nothing
// left worth holding its Instance (and its datamodel, and its interpreter
// state) resident for.
func instanceFinished(inst *statecharts.Instance) bool {
	select {
	case <-inst.Done():
		return true
	default:
		return false
	}
}

// reapFinished frees every resident actor -- durable or not -- whose
// Instance has already finished (see instanceFinished), immediately
// rather than waiting for idle-timeout or residency pressure to notice.
// Without this, an actor that reaches its own top-level final state stays
// resident (and keeps hogging memory) forever unless something else
// happens to evict it, since nothing else in the system ever asks whether
// a resident actor is actually still doing anything.
//
// Called from admit (opportunistically, before any residency-limit
// decision) and from runSweep (once a sweep round runs at all); the
// overwhelmingly common case -- a chart reaching its final state while
// processing a message -- is instead caught inline by deliver, immediately
// after the Deliver call that (maybe) triggered it, rather than waiting for
// either of those.
func (s *System) reapFinished() {
	s.tableMu.Lock()
	entries := make([]*actorEntry, 0, len(s.table))
	for _, e := range s.table {
		entries = append(entries, e)
	}
	s.tableMu.Unlock()

	for _, e := range entries {
		if !e.mu.TryLock() {
			continue
		}
		if inst := e.instance.Load(); inst != nil && instanceFinished(inst) {
			_ = s.evictLocked(context.Background(), e)
		}
		e.mu.Unlock()
	}
}

// pickEvictionVictim returns the least-recently-active durable resident
// actor other than exclude, already locked -- callers must unlock it,
// whether or not they go on to call evictLocked with it. Non-durable actors
// are never returned: they have no Log to rebuild themselves from, so
// evicting one would destroy it rather than hibernate it. An entry whose mu
// is already held elsewhere (itself mid activation or eviction) is skipped
// rather than waited on, so eviction never blocks on unrelated in-flight
// work. An entry with at least one active <invoke> is also skipped, never
// picked: Rehydrate cannot resume a real invocation (ADR 0010), so paging
// one out would either strand it running orphaned or -- once Rehydrate's
// own mid-invoke error.communication synthesis is in play -- force it into
// recovery for no reason other than freeing up residency, rather than
// because it actually went idle.
func (s *System) pickEvictionVictim(ctx context.Context, exclude *actorEntry) *actorEntry {
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
		inst := e.instance.Load()
		if inst == nil {
			e.mu.Unlock()
			continue
		}
		if hasInvokes, err := inst.HasActiveInvokes(ctx); err != nil || hasInvokes {
			e.mu.Unlock()
			continue
		}
		return e
	}
	return nil
}

// evictLocked checkpoints and stops entry's live Instance by calling
// Instance.Checkpoint. Its persistence callback runs while the actor is
// paused, so LastSeq and SnapshotStore.Save see the exact same boundary and
// no timer can append between snapshot capture and sequence capture. A
// failed callback leaves the Instance running. Callers must hold entry.mu;
// for a still-running entry, callers must also know entry.durable is true (the
// snapshot/log path below has no meaning for a non-durable actor, which
// has no Log to check it against). An entry whose Instance has already
// finished on its own (instanceFinished) is freed the same way regardless
// of durability, skipping Snapshot/Log entirely: there is nothing left to
// capture that isn't already durably reflected in the Log that led here,
// and Instance.Snapshot only works against a still-live actor goroutine
// anyway.
func (s *System) evictLocked(ctx context.Context, entry *actorEntry) error {
	inst := entry.instance.Load()
	if inst == nil {
		return nil
	}
	if instanceFinished(inst) {
		_ = inst.Stop(ctx)
		entry.instance.Store(nil)
		s.notifyResidency(entry, ResidencyPagedOut)
		return nil
	}
	err := inst.Checkpoint(ctx, func(snap statecharts.Snapshot) error {
		sessionID := statecharts.SessionID(entry.name)
		seq, err := s.cfg.log.LastSeq(ctx, sessionID)
		if err != nil {
			return fmt.Errorf("actors: last seq %q: %w", entry.name, err)
		}
		if err := s.cfg.snapshots.Save(ctx, sessionID, statecharts.Checkpoint{Snapshot: snap, Seq: seq}); err != nil {
			return fmt.Errorf("actors: save checkpoint %q: %w", entry.name, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("actors: checkpoint %q: %w", entry.name, err)
	}
	entry.instance.Store(nil)
	s.notifyResidency(entry, ResidencyPagedOut)
	return nil
}

// deliver is the single choke point every message into the system passes
// through, whether from Tell or from asynchronous peer delivery
// (deliverAsync). It acquires name's live Instance -- paging it in first if
// it names a known but not currently resident durable actor -- write-ahead
// calls Deliver, all while holding entry.mu for name. A durable Instance's
// ingress hook appends ev immediately before applying it on the actor
// goroutine, so invocation results use this same write-ahead boundary.
//
// Holding entry.mu across this entire sequence, not just the pointer read,
// is what makes delivery and eviction for the same name mutually
// exclusive: an idle-timeout or residency-limit eviction (evictLocked,
// runSweep, admit) cannot Snapshot-and-stop this same entry while a
// delivery to it is in progress, and cannot run between this method's
// Deliver call. Two concurrent callers targeting the same durable name are
// serialized here, and the Instance actor goroutine serializes every other
// ingress source, so write-ahead log order always matches application order.
//
// This blocks its caller's goroutine for an entire macrostep (activation,
// possibly a full replay, plus one Deliver), but never the goroutine of an
// actor's own interpreter: Tell runs on whatever goroutine an application
// called it from, and deliverAsync runs on the System dispatcher (see
// router.go) -- neither is the target's own interpreter goroutine. That is
// the same non-blocking contract IOProcessor.Send itself relies on.
func (s *System) deliver(ctx context.Context, name statecharts.Identifier, ev statecharts.Event) error {
	if s.stopped.Load() {
		return fmt.Errorf("actors: %q: %w", name, ErrSystemStopped)
	}
	entry, _, ok := s.resolveTarget(name)
	if !ok {
		return fmt.Errorf("actors: unknown actor %q: %w", name, ErrUnknownActor)
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

	err := inst.Deliver(ctx, ev)

	// The overwhelmingly common way an actor reaches its own top-level
	// final state is by processing a message -- catching it right here,
	// still holding entry.mu, frees it immediately rather than leaving it
	// resident until the next admit or sweep round happens to notice (see
	// reapFinished).
	if instanceFinished(inst) {
		_ = s.evictLocked(context.Background(), entry)
	}

	return err
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
func (s *System) deliverAsync(ctx context.Context, target statecharts.Identifier, ev statecharts.Event, origin statecharts.Dispatcher, sendID statecharts.Identifier) {
	if err := s.deliver(ctx, target, ev); err != nil {
		s.reportDeliveryFailure(ctx, origin, sendID, target, err)
	}
}

// reportDeliveryFailure delivers a synthetic error.communication event to
// origin, mirroring the shape the interpreter core itself gives a
// synchronously-discovered dispatch failure (see interpreter.go,
// reportCommError), so a chart handles both the same way regardless of
// whether the failure was discovered during Send or afterward.
func (s *System) reportDeliveryFailure(ctx context.Context, origin statecharts.Dispatcher, sendID, target statecharts.Identifier, cause error) {
	if origin == nil {
		return
	}
	ev := statecharts.Event{
		Name:   statecharts.ErrEventCommunication,
		Type:   statecharts.EventPlatform,
		SendID: sendID,
		Data:   fmt.Errorf("actors: deliver to %q failed: %w", target, cause),
	}
	_ = origin.Deliver(ctx, ev)
}

var errDispatchQueueFull = errors.New("actors: peer dispatch queue is full")

func (s *System) enqueueDispatch(job func()) error {
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	if s.stopped.Load() || s.dispatchClosed {
		return ErrSystemStopped
	}
	if len(s.dispatchQueue) >= s.cfg.dispatchLimit {
		return errDispatchQueueFull
	}
	if !s.dispatchRunning {
		s.dispatchRunning = true
		s.dispatchDone = make(chan struct{})
		go s.runDispatcher()
	}
	s.dispatchQueue = append(s.dispatchQueue, job)
	s.dispatchCond.Signal()
	return nil
}

func (s *System) runDispatcher() {
	defer func() {
		s.dispatchMu.Lock()
		close(s.dispatchDone)
		s.dispatchMu.Unlock()
	}()
	for {
		s.dispatchMu.Lock()
		for len(s.dispatchQueue) == 0 && !s.dispatchClosed {
			s.dispatchCond.Wait()
		}
		if len(s.dispatchQueue) == 0 && s.dispatchClosed {
			s.dispatchMu.Unlock()
			return
		}
		job := s.dispatchQueue[0]
		s.dispatchQueue[0] = nil
		s.dispatchQueue = s.dispatchQueue[1:]
		s.dispatchMu.Unlock()
		job()
	}
}

func (s *System) closeDispatcher() <-chan struct{} {
	s.dispatchMu.Lock()
	s.dispatchClosed = true
	done := s.dispatchDone
	s.dispatchCond.Broadcast()
	s.dispatchMu.Unlock()
	return done
}

func awaitDoneWithContext(ctx context.Context, done <-chan struct{}) error {
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
// idleTimeout, then reschedules itself. It also reaps every resident actor
// (durable or not) that has already finished on its own, regardless of
// idle time -- the periodic-sweep counterpart to deliver's inline check and
// admit's opportunistic one (see reapFinished), for a finished actor that
// nothing else happens to touch again.
func (s *System) runSweep() {
	if s.stopped.Load() {
		return
	}
	s.reapFinished()

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
		inst := e.instance.Load()
		if inst == nil {
			e.mu.Unlock()
			continue
		}
		// An actor with an active <invoke> is left resident regardless of
		// idle time -- see pickEvictionVictim's own doc comment for why
		// paging one out is never safe. A query failure is treated the same
		// way (skip this round rather than risk evicting a possibly-active
		// invocation), reported through the same onSweepError callback as
		// any other sweep failure.
		if hasInvokes, err := inst.HasActiveInvokes(context.Background()); err != nil {
			if s.cfg.onSweepError != nil {
				s.cfg.onSweepError(e.name, err)
			}
			e.mu.Unlock()
			continue
		} else if hasInvokes {
			e.mu.Unlock()
			continue
		}
		if err := s.evictLocked(context.Background(), e); err != nil && s.cfg.onSweepError != nil {
			s.cfg.onSweepError(e.name, err)
		}
		e.mu.Unlock()
	}

	s.armSweep()
}

// Stop stops the entire system. Every durable resident actor is
// checkpointed and stopped (Instance.Checkpoint and SnapshotStore.Save,
// exactly as idle-timeout eviction does it). Every
// non-durable resident actor is simply stopped, its state lost, since it
// has no Log to rebuild itself from. The idle-timeout sweep is cancelled.
// Stop is idempotent after successful cleanup. If a call's context expires,
// a later call retries any entries that remain resident.
//
// Stop returns every actor's teardown error via errors.Join, not just
// whichever one happened to finish first -- one wedged or misbehaving actor
// never hides another's failure. A caller that cares about a specific cause
// should unwrap the result (errors.Is, errors.As, or a type assertion on
// the interface{ Unwrap() []error } an errors.Join result implements)
// rather than assume it names a single error. Stop returns nil once every
// actor tears down cleanly.
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
// while those goroutines keep trying in the background. Accepted peer work
// is likewise drained by the dispatcher, with its wait bounded by ctx.
func (s *System) Stop(ctx context.Context) error {
	s.tableMu.Lock()
	firstStop := s.stopped.CompareAndSwap(false, true)
	entries := make([]*actorEntry, 0, len(s.table))
	for _, e := range s.table {
		entries = append(entries, e)
	}
	s.tableMu.Unlock()

	var dispatchDone <-chan struct{}
	if firstStop {
		dispatchDone = s.closeDispatcher()
		s.sweepMu.Lock()
		if s.sweepCancel != nil {
			s.sweepCancel()
		}
		s.sweepMu.Unlock()
	} else {
		s.dispatchMu.Lock()
		dispatchDone = s.dispatchDone
		s.dispatchMu.Unlock()
	}

	var errMu sync.Mutex
	var errs []error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		errs = append(errs, err)
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
					s.notifyResidency(e, ResidencyPagedOut)
				}
			}
			recordErr(err)
		}(e)
	}
	recordErr(awaitWithContext(ctx, &wg))
	recordErr(awaitDoneWithContext(ctx, dispatchDone))

	return errors.Join(errs...)
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
