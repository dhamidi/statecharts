package statecharts

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// interpretation is the mutable, single-goroutine-owned state of one running
// chart: the active configuration, recorded history, both event queues, and
// the running flag -- exactly the SCXML "global" algorithm state (Appendix
// D), minus the datamodel (owned opaquely by the caller). interpretation
// has no goroutines or channels of its own; Instance is the actor wrapper
// that drives it from a single goroutine, and is also what actually starts
// <invoke>'s external services (see invoke.go's invokeRunnerFunc) for the
// same reason it owns delayed-send timers: spawning goroutines is an actor
// concern, not a core-interpreter one.
type interpretation struct {
	chart         *Chart
	datamodel     any
	sessionID     SessionID  // SCXML 5.10's _sessionid, bound for this session's lifetime
	name          Identifier // SCXML 5.10's _name, bound to chart.ID() for this session's lifetime
	configuration map[*compiledState]bool
	historyValue  map[*compiledState][]*compiledState
	internalQueue []Event
	externalQueue []Event
	running       bool
	lastEvent     Event
	hasLastEvent  bool

	io      IOProcessor
	clock   Clock
	logger  Logger
	pending map[Identifier]*pendingSendRecord
	sendSeq int

	// statesToInvoke, activeInvokes, invokesByID, and invokeSeq are the
	// <invoke> bookkeeping (SCXML 6.4): statesToInvoke accumulates states
	// entered during the current macrostep whose invokes haven't started
	// yet (processInvokes starts them once the macrostep is stable, and
	// exitState removes a state again if it's exited first -- so a state
	// entered and exited within one macrostep is never invoked at all);
	// activeInvokes and invokesByID index the ones actually running, by
	// owning state (for cancellation on exit) and by ID (for routing
	// <finalize> and "#_<invokeid>" sends).
	statesToInvoke map[*compiledState]bool
	activeInvokes  map[*compiledState][]*runningInvoke
	invokesByID    map[Identifier]*runningInvoke
	invokeSeq      int
	startInvoke    invokeRunnerFunc

	// restored is set by restoreFrom (snapshot.go) to tell Instance.run to
	// skip the normal start() bootstrap -- the configuration/queues/history
	// are already populated from a Snapshot.
	restored bool

	// timerFiredHook, if set, is called synchronously (always on this
	// interpretation's single owning goroutine) immediately before a fired
	// delayed-send's event is applied. It is the seam a Log implementation
	// uses to satisfy the write-ahead requirement for timer-fired events --
	// the one kind of inbound message with no explicit Instance.Send call
	// site for an application to hook itself (see log.go,
	// LoggingTimerFiredHook). A non-nil error aborts the instance; the
	// error is picked up via hookErr by Instance.run.
	timerFiredHook func(sendID Identifier, ev Event) error
	hookErr        error
}

// pendingSendRecord is the interpreter-core-owned bookkeeping for one
// outstanding delayed <send>. It lives here, not inside whatever
// IOProcessor is plugged in, so Snapshot can capture it regardless of which
// IOProcessor implementation is in use.
type pendingSendRecord struct {
	sendID Identifier
	target Identifier
	typ    Identifier
	event  Event
	fireAt time.Time
	stop   func() bool
}

func newInterpretation(chart *Chart, datamodel any) *interpretation {
	return &interpretation{
		chart:          chart,
		datamodel:      datamodel,
		configuration:  map[*compiledState]bool{},
		historyValue:   map[*compiledState][]*compiledState{},
		pending:        map[Identifier]*pendingSendRecord{},
		io:             NoopIOProcessor,
		clock:          NewRealClock(),
		logger:         NoopLogger,
		statesToInvoke: map[*compiledState]bool{},
		activeInvokes:  map[*compiledState][]*runningInvoke{},
		invokesByID:    map[Identifier]*runningInvoke{},
		startInvoke:    noopInvokeRunner,
	}
}

func isAtomicKind(s *compiledState) bool {
	return s.kind == KindAtomic || s.kind == KindFinal
}

func realChildren(s *compiledState) []*compiledState {
	var result []*compiledState
	for _, c := range s.children {
		if c.kind != KindHistory {
			result = append(result, c)
		}
	}
	return result
}

// properAncestors returns state's ancestors, nearest first, stopping before
// (not including) stop. stop == nil walks all the way to the chart root.
func properAncestors(state, stop *compiledState) []*compiledState {
	var result []*compiledState
	for p := state.parent; p != nil && p != stop; p = p.parent {
		result = append(result, p)
	}
	return result
}

// isDescendant reports whether ancestor is a proper ancestor of s.
func isDescendant(s, ancestor *compiledState) bool {
	if ancestor == nil {
		return false
	}
	for p := s.parent; p != nil; p = p.parent {
		if p == ancestor {
			return true
		}
	}
	return false
}

func isCompoundOrRoot(s, root *compiledState) bool {
	return s.kind == KindCompound || s == root
}

func sortAsc(set map[*compiledState]bool) []*compiledState {
	result := make([]*compiledState, 0, len(set))
	for s := range set {
		result = append(result, s)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].docOrder < result[j].docOrder })
	return result
}

func sortDesc(set map[*compiledState]bool) []*compiledState {
	result := sortAsc(set)
	sort.Slice(result, func(i, j int) bool { return result[i].docOrder > result[j].docOrder })
	return result
}

// --- queues -----------------------------------------------------------

func (ip *interpretation) enqueueInternal(ev Event) {
	ip.internalQueue = append(ip.internalQueue, ev)
}

func (ip *interpretation) enqueueExternal(ev Event) {
	ip.externalQueue = append(ip.externalQueue, ev)
}

// enqueue routes ev onto the internal or external queue based on ev.Type --
// this is what lets both explicit application Sends and (later, in
// instance.go) fired delayed-send timers be delivered through one call.
func (ip *interpretation) enqueue(ev Event) {
	if ev.Type == EventExternal {
		ip.enqueueExternal(ev)
	} else {
		ip.enqueueInternal(ev)
	}
}

func (ip *interpretation) reportError(err error) {
	ip.enqueueInternal(Event{Name: ErrEventExecution, Type: EventPlatform, Data: err})
}

func (ip *interpretation) reportCommError(err error) {
	ip.enqueueInternal(Event{Name: ErrEventCommunication, Type: EventPlatform, Data: err})
}

// --- <send> / <cancel> (SCXML 6.2, 6.3) ---------------------------------
//
// Delay timer bookkeeping lives here, in the interpreter core, rather than
// inside whatever IOProcessor is plugged in. Only genuinely-external
// dispatch (a non-empty Target other than "#_internal") is ever routed to
// IOProcessor.Send, and always with the delay already resolved to zero
// (immediate). "#_internal" and the default/empty target (this session's
// own external queue) are delivered directly, without involving
// IOProcessor at all -- they are not real I/O.

// SendOptions configures a scheduled event, mirroring <send>'s attributes.
type SendOptions struct {
	SendID Identifier // auto-generated if empty
	Target Identifier // "" = own external queue (default); "#_internal"; or an external target
	Type   Identifier // IOProcessor selector, meaningful for external targets only
	Data   any
	Delay  time.Duration // 0 = dispatch immediately
}

func (ip *interpretation) doSend(name Identifier, opts SendOptions) {
	sendID := opts.SendID
	if sendID == "" {
		ip.sendSeq++
		sendID = Identifier(fmt.Sprintf("send.%d", ip.sendSeq))
	}
	ev := Event{Name: name, Data: opts.Data, SendID: sendID}

	if opts.Delay <= 0 {
		ip.dispatchNow(sendID, opts.Target, opts.Type, ev)
		return
	}

	rec := &pendingSendRecord{
		sendID: sendID,
		target: opts.Target,
		typ:    opts.Type,
		event:  ev,
		fireAt: ip.clock.Now().Add(opts.Delay),
	}
	ip.pending[sendID] = rec
	rec.stop = ip.clock.AfterFunc(opts.Delay, func() { ip.handleTimerFire(sendID) })
}

func (ip *interpretation) dispatchNow(sendID, target, typ Identifier, ev Event) {
	switch {
	case target == "#_internal":
		ev.Type = EventInternal
		ip.enqueueInternal(ev)
	case target == "":
		ev.Type = EventExternal
		ip.enqueueExternal(ev)
	case strings.HasPrefix(string(target), "#_"):
		// SCXML 6.4.4: "#_<invokeid>" addresses a specific running
		// invocation. An unrecognized invoke ID (already finished, never
		// existed, or belongs to a different session entirely -- there is
		// no "#_parent" here, since this package doesn't model child
		// sessions, see ADR 0005) falls through to the IOProcessor like
		// any other unhandled target, which reports it as a communication
		// error rather than silently dropping it.
		if ri, ok := ip.invokesByID[Identifier(strings.TrimPrefix(string(target), "#_"))]; ok && ri.incoming != nil {
			select {
			case ri.incoming <- ev:
			default: // never block the interpreter on a slow/absent reader
			}
			return
		}
		fallthrough
	default:
		if ip.io == nil {
			ip.reportCommError(fmt.Errorf("statecharts: no IOProcessor configured for send target %q", target))
			return
		}
		req := SendRequest{SendID: sendID, Target: target, Type: typ, Event: ev.Name, Data: ev.Data}
		if err := ip.io.Send(context.Background(), req); err != nil {
			ip.reportCommError(err)
		}
	}
}

// handleTimerFire is what a pending send's timer actually schedules. It
// must only ever run on the goroutine that owns this interpretation (for a
// standalone interpretation under test, that's whichever goroutine calls
// ManualClock.Advance; for an Instance, instance.go's actorClock ensures
// this runs on the actor's own goroutine, never a raw timer goroutine).
// It gives timerFiredHook a chance to run (and veto) before the event is
// actually applied.
func (ip *interpretation) handleTimerFire(sendID Identifier) {
	rec, ok := ip.pending[sendID]
	if !ok {
		return // already cancelled
	}
	if ip.timerFiredHook != nil {
		if err := ip.timerFiredHook(sendID, rec.event); err != nil {
			ip.hookErr = err
			ip.running = false
			return
		}
	}
	ip.fireTimer(sendID)
}

// fireTimer applies a delayed send whose timer has elapsed, unconditionally
// (timerFiredHook has already run, if configured).
func (ip *interpretation) fireTimer(sendID Identifier) {
	rec, ok := ip.pending[sendID]
	if !ok {
		return // already cancelled, or fired twice (shouldn't happen)
	}
	delete(ip.pending, sendID)
	ip.dispatchNow(rec.sendID, rec.target, rec.typ, rec.event)
}

// doCancel best-effort cancels a pending delayed send. Per SCXML, a miss
// (unknown or already-fired sendID) is not an error.
func (ip *interpretation) doCancel(sendID Identifier) {
	rec, ok := ip.pending[sendID]
	if !ok {
		return
	}
	delete(ip.pending, sendID)
	if rec.stop != nil {
		rec.stop()
	}
	if ip.io != nil {
		_ = ip.io.Cancel(context.Background(), sendID)
	}
}

// --- ExecContext plumbing ----------------------------------------------

// doLog is <log>'s implementation: it calls straight through to whichever
// Logger this interpretation was configured with, with no queue and no
// dispatch-failure path -- unlike doSend, a Logger call cannot produce a
// platform error event, since it never reaches an IOProcessor.
func (ip *interpretation) doLog(label string, data any) {
	if ip.logger != nil {
		ip.logger.Log(label, data)
	}
}

// ioProcessors reports the current IOProcessor's advertised addresses, per
// SCXML 5.10's _ioprocessors, if it implements IOProcessorDescriber -- nil
// otherwise (e.g. NoopIOProcessor/LocalIOProcessor, neither of which has a
// transport to advertise an address for).
func (ip *interpretation) ioProcessors() []IOProcessorInfo {
	d, ok := ip.io.(IOProcessorDescriber)
	if !ok {
		return nil
	}
	return d.IOProcessors()
}

func (ip *interpretation) execContext() ExecContext {
	return ExecContext{
		event:     ip.lastEvent,
		hasEvent:  ip.hasLastEvent,
		datamodel: ip.datamodel,
		sessionID: string(ip.sessionID),
		name:      ip.name,
		active: func(id Identifier) bool {
			s := ip.chart.byID[id]
			return s != nil && ip.configuration[s]
		},
		raise:        ip.enqueueInternal,
		send:         ip.doSend,
		cancel:       ip.doCancel,
		log:          ip.doLog,
		ioProcessors: ip.ioProcessors,
	}
}

func (ip *interpretation) runActions(actions []ActionFunc) {
	ec := ip.execContext()
	for _, a := range actions {
		if a == nil {
			continue
		}
		if err := a(ec); err != nil {
			ip.reportError(err)
		}
	}
}

// activeStates returns the current configuration's state IDs in document order.
func (ip *interpretation) activeStates() []Identifier {
	var ids []Identifier
	for _, s := range ip.chart.order {
		if ip.configuration[s] {
			ids = append(ids, s.id)
		}
	}
	return ids
}

// --- entry set computation (SCXML D.2) ---------------------------------

func (ip *interpretation) addDescendantStatesToEnter(state *compiledState, entrySet, forDefault map[*compiledState]bool) {
	if state.kind == KindHistory {
		if recorded, ok := ip.historyValue[state]; ok {
			for _, s := range recorded {
				ip.addDescendantStatesToEnter(s, entrySet, forDefault)
			}
			for _, s := range recorded {
				ip.addAncestorStatesToEnter(s, state.parent, entrySet, forDefault)
			}
		} else if def := ip.chart.byID[state.initial]; def != nil {
			ip.addDescendantStatesToEnter(def, entrySet, forDefault)
			ip.addAncestorStatesToEnter(def, state.parent, entrySet, forDefault)
		}
		return
	}

	entrySet[state] = true
	switch state.kind {
	case KindCompound:
		forDefault[state] = true
		if initial := ip.chart.byID[state.initial]; initial != nil {
			ip.addDescendantStatesToEnter(initial, entrySet, forDefault)
			ip.addAncestorStatesToEnter(initial, state, entrySet, forDefault)
		}
	case KindParallel:
		for _, child := range realChildren(state) {
			ip.addDescendantStatesToEnter(child, entrySet, forDefault)
		}
	}
}

func (ip *interpretation) addAncestorStatesToEnter(state, ancestor *compiledState, entrySet, forDefault map[*compiledState]bool) {
	for _, anc := range properAncestors(state, ancestor) {
		entrySet[anc] = true
		if anc.kind != KindParallel {
			continue
		}
		for _, child := range realChildren(anc) {
			if ip.entrySetCovers(entrySet, child) {
				continue
			}
			ip.addDescendantStatesToEnter(child, entrySet, forDefault)
		}
	}
}

// entrySetCovers reports whether child (or some descendant of it) is
// already present in entrySet, meaning it needs no default entry.
func (ip *interpretation) entrySetCovers(entrySet map[*compiledState]bool, child *compiledState) bool {
	if entrySet[child] {
		return true
	}
	for s := range entrySet {
		if isDescendant(s, child) {
			return true
		}
	}
	return false
}

func (ip *interpretation) effectiveTargetStates(t *compiledTransition) []*compiledState {
	var result []*compiledState
	seen := map[*compiledState]bool{}
	for _, id := range t.target {
		if s := ip.chart.byID[id]; s != nil {
			ip.collectEffectiveTarget(s, &result, seen, 0)
		}
	}
	return result
}

func (ip *interpretation) collectEffectiveTarget(s *compiledState, result *[]*compiledState, seen map[*compiledState]bool, depth int) {
	if depth > 32 {
		return // guard against pathological history reference cycles
	}
	if s.kind == KindHistory {
		if recorded, ok := ip.historyValue[s]; ok {
			for _, r := range recorded {
				if !seen[r] {
					seen[r] = true
					*result = append(*result, r)
				}
			}
		} else if def := ip.chart.byID[s.initial]; def != nil {
			ip.collectEffectiveTarget(def, result, seen, depth+1)
		}
		return
	}
	if !seen[s] {
		seen[s] = true
		*result = append(*result, s)
	}
}

func (ip *interpretation) allDescendants(states []*compiledState, ancestor *compiledState) bool {
	for _, s := range states {
		if !isDescendant(s, ancestor) {
			return false
		}
	}
	return true
}

func (ip *interpretation) findLCCA(states []*compiledState) *compiledState {
	if len(states) == 0 {
		return nil
	}
	head := states[0]
	for _, anc := range properAncestors(head, nil) {
		if !isCompoundOrRoot(anc, ip.chart.root) {
			continue
		}
		ok := true
		for _, s := range states {
			if !isDescendant(s, anc) {
				ok = false
				break
			}
		}
		if ok {
			return anc
		}
	}
	return ip.chart.root
}

func (ip *interpretation) transitionDomain(t *compiledTransition) *compiledState {
	targets := ip.effectiveTargetStates(t)
	if len(targets) == 0 {
		return nil
	}
	if t.internal && t.source.kind == KindCompound && ip.allDescendants(targets, t.source) {
		return t.source
	}
	all := append([]*compiledState{t.source}, targets...)
	return ip.findLCCA(all)
}

func (ip *interpretation) computeEntrySet(transitions []*compiledTransition) ([]*compiledState, map[*compiledState]bool) {
	entrySet := map[*compiledState]bool{}
	forDefault := map[*compiledState]bool{}
	for _, t := range transitions {
		for _, id := range t.target {
			if target := ip.chart.byID[id]; target != nil {
				ip.addDescendantStatesToEnter(target, entrySet, forDefault)
			}
		}
		domain := ip.transitionDomain(t)
		for _, s := range ip.effectiveTargetStates(t) {
			ip.addAncestorStatesToEnter(s, domain, entrySet, forDefault)
		}
	}
	return sortAsc(entrySet), forDefault
}

func (ip *interpretation) computeExitSet(transitions []*compiledTransition) map[*compiledState]bool {
	exitSet := map[*compiledState]bool{}
	for _, t := range transitions {
		if len(t.target) == 0 {
			continue
		}
		domain := ip.transitionDomain(t)
		for s := range ip.configuration {
			if isDescendant(s, domain) {
				exitSet[s] = true
			}
		}
	}
	return exitSet
}

// --- enter / exit --------------------------------------------------------

func (ip *interpretation) isInFinalState(s *compiledState) bool {
	switch s.kind {
	case KindCompound:
		for _, c := range realChildren(s) {
			if c.kind == KindFinal && ip.configuration[c] {
				return true
			}
		}
		return false
	case KindParallel:
		for _, c := range realChildren(s) {
			if !ip.isInFinalState(c) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (ip *interpretation) enterState(s *compiledState, _ bool) {
	ip.configuration[s] = true
	ip.runActions(s.onEntry)

	if len(s.invokes) > 0 {
		// Deferred to the end of the macrostep by processInvokes, per
		// SCXML mainEventLoop -- not started here, so a state entered and
		// exited again within the same macrostep is never invoked.
		ip.statesToInvoke[s] = true
	}

	if s.kind != KindFinal {
		return
	}
	if s.parent == nil || s.parent == ip.chart.root {
		// a top-level final state (root itself is Final, or its direct
		// parent is the chart root) ends the machine.
		ip.running = false
		return
	}
	parent := s.parent

	var data any
	if s.done != nil {
		data = s.done(ip.execContext())
	}
	ip.enqueueInternal(Event{
		Name: Identifier("done.state." + string(parent.id)),
		Type: EventInternal,
		Data: data,
	})

	if grandparent := parent.parent; grandparent != nil && grandparent.kind == KindParallel {
		allDone := true
		for _, c := range realChildren(grandparent) {
			if !ip.isInFinalState(c) {
				allDone = false
				break
			}
		}
		if allDone {
			ip.enqueueInternal(Event{
				Name: Identifier("done.state." + string(grandparent.id)),
				Type: EventInternal,
			})
		}
	}
}

func (ip *interpretation) exitState(s *compiledState) {
	ip.runActions(s.onExit)
	ip.cancelInvokes(s)
	delete(ip.statesToInvoke, s)
	delete(ip.configuration, s)
}

// cancelInvokes stops every invocation currently running on behalf of s --
// SCXML 6.4.2/6.4.3: cancellation "MUST act as if it were the final onexit
// handler in the invoking state", so this runs immediately after s.onExit,
// still as part of exiting s, whether s is being exited by an ordinary
// transition or as part of exitInterpreter's final cleanup.
func (ip *interpretation) cancelInvokes(s *compiledState) {
	if len(ip.activeInvokes[s]) == 0 {
		return
	}
	for _, ri := range ip.activeInvokes[s] {
		delete(ip.invokesByID, ri.id)
		if ri.cancel != nil {
			ri.cancel()
		}
	}
	delete(ip.activeInvokes, s)
}

// processInvokes starts every <invoke> belonging to a state entered since
// the last call, in entry order, then in document order per state --
// SCXML mainEventLoop's "for state in statesToInvoke.sort(entryOrder): for
// inv in state.invoke.sort(documentOrder): invoke(inv)", run once the
// current macrostep has settled (no eventless transitions or internal
// events left to process).
func (ip *interpretation) processInvokes() {
	if len(ip.statesToInvoke) == 0 {
		return
	}
	pending := sortAsc(ip.statesToInvoke)
	ip.statesToInvoke = map[*compiledState]bool{}
	for _, s := range pending {
		for i, spec := range s.invokes {
			ip.beginInvoke(s, i, spec)
		}
	}
}

func (ip *interpretation) beginInvoke(s *compiledState, specIndex int, spec *compiledInvoke) {
	id := spec.id
	if id == "" {
		ip.invokeSeq++
		id = Identifier(fmt.Sprintf("%s.invoke%d", s.id, ip.invokeSeq))
	}
	var params any
	if spec.params != nil {
		params = spec.params(ip.execContext())
	}
	cancel, incoming := ip.startInvoke(id, spec, params)
	ri := &runningInvoke{id: id, state: s, specIndex: specIndex, finalize: spec.finalize, autoForward: spec.autoForward, cancel: cancel, incoming: incoming}
	ip.activeInvokes[s] = append(ip.activeInvokes[s], ri)
	ip.invokesByID[id] = ri
}

// applyInvokeSideEffects runs whichever of two per-invocation side effects
// apply to an external event, for every currently active invocation --
// SCXML mainEventLoop's "for state in configuration: for inv in
// state.invoke: if inv.invokeid == externalEvent.invokeid:
// applyFinalize(...); if inv.autoforward: send(inv.id, externalEvent)",
// run once the event is dequeued, before transitions are selected for it:
//
//   - <finalize> (SCXML 6.5): the invocation whose InvokeID matches ev's
//     gets its finalize content run, so it can normalize returned data
//     before any transition's cond inspects it.
//   - autoforward (SCXML 6.4.1): every invocation configured with it gets
//     an exact copy of ev on its Incoming channel, regardless of ev's own
//     InvokeID -- including, potentially, its own, since the spec draws
//     no such exception.
//
// ip.invokesByID only ever holds invocations belonging to a currently
// active state (cancelInvokes removes them the moment their state exits),
// so this is already scoped to "for state in configuration" without
// walking the configuration separately.
func (ip *interpretation) applyInvokeSideEffects(ev Event) {
	if len(ip.invokesByID) == 0 {
		return
	}
	for _, id := range sortedInvokeIDs(ip.invokesByID) {
		ri := ip.invokesByID[id]
		if ev.InvokeID != "" && ri.id == ev.InvokeID {
			ip.runActions(ri.finalize)
		}
		if ri.autoForward && ri.incoming != nil {
			select {
			case ri.incoming <- ev:
			default: // never block the interpreter on a slow/absent reader
			}
		}
	}
}

// sortedInvokeIDs returns m's keys in a deterministic order, so
// applyInvokeSideEffects's side effects (running <finalize>, forwarding to
// autoforwarding invocations) happen in a repeatable sequence across runs
// rather than following Go's randomized map iteration.
func sortedInvokeIDs(m map[Identifier]*runningInvoke) []Identifier {
	ids := make([]Identifier, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// exitInterpreter runs whenever running has just become false -- because a
// top-level final state was entered, or because the caller cancelled
// processing (Instance.Stop) -- and exits every state still left in the
// configuration, in exit order, running each one's onexit handlers. This is
// SCXML Appendix D's exitInterpreter() procedure: reaching a stable point
// with running=false is not itself the end of processing, since states
// other than the one whose entry flipped running (ancestors, and siblings
// in an active parallel region) are typically still in the configuration
// and their onexit content has never run. Calling this on an
// already-empty configuration (e.g. a second, redundant call) is a no-op.
func (ip *interpretation) exitInterpreter() {
	for _, s := range sortDesc(ip.configuration) {
		ip.exitState(s)
	}
}

func (ip *interpretation) enterStates(transitions []*compiledTransition) {
	ordered, forDefault := ip.computeEntrySet(transitions)
	for _, s := range ordered {
		ip.enterState(s, forDefault[s])
	}
}

func (ip *interpretation) exitStates(transitions []*compiledTransition) {
	exitSet := ip.computeExitSet(transitions)
	ordered := sortDesc(exitSet)

	// Record history for every history pseudostate belonging to an exited
	// state, BEFORE any state is actually removed from the configuration.
	for _, s := range ordered {
		for _, child := range s.children {
			if child.kind != KindHistory {
				continue
			}
			recordedSet := map[*compiledState]bool{}
			if child.historyKind == Deep {
				for cfgState := range ip.configuration {
					if isAtomicKind(cfgState) && isDescendant(cfgState, s) {
						recordedSet[cfgState] = true
					}
				}
			} else {
				for cfgState := range ip.configuration {
					if cfgState.parent == s {
						recordedSet[cfgState] = true
					}
				}
			}
			ip.historyValue[child] = sortAsc(recordedSet)
		}
	}

	for _, s := range ordered {
		ip.exitState(s)
	}
}

// --- transition selection (SCXML D.2 / 3.13) ----------------------------

func (ip *interpretation) atomicStatesInDocOrder() []*compiledState {
	var result []*compiledState
	for _, s := range ip.chart.order {
		if isAtomicKind(s) && ip.configuration[s] {
			result = append(result, s)
		}
	}
	return result
}

func (ip *interpretation) eventMatches(t *compiledTransition, name Identifier) bool {
	for _, d := range t.events {
		if d.Matches(name) {
			return true
		}
	}
	return false
}

func (ip *interpretation) condMatches(t *compiledTransition) bool {
	if t.cond == nil {
		return true
	}
	return t.cond(ip.execContext())
}

func (ip *interpretation) selectTransitions(ev Event) []*compiledTransition {
	var enabled []*compiledTransition
	for _, s := range ip.atomicStatesInDocOrder() {
		chain := append([]*compiledState{s}, properAncestors(s, nil)...)
	branchLoop:
		for _, s2 := range chain {
			for _, t := range s2.transitions {
				if len(t.events) == 0 || !ip.eventMatches(t, ev.Name) || !ip.condMatches(t) {
					continue
				}
				enabled = append(enabled, t)
				break branchLoop
			}
		}
	}
	return ip.removeConflictingTransitions(enabled)
}

func (ip *interpretation) selectEventlessTransitions() []*compiledTransition {
	var enabled []*compiledTransition
	for _, s := range ip.atomicStatesInDocOrder() {
		chain := append([]*compiledState{s}, properAncestors(s, nil)...)
	branchLoop:
		for _, s2 := range chain {
			for _, t := range s2.transitions {
				if len(t.events) != 0 || !ip.condMatches(t) {
					continue
				}
				enabled = append(enabled, t)
				break branchLoop
			}
		}
	}
	return ip.removeConflictingTransitions(enabled)
}

func exitSetsIntersect(a, b map[*compiledState]bool) bool {
	small, big := a, b
	if len(big) < len(small) {
		small, big = big, small
	}
	for s := range small {
		if big[s] {
			return true
		}
	}
	return false
}

func (ip *interpretation) removeConflictingTransitions(enabled []*compiledTransition) []*compiledTransition {
	var filtered []*compiledTransition
	for _, t1 := range enabled {
		preempted := false
		var toRemove []*compiledTransition
		exit1 := ip.computeExitSet([]*compiledTransition{t1})
		for _, t2 := range filtered {
			exit2 := ip.computeExitSet([]*compiledTransition{t2})
			if !exitSetsIntersect(exit1, exit2) {
				continue
			}
			if isDescendant(t1.source, t2.source) {
				toRemove = append(toRemove, t2)
			} else {
				preempted = true
				break
			}
		}
		if preempted {
			continue
		}
		if len(toRemove) > 0 {
			remove := map[*compiledTransition]bool{}
			for _, t := range toRemove {
				remove[t] = true
			}
			kept := filtered[:0:0]
			for _, t := range filtered {
				if !remove[t] {
					kept = append(kept, t)
				}
			}
			filtered = kept
		}
		filtered = append(filtered, t1)
	}
	return filtered
}

// --- microstep / macrostep -----------------------------------------------

func (ip *interpretation) microstep(transitions []*compiledTransition) {
	ip.exitStates(transitions)
	for _, t := range transitions {
		ip.runActions(t.actions)
	}
	ip.enterStates(transitions)
}

// runToStable drains eventless transitions and the internal queue until
// neither yields further progress (a "macrostep" tail, SCXML mainEventLoop).
func (ip *interpretation) runToStable() {
	for ip.running {
		transitions := ip.selectEventlessTransitions()
		if len(transitions) == 0 {
			if len(ip.internalQueue) == 0 {
				// The macrostep is otherwise done: start any invocations
				// for states entered during it (SCXML mainEventLoop's
				// "for state in statesToInvoke... invoke(inv)"), then loop
				// once more in case doing so raised anything -- mirroring
				// "if not internalQueue.isEmpty(): continue" immediately
				// after that step.
				ip.processInvokes()
				if len(ip.internalQueue) == 0 {
					return
				}
				continue
			}
			ev := ip.internalQueue[0]
			ip.internalQueue = ip.internalQueue[1:]
			ip.lastEvent, ip.hasLastEvent = ev, true
			transitions = ip.selectTransitions(ev)
		}
		if len(transitions) > 0 {
			ip.microstep(transitions)
		}
	}
}

// start enters the chart's initial configuration and runs to the first
// stable point (interpret(), minus datamodel/global-script concerns).
func (ip *interpretation) start() {
	entrySet := map[*compiledState]bool{}
	forDefault := map[*compiledState]bool{}
	root := ip.chart.root
	switch root.kind {
	case KindCompound:
		if initial := ip.chart.byID[root.initial]; initial != nil {
			ip.addDescendantStatesToEnter(initial, entrySet, forDefault)
			ip.addAncestorStatesToEnter(initial, root, entrySet, forDefault)
		}
	case KindParallel:
		for _, child := range realChildren(root) {
			ip.addDescendantStatesToEnter(child, entrySet, forDefault)
		}
	default:
		entrySet[root] = true
	}

	ip.running = true
	for _, s := range sortAsc(entrySet) {
		ip.enterState(s, forDefault[s])
	}
	ip.runToStable()
}

// processNextExternal dequeues and processes exactly one external event
// (one SCXML "macrostep"), then drains to stability again. It returns false
// if there was nothing to process (the interpretation is stopped, or the
// external queue is empty -- the caller must enqueue more and/or stop).
func (ip *interpretation) processNextExternal() bool {
	if !ip.running || len(ip.externalQueue) == 0 {
		return false
	}
	ev := ip.externalQueue[0]
	ip.externalQueue = ip.externalQueue[1:]
	ip.lastEvent, ip.hasLastEvent = ev, true
	ip.applyInvokeSideEffects(ev)

	transitions := ip.selectTransitions(ev)
	if len(transitions) > 0 {
		ip.microstep(transitions)
	}
	ip.runToStable()
	return true
}
