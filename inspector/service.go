// Package inspector provides a transport-neutral, non-activating view of actor systems.
package inspector

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	statecharts "github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
)

// AuditRecord describes one attempted SendEvent. It intentionally contains no
// event payload.
type AuditRecord struct {
	// Timestamp is when the command was attempted.
	Timestamp time.Time
	// System and ActorID identify the command target.
	System  string
	ActorID actors.ActorID
	// EventName identifies the event without retaining its payload.
	EventName statecharts.Identifier
	// Authorized reports whether authorization succeeded.
	Authorized bool
	// Outcome is one of "denied", "failure", or "success".
	Outcome string
	// Error contains the public command error for denied and failed commands.
	Error string
}

// AuditSink receives command audit records. Its errors and panics are ignored.
type AuditSink func(context.Context, AuditRecord) error

// DiagnosticSink receives non-fatal inspector errors. Its panics are ignored.
type DiagnosticSink func(context.Context, error)

// Option configures a Service. Invalid numeric values and nil clocks are
// normalized to their documented defaults by New.
type Option func(*config)
type config struct {
	authorizer             Authorizer
	redactor               Redactor
	audit                  AuditSink
	diagnostic             DiagnosticSink
	ringSize, sourceBuffer int
	clock                  statecharts.Clock
}

// WithAuthorizer sets the authorization policy. Nil denies all operations.
func WithAuthorizer(a Authorizer) Option { return func(c *config) { c.authorizer = a } }

// WithRedactor sets the policy applied before data leaves the service.
func WithRedactor(r Redactor) Option { return func(c *config) { c.redactor = r } }

// WithAuditSink sets the command audit sink.
func WithAuditSink(a AuditSink) Option { return func(c *config) { c.audit = a } }

// WithDiagnosticSink sets the asynchronous diagnostic sink.
func WithDiagnosticSink(d DiagnosticSink) Option { return func(c *config) { c.diagnostic = d } }

// WithRingSize sets retained records per system. Values below one select 1024.
func WithRingSize(n int) Option { return func(c *config) { c.ringSize = n } }

// WithSourceBuffer sets the actor observation buffer. Values outside the
// actors package's accepted range select its default.
func WithSourceBuffer(n int) Option { return func(c *config) { c.sourceBuffer = n } }

// WithClock sets timestamps' clock. Nil selects a real clock.
func WithClock(clock statecharts.Clock) Option { return func(c *config) { c.clock = clock } }

// Service is an isolated registry and bounded observation broker. It does not
// own or stop registered actor systems.
type Service struct {
	mu      sync.Mutex
	cfg     config
	systems map[string]*registered
	closed  bool
	wg      sync.WaitGroup
}
type registered struct {
	system      *actors.System
	source      *actors.ObservationSubscription
	mu          sync.Mutex
	sequence    uint64
	ring        []StreamRecord
	subscribers map[uint64]*subscriber
	nextSub     uint64
	closed      bool
}

// New constructs a lightweight service. The default ring size is 1024 and
// the default source buffer is actors.DefaultObservationBuffer.
func New(options ...Option) *Service {
	c := config{ringSize: 1024, sourceBuffer: actors.DefaultObservationBuffer, clock: statecharts.NewRealClock()}
	for _, option := range options {
		if option != nil {
			option(&c)
		}
	}
	if c.ringSize < 1 {
		c.ringSize = 1024
	}
	if c.sourceBuffer < 1 || c.sourceBuffer > actors.MaxObservationBuffer {
		c.sourceBuffer = actors.DefaultObservationBuffer
	}
	if c.clock == nil {
		c.clock = statecharts.NewRealClock()
	}
	return &Service{cfg: c, systems: map[string]*registered{}}
}

var (
	// ErrServiceClosed is returned by every operation after Service.Close.
	ErrServiceClosed = errors.New("inspector: service closed")
	// ErrUnknownSystem reports an unregistered system name.
	ErrUnknownSystem = errors.New("inspector: unknown system")
	// ErrDuplicateSystem reports a duplicate registration name.
	ErrDuplicateSystem = errors.New("inspector: duplicate system")
)

// RegisterSystem registers a non-empty unique name and starts draining its
// observation stream.
func (s *Service) RegisterSystem(name string, system *actors.System) error {
	if name == "" {
		return errors.New("inspector: empty system name")
	}
	if system == nil {
		return errors.New("inspector: nil system")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrServiceClosed
	}
	if _, ok := s.systems[name]; ok {
		return fmt.Errorf("%w: %q", ErrDuplicateSystem, name)
	}
	sub, err := system.SubscribeObservations(s.cfg.sourceBuffer)
	if err != nil {
		return err
	}
	r := &registered{system: system, source: sub, subscribers: map[uint64]*subscriber{}}
	s.systems[name] = r
	s.wg.Add(1)
	go s.drain(name, r)
	return nil
}

func (s *Service) authorize(ctx context.Context, op Operation, system string, actor actors.ActorID) error {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return ErrServiceClosed
	}
	if s.cfg.authorizer == nil {
		return ErrUnauthorized
	}
	allowed := false
	func() {
		defer func() { _ = recover() }()
		allowed = s.cfg.authorizer.Authorize(ctx, AuthorizationRequest{op, system, actor}) == nil
	}()
	if !allowed {
		return ErrUnauthorized
	}
	return nil
}
func (s *Service) lookup(name string) (*registered, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrServiceClosed
	}
	r := s.systems[name]
	if r == nil {
		return nil, ErrUnknownSystem
	}
	return r, nil
}

// Systems returns authorized registration names in lexical order.
func (s *Service) Systems(ctx context.Context) ([]string, error) {
	if err := s.authorize(ctx, OperationListSystems, "", ""); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrServiceClosed
	}
	out := make([]string, 0, len(s.systems))
	for n := range s.systems {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

// QueryActors delegates a non-activating directory query to one named system.
func (s *Service) QueryActors(ctx context.Context, system string, q actors.ActorQuery) (actors.ActorPage, error) {
	if err := s.authorize(ctx, OperationListActors, system, ""); err != nil {
		return actors.ActorPage{}, err
	}
	r, e := s.lookup(system)
	if e != nil {
		return actors.ActorPage{}, e
	}
	return r.system.QueryActors(ctx, q)
}

// InspectActor returns metadata and, when resident, a redacted stable inspection.
func (s *Service) InspectActor(ctx context.Context, system string, id actors.ActorID) (actors.ActorInfo, *statecharts.InstanceInspection, error) {
	if err := s.authorize(ctx, OperationReadActor, system, id); err != nil {
		return actors.ActorInfo{}, nil, err
	}
	r, e := s.lookup(system)
	if e != nil {
		return actors.ActorInfo{}, nil, e
	}
	info, in, e := r.system.InspectActor(ctx, id)
	if e != nil || in == nil {
		return info, nil, e
	}
	out, e := s.redactInspection(ctx, system, id, *in)
	if e != nil {
		return info, nil, e
	}
	return info, &out, nil
}

// InspectActorDefinition returns the separately authorized immutable definition view.
func (s *Service) InspectActorDefinition(ctx context.Context, system string, id actors.ActorID) (actors.ActorDefinition, error) {
	if e := s.authorize(ctx, OperationReadDefinition, system, id); e != nil {
		return actors.ActorDefinition{}, e
	}
	r, e := s.lookup(system)
	if e != nil {
		return actors.ActorDefinition{}, e
	}
	return r.system.InspectActorDefinition(ctx, id)
}

// QueryActorHistory returns a redacted page of authoritative durable input history.
func (s *Service) QueryActorHistory(ctx context.Context, system string, id actors.ActorID, q actors.ActorHistoryQuery) (actors.ActorHistoryPage, error) {
	if e := s.authorize(ctx, OperationReadHistory, system, id); e != nil {
		return actors.ActorHistoryPage{}, e
	}
	r, e := s.lookup(system)
	if e != nil {
		return actors.ActorHistoryPage{}, e
	}
	p, e := r.system.QueryActorHistory(ctx, id, q)
	if e != nil {
		return p, e
	}
	for i := range p.Entries {
		v, x := s.redactValue(ctx, system, id, CategoryEvent, p.Entries[i].Event.Data)
		if x != nil {
			return actors.ActorHistoryPage{}, x
		}
		p.Entries[i].Event.Data = v
	}
	return p, nil
}

func (s *Service) redactValue(ctx context.Context, system string, id actors.ActorID, c RedactionCategory, v statecharts.Value) (statecharts.Value, error) {
	if s.cfg.redactor == nil {
		return v.Clone(), nil
	}
	var x statecharts.Value
	var e error
	func() {
		defer func() {
			if recover() != nil {
				e = ErrRedaction
			}
		}()
		x, e = s.cfg.redactor.RedactValue(ctx, RedactionContext{system, id, c}, v.Clone())
	}()
	if e != nil {
		return statecharts.Value{}, fmt.Errorf("%w: %s", ErrRedaction, c)
	}
	return x.Clone(), nil
}
func (s *Service) redactInspection(ctx context.Context, system string, id actors.ActorID, in statecharts.InstanceInspection) (statecharts.InstanceInspection, error) {
	var e error
	in.Datamodel, e = s.redactValue(ctx, system, id, CategoryDatamodel, in.Datamodel)
	if e != nil {
		return statecharts.InstanceInspection{}, e
	}
	for i := range in.InternalQueue {
		in.InternalQueue[i].Data, e = s.redactValue(ctx, system, id, CategoryEvent, in.InternalQueue[i].Data)
		if e != nil {
			return statecharts.InstanceInspection{}, e
		}
	}
	for i := range in.ExternalQueue {
		in.ExternalQueue[i].Data, e = s.redactValue(ctx, system, id, CategoryEvent, in.ExternalQueue[i].Data)
		if e != nil {
			return statecharts.InstanceInspection{}, e
		}
	}
	for i := range in.PendingSends {
		in.PendingSends[i].Event.Data, e = s.redactValue(ctx, system, id, CategoryPendingSend, in.PendingSends[i].Event.Data)
		if e != nil {
			return statecharts.InstanceInspection{}, e
		}
	}
	if s.cfg.redactor != nil {
		for i := range in.ActiveInvokes {
			in.ActiveInvokes[i].Source, e = s.redactText(ctx, RedactionContext{system, id, CategoryInvocation}, in.ActiveInvokes[i].Source)
			if e != nil {
				return statecharts.InstanceInspection{}, e
			}
		}
	}
	return in, nil
}

func (s *Service) redactText(ctx context.Context, rc RedactionContext, text string) (out string, err error) {
	if s.cfg.redactor == nil {
		return text, nil
	}
	defer func() {
		if recover() != nil {
			out, err = "", ErrRedaction
		}
	}()
	out, err = s.cfg.redactor.RedactText(ctx, rc, text)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrRedaction, rc.Category)
	}
	return out, nil
}

// SendEvent validates and sends exactly one cloned external event through System.Tell.
func (s *Service) SendEvent(ctx context.Context, system string, id actors.ActorID, name statecharts.Identifier, data statecharts.Value) (err error) {
	record := AuditRecord{Timestamp: s.cfg.clock.Now().UTC(), System: system, ActorID: id, EventName: name}
	defer func() {
		if errors.Is(err, ErrUnauthorized) {
			record.Outcome = "denied"
			record.Error = err.Error()
		} else if err != nil {
			record.Error = err.Error()
			record.Outcome = "failure"
		} else {
			record.Outcome = "success"
		}
		s.audit(ctx, record)
	}()
	if e := s.authorize(ctx, OperationSendEvent, system, id); e != nil {
		return e
	}
	record.Authorized = true
	if _, e := statecharts.NewIdentifier(string(id)); e != nil {
		return e
	}
	if _, e := statecharts.NewIdentifier(string(name)); e != nil {
		return e
	}
	if _, e := data.Wire(); e != nil {
		return e
	}
	r, e := s.lookup(system)
	if e != nil {
		return e
	}
	return r.system.Tell(ctx, id, statecharts.Event{Name: name, Type: statecharts.EventExternal, Data: data.Clone()})
}
func (s *Service) audit(ctx context.Context, r AuditRecord) {
	if s.cfg.audit == nil {
		return
	}
	defer func() { _ = recover() }()
	_ = s.cfg.audit(ctx, r)
}

// Close stops inspector drains and streams without stopping registered systems.
func (s *Service) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	rs := make([]*registered, 0, len(s.systems))
	for _, r := range s.systems {
		rs = append(rs, r)
	}
	s.systems = map[string]*registered{}
	s.mu.Unlock()
	for _, r := range rs {
		r.source.Close()
	}
	s.wg.Wait()
	for _, r := range rs {
		r.closeSubscribers()
	}
}
