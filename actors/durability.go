package actors

import (
	"context"
	"errors"
	"sync"

	"github.com/dhamidi/statecharts"
)

// durableProcessor is one actor's outbox boundary. A separate wrapper is
// used for every registered processor type so no side effect can bypass the
// actor's durable intent log.
type durableProcessor struct {
	storage          statecharts.DurableLog
	sid              statecharts.SessionID
	typ              statecharts.Identifier
	io               statecharts.IOProcessor
	reportDispatcher statecharts.Dispatcher
	recovery         *durableRecovery

	mu       sync.Mutex
	messages map[statecharts.DeliveryID]statecharts.OutboundMessage
	disp     statecharts.Dispatcher
}

type durableRecovery struct {
	once       sync.Once
	err        error
	messages   []statecharts.OutboundMessage
	processors map[statecharts.Identifier]*durableProcessor
}

func newDurableRecovery(messages []statecharts.OutboundMessage) *durableRecovery {
	return &durableRecovery{messages: messages, processors: make(map[statecharts.Identifier]*durableProcessor)}
}

func newDurableProcessor(storage statecharts.DurableLog, sid statecharts.SessionID, typ statecharts.Identifier, io statecharts.IOProcessor, reportDispatcher statecharts.Dispatcher, recovery *durableRecovery) *durableProcessor {
	p := &durableProcessor{storage: storage, sid: sid, typ: typ, io: io, reportDispatcher: reportDispatcher, recovery: recovery, messages: make(map[statecharts.DeliveryID]statecharts.OutboundMessage, len(recovery.messages))}
	for _, m := range recovery.messages {
		if m.Request.Type == typ {
			p.messages[m.DeliveryID] = m
		}
	}
	recovery.processors[typ] = p
	return p
}

func (p *durableProcessor) Attach(d statecharts.Dispatcher) { p.disp = d; p.io.Attach(d) }

func (p *durableProcessor) IOProcessors() []statecharts.IOProcessorInfo {
	if d, ok := p.io.(statecharts.IOProcessorDescriber); ok {
		return d.IOProcessors()
	}
	return nil
}

func (p *durableProcessor) Send(ctx context.Context, req statecharts.SendRequest) error {
	req.Data = req.Data.Clone()
	m := statecharts.OutboundMessage{SessionID: p.sid, DeliveryID: req.DeliveryID, Request: req, Status: statecharts.OutboundPending}
	// Persist the canonical intent before the transport call.
	if err := p.storage.StoreOutbound(ctx, m); err != nil {
		return err
	}
	p.mu.Lock()
	p.messages[req.DeliveryID] = m
	p.mu.Unlock()

	if ack, ok := p.io.(statecharts.AcknowledgingIOProcessor); ok {
		err := ack.SendWithAck(ctx, req, func(err error) { p.complete(req, err, false, true) })
		if err != nil {
			p.complete(req, err, true, false)
			return err
		}
		return nil
	}
	err := p.io.Send(ctx, req)
	p.complete(req, err, true, false)
	return err
}

func (p *durableProcessor) ReplaySend(_ context.Context, req statecharts.SendRequest) error {
	p.mu.Lock()
	m, ok := p.messages[req.DeliveryID]
	p.mu.Unlock()
	if !ok || m.Status == statecharts.OutboundPending {
		return nil
	}
	if !m.Result.Synchronous || m.Result.Error == "" {
		return nil
	}
	if m.Result.Execution {
		return recordedExecutionError(m.Result.Error)
	}
	return recordedCommunicationError(m.Result.Error)
}

func (p *durableProcessor) Recover(ctx context.Context) error {
	p.recovery.once.Do(func() {
		for _, m := range p.recovery.messages {
			processor := p.recovery.processors[m.Request.Type]
			if processor == nil {
				p.recovery.err = errors.Join(p.recovery.err, ErrDurableIOProcessorUnavailable)
				continue
			}
			if m.Status == statecharts.OutboundResolved {
				if m.Result.Error != "" && !m.Result.Synchronous {
					processor.report(m.Request, m.Result)
				}
				continue
			}
			processor.recoverSend(ctx, m.Request)
		}
	})
	return p.recovery.err
}

func (p *durableProcessor) recoverSend(ctx context.Context, req statecharts.SendRequest) {
	if ack, ok := p.io.(statecharts.AcknowledgingIOProcessor); ok {
		if err := ack.SendWithAck(ctx, req, func(err error) { p.complete(req, err, false, true) }); err != nil {
			p.complete(req, err, false, true)
		}
		return
	}
	err := p.io.Send(ctx, req)
	p.complete(req, err, false, err != nil)
}

func (p *durableProcessor) complete(req statecharts.SendRequest, err error, synchronous, report bool) {
	r := statecharts.OutboundResult{Synchronous: synchronous}
	if err != nil {
		r.Error = err.Error()
		var executionError statecharts.SendExecutionError
		r.Execution = errors.As(err, &executionError)
	}
	if resolveErr := p.storage.ResolveOutbound(context.Background(), p.sid, req.DeliveryID, r); resolveErr != nil {
		return
	}
	p.mu.Lock()
	m := p.messages[req.DeliveryID]
	m.Status = statecharts.OutboundResolved
	m.Result = r
	p.messages[req.DeliveryID] = m
	p.mu.Unlock()
	if report && err != nil {
		p.report(req, r)
	}
}

func (p *durableProcessor) report(req statecharts.SendRequest, result statecharts.OutboundResult) {
	disp := p.reportDispatcher
	if disp == nil {
		disp = p.disp
	}
	if disp == nil {
		return
	}
	name := statecharts.ErrEventCommunication
	if result.Execution {
		name = statecharts.ErrEventExecution
	}
	_ = disp.Deliver(context.Background(), statecharts.Event{
		Name: name, Type: statecharts.EventPlatform, SendID: req.SendID,
		Data: statecharts.PlatformErrorValue(name, errors.New(result.Error)), DeliveryID: statecharts.DeliveryID(string(req.DeliveryID) + ":result"),
	})
}

type actorIngressDispatcher struct {
	sys  *System
	name statecharts.Identifier
}

func (d actorIngressDispatcher) Deliver(ctx context.Context, ev statecharts.Event) error {
	return d.sys.enqueueDispatch(func() { _ = d.sys.deliver(context.Background(), d.name, ev) })
}

type recordedExecutionError string

func (e recordedExecutionError) Error() string     { return string(e) }
func (recordedExecutionError) SendExecutionError() {}

type recordedCommunicationError string

func (e recordedCommunicationError) Error() string { return string(e) }
