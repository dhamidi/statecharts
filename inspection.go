package statecharts

import (
	"context"
	"fmt"
	"sort"
)

// InstanceInspection is an independently owned, point-in-time view captured
// at a stable macrostep boundary on an Instance's goroutine.
type InstanceInspection struct {
	SessionID     SessionID
	Revision      RevisionID
	Running       bool
	Configuration []Identifier
	HistoryValue  map[Identifier][]Identifier
	Datamodel     Value
	InternalQueue []Event
	ExternalQueue []Event
	PendingSends  []PendingSend
	ActiveInvokes []ActiveInvoke
}

type inspectionResult struct {
	inspection InstanceInspection
	err        error
}

// Inspect returns a stable live view without invoking snapshot codecs.
func (in *Instance) Inspect(ctx context.Context) (InstanceInspection, error) {
	req := actorRequest{kind: reqInspect, inspectOut: make(chan inspectionResult, 1)}
	if err := in.submit(ctx, req); err != nil {
		return InstanceInspection{}, err
	}
	select {
	case result := <-req.inspectOut:
		return result.inspection, result.err
	case <-in.doneCh:
		select {
		case result := <-req.inspectOut:
			return result.inspection, result.err
		default:
			return InstanceInspection{}, ErrInstanceStopped
		}
	case <-ctx.Done():
		return InstanceInspection{}, ctx.Err()
	}
}

func (in *Instance) buildInspection() (InstanceInspection, error) {
	ip := in.ip
	datamodel, err := inspectDatamodel(in.session)
	if err != nil {
		return InstanceInspection{}, fmt.Errorf("statecharts: inspect datamodel: %w", err)
	}
	result := InstanceInspection{
		SessionID:     ip.sessionID,
		Revision:      in.chart.revision,
		Running:       ip.running,
		Configuration: append([]Identifier(nil), ip.activeStates()...),
		HistoryValue:  make(map[Identifier][]Identifier, len(ip.historyValue)),
		Datamodel:     datamodel.Clone(),
		InternalQueue: cloneEvents(ip.internalQueue),
		ExternalQueue: cloneEvents(ip.externalQueue),
	}
	for history, states := range ip.historyValue {
		ids := make([]Identifier, len(states))
		for i, state := range states {
			ids[i] = state.id
		}
		result.HistoryValue[history.id] = ids
	}
	pending := make([]*pendingSendRecord, 0, len(ip.pending))
	for record := range ip.pending {
		pending = append(pending, record)
	}
	sort.Slice(pending, func(i, j int) bool {
		if !pending[i].fireAt.Equal(pending[j].fireAt) {
			return pending[i].fireAt.Before(pending[j].fireAt)
		}
		if pending[i].order != pending[j].order {
			return pending[i].order < pending[j].order
		}
		return pending[i].sendID < pending[j].sendID
	})
	for _, record := range pending {
		result.PendingSends = append(result.PendingSends, PendingSend{
			SendID: record.sendID, Target: record.target, Type: record.typ,
			Event: cloneEvent(record.event), FireAt: record.fireAt,
		})
	}
	for _, invokes := range ip.activeInvokes {
		for _, invoke := range invokes {
			result.ActiveInvokes = append(result.ActiveInvokes, ActiveInvoke{
				State: invoke.state.id, DefinitionID: invoke.spec.definitionID,
				ID: invoke.id, Type: invoke.typ, Source: invoke.source,
			})
		}
	}
	sort.Slice(result.ActiveInvokes, func(i, j int) bool {
		return result.ActiveInvokes[i].ID < result.ActiveInvokes[j].ID
	})
	return result, nil
}

func inspectDatamodel(session DatamodelSession) (_ Value, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("statecharts: datamodel inspection panicked: %v", value)
		}
	}()
	return session.Inspect()
}
