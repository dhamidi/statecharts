package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
	"github.com/dhamidi/statecharts/sqllog/sqlite3"
)

var plans = map[string]int64{"starter": 900, "growth": 2900, "scale": 9900}
var scenarios = map[string]bool{"success": true, "hard_decline": true, "retryable_decline": true, "communication_failure": true, "accepted_delayed_success": true, "duplicate_result": true, "stale_out_of_order": true, "lost_result": true, "idempotency_replay": true}

type runtime struct {
	store    *sqlite3.Storage
	system   *actors.System
	payments *payments
	mu       sync.RWMutex
	ids      map[string]bool
	watchers map[string]map[chan struct{}]struct{}
}

func openRuntime(ctx context.Context, path string) (*runtime, error) {
	store, err := sqlite3.Open(path)
	if err != nil {
		return nil, err
	}
	if err = initPayments(store.DB()); err != nil {
		return nil, err
	}
	r := &runtime{store: store, ids: map[string]bool{}, watchers: map[string]map[chan struct{}]struct{}{}}
	p := newPayments(store.DB(), r.notify)
	r.payments = p
	r.system = actors.NewSystem(actors.WithStorage(store), actors.WithIdleTimeout(0), actors.WithIOProcessor("payments", p.factory), actors.WithResidencyObserver(func(c actors.ResidencyChange) {}))
	chart, err := subscriptionChart()
	if err != nil {
		return nil, err
	}
	if err = r.system.Register(chart); err != nil {
		return nil, err
	}
	rows, _ := store.DB().Query(`SELECT actor_id FROM statechart_actor WHERE chart_id=?`, chartID)
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var id string
			if rows.Scan(&id) == nil {
				r.ids[id] = true
			}
		}
	}
	return r, nil
}
func (r *runtime) close(ctx context.Context) error {
	err := r.system.Stop(ctx)
	r.payments.Close()
	if err != nil {
		return fmt.Errorf("stop: %w", err)
	}
	return nil
}
func (r *runtime) create(ctx context.Context, id, plan, scenario string, q int64) error {
	r.mu.RLock()
	exists := r.ids[id]
	r.mu.RUnlock()
	if exists {
		return errConflict
	}
	if !strings.HasPrefix(id, "sub.") || len(id) > 64 {
		return fmt.Errorf("id must start sub.")
	}
	price, ok := plans[plan]
	if !ok {
		return fmt.Errorf("unknown plan")
	}
	if !scenarios[scenario] || q < 1 || q > 100 {
		return fmt.Errorf("invalid scenario or quantity")
	}
	if err := r.system.Spawn(ctx, statecharts.Identifier(id), chartID, actors.Durable()); err != nil {
		return err
	}
	payload := value(map[string]any{"id": id, "plan": plan, "unit_amount": price, "quantity": q, "scenario": scenario}) // initial variables are set by a trusted configure event
	if err := r.system.Tell(ctx, statecharts.Identifier(id), statecharts.Event{Name: "configure", Type: statecharts.EventExternal, Data: payload}); err != nil {
		return err
	}
	r.mu.Lock()
	r.ids[id] = true
	r.mu.Unlock()
	r.notify(id)
	return nil
}
func (r *runtime) tell(ctx context.Context, id, event string, payload any) error {
	r.mu.RLock()
	ok := r.ids[id]
	r.mu.RUnlock()
	if !ok {
		return errUnknown
	}
	err := r.system.Tell(ctx, statecharts.Identifier(id), statecharts.Event{Name: statecharts.Identifier(event), Type: statecharts.EventExternal, Data: value(payload)})
	r.notify(id)
	return err
}
func (r *runtime) projection(ctx context.Context, id string) (map[string]any, error) {
	r.mu.RLock()
	ok := r.ids[id]
	r.mu.RUnlock()
	if !ok {
		return nil, errUnknown
	}
	if err := r.system.Spawn(ctx, statecharts.Identifier(id), chartID, actors.Durable()); err != nil {
		return nil, err
	}
	_, in, err := r.system.InspectActor(ctx, statecharts.Identifier(id))
	if err != nil {
		return nil, err
	}
	if in == nil {
		return nil, fmt.Errorf("subscription unavailable")
	}
	m, err := asMap(in.Datamodel)
	if err != nil {
		return nil, err
	}
	states := make([]string, len(in.Configuration))
	for i, s := range in.Configuration {
		states[i] = string(s)
	}
	m["id"] = id
	m["states"] = states
	var acts []map[string]any
	rows, _ := r.store.DB().Query(`
		SELECT kind,idempotency_key,status,scenario,operation,correlation,attempt,result_id,amount,currency
		FROM (
			SELECT 'payment' AS kind,idempotency_key,status,scenario,operation,correlation,attempt,result_id,amount,currency,created_at
			FROM payment_ledger WHERE subscription=?
			UNION ALL
			SELECT kind,idempotency_key,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,created_at
			FROM payment_activity WHERE subscription=?
		)
		ORDER BY created_at DESC LIMIT 12`, id, id)
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var kind, key string
			var status, scenario, operation, correlation, resultID, currency sql.NullString
			var attempt, amount sql.NullInt64
			if rows.Scan(&kind, &key, &status, &scenario, &operation, &correlation, &attempt, &resultID, &amount, &currency) != nil {
				continue
			}
			record := map[string]any{"kind": kind, "key": key}
			if status.Valid {
				record["status"], record["scenario"], record["operation"] = status.String, scenario.String, operation.String
				record["correlation"], record["attempt"], record["result_id"] = correlation.String, attempt.Int64, resultID.String
				record["amount"], record["currency"] = amount.Int64, currency.String
			}
			acts = append(acts, record)
		}
	}
	m["activity"] = acts
	return m, nil
}

var (
	errConflict = fmt.Errorf("subscription already exists")
	errUnknown  = fmt.Errorf("unknown subscription")
)

func (r *runtime) list() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	x := make([]string, 0, len(r.ids))
	for id := range r.ids {
		x = append(x, id)
	}
	sort.Strings(x)
	return x
}
func (r *runtime) notify(id string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for c := range r.watchers[id] {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}
