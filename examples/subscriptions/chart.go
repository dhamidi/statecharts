package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/datamodel/ecmascript"
)

const (
	chartID     statecharts.Identifier = "subscription"
	lookupTimer                        = "lookup.timer"
	retryTimer                         = "retry.timer"
)

func js(s string) statecharts.Expression {
	e, err := ecmascript.Source(s)
	if err != nil {
		panic(err)
	}
	return e
}
func subscriptionChart() (*statecharts.Chart, error) {
	m, e := ecmascript.New()
	if e != nil {
		return nil, e
	}
	assign := func(l, x string) statecharts.Executable {
		return statecharts.NewAssignExecutable(statecharts.AssignDefinition{Location: js(l), Expr: js(x)})
	}
	script := func(x string) statecharts.Executable {
		return statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: js(x)})
	}
	valid := js(`_event.data.operation===operation && _event.data.attempt===attempt && _event.data.correlation===correlation && _event.data.idempotency_key===idempotency_key && _event.data.period===period && _event.data.amount===amount && _event.data.currency===currency`)
	success := func(target string) statecharts.StateOption {
		return statecharts.On("payment.succeeded", statecharts.If(valid), statecharts.Target(statecharts.Identifier(target)), statecharts.Then(statecharts.CancelSend(lookupTimer), assign("paid_period", "_event.data.period"), assign("last_result_id", "_event.data.result_id"), assign("last_result", `"succeeded"`)))
	}
	startLookupTimer := func(delay time.Duration) statecharts.Executable {
		return statecharts.Send(lookupTimer, statecharts.SendID(lookupTimer), statecharts.SendDelay(delay))
	}
	charge := statecharts.Send("charge", statecharts.SendType("payments"), statecharts.SendTarget("processor"), statecharts.SendContent(js(`({subscription:id,operation:operation,attempt:attempt,correlation:correlation,idempotency_key:idempotency_key,fingerprint:operation+":"+attempt+":"+amount+":"+currency,period:period,amount:amount,currency:currency,scenario:scenario})`)))
	reconcile := statecharts.Send("reconcile", statecharts.SendType("payments"), statecharts.SendTarget("processor"), statecharts.SendContent(js(`({subscription:id,idempotency_key:idempotency_key})`)))
	stale := func(name string) statecharts.StateOption {
		return statecharts.On(name, statecharts.If(js(`!(_event.data.operation===operation && _event.data.attempt===attempt && _event.data.correlation===correlation && _event.data.idempotency_key===idempotency_key && _event.data.period===period && _event.data.amount===amount && _event.data.currency===currency)`)), statecharts.Then(assign("stale_count", "Math.min(stale_count+1,1000)")))
	}
	setup := statecharts.Atomic("setup",
		statecharts.On("configure",
			statecharts.Target("active"),
			statecharts.Then(
				assign("id", "_event.data.id"),
				assign("plan", "_event.data.plan"),
				assign("unit_amount", "_event.data.unit_amount"),
				assign("quantity", "_event.data.quantity"),
				assign("scenario", "_event.data.scenario"),
			),
		),
	)
	charging := statecharts.Atomic("charging",
		statecharts.OnEntry(
			script(`amount=unit_amount*quantity;attempt++;operation=id+":"+period;correlation=operation+":"+attempt;idempotency_key=correlation`),
			charge,
			startLookupTimer(3*time.Second),
		),
		success("current"),
		stale("payment.succeeded"),
		statecharts.On("payment.hard_decline",
			statecharts.If(valid),
			statecharts.Target("past_due"),
			statecharts.Then(statecharts.CancelSend(lookupTimer), assign("last_result_id", "_event.data.result_id")),
		),
		statecharts.On("payment.retryable_decline",
			statecharts.If(valid),
			statecharts.Target("retry_wait"),
			statecharts.Then(statecharts.CancelSend(lookupTimer)),
		),
		statecharts.On("payment.accepted",
			statecharts.If(valid),
			statecharts.Target("awaiting"),
			statecharts.Then(statecharts.CancelSend(lookupTimer)),
		),
		statecharts.On(lookupTimer, statecharts.Target("reconciliation")),
		statecharts.On("error.communication",
			statecharts.Target("reconciliation"),
			statecharts.Then(statecharts.CancelSend(lookupTimer)),
		),
	)
	awaiting := statecharts.Atomic("awaiting",
		statecharts.OnEntry(startLookupTimer(2*time.Second)),
		success("current"),
		stale("payment.succeeded"),
		statecharts.On(lookupTimer, statecharts.Target("reconciliation")),
	)
	retryWait := statecharts.Atomic("retry_wait",
		statecharts.OnEntry(statecharts.Send(retryTimer,
			statecharts.SendID(retryTimer),
			statecharts.SendDelay(30*time.Millisecond),
		)),
		statecharts.On(retryTimer,
			statecharts.If(js(`attempt<max_attempts`)),
			statecharts.Target("charging"),
		),
		statecharts.On(retryTimer, statecharts.Target("past_due")),
	)
	reconciliation := statecharts.Atomic("reconciliation",
		statecharts.OnEntry(reconcile),
		success("current"),
		stale("payment.succeeded"),
		statecharts.On("payment.missing", statecharts.If(valid), statecharts.Target("charging")),
		statecharts.On("payment.hard_decline", statecharts.If(valid), statecharts.Target("past_due")),
		statecharts.On("payment.retryable_decline", statecharts.If(valid), statecharts.Target("retry_wait")),
	)
	current := statecharts.Atomic("current",
		statecharts.On("payment.succeeded",
			statecharts.If(js(`_event.data.result_id===last_result_id`)),
			statecharts.Then(assign("duplicate_count", "Math.min(duplicate_count+1,1000)")),
		),
		stale("payment.succeeded"),
		statecharts.On("period.advance",
			statecharts.If(js(`_event.data.period>period`)),
			statecharts.Target("charging"),
			statecharts.Then(assign("period", "_event.data.period"), assign("attempt", "0")),
		),
		statecharts.On("retry", statecharts.Target("charging")),
	)
	billing := statecharts.Compound("billing", "charging",
		statecharts.Children(
			charging,
			awaiting,
			retryWait,
			reconciliation,
			current,
			statecharts.Atomic("past_due", statecharts.On("retry", statecharts.Target("charging"))),
		),
	)
	entitlement := statecharts.Compound("entitlement", "grace",
		statecharts.Children(
			statecharts.Atomic("grace",
				statecharts.On("payment.succeeded", statecharts.If(valid), statecharts.Target("enabled")),
				statecharts.On("payment.hard_decline", statecharts.If(valid), statecharts.Target("disabled")),
			),
			statecharts.Atomic("enabled",
				statecharts.On("period.advance", statecharts.Target("grace")),
				statecharts.On("payment.hard_decline", statecharts.If(valid), statecharts.Target("disabled")),
			),
			statecharts.Atomic("disabled",
				statecharts.On("payment.succeeded", statecharts.If(valid), statecharts.Target("enabled")),
			),
		),
	)
	active := statecharts.Parallel("active",
		statecharts.On("cancel",
			statecharts.Target("cancelled"),
			statecharts.Then(statecharts.CancelSend(lookupTimer), statecharts.CancelSend(retryTimer)),
		),
		statecharts.On("scenario.set", statecharts.Then(assign("scenario", "_event.data.scenario"))),
		stale("payment.succeeded"),
		stale("payment.hard_decline"),
		stale("payment.retryable_decline"),
		stale("payment.accepted"),
		statecharts.Children(billing, entitlement),
	)
	root := statecharts.Compound(chartID, "setup",
		statecharts.Children(setup, active, statecharts.Atomic("cancelled")),
	)
	initialData := []statecharts.DataDefinition{
		data("id", `""`),
		data("plan", `""`),
		data("unit_amount", `0`),
		data("quantity", `1`),
		data("amount", `0`),
		data("currency", `"USD"`),
		data("period", `1`),
		data("paid_period", `0`),
		data("attempt", `0`),
		data("max_attempts", `3`),
		data("scenario", `"success"`),
		data("operation", `""`),
		data("correlation", `""`),
		data("idempotency_key", `""`),
		data("last_result_id", `""`),
		data("last_result", `""`),
		data("duplicate_count", `0`),
		data("stale_count", `0`),
	}
	return statecharts.Build(root, m,
		statecharts.WithRevisionSalt("subscriptions-v2"),
		statecharts.WithData(initialData...),
	)
}
func data(id statecharts.Identifier, s string) statecharts.DataDefinition {
	e := js(s)
	return statecharts.DataDefinition{ID: id, Expr: &e}
}
func value(v any) statecharts.Value {
	x, e := statecharts.ValueFromJSON(v)
	if e != nil {
		panic(e)
	}
	return x
}
func asMap(v statecharts.Value) (map[string]any, error) {
	x, e := v.JSONValue()
	if e != nil {
		return nil, e
	}
	m, ok := x.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("datamodel is %T", x)
	}
	return m, nil
}
func jsonValue(v statecharts.Value) []byte { b, _ := json.Marshal(v); return b }
