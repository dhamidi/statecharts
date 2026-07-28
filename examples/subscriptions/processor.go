package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dhamidi/statecharts"
)

type payments struct {
	db          *sql.DB
	notify      func(string)
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	mu          sync.RWMutex
	dispatchers map[string]statecharts.Dispatcher
}
type paymentBinding struct {
	p *payments
	d statecharts.Dispatcher
}

func newPayments(db *sql.DB, notify func(string)) *payments {
	ctx, cancel := context.WithCancel(context.Background())
	p := &payments{db: db, notify: notify, ctx: ctx, cancel: cancel, dispatchers: map[string]statecharts.Dispatcher{}}
	p.wg.Add(1)
	go p.work()
	return p
}
func (p *payments) Close()                           { p.cancel(); p.wg.Wait() }
func (p *payments) factory() statecharts.IOProcessor { return &paymentBinding{p: p} }
func (b *paymentBinding) Attach(d statecharts.Dispatcher) {
	b.d = d
	identified, ok := d.(interface{ ID() statecharts.SessionID })
	if !ok {
		return
	}
	b.p.mu.Lock()
	b.p.dispatchers[string(identified.ID())] = d
	b.p.mu.Unlock()
}

func (b *paymentBinding) Send(ctx context.Context, r statecharts.SendRequest) error {
	m, ok := r.Data.AsMap()
	if !ok {
		return errors.New("payments: payload must be object")
	}
	str := func(k string) string { v, _ := m[k].AsString(); return v }
	integer := func(k string) int64 { v, _ := m[k].AsInt64(); return v }
	sub, key := str("subscription"), str("idempotency_key")
	if sub == "" {
		return errors.New("payments: missing subscription")
	}
	b.p.mu.Lock()
	b.p.dispatchers[sub] = b.d
	b.p.mu.Unlock()
	if r.Event == "reconcile" {
		return b.p.lookup(sub, key)
	}
	fp, scenario, currency := str("fingerprint"), str("scenario"), str("currency")
	op, corr := str("operation"), str("correlation")
	period, attempt, amount := integer("period"), integer("attempt"), integer("amount")
	if key == "" || fp == "" || op == "" || corr == "" || amount < 0 || currency != "USD" {
		return errors.New("payments: invalid charge")
	}
	tx, err := b.p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var oldFP, status, resultID, oldOp, oldCorr, oldCurrency string
	var oldAttempt, oldPeriod, oldAmount int64
	err = tx.QueryRow(`SELECT fingerprint,status,result_id,operation,correlation,attempt,period,amount,currency FROM payment_ledger WHERE idempotency_key=?`, key).Scan(&oldFP, &status, &resultID, &oldOp, &oldCorr, &oldAttempt, &oldPeriod, &oldAmount, &oldCurrency)
	if err == nil {
		if oldFP != fp {
			return errors.New("payments: idempotency conflict")
		}
		if err = b.p.insertJob(tx, sub, "payment."+status, resultPayload(oldOp, oldAttempt, oldCorr, key, resultID, oldPeriod, oldAmount, oldCurrency, status), time.Now()); err != nil {
			return err
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	status = "succeeded"
	if scenario == "hard_decline" {
		status = "hard_decline"
	}
	if scenario == "retryable_decline" && attempt == 1 {
		status = "retryable_decline"
	}
	resultID = fmt.Sprintf("result:%s", key)
	_, err = tx.Exec(`INSERT INTO payment_ledger(idempotency_key,fingerprint,subscription,operation,correlation,attempt,period,amount,currency,status,result_id,scenario,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, key, fp, sub, op, corr, attempt, period, amount, currency, status, resultID, scenario, time.Now().UTC())
	if err != nil {
		return err
	}
	payload := resultPayload(op, attempt, corr, key, resultID, period, amount, currency, status)
	if scenario == "communication_failure" {
		if _, err = tx.Exec(`INSERT INTO payment_activity(subscription,kind,idempotency_key,created_at) VALUES(?,?,?,?)`, sub, "communication_error", key, time.Now().UTC()); err != nil {
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		return errors.New("payments: simulated transport acknowledgement failure")
	}
	if scenario == "lost_result" {
		if err = tx.Commit(); err != nil {
			return err
		}
		return nil
	}
	if scenario == "accepted_delayed_success" {
		accepted := resultPayload(op, attempt, corr, key, resultID+":accepted", period, amount, currency, "accepted")
		if err = b.p.insertJob(tx, sub, "payment.accepted", accepted, time.Now()); err != nil {
			return err
		}
		if err = b.p.insertJob(tx, sub, "payment.succeeded", payload, time.Now().Add(1500*time.Millisecond)); err != nil {
			return err
		}
	} else {
		if err = b.p.insertJob(tx, sub, "payment."+status, payload, time.Now()); err != nil {
			return err
		}
	}
	if scenario == "duplicate_result" || scenario == "idempotency_replay" {
		if err = b.p.insertJob(tx, sub, "payment."+status, payload, time.Now().Add(40*time.Millisecond)); err != nil {
			return err
		}
	}
	if scenario == "stale_out_of_order" {
		stale := resultPayload(op, attempt, corr, key+":stale", resultID+":stale", period-1, amount, currency, "succeeded")
		if err = b.p.insertJob(tx, sub, "payment.succeeded", stale, time.Now().Add(40*time.Millisecond)); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func resultPayload(op string, attempt int64, corr, key, rid string, period, amount int64, currency, status string) map[string]any {
	return map[string]any{"operation": op, "attempt": attempt, "correlation": corr, "idempotency_key": key, "result_id": rid, "period": period, "amount": amount, "currency": currency, "status": status}
}
func (p *payments) insertJob(tx *sql.Tx, sub, event string, payload map[string]any, due time.Time) error {
	b, _ := json.Marshal(payload)
	_, e := tx.Exec(`INSERT INTO payment_jobs(subscription,event,payload,due_at) VALUES(?,?,?,?)`, sub, event, b, due.UnixMilli())
	return e
}
func (p *payments) lookup(sub, key string) error {
	var op, corr, currency, status, rid string
	var attempt, period, amount int64
	err := p.db.QueryRow(`SELECT operation,correlation,attempt,period,amount,currency,status,result_id FROM payment_ledger WHERE idempotency_key=?`, key).Scan(&op, &corr, &attempt, &period, &amount, &currency, &status, &rid)
	if errors.Is(err, sql.ErrNoRows) {
		status = "missing"
		op = sub + ":unknown"
		currency = "USD"
	} else if err != nil {
		return err
	}
	tx, e := p.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if _, e = tx.Exec(`INSERT INTO payment_activity(subscription,kind,idempotency_key,created_at) VALUES(?,?,?,?)`, sub, "lookup", key, time.Now().UTC()); e != nil {
		return e
	}
	if e = p.insertJob(tx, sub, "payment."+status, resultPayload(op, attempt, corr, key, rid, period, amount, currency, status), time.Now()); e != nil {
		return e
	}
	return tx.Commit()
}
func (p *payments) work() {
	defer p.wg.Done()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-tick.C:
			p.drain()
		}
	}
}
func (p *payments) drain() {
	rows, e := p.db.Query(`SELECT id,subscription,event,payload FROM payment_jobs WHERE delivered_at IS NULL AND due_at<=? ORDER BY id LIMIT 20`, time.Now().UnixMilli())
	if e != nil {
		return
	}
	type job struct {
		id         int64
		sub, event string
		payload    []byte
	}
	var jobs []job
	for rows.Next() {
		var j job
		if rows.Scan(&j.id, &j.sub, &j.event, &j.payload) == nil {
			jobs = append(jobs, j)
		}
	}
	rows.Close()
	for _, j := range jobs {
		p.mu.RLock()
		d := p.dispatchers[j.sub]
		p.mu.RUnlock()
		if d == nil {
			continue
		}
		var x any
		if json.Unmarshal(j.payload, &x) != nil {
			continue
		}
		if d.Deliver(p.ctx, statecharts.Event{Name: statecharts.Identifier(j.event), Type: statecharts.EventExternal, Data: value(x)}) == nil {
			p.db.Exec(`UPDATE payment_jobs SET delivered_at=? WHERE id=? AND delivered_at IS NULL`, time.Now().UnixMilli(), j.id)
			if p.notify != nil {
				p.notify(j.sub)
			}
		}
	}
}
func initPayments(db *sql.DB) error {
	_, e := db.Exec(`CREATE TABLE IF NOT EXISTS payment_ledger(idempotency_key TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,subscription TEXT NOT NULL,operation TEXT NOT NULL,correlation TEXT NOT NULL,attempt INTEGER NOT NULL,period INTEGER NOT NULL,amount INTEGER NOT NULL,currency TEXT NOT NULL,status TEXT NOT NULL,result_id TEXT NOT NULL,scenario TEXT NOT NULL,created_at TEXT NOT NULL);CREATE TABLE IF NOT EXISTS payment_jobs(id INTEGER PRIMARY KEY AUTOINCREMENT,subscription TEXT NOT NULL,event TEXT NOT NULL,payload BLOB NOT NULL,due_at INTEGER NOT NULL,delivered_at INTEGER);CREATE TABLE IF NOT EXISTS payment_activity(id INTEGER PRIMARY KEY AUTOINCREMENT,subscription TEXT NOT NULL,kind TEXT NOT NULL,idempotency_key TEXT NOT NULL,created_at TEXT NOT NULL);`)
	return e
}
