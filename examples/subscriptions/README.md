# Durable subscription billing

Run `go run . serve --addr :8080 --db subscriptions.db`.

Each subscription is a durable actor using the isolated ECMAScript datamodel.
JavaScript calculates exact integer minor-unit invoices, owns correlation and
retry/freshness policy, and constructs serializable `payments` sends. Go owns
trusted plan prices, HTTP validation, SQLite, projections, and fake processor
mechanics. The processor ledger makes idempotency and lost-result lookup survive
restart; sends enqueue callbacks and never simulate latency inline.

Each period is one logical operation. Known retryable declines create a fresh
attempt/idempotency key; communication failures reconcile the same key before
another charge. Processor settlements and delayed callback jobs are durable.
GET and SSE use `Spawn` as an explicit, idempotent activation path after a
restart. Processor bindings learn the durable session identity during
activation, so reading a projection does not append query events or outbound
effects to the actor's log.
