# Durable counters

A single binary demonstrates seven durable color counters under deliberate
residency pressure (only three actors may be resident), an idempotent load
writer, a reconnecting event-stream reader, and a live Datastar 1.0.2 server
UI. Writer and reader each model their server connection as an ephemeral
statechart actor; the counter values and processed write IDs live in the
durable actors.

```sh
./run serve                              # UI/API on :8080, data/counters.db
./run writer --rate 40 red blue          # terminal load dashboard
./run reader red blue                     # terminal event-stream dashboard
```

Run those commands in three terminals, then open the server in a browser. Its
main UI shows all seven counters, labels each `resident` or `paged out`, and
increments a counter when its card is selected. Activating a fourth color
shows the actor as `hydrating` while its durable log is replayed, then visibly
evicts one of the previous three while all seven remain addressable. The count
changes only after that increment has entered the durable log and reached the
server projection. The UI is server-rendered and live-patched over SSE. Its
vendored Datastar bundle is embedded in the binary, so it has no browser-time
CDN dependency.

The server runs durable counters in a `counters` actor system and the
projection hub plus one ephemeral actor per SSE connection in an isolated
`ui` system. Counter projections reach the hub through an `actors.Bridge`;
the hub actively sends snapshots to the connection actors; and connection
actors emit bytes through the custom `"sse"` IOProcessor type. HTTP handlers
only attach and drain transport channels rather than reading shared actor
state to manufacture updates.

Every chart uses `statecharts.New` with ordinary typed Go state. Short behavior
names are scoped to that chart and chart nodes contain only serializable,
versioned references to them; runtime transports, channels, callbacks, and log
storage are not part of snapshot data. Each of the seven dashboard cards
displays its actor's pinned chart revision in addition to residency and value;
`GET /actors` reports pins for every actor, including canaries.

## Inspect, edit, and publish

The server exposes a deliberately small, unauthenticated administration API
for the complete-definition deployment loop. It is an example seam, not a
recommendation to expose these endpoints without your application's normal
authentication and authorization.

Start with the Go-built `counter-v1` definition, export it through the optional
JSON surface codec, and inspect the current revision:

```sh
curl -s http://127.0.0.1:8080/definitions | jq
curl -s http://127.0.0.1:8080/definitions/counter > counter-v1.json
```

The server has both `v1` (+1) and `v2` (+2) implementations of
`counter.apply-idempotent-increment` in its chart-local Go registry. The
initial Go definition references `v1`. Edit that reference as ordinary data
and give the chart revision an operator-visible salt:

```sh
jq '(.root.transitions[].actions[][] |
     select(.call.function.name? == "counter.apply-idempotent-increment") |
     .call.function.version) = "v2" |
    .revisionSalt = "counter-v2"' \
  counter-v1.json > counter-v2.json

curl -fsS -X POST --data-binary @counter-v2.json \
  http://127.0.0.1:8080/definitions/counter/validate | jq
curl -fsS -X PUT --data-binary @counter-v2.json \
  http://127.0.0.1:8080/definitions/counter | jq
```

Validation compiles the complete candidate without publication. Publication
stores and atomically selects the complete revision; malformed JSON, invalid
definitions, and unresolved function versions return actionable errors and do
not move the current pointer.

The original seven actors were spawned before publication and remain pinned
to `v1`, including after paging and rehydration. Spawn a hierarchical canary
after publication, send one unique write to each, and compare both behavior and
revision pins:

```sh
curl -fsS -X POST http://127.0.0.1:8080/actors/blue.canary
curl -fsS -X POST http://127.0.0.1:8080/counters/red/writes/demo-old
curl -fsS -X POST http://127.0.0.1:8080/counters/blue.canary/writes/demo-new
curl -s http://127.0.0.1:8080/counters/red | jq
curl -s http://127.0.0.1:8080/counters/blue.canary | jq
curl -s http://127.0.0.1:8080/actors | jq
```

The red value advances by one on its retained revision; the canary advances by
two on the new revision. `GET /definitions/counter?revision=<revision>` exports
either retained definition while it remains pinned. Revisions can be copied
from publication responses or the actor listing. This is revision coexistence
inside one deployment, not old-format compatibility.

Definition artifacts and durable actor pins survive process restart. The
current revision is deployment configuration: this example's Go startup path
registers `counter-v1`, so republish `counter-v2.json` after restarting when it
should remain the revision selected by future actors. Every named Go function
version referenced by a non-terminal actor must remain registered so its
pinned definition can compile during rehydration. Terminal actors release
their revision pin; an application may then collect an unreferenced,
non-current definition with `System.CollectDefinition`.

Positional color names select the counters exercised by `writer` or observed
by `reader`; omitting them selects all seven. The writer terminal shows
per-color request rates, totals, retries, in-flight requests, and Unicode-block
sparklines. The reader terminal shows the latest streamed value and residency
for each selected counter.

The writer uses exponential inter-arrival times (a Poisson process), chooses
colors with a non-uniform Zipf distribution, and retries each unique write ID
with capped exponential backoff. Stop `serve` while the other two commands
continue, wait, and restart it: writer retries cannot double-count, the reader
reconnects, and the seven values are reconstructed from SQLite before the HTTP
listener opens. Browser tabs may likewise disconnect and reconnect; each SSE
connection starts with a complete snapshot.

Useful overrides are `serve --addr --db`,
`writer --server --rate --max-in-flight`, and `reader --server --n`.
All commands stop cleanly on SIGINT/SIGTERM.
