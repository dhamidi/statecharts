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
visibly evicts one of the previous three while all seven remain addressable.
The UI is server-rendered and live-patched over SSE. Its vendored Datastar
bundle is embedded in the binary, so it has no browser-time CDN dependency.

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
