# ai-agent

A multi-conversation AI agent workspace built on `github.com/dhamidi/statecharts`
and its `actors` package, demonstrating the library's flagship claim end to
end: **everything recovers when the server goes down and comes back up.** A
conversation in progress survives a server restart with no visible gap (the
durable `ConversationActor` rehydrates from its `Log` and resumes exactly
where it left off); a connected client survives the server being down,
reconnecting and resuming its event stream with no missed and no duplicated
messages; a tool call in flight when the tool-capable client disconnects is
not lost, and the executor role for a tool is a leased, single-owner
assignment that hands off automatically; and the list of conversations
itself -- not just any one conversation -- survives a restart too.

**This is a toy demo: no authentication, binds to `127.0.0.1` only, and its
`shell_command` tool executes arbitrary shell input from whichever client
currently holds its lease. Do not expose it beyond your own machine.**

## Running it

The quickest way to try it:

```sh
./run
```

This builds `cmd/ai-agent` and runs it in *embedded mode*: one process
running both the workspace server (on a random loopback port) and a client
(with the `shell_command` tool enabled), pointed at each other. It prints
the client's own UI URL -- open it in a browser.

Embedded mode can't demonstrate the recovery story by itself (server and
client share a process, so killing one kills the other). For that, run the
server and client(s) as separate processes:

```sh
# terminal 1
go run ./cmd/ai-agent serve --llm=echo

# terminal 2 (a tool-capable client)
go run ./cmd/ai-agent connect --conversation=demo --tools=shell_command

# terminal 3 (a viewer-only client -- no --conversation needed, use its own sidebar)
go run ./cmd/ai-agent connect
```

Each `connect` prints its own local UI URL. `serve` accepts `--addr`
(default `:8080`) and `--db` (default `data/ai-agent.db`); `connect` accepts
`--server` (default `http://127.0.0.1:8080`).

### Using a real model

By default every mode uses `EchoProvider`, a fast, offline, deterministic
stand-in (see below). To talk to a real model instead, pass `--llm=genai
--llm-model=gemini-2.5-flash` (or another current model) to `serve` (or to
bare embedded mode) with `GEMINI_API_KEY` set in your environment. This
exercises the identical thinking/streaming/tool-call consumer code the
`echo` provider does, just backed by a real decision-maker.

## The sidebar

The conversation list lives behind a "☰ Conversations" button, in a native
`<dialog>` rather than a permanent column -- so it scales the same way
whether the workspace has 3 conversations or 300, and the same way on any
screen size with no separate mobile layout needed for it. Each entry shows
its title and a state badge (`idle`, `thinking`, or `running tool`); a
filter box above the list narrows it by title, entirely client-side (a
[Datastar](https://data-star.dev) `data-show` expression, no server round
trip per keystroke); a small form at the bottom creates a new conversation.
A connection banner at the very top shows this client's own link status
(`connected`, `connecting`, or `reconnecting`). Clicking a conversation
switches the client's own link to it; the transcript for each conversation
is completely isolated from the others.

The page is genuinely live, via Datastar (vendored directly into the
binary, `internal/client/static/datastar.js` -- no CDN, no build step, no
framework, just one `<script type="module">` tag and a handful of
declarative `data-*` attributes). Each open tab holds a single persistent
`GET /events` SSE connection (`data-init="@get('/events')"` on `<body>`
opens it once, on load); the local `UIServerActor` pushes a
`datastar-patch-elements` event -- a re-rendered HTML fragment matched into
place by element `id` -- every time a message, delta, tool call,
link-status, or sidebar change lands, so the page updates the instant
something happens, with zero additional requests. The sidebar's own data
(every conversation on the *remote* workspace server, not just the one this
tab has open) is kept current by a second, equally real-time mechanism: the
client process holds exactly one upstream `GET /directory/events`
connection to the server for its whole lifetime (`internal/client`'s
`directorylink`, structured just like the per-conversation link), receiving
one changed entry at a time rather than the whole list re-sent on every
single change, and fans that one connection's updates out to however many
browser tabs are open locally -- so opening more tabs never opens more
connections to the workspace server, and a workspace with hundreds of
conversations doesn't re-transmit hundreds of records just because one of
them changed state. The `<dialog>` itself is static page structure, so an
open dialog's own state is never disturbed by a sidebar update arriving
while it's open. The list (and its titles) survives a server restart along
with everything else.

## The `!` convention (EchoProvider only)

`EchoProvider` needs no credentials and makes no network calls, but it
still needs to be told when to stream a plain reply, when to "think," and
when to decide on a tool call, since there's no real model behind it to
decide on its own. It looks at the *last* message in the conversation:

- If it's a tool result, EchoProvider streams that tool's own output,
  upper-cased, as its final reply (the "give the model the result back"
  half of a tool call).
- Otherwise, a message starting with `!` is EchoProvider's tool-call
  trigger: it emits one `thinking` chunk, then decides to call
  `shell_command` with everything after the `!` as the command. For
  example, sending `!echo hi` makes EchoProvider "decide" to run `echo hi`.
- Any other message streams its own upper-cased text back, word by word,
  roughly 30ms apart -- slow enough to see it streaming in the UI, fast
  enough not to be tedious.

This convention has no meaning to `--llm=genai`'s real provider, which
decides on its own when to call a tool.

## Why each actor is its own actor

**Server** (`internal/server`):
- `ConversationActor` (durable) -- one per conversation: `idle` /
  `thinking` / `awaiting_tool`, holding the full message history. Durable
  so a conversation survives a restart exactly where it left off.
- `llmrequest` (non-durable, one per turn) -- a single LLM turn has its own
  multi-step lifecycle (thinking deltas, text deltas, then a result) and
  needs to deliver a variable, unbounded number of events to more than one
  place (live subscribers, and eventually the owning conversation) --
  exactly what this codebase already means by "actor." `<invoke>` isn't
  used for this because an invoke on a *durable* actor's state would
  restart for real on every replay after a restart (see `Instance.startInvoke`);
  spawning a dedicated, non-durable actor per turn sidesteps that entirely.
- `LLMDispatchProcessor` (a `statecharts.IOProcessor`, installed via
  `actors.WithFallback`) -- the only way a chart action (which only ever
  gets an `ExecContext`, never a `*actors.System`) can spawn a new actor and
  drive a real streaming provider call from a goroutine.
- `ToolRegistryActor` (non-durable, singleton) -- the executor role for a
  tool name is global across the server, not per-conversation, so it's one
  actor, not one per conversation: `map[tool]lease{owner, expiresAt}`, with
  a periodic sweep expiring stale leases.
- `FanoutActor` (non-durable, singleton) -- routes live message/delta
  traffic to every subscribed connection for a conversation; has no idea
  tools exist.
- `UserActor` (durable, singleton) -- the one (demo-only) user's whole
  workspace: every conversation ever created, and its last-known state.
- `DirectoryActor` (non-durable, singleton) -- a live mirror of
  `UserActor`'s conversation map, safe for `GET /conversations` to query
  synchronously via a reply channel (a *durable* actor can't be queried
  that way -- `system.deliver`'s write-ahead logging fails outright on a
  channel-typed `Event.Data`). Primed at startup entirely by ordinary actor
  `Send`s from `UserActor`'s own already-rehydrated state, never by reading
  its Log directly. Also answers `GET /directory/events`: a long-lived SSE
  stream, one changed entry pushed per change (not the whole list
  re-serialized every time -- see the sidebar section above), so a client's
  own sidebar is push-driven rather than polled.
- `ConnectionActor` (non-durable, one per active SSE request) -- subscribes
  to `FanoutActor` for live traffic and asks its own conversation to replay
  history it doesn't already have, both by ordinary actor `Send` (never a
  direct Log read), then holds and periodically renews a lease for every
  tool the connection advertises.

**Client** (`internal/client`):
- `LinkActor` (singleton) -- the SSE link to the server: connecting,
  reconnecting with backoff, and switching conversations, each a real state
  the chart is in.
- `DirectoryLinkActor` (singleton) -- structurally the same as `LinkActor`
  (connect / reconnect-with-backoff over `<invoke>`), but for exactly one
  thing: the server's single `GET /directory/events` stream, for this
  client process's whole lifetime, forwarding each changed entry to `ui` so
  the sidebar is push-driven. Its own connection is entirely independent of
  how many browser tabs are open, or which conversation any of them has
  selected.
- `ToolActor` (singleton) -- runs one tool call at a time via `<invoke>`
  (fine here: `ToolActor` is non-durable, so the replay-unsafety reason
  `<invoke>` is avoided for server-side actors doesn't apply).
- `UIServerActor` (singleton) -- the local browser UI, itself an `<invoke>`
  running an HTTP server for as long as the actor is alive.

## Known limitations

- No authentication, no multi-user support -- see the warning at the top.
- A closed connection's `ConnectionActor` has no explicit removal API on
  `actors.System` for a still-durable-but-otherwise-non-actor cleanup path;
  it simply reaches its own terminal `closed` state, which the actor system
  now frees automatically (see the library's automatic eviction of
  finished actors) -- nothing to worry about in practice, just noted here
  since there's no *explicit* per-actor removal call being made.
- The executor lease for a tool is one connection at a time, server-wide,
  not one per conversation -- so if several conversations decide to call
  the same tool around the same moment, whichever single connection holds
  the lease executes them one after another (each conversation's own
  `awaiting_tool` state is still independently correct throughout; they
  just wait their turn at the executor rather than running concurrently).
  Connect a second tool-capable client to add a second executor. If
  *nobody* ever claims the lease for long enough to actually run the call --
  no tool-capable client connected, or the one that was has been gone long
  enough for its lease to lapse -- `ConversationActor` retries the offer
  every 10s and gives up after 60s (six retries; see `conversation.go`'s
  `toolOfferMaxRetries`), synthesizing a tool error and letting the LLM
  react to it rather than sitting in `awaiting_tool` forever.
- Conversation titles are set once, at creation; there's no rename or
  delete.
