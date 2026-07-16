# Statechart Arena

Arena is a small authoritative multiplayer game and a deliberately realistic
actor-system example. One process hosts one `actors.System`, an initial match
actor, three AI actors, and one ephemeral connection actor per WebSocket.
Additional matches can be spawned in the same process. Creatures move on a
deterministic grid, collect power charges, and fire projectiles at one another.

```sh
./run serve                       # http://127.0.0.1:8080
./run serve --bots 6 --tick 80ms
```

Open two browser tabs to add human creatures. WASD moves and Space fires. The
plain-JavaScript Web Components render only snapshots from the server; they do
not predict or mutate game state. Reloading or reconnecting with the same
browser identity resubscribes to the existing authoritative creature.

## Architecture

`arena.match` is the sole owner of the world, deterministic random state,
input sequence numbers, simulation tick, subscribers, projectiles, powerups,
health, and scores. It self-schedules exactly one next tick. Input is applied
serially and stale sequence numbers are ignored.

Every `arena.connection` actor validates client envelopes, translates them to
canonical `Value` messages, and sends them to the match through ordinary actor
routing. Match snapshots travel back through that actor. The actor emits text
frames through the custom `arena.websocket` IOProcessor; the HTTP handler only
upgrades the socket, attaches its opaque capability, and drains inbound bytes.
Socket pointers and channels never enter actor state or `Value`.

The match gives each connection a lease on its player. A replacement
connection atomically supersedes that lease, so an old socket cannot move or
later delete the reconnected player. A disconnected player is visibly marked
as rejoining and expires after a ten-second grace period if no replacement
arrives.

`arena.bot` actors subscribe to the same snapshots and emit the same
`player.input` protocol as humans. Their simple deterministic policy pursues a
powerup or creature and fires when aligned.

Matches are intentionally ephemeral in this example: a process restart starts
a new game. Making them durable is a spawn-policy change plus storage, but a
real game must first choose its product semantics for restart, match expiry,
and recovery rather than accidentally promising persistence.

## Hot-deploy a behavior

The binary retains both `v1` and `v2` of `arena.match.apply-input`. The Go-built
definition initially references `v1`, which moves one cell. Export the complete
definition, switch that reference to `v2`, validate it, and publish it:

```sh
curl -fsS http://127.0.0.1:8080/definitions/match > match-v1.json
jq '(.root.transitions[].actions[][] |
     select(.call.function.name? == "arena.match.apply-input") |
     .call.function.version) = "v2" |
    .revisionSalt = "movement-v2"' \
  match-v1.json > match-v2.json

curl -fsS -X POST --data-binary @match-v2.json \
  http://127.0.0.1:8080/definitions/match/validate | jq
curl -fsS -X PUT --data-binary @match-v2.json \
  http://127.0.0.1:8080/definitions/match | jq
curl -fsS -X POST http://127.0.0.1:8080/matches/match.canary | jq
```

The running `match.main` remains pinned to one-cell movement. Open
`/?match=match.canary`: the newly spawned match is pinned to the published
revision and moves two cells. Both UIs display their match's pinned revision.
`GET /matches` shows the coexistence and
`GET /definitions/match?revision=<revision>` exports either retained program.

The tests exercise deterministic collisions and powerups, stale-input
rejection, one-successor tick scheduling, I/O capability isolation, AI use of
the player protocol, reconnect/resubscribe behavior, lease takeover and
expiry, and the old/new match revision split under whole-definition
publication.
