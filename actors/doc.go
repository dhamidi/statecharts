// Package actors builds virtual actors on top of statecharts.Chart,
// statecharts.Instance, statecharts.IOProcessor, and statecharts.Log: named,
// addressable instances that can be spawned durable so a process restart
// resumes them exactly where they left off, and that page into and out of
// memory automatically as they go idle or as the system comes under
// resident-actor pressure.
//
// NewSystem builds a System from functional options: WithStorage supplies
// the durability boundary backing every Durable actor,
// WithIdleTimeout and WithResidencyLimit control automatic paging, and
// WithClock controls time. WithNodeName supplies the routing location without
// changing stable actor IDs or their keys in the System's isolated Storage.
//
// Register every Chart a System will ever spawn, then Spawn actors by stable
// ActorID under those charts' kinds (a chart's own ID, see
// statecharts.Chart.ID). Actor IDs are Identifiers and may be hierarchical.
// Spawn without Durable behaves like statecharts.New plus Start and keeps no
// history -- if the process restarts, the actor is gone. Spawn with Durable
// records every message to the system's Log before applying it, so the same
// name can later be paged out and paged back in, even in a different
// process against the same Log, landing in exactly the state it was in
// before.
//
// Every actor a System spawns addresses its peers by actor ID or by an
// "<actor-id>@<node>" routing key, through the same routing IOProcessor the
// System wires into every Instance it spawns.
// Sending to a peer from inside a chart is ordinary executable content --
// ec.Send with Target set to the peer's name. Application code outside any
// chart uses System.Tell the same way. Every event a System delivers
// carries Origin set to the sender's routable key, so a reply is just another
// Send targeting ev.Origin.
//
// A System's own routing only ever resolves names it spawned itself.
// WithSCXMLPeer gives it an SCXML peer to try for unknown locations, which is
// how two independent Systems address each other: Bridge is a ready-made
// fallback that forwards "billing@warehouse-b" to another System's
// "billing" actor ID and stamps replies with the source node so they route
// back the same way.
//
// A durable actor idle past WithIdleTimeout, or evicted early to satisfy
// WithResidencyLimit, is checkpointed and stopped. The next message
// addressed to it pages it back in transparently, through the same
// Rehydrate path a durable Spawn uses. Only durable actors are ever paged
// out this way -- a non-durable actor has no Log to rebuild itself from, so
// it stays resident for as long as the System itself runs.
package actors
