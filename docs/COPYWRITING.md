# Copywriting guide

This project has two kinds of prose: Go doc comments (what `go doc` and
pkg.go.dev show a user) and Markdown docs (README, guides). Both should
read like something a careful engineer wrote for another engineer who
hasn't seen the code yet — not like a design discussion, and not like
marketing copy.

## Voice

Write the way the Go standard library's documentation is written: short
declarative sentences, precise nouns, no filler. Say what a thing does, not
that it "provides functionality for" doing it. Prefer "Send enqueues ev and
blocks until it is processed" over "Send is responsible for handling the
enqueueing of events."

Two sentences beat one long one. If a sentence needs a semicolon to hold
two ideas together, consider whether they're actually two sentences.

Avoid hedging and throat-clearing: no "note that", "it's worth mentioning",
"in general". State the fact.

## What's in bounds

- **The SCXML spec.** This library exists to implement the W3C SCXML
  recommendation faithfully. Citing section numbers, algorithm names
  (`computeExitSet`, `microstep`), and SCXML terminology (event descriptor,
  configuration, history) is expected and helpful — it's how a reader
  checks a claim against the source of truth.
- **How the codebase actually fits together.** Assume the reader has the
  source open. It's fine, and often necessary, to name the type or
  function that a piece of behavior depends on (e.g. "Snapshot is what
  `Rehydrate` restores from"). Explaining *why* a non-obvious design choice
  was made — in terms of the tradeoff itself, not who chose it or when — is
  in bounds and valuable.

## What's out of bounds

- **Architecture Decision Records.** Never write "see docs/adr/000N" or
  "per ADR 3" in a doc comment or a Markdown doc. ADRs are a project's own
  paper trail for itself; a user reading `go doc` has no access to them and
  shouldn't need to. If an ADR's reasoning is worth surfacing to a reader,
  restate the reasoning in place — don't cite the document.
- **Build phases, milestones, and process.** Never write "in v1", "this
  session", "during implementation", "the first pass", "discovered while
  building this", or similar. Documentation describes the artifact as it
  is, not the history of how it came to be that way. If a limitation is
  real and worth documenting (a feature isn't implemented yet), say what's
  missing and why it's out of scope on its own merits — not when someone
  plans to get to it.
- **Version numbers, for a library that doesn't have meaningful ones yet.**
  Don't write "in this version" or pin behavior to a release. Describe
  what the code does today, plainly, as a fact about the code rather than
  a fact about a release.
- **Naming the people or process behind a decision.** No "the team
  decided", "we chose", "after discussion". State the decision and its
  reasoning as a property of the design, not a historical event.

## Go doc comments specifically

- Every exported identifier gets a doc comment, starting with its own
  name, per Go convention (`// Send enqueues...`, not `// This function
  enqueues...`).
- Say what the thing does before you say how it does it. Implementation
  detail belongs after the first sentence, or in an unexported helper's own
  comment.
- Prefer documenting *contracts* over *mechanisms*: what does the caller
  get to assume is true after calling this? What must the caller not do
  (e.g. "must never call Send from within a callback running on the same
  Instance")?
- Package-level documentation (`doc.go`) should read as a short tour: what
  the package is for, its two or three central types, and how they compose
  — in the order a reader would actually use them, not the order they're
  declared in the source.
- It is fine, and encouraged, for internal (unexported) comments to explain
  reasoning the same way exported ones do. The ADR/version/process
  restrictions above apply everywhere in the codebase, not only to exported
  doc comments.

## Evaluating documentation

Before considering a doc comment or a Markdown section done, check it
against these:

1. **Would it survive being read with no other context?** A reader who
   opens `go doc` for one type, with nothing else in front of them, should
   be able to understand what it's for and how to use it correctly.
2. **Does every claim point at something checkable?** A spec section
   number or a sibling type name lets a skeptical reader verify a claim.
   "See docs/adr/N" does not — the ADR isn't part of the artifact being
   documented.
3. **Is it still true if you delete the word "currently" or "for now"?**
   If not, it's describing a plan or a process, not the code.
4. **Could you replace a phrase with a plainer one and lose nothing?**
   "Is responsible for coordinating the dispatch of" → "dispatches".
   Prefer the plainer one every time.
5. **Does it explain a contract, or just narrate the code?** "iterates
   the map and calls f for each entry" restates what's already visible in
   the function. "f is called once per pending send, in SendID order" is
   the contract a caller can rely on.
6. **Read it out loud.** If it sounds like a person explaining something
   to a colleague, it's right. If it sounds like a commit message or a
   status update, rewrite it.
