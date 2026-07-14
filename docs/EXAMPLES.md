# Examples

Each example under `examples/` is a self-contained, runnable demonstration of
the library in a realistic scenario. Rules every example follows:

- Its own `go.mod`, so it doesn't pollute the root module's dependencies (or
  anyone else's, if they only import the root package).
- A `replace github.com/dhamidi/statecharts => ../..` (adjusted for its own
  depth) pointing at the checked-out repo, since the example is developed
  alongside the library it demonstrates, not against a released version.
- A `./run` script at the example's root that builds and runs it with no
  further setup beyond a working Go toolchain (and, where an example says so
  in its own README, credentials for a real external service it can
  optionally talk to).
- A `README.md` explaining what the example shows, how to run it, and what
  to expect to see.
- No tests. Examples exist to show the package working end-to-end, in a way
  a human runs and watches, not to be verified by `go test`.
