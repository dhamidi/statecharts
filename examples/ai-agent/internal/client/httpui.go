package client

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/htmlutil"
	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// staticFiles vendors Datastar (https://data-star.dev) directly into the
// binary -- no CDN dependency at runtime, no npm/build step, consistent
// with this example's no-third-party-JS-framework approach otherwise.
// Datastar is what turns this server-rendered UI into a real-time one: the
// browser's own /events connection (see handleDatastarEvents) receives
// datastar-patch-elements SSE events and morphs them into the DOM by
// element id, with no hand-written JS anywhere in this example.
//
//go:embed static/datastar.js
var staticFiles embed.FS

// uiHTTP holds what UIServerActor's HTTP handlers need: sys to Tell "link"
// (switch) and query "ui" itself (get_snapshot/subscribe_browser), and
// serverAddr for the direct, synchronous calls to the remote workspace
// server (listing conversations for the sidebar, and POST /send) that --
// like get_snapshot -- have no lifecycle of their own worth modeling as an
// actor.
type uiHTTP struct {
	sys        *actors.System
	serverAddr string
}

// buildRunHTTPServer returns UIServerActor's own Invoke: a local HTTP
// server on a random loopback port, printed to stdout, serving for as long
// as "ui" is active.
func buildRunHTTPServer(sys *actors.System, serverAddr string) statecharts.InvokeFunc {
	h := &uiHTTP{sys: sys, serverAddr: serverAddr}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", h.handleIndex)
	mux.HandleFunc("GET /events", h.handleDatastarEvents)
	mux.HandleFunc("POST /conversations", h.handleCreate)
	mux.HandleFunc("POST /send", h.handleSend)
	mux.HandleFunc("GET /favicon.ico", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.Handle("GET /static/", http.FileServerFS(staticFiles))

	return func(ctx context.Context, params any, io statecharts.InvokeIO) (any, error) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		fmt.Printf("ai-agent UI: http://%s/\n", ln.Addr())

		srv := &http.Server{Handler: mux}
		errCh := make(chan error, 1)
		go func() { errCh <- srv.Serve(ln) }()

		select {
		case <-ctx.Done():
			_ = srv.Shutdown(context.Background())
			return nil, nil
		case err := <-errCh:
			if err != nil && err != http.ErrServerClosed {
				return nil, err
			}
			return nil, nil
		}
	}
}

// remoteURL joins path segments onto h.serverAddr, e.g.
// remoteURL("conversations", id, "messages").
func (h *uiHTTP) remoteURL(segments ...string) (*url.URL, error) {
	u, err := url.Parse(h.serverAddr)
	if err != nil {
		return nil, err
	}
	return u.JoinPath(segments...), nil
}

// conversationLink is this UI's own local link to select or return to
// conversation id, e.g. for a sidebar entry or a post-action redirect.
func conversationLink(id string) string {
	u := &url.URL{Path: "/"}
	if id != "" {
		u.RawQuery = url.Values{"conversation": {id}}.Encode()
	}
	return u.String()
}

func (h *uiHTTP) fetchConversations(ctx context.Context) ([]protocol.ConversationSummary, error) {
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	u, err := h.remoteURL("conversations")
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var items []protocol.ConversationSummary
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		return nil, err
	}
	return items, nil
}

func (h *uiHTTP) handleIndex(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var snap uiSnapshot
	var err error
	if convParam := r.URL.Query().Get("conversation"); convParam != "" {
		id := protocol.ConversationID(convParam)
		// Tell "link" to actually redial SSE for the new conversation --
		// asynchronously, from link's own perspective, forwarding its own
		// "conversation_switched" to "ui" once it gets around to actually
		// processing the "switch" (see link.go's handleSwitch). This
		// handler doesn't wait on that: it separately asks "ui" itself,
		// via switch_and_snapshot, to apply the exact same reset
		// (idempotent -- see ui.go's resetForSwitch) and hand back the
		// resulting snapshot in one atomic round trip, so the page
		// rendered in THIS response already reflects the new conversation
		// without this handler ever emitting "conversation_switched"
		// itself -- that stays exclusively LinkActor's own notification.
		_ = h.sys.Tell(ctx, "link", statecharts.Event{
			Name: "switch", Type: statecharts.EventExternal,
			Data: switchRequest{ConversationID: id},
		})
		snap, err = getUISnapshotForSwitch(ctx, h.sys, id)
	} else {
		snap, err = getUISnapshot(ctx, h.sys)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if snap.Conversations == nil {
		// directorylink's first push normally lands well before any
		// browser ever requests a page, but on a cold start there's a
		// narrow window where it hasn't yet -- fall back to a one-off
		// direct fetch for this single render rather than showing an
		// empty sidebar. Not a recurring poll: this whole function only
		// runs once per full page navigation.
		if convos, convErr := h.fetchConversations(ctx); convErr == nil {
			snap.Conversations = convos
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = renderPage(snap).WriteHTML(w)
}

// handleDatastarEvents is this client's own real-time channel to the
// browser: one persistent SSE connection per open tab (started by
// data-init="@get('/events')" on <body> -- see renderPage), subscribed to
// UIServerActor's own live pushes -- message/delta/tool-call/link-status
// changes (see ui.go's broadcast), and now also the sidebar, kept current
// by directorylink's own single upstream subscription (see ui.go's
// applyDirectorySnapshot) -- so this handler is pure push, no polling of
// any kind, and opening more browser tabs never opens more connections to
// the workspace server.
func (h *uiHTTP) handleDatastarEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	reply := make(chan chan string, 1)
	if err := h.sys.Tell(ctx, "ui", statecharts.Event{
		Name: "subscribe_browser", Type: statecharts.EventExternal, Data: browserSubscribeRequest{Reply: reply},
	}); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	var patches chan string
	select {
	case patches = <-reply:
	case <-ctx.Done():
		return
	}
	defer func() {
		_ = h.sys.Tell(context.Background(), "ui", statecharts.Event{
			Name: "unsubscribe_browser", Type: statecharts.EventExternal, Data: browserUnsubscribeRequest{Channel: patches},
		})
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)

	writeAll := func() {
		snap, _ := getUISnapshot(ctx, h.sys)
		_, _ = fmt.Fprint(w, datastarPatch(renderToString(renderLinkBanner(snap.LinkStatus))))
		_, _ = fmt.Fprint(w, datastarPatch(renderToString(renderSidebar(snap))))
		_, _ = fmt.Fprint(w, datastarPatch(renderToString(renderMain(snap))))
		if flusher != nil {
			flusher.Flush()
		}
	}
	// Prime a freshly opened tab with everything, immediately: it may have
	// connected between two of ui's own pushes (most commonly, the link
	// finishing its own connect/reconnect before this tab's own /events
	// request ever reached ui -- see BuildLinkChart's own "connected"
	// handler, which only fires once per transition, not once per tab), so
	// its very first paint (server-rendered by handleIndex) could already
	// be stale by the time this stream goes live.
	writeAll()

	for {
		select {
		case frame, ok := <-patches:
			if !ok {
				return
			}
			_, _ = fmt.Fprint(w, frame)
			if flusher != nil {
				flusher.Flush()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (h *uiHTTP) handleCreate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	title := r.FormValue("title")

	body, err := json.Marshal(protocol.CreateConversationRequest{Title: title})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	u, err := h.remoteURL("conversations")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, u.String(), strings.NewReader(string(body)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	var created protocol.CreateConversationResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	_ = h.sys.Tell(r.Context(), "link", statecharts.Event{
		Name: "switch", Type: statecharts.EventExternal,
		Data: switchRequest{ConversationID: created.ID},
	})
	http.Redirect(w, r, conversationLink(created.ID.String()), http.StatusSeeOther)
}

// handleSend is called via Datastar's own @post action (see .send-form's
// data-on:submit in renderMain), not a native form POST: submitting a
// message must never trigger a full page navigation, since the live SSE
// push (UIServerActor's own pushMain, see ui.go) already lands the new
// message -- in every open tab, including this one -- well before a
// redirect-and-reload round trip would. So this handler responds with a
// bare 204 (or a 4xx on a genuine error) rather than http.Redirect: the
// browser's fetch just resolves, the SSE push does the actual UI update,
// and .send-form's own data-on:submit clears the input client-side.
func (h *uiHTTP) handleSend(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	convID := r.FormValue("conversation")
	text := r.FormValue("text")
	if convID == "" || text == "" {
		http.Error(w, "conversation and text are required", http.StatusBadRequest)
		return
	}

	body, err := json.Marshal(protocol.SendMessageRequest{Text: text})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	u, err := h.remoteURL("conversations", convID, "messages")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, u.String(), strings.NewReader(string(body)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		http.Error(w, fmt.Sprintf("client: POST %s: unexpected status %s", u, resp.Status), http.StatusBadGateway)
		return
	}

	// No redirect, no body: the live SSE push (pushMain, triggered by the
	// remote workspace server's own reply to the POST above) is what
	// actually updates every open tab's transcript, this one included.
	// Datastar's fetch actions treat a bare 204 as a clean success (no
	// retry, no console warning) -- see Te's status handling in
	// datastar.js.
	w.WriteHeader(http.StatusNoContent)
}

// pageCSS is the whole UI's styling: plain CSS, no build step, matching
// this example's no-build-step approach otherwise (see internal/htmlutil,
// and the Datastar vendoring above).
const pageCSS = `
:root {
	/* Base text/surface colors: an earthy/forestry palette -- warm soil
	   browns for ink, parchment/paper tones for surfaces, khaki borders --
	   rather than a neutral gray scale. --color-surface sits a shade
	   lighter than --color-bg (paper vs. the room it's sitting in) so
	   cards/bubbles still lift off the page without resorting to true
	   white, which would read as sterile/tech against the rest of this
	   palette. */
	--color-text: #2b2418;
	--color-text-secondary: #5c5240;
	--color-text-subtle: #8a7d63;
	--color-bg: #f3efe2;
	--color-surface: #faf7ee;
	--color-surface-hover: #e9e2cd;
	--color-border: #d8cdae;
	--color-border-input: #b7a97e;
	--color-overlay: rgba(43, 36, 24, .45);
	--color-shadow: rgba(43, 36, 24, .18);

	/* Semantic status colors, each an -fg/-bg pair: four hues spaced around
	   the wheel (olive/ochre/rust/wine) so no two are mistakable even when
	   several are visible at once, each fg chosen dark/saturated enough to
	   read clearly against its own bg tint (not against the page surface --
	   these are always used as a tinted badge or banner, never bare text).
	   --color-success is used for both the network link's "connected"
	   state and a conversation's "idle" badge -- deliberately the same
	   color, since both mean "all good" and never appear together in a way
	   that would confuse the two. --color-danger, by contrast, is split
	   into a "network" and a "conversation" variant: LinkActor's own
	   reconnect banner and a conversation's awaiting-tool badge are both
	   "something's blocked" states that CAN be on screen at the same time
	   (banner up top, badge in the sidebar), so they get distinct hues --
	   rust (orange-leaning red) for the link, wine (purple-leaning red)
	   for the conversation -- rather than one color shared by both.
	   --color-success (olive/leaf green) is deliberately more yellow than
	   --color-accent (pine) below, so the two greens in this palette stay
	   distinguishable rather than reading as "the same green, twice." */
	--color-success-bg: #e3ecd0;
	--color-success-fg: #4a7c2f;
	--color-warning-bg: #f0e0b0;
	--color-warning-fg: #8a5a1f;
	--color-info-bg: #e6e0cd;
	--color-info-fg: #6b5f47;
	--color-danger-network-bg: #edd6c4;
	--color-danger-network-fg: #a1442b;
	--color-danger-conversation-bg: #e8d3d6;
	--color-danger-conversation-fg: #7a2e3d;

	/* Accent: the currently-selected conversation link and the user's own
	   chat bubbles. A deep pine/forest green, blue-leaning enough to read
	   distinctly from --color-success's warmer olive above. -hover/-active
	   are fixed, pre-mixed shades (rather than a runtime color-mix()) so a
	   primary button's hover/press states stay token-driven instead of
	   one-off hex, without depending on color-mix support. */
	--color-accent-bg: #dde6cf;
	--color-accent-fg: #2f5233;
	--color-accent-fg-hover: #24402a;
	--color-accent-fg-active: #1a2f1e;

	/* One-off surfaces that don't fit a status/accent semantic. */
	--color-bubble-assistant-bg: #eae2c9;
	--color-bubble-tool-bg: #f2e8c8;

	/* Typographic scale: four sizes tied to semantic roles (meta labels,
	   secondary/dense text, body copy, and the one mobile-only size that
	   isn't really about hierarchy at all) rather than the six-odd ad hoc
	   pixel values this used to be. --font-size-lg stays pinned at 16px
	   for a non-negotiable reason: iOS Safari zooms the page in on focus
	   of any input below 16px, so the mobile media query below reuses
	   this token for text inputs instead of picking its own "large"
	   value -- don't lower it. */
	--font-size-xs: 11px;
	--font-size-sm: 13px;
	--font-size-base: 14px;
	--font-size-lg: 16px;
	--line-height-base: 1.45;

	/* Spacing scale: a 4px base grid, replacing what used to be a long tail
	   of near-arbitrary padding/margin/gap literals (1px, 2px, 6px, 7px,
	   9px, 10px, 14px, 18px, ...) with six steps that every spacing
	   declaration in this file now snaps to the nearest of. This
	   deliberately merges some previously-distinct values -- e.g.
	   .new-form's old gap: 4px and .send-form's old gap: 6px both round to
	   --space-1 below, which is fine (arguably a bugfix: two nearly
	   identical forms no longer differ in internal spacing for no reason). */
	--space-1: 4px;
	--space-2: 8px;
	--space-3: 12px;
	--space-4: 16px;
	--space-5: 20px;
	--space-6: 24px;

	/* Radius scale: every corner in the app is square by design (an
	   intentional, considered choice -- not an oversight), so all four
	   steps are 0. Kept as a scale (rather than deleting border-radius
	   from every rule) so every component still references a role
	   (--radius-sm for inputs/buttons, --radius-pill for badges, etc.)
	   instead of a bare 0 sprinkled everywhere -- if a future pass ever
	   wants rounding back, or wants it on some elements but not others,
	   there's one place to change it per role rather than a grep-and-edit
	   across the whole file. */
	--radius-sm: 0;
	--radius-md: 0;
	--radius-lg: 0;
	--radius-pill: 0;

	/* Elevation scale: subtle, restrained shadows -- reusing --color-shadow
	   as the one shared shadow color rather than introducing new literals.
	   --shadow-sm is for surfaces that sit just barely above the page
	   (topbar, buttons, bubbles); --shadow-md is for genuinely floating
	   surfaces (the sidebar dialog); --shadow-focus is the accent-colored
	   ring used for :focus-visible everywhere, in place of the browser's
	   default outline -- a fixed rgba() matching --color-accent-fg (#2f5233)
	   at 30% opacity (a touch stronger than a typical bright-accent ring,
	   since this pine green needs more opacity than a vivid blue/violet
	   would to still read clearly against the parchment surfaces), since
	   CSS custom properties can't derive an alpha variant of another
	   property's color without color-mix(), which isn't worth the support
	   risk here. */
	--shadow-sm: 0 1px 2px var(--color-shadow);
	--shadow-md: 0 12px 32px -8px var(--color-shadow);
	--shadow-focus: 0 0 0 3px rgba(47, 82, 51, .3);

	/* Transition timing: one shared duration/easing so every interactive
	   element animates the same way instead of some snapping and some
	   easing. */
	--transition-fast: 150ms ease;
}
* { box-sizing: border-box; }
html, body { height: 100%; }
body {
	margin: 0; display: flex; flex-direction: column;
	font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
	font-size: var(--font-size-base); line-height: var(--line-height-base);
	color: var(--color-text); background: var(--color-bg);
}
::selection { background: var(--color-accent-fg); color: #fff; }
/* One shared focus treatment for every interactive element: a soft accent
   ring via box-shadow rather than the browser's default outline, which
   varies wildly between browsers and often gets clipped by overflow:hidden
   ancestors. Applied via :focus-visible so mouse clicks don't show a ring,
   only keyboard/AT focus does -- never removed without this replacement. */
.sidebar-toggle:focus-visible, .dialog-close:focus-visible,
.conv-link:focus-visible, .new-form button:focus-visible,
.send-form button:focus-visible {
	outline: none; box-shadow: var(--shadow-focus);
}
.topbar {
	flex: none; padding: var(--space-2) var(--space-4); background: var(--color-surface);
	box-shadow: var(--shadow-sm); position: relative; z-index: 1;
}
.sidebar-toggle, .dialog-close {
	padding: var(--space-1) var(--space-3); border-radius: var(--radius-sm); border: 1px solid var(--color-border-input); background: var(--color-surface);
	cursor: pointer; font-size: var(--font-size-sm); color: var(--color-text-secondary);
	box-shadow: var(--shadow-sm);
	transition: background var(--transition-fast), border-color var(--transition-fast), box-shadow var(--transition-fast), transform var(--transition-fast);
}
.sidebar-toggle:hover, .dialog-close:hover { background: var(--color-surface-hover); border-color: var(--color-text-subtle); box-shadow: var(--shadow-md); }
.sidebar-toggle:active, .dialog-close:active { transform: translateY(1px); box-shadow: none; }
.link-banner {
	flex: none; padding: var(--space-1) var(--space-4); font-size: var(--font-size-xs); font-weight: 600;
	text-align: center; letter-spacing: .02em;
}
.link-connected { background: var(--color-success-bg); color: var(--color-success-fg); }
.link-connecting, .link-idle { background: var(--color-info-bg); color: var(--color-info-fg); }
.link-reconnecting { background: var(--color-danger-network-bg); color: var(--color-danger-network-fg); animation: pulse 1s ease-in-out infinite; }
@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: .55; } }

/* The conversation list lives behind .sidebar-toggle, in this native
   <dialog> -- rather than a permanent column -- so it scales the same way
   whether the workspace has 3 conversations or 300, and on any screen size
   without a separate mobile layout for it. Only its own child #sidebar
   (see renderSidebar) is ever replaced by a live Datastar patch, never this
   <dialog> element itself, so an open/closed dialog's own state survives
   every sidebar update untouched. */
.sidebar-dialog {
	padding: var(--space-4); border: none; border-radius: var(--radius-lg); box-shadow: var(--shadow-md);
	width: min(340px, 92vw); max-height: 80vh; margin: auto;
}
/* A <dialog> with no "open" attribute is display:none by the UA stylesheet
   already -- flex only needs to apply once showModal() sets [open], or it
   would override that default and the dialog would render permanently
   visible regardless of open/closed state. */
.sidebar-dialog[open] { display: flex; flex-direction: column; }
.sidebar-dialog::backdrop { background: var(--color-overlay); }
.sidebar { display: flex; flex-direction: column; flex: 1 1 auto; min-height: 0; }
.sidebar h3 { margin: 0 0 var(--space-2); font-size: var(--font-size-sm); text-transform: uppercase; letter-spacing: .04em; color: var(--color-text-subtle); }
.conv-filter {
	padding: var(--space-2); margin-bottom: var(--space-2); border: 1px solid var(--color-border-input); border-radius: var(--radius-sm);
	flex: none; font-size: var(--font-size-base); background: var(--color-surface); color: var(--color-text);
	transition: border-color var(--transition-fast), box-shadow var(--transition-fast);
}
.conv-filter:focus, .conv-filter:focus-visible {
	outline: none; border-color: var(--color-accent-fg); box-shadow: var(--shadow-focus);
}
.sidebar-list { overflow-y: auto; flex: 1 1 auto; min-height: 0; }
/* .placeholder's own rule (below, shared with #message-list) only sets
   vertical padding; give it the same horizontal inset as .conv-link here so
   the "no conversations yet" message lines up with where rows would sit
   instead of touching the sidebar's edges. */
.sidebar-list .placeholder { padding: var(--space-2); }
.conv-link {
	display: block; padding: var(--space-2); border-radius: var(--radius-sm); text-decoration: none;
	color: var(--color-text-secondary); margin-bottom: var(--space-1); overflow-wrap: anywhere;
	border-left: 3px solid transparent;
	transition: background var(--transition-fast), border-color var(--transition-fast), color var(--transition-fast);
}
.conv-link:hover { background: var(--color-surface-hover); }
.conv-link.active { background: var(--color-accent-bg); color: var(--color-accent-fg); font-weight: 600; border-left-color: var(--color-accent-fg); }
.badge {
	display: inline-block; padding: var(--space-1) var(--space-2); border-radius: var(--radius-pill); font-size: var(--font-size-xs);
	margin-left: var(--space-1); white-space: nowrap; font-weight: 600; letter-spacing: .02em;
}
.badge-idle { background: var(--color-success-bg); color: var(--color-success-fg); }
.badge-thinking { background: var(--color-warning-bg); color: var(--color-warning-fg); }
.badge-awaiting_tool { background: var(--color-danger-conversation-bg); color: var(--color-danger-conversation-fg); }
.new-form { margin-top: var(--space-3); display: flex; gap: var(--space-2); flex: none; }
.new-form input {
	flex: 1; min-width: 0; padding: var(--space-2); font-size: var(--font-size-base);
	border: 1px solid var(--color-border-input); border-radius: var(--radius-sm); background: var(--color-surface); color: var(--color-text);
	transition: border-color var(--transition-fast), box-shadow var(--transition-fast);
}
.new-form input:focus, .new-form input:focus-visible {
	outline: none; border-color: var(--color-accent-fg); box-shadow: var(--shadow-focus);
}
.new-form button {
	flex: none; padding: var(--space-2) var(--space-3); border: none; border-radius: var(--radius-sm);
	background: var(--color-accent-fg); color: #fff; font-weight: 600; box-shadow: var(--shadow-sm);
	transition: background var(--transition-fast), box-shadow var(--transition-fast), transform var(--transition-fast);
}
.new-form button:hover { background: var(--color-accent-fg-hover); box-shadow: var(--shadow-md); }
.new-form button:active { background: var(--color-accent-fg-active); transform: translateY(1px); box-shadow: none; }
.dialog-close { margin-top: var(--space-3); flex: none; align-self: flex-end; }
.layout { flex: 1; min-height: 0; display: flex; }
/* .main is a full-height flex column (its own height comes from .layout's
   flex: 1 inside a min-height:0 ancestor chain up to <body>, itself a
   height:100% flex column -- see html, body above): #message-list is the
   ONLY child that scrolls (flex: 1; min-height: 0; overflow-y: auto), so
   .send-form -- a flex: none sibling, not a flow child of that scrolling
   region -- stays pinned to the bottom of the viewport no matter how long
   the transcript grows. This needs renderMain to wrap the message bubbles
   in their own #message-list div, distinct from the form, rather than the
   older flat list of bubbles-then-form as direct children of #main. */
.main { flex: 1; min-height: 0; max-width: 800px; margin: 0 auto; width: 100%; display: flex; flex-direction: column; }
#message-list { flex: 1; min-height: 0; overflow-y: auto; overflow-wrap: break-word; padding: var(--space-5) var(--space-6); }
.placeholder { color: var(--color-text-subtle); padding: var(--space-2) 0; }
/* .bubble-user/.bubble-assistant are plain-language chat content, so they
   get --radius-lg plus a barely-there lift (--shadow-sm) to read as
   distinct surfaces rather than flat paint swatches. .bubble-tool and
   .bubble-toolcall are conceptually code blocks -- monospace, structured
   data -- so instead of (or in addition to) the shadow they get a thin
   border, closer to how a code fence reads than a chat bubble. */
.bubble { margin: var(--space-2) 0; padding: var(--space-2) var(--space-3); border-radius: var(--radius-lg); overflow-wrap: break-word; white-space: pre-wrap; }
.bubble-role { font-size: var(--font-size-xs); text-transform: uppercase; letter-spacing: .03em; opacity: .6; display: block; margin-bottom: var(--space-1); }
.bubble-user { background: var(--color-accent-bg); box-shadow: var(--shadow-sm); }
.bubble-assistant { background: var(--color-bubble-assistant-bg); box-shadow: var(--shadow-sm); }
.bubble-tool {
	background: var(--color-bubble-tool-bg); border: 1px solid var(--color-border);
	font-family: ui-monospace, Menlo, Consolas, monospace; font-size: var(--font-size-sm);
}
.bubble-thinking { color: var(--color-text-subtle); font-style: italic; background: transparent; padding-left: 0; }
.bubble-toolcall {
	background: var(--color-warning-bg); color: var(--color-warning-fg); border: 1px solid var(--color-warning-fg);
	font-family: ui-monospace, Menlo, Consolas, monospace; font-size: var(--font-size-sm);
}
/* flex: none (not a scrolling flow child of #message-list) plus its own
   background + border-top is what keeps the composer legible and pinned
   below the transcript instead of scrolling away with it. */
.send-form { flex: none; display: flex; gap: var(--space-2); padding: var(--space-3) var(--space-6) var(--space-4); border-top: 1px solid var(--color-border); background: var(--color-bg); }
.send-form input[type=text] {
	flex: 1; min-width: 0; padding: var(--space-2); font-size: var(--font-size-base);
	border: 1px solid var(--color-border-input); border-radius: var(--radius-sm); background: var(--color-surface); color: var(--color-text);
	transition: border-color var(--transition-fast), box-shadow var(--transition-fast);
}
.send-form input[type=text]:focus, .send-form input[type=text]:focus-visible {
	outline: none; border-color: var(--color-accent-fg); box-shadow: var(--shadow-focus);
}
.send-form button {
	flex: none; padding: var(--space-2) var(--space-4); border: none; border-radius: var(--radius-sm);
	background: var(--color-accent-fg); color: #fff; font-weight: 600; box-shadow: var(--shadow-sm);
	transition: background var(--transition-fast), box-shadow var(--transition-fast), transform var(--transition-fast);
}
.send-form button:hover { background: var(--color-accent-fg-hover); box-shadow: var(--shadow-md); }
.send-form button:active { background: var(--color-accent-fg-active); transform: translateY(1px); box-shadow: none; }
/* Form controls don't inherit body's font-size the way ordinary flow
   elements (like .bubble) do -- browsers give <button>/<input> their own
   UA-stylesheet default (commonly ~13.3px) that ignores the page's typeface
   otherwise, so this has to be set explicitly to land buttons on the same
   base tier as everything else instead of that silently mismatched default. */
button { cursor: pointer; font-size: var(--font-size-base); }

@media (max-width: 700px) {
	#message-list { padding: var(--space-3) var(--space-4); }
	.send-form { padding: var(--space-2) var(--space-4) var(--space-3); }
	/* 16px keeps iOS Safari from zooming the page in on focus -- that's
	   --font-size-lg, referenced here rather than repeated as a literal,
	   but still exactly 16px. */
	.new-form input, .send-form input[type=text], .conv-filter { font-size: var(--font-size-lg); padding: var(--space-2); }
	.new-form button, .send-form button { padding: var(--space-2) var(--space-3); font-size: var(--font-size-lg); }
}
`

// renderToString flattens el to a string -- what handleDatastarEvents
// needs to embed a fragment inside a "data: elements ..." SSE line (see
// ui.go's datastarPatch).
func renderToString(el htmlutil.HTMLElement) string {
	var sb strings.Builder
	_ = el.WriteHTML(&sb)
	return sb.String()
}

// linkStatusLabel renders LinkActor's own connection state as a short,
// human phrase.
func linkStatusLabel(status string) string {
	switch status {
	case "connected":
		return "● connected to server"
	case "connecting":
		return "connecting to server…"
	case "reconnecting":
		return "⚠ reconnecting to server…"
	case "idle":
		return "no conversation selected"
	default:
		return "connecting to server…"
	}
}

// renderLinkBanner is this client's own connection indicator: always
// visible (unlike the old per-conversation-pane text it replaces), so it's
// exactly as informative on the "select a conversation" placeholder screen
// as it is inside a transcript -- the whole point of a server-down /
// reconnect test is to watch this flip, not to have it buried below a
// conversation you may not even have open yet. Its own id is what lets
// Datastar morph a live update into place (see ui.go's recordLinkStatus).
func renderLinkBanner(status string) *htmlutil.Element {
	if status == "" {
		// The brief window before "link" (LinkActor) reports its own real
		// state -- see ui.go's newUIModel -- looks the same as its actual
		// initial state, "idle": neither one means a dial is in flight.
		status = "idle"
	}
	return htmlutil.New("div", map[string]string{"id": "link-banner", "class": "link-banner link-" + status},
		htmlutil.Text(linkStatusLabel(status)))
}

func renderPage(snap uiSnapshot) *htmlutil.Element {
	head := []htmlutil.HTMLElement{
		htmlutil.New("title", nil, htmlutil.Text("ai-agent")),
		htmlutil.New("meta", map[string]string{"charset": "utf-8"}),
		htmlutil.New("meta", map[string]string{"name": "viewport", "content": "width=device-width, initial-scale=1"}),
		htmlutil.New("style", nil, htmlutil.Raw(pageCSS)),
		htmlutil.New("script", map[string]string{"type": "module", "src": "/static/datastar.js"}),
	}
	return htmlutil.New("html", nil,
		htmlutil.New("head", nil, head...),
		// data-init runs once, as soon as this attribute lands in the DOM,
		// opening this tab's own real-time /events connection -- see
		// handleDatastarEvents and https://data-star.dev/reference/sse_events.
		htmlutil.New("body", map[string]string{"data-init": "@get('/events')"},
			htmlutil.New("div", map[string]string{"class": "topbar"},
				htmlutil.New("button", map[string]string{
					"type": "button", "class": "sidebar-toggle",
					"data-on:click": "document.getElementById('sidebar-dialog').showModal()",
				}, htmlutil.Text("☰ Conversations")),
			),
			renderLinkBanner(snap.LinkStatus),
			// The <dialog> itself is static page structure, rendered once
			// per navigation -- only its #sidebar child is ever replaced by
			// a live patch (see renderSidebar), so opening/closing it is
			// never disturbed by a sidebar update arriving while it's open.
			htmlutil.New("dialog", map[string]string{"id": "sidebar-dialog", "class": "sidebar-dialog"},
				renderSidebar(snap),
				htmlutil.New("button", map[string]string{
					"type": "button", "class": "dialog-close",
					"data-on:click": "document.getElementById('sidebar-dialog').close()",
				}, htmlutil.Text("Close")),
			),
			htmlutil.New("div", map[string]string{"class": "layout"},
				renderMain(snap),
			),
		),
	)
}

// stateLabel renders a conversation's state as a short, human phrase
// rather than the raw wire enum value.
func stateLabel(s protocol.ConversationState) string {
	switch s {
	case protocol.ConversationThinking:
		return "thinking"
	case protocol.ConversationAwaitingTool:
		return "running tool"
	default:
		return "idle"
	}
}

// renderSidebar's own id is what lets Datastar morph in a live push every
// time the workspace's conversation list changes -- see ui.go's
// applyDirectorySnapshot/applyDirectoryUpsert/pushSidebar, fed by
// directorylink's single upstream SSE subscription, never a per-tab poll.
// The filter input's data-bind/data-show pair is pure client-side Datastar
// reactivity (see https://data-star.dev/reference/plugins/attributes) --
// filtering a workspace of hundreds of conversations by title never round
// trips to any server.
func renderSidebar(snap uiSnapshot) *htmlutil.Element {
	rows := make([]htmlutil.HTMLElement, 0, len(snap.Conversations))
	for _, c := range snap.Conversations {
		class := "conv-link"
		if c.ID == snap.ConversationID {
			class += " active"
		}
		rows = append(rows, htmlutil.New("a",
			map[string]string{
				"href":       conversationLink(c.ID.String()),
				"class":      class,
				"data-title": strings.ToLower(c.Title),
				"data-show":  "$convfilter=='' || el.dataset.title.includes($convfilter.toLowerCase())",
			},
			htmlutil.Text(c.Title),
			htmlutil.New("span", map[string]string{"class": "badge badge-" + string(c.State)}, htmlutil.Text(stateLabel(c.State))),
		))
	}
	// The workspace-is-empty case is handled here, server-side, since
	// snap.Conversations is already known at render time. A "filter matches
	// nothing" empty state (convfilter typed but no .conv-link's data-show
	// passes) is a related but distinct case -- it'd need its own
	// client-side Datastar reactive expression to show/hide, since filtering
	// itself never round trips to the server (see the data-show comment
	// above). Left as a known gap rather than bolted on here.
	if len(snap.Conversations) == 0 {
		rows = append(rows, htmlutil.New("p", map[string]string{"class": "placeholder"}, htmlutil.Text("No conversations yet -- create one below.")))
	}
	return htmlutil.New("div", map[string]string{"id": "sidebar", "class": "sidebar"},
		htmlutil.New("h3", nil, htmlutil.Text("Conversations")),
		htmlutil.New("input", map[string]string{
			"type": "search", "class": "conv-filter", "placeholder": "Filter conversations…",
			"data-bind": "convfilter",
		}),
		htmlutil.New("div", map[string]string{"class": "sidebar-list"}, rows...),
		// Deliberately left as a native form POST, unlike .send-form: this
		// only reloads once per conversation CREATED, not once per message
		// SENT, so it isn't the wasteful reload .send-form was (the actual
		// bug this file's @post conversion exists to fix). handleCreate's
		// redirect also does real, wanted work here -- switching the
		// browser into the newly created conversation -- which a bare
		// @post response can't do without extra plumbing (e.g. a client
		// script reading the new id back out of the response and doing
		// window.location itself). Not worth the complexity for a
		// once-per-conversation action.
		htmlutil.New("form", map[string]string{"method": "POST", "action": "/conversations", "class": "new-form"},
			htmlutil.New("input", map[string]string{"name": "title", "placeholder": "New conversation title"}),
			htmlutil.New("button", map[string]string{"type": "submit"}, htmlutil.Text("+ New")),
		),
	)
}

func bubble(class, role, text string) *htmlutil.Element {
	return htmlutil.New("div", map[string]string{"class": "bubble " + class},
		htmlutil.New("span", map[string]string{"class": "bubble-role"}, htmlutil.Text(role)),
		htmlutil.Text(text),
	)
}

// renderMain's own id is what lets Datastar morph in a live push from
// ui.go's pushMain every time the open conversation's own state changes.
//
// #main is always exactly two children: #message-list (the only child that
// scrolls -- every bubble, including the placeholder-conversation state,
// lives inside it) and, when a conversation is open, .send-form as a
// flex:none sibling right after it -- see the .main/#message-list/.send-form
// layout in pageCSS, which is what keeps the composer pinned to the bottom
// of the viewport instead of scrolling away with a long transcript.
func renderMain(snap uiSnapshot) *htmlutil.Element {
	if snap.ConversationID == "" {
		return htmlutil.New("div", map[string]string{"id": "main", "class": "main"},
			htmlutil.New("div", map[string]string{"id": "message-list"},
				htmlutil.New("p", map[string]string{"class": "placeholder"}, htmlutil.Text("Select or create a conversation from the sidebar.")),
			),
		)
	}

	var children []htmlutil.HTMLElement
	for _, m := range snap.Messages {
		children = append(children, bubble("bubble-"+string(m.Role), string(m.Role), m.Text))
	}
	if snap.ThinkingDelta != "" {
		children = append(children, bubble("bubble-thinking", "thinking", snap.ThinkingDelta))
	}
	if snap.TextDelta != "" {
		children = append(children, bubble("bubble-assistant", "assistant", snap.TextDelta))
	}
	if snap.PendingToolCall != nil {
		children = append(children, bubble("bubble-toolcall", "tool call",
			fmt.Sprintf("%s %v", snap.PendingToolCall.Name, map[string]any(snap.PendingToolCall.Args))))
	}

	return htmlutil.New("div", map[string]string{"id": "main", "class": "main"},
		htmlutil.New("div", map[string]string{"id": "message-list"}, children...),
		// Submitting is a Datastar @post, not a native form POST: no
		// navigation, no reload. contentType:'form' is required explicitly
		// -- @post's own default is 'json' (see Te's contentType:u="json" in
		// datastar.js), which would send this tab's reactive $signals
		// instead of this form's own fields. submit__prevent stops the
		// browser's native submit-and-navigate outright (Datastar's "on"
		// plugin already auto-prevents default for data-on:submit on a
		// <form>, but this is explicit rather than relying on that).
		// evt.target.reset() clears the text input for the next message --
		// safe to run right after @post(...) with no await, since @post's
		// own FormData read and the underlying fetch() call both happen
		// synchronously before the first await inside it (see Ln/Te in
		// datastar.js). The actual transcript update comes from the live
		// SSE push (pushMain, see ui.go), not from this response.
		htmlutil.New("form", map[string]string{
			"class":                    "send-form",
			"data-on:submit__prevent": "@post('/send', {contentType: 'form'}); evt.target.reset()",
		},
			htmlutil.New("input", map[string]string{"type": "hidden", "name": "conversation", "value": snap.ConversationID.String()}),
			htmlutil.New("input", map[string]string{"type": "text", "name": "text", "required": "required", "autofocus": "autofocus", "autocomplete": "off"}),
			htmlutil.New("button", map[string]string{"type": "submit"}, htmlutil.Text("Send")),
		),
	)
}
