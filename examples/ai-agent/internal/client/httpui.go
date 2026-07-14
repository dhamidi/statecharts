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

func (h *uiHTTP) handleSend(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	convID := r.FormValue("conversation")
	text := r.FormValue("text")
	if convID == "" || text == "" {
		http.Redirect(w, r, conversationLink(convID), http.StatusSeeOther)
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

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, u.String(), strings.NewReader(string(body)))
		if err != nil {
			lastErr = err
			break
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			lastErr = fmt.Errorf("client: POST %s: unexpected status %s", u, resp.Status)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		lastErr = nil
		break
	}
	if lastErr != nil {
		http.Error(w, lastErr.Error(), http.StatusBadGateway)
		return
	}

	http.Redirect(w, r, conversationLink(convID), http.StatusSeeOther)
}

// pageCSS is the whole UI's styling: plain CSS, no build step, matching
// this example's no-build-step approach otherwise (see internal/htmlutil,
// and the Datastar vendoring above).
const pageCSS = `
* { box-sizing: border-box; }
html, body { height: 100%; }
body {
	margin: 0; display: flex; flex-direction: column;
	font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
	color: #1a1a1a; background: #fafafa;
}
.topbar { flex: none; padding: 8px 16px; background: #fff; border-bottom: 1px solid #e0e0e0; }
.sidebar-toggle, .dialog-close {
	padding: 6px 12px; border-radius: 6px; border: 1px solid #ccc; background: #fff;
	cursor: pointer; font-size: 14px;
}
.sidebar-toggle:hover, .dialog-close:hover { background: #f3f3f3; }
.link-banner {
	flex: none; padding: 6px 16px; font-size: 12px; font-weight: 600;
	text-align: center; letter-spacing: .02em;
}
.link-connected { background: #e6f4ea; color: #1e7e34; }
.link-connecting { background: #eee; color: #666; }
.link-reconnecting { background: #fde8e8; color: #c0392b; animation: pulse 1s ease-in-out infinite; }
@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: .55; } }

/* The conversation list lives behind .sidebar-toggle, in this native
   <dialog> -- rather than a permanent column -- so it scales the same way
   whether the workspace has 3 conversations or 300, and on any screen size
   without a separate mobile layout for it. Only its own child #sidebar
   (see renderSidebar) is ever replaced by a live Datastar patch, never this
   <dialog> element itself, so an open/closed dialog's own state survives
   every sidebar update untouched. */
.sidebar-dialog {
	padding: 16px; border: none; border-radius: 10px; box-shadow: 0 8px 30px rgba(0,0,0,.25);
	width: min(340px, 92vw); max-height: 80vh; margin: auto;
}
/* A <dialog> with no "open" attribute is display:none by the UA stylesheet
   already -- flex only needs to apply once showModal() sets [open], or it
   would override that default and the dialog would render permanently
   visible regardless of open/closed state. */
.sidebar-dialog[open] { display: flex; flex-direction: column; }
.sidebar-dialog::backdrop { background: rgba(0,0,0,.35); }
.sidebar { display: flex; flex-direction: column; flex: 1 1 auto; min-height: 0; }
.sidebar h3 { margin: 0 0 10px; font-size: 13px; text-transform: uppercase; letter-spacing: .04em; color: #888; }
.conv-filter { padding: 7px 9px; margin-bottom: 10px; border: 1px solid #ccc; border-radius: 6px; flex: none; }
.sidebar-list { overflow-y: auto; flex: 1 1 auto; min-height: 0; }
.conv-link {
	display: block; padding: 8px 10px; border-radius: 6px; text-decoration: none;
	color: #333; margin-bottom: 2px; overflow-wrap: anywhere;
}
.conv-link.active { background: #e8f0fe; color: #1a56db; font-weight: 600; }
.badge { display: inline-block; padding: 1px 7px; border-radius: 10px; font-size: 11px; margin-left: 4px; white-space: nowrap; }
.badge-idle { background: #e6f4ea; color: #1e7e34; }
.badge-thinking { background: #fff4e5; color: #a5600a; }
.badge-awaiting_tool { background: #fde8e8; color: #c0392b; }
.new-form { margin-top: 12px; display: flex; gap: 4px; flex: none; }
.new-form input { flex: 1; min-width: 0; padding: 6px; }
.dialog-close { margin-top: 12px; flex: none; align-self: flex-end; }
.layout { flex: 1; min-height: 0; display: flex; }
.main { flex: 1; padding: 20px 24px; max-width: 800px; margin: 0 auto; overflow-wrap: break-word; overflow-y: auto; }
.placeholder { color: #888; padding: 8px 0; }
.bubble { margin: 8px 0; padding: 8px 12px; border-radius: 10px; overflow-wrap: break-word; white-space: pre-wrap; }
.bubble-role { font-size: 11px; text-transform: uppercase; letter-spacing: .03em; opacity: .6; display: block; margin-bottom: 2px; }
.bubble-user { background: #e8f0fe; }
.bubble-assistant { background: #f0f0f0; }
.bubble-tool { background: #fff8e1; font-family: ui-monospace, Menlo, Consolas, monospace; font-size: 13px; }
.bubble-thinking { color: #888; font-style: italic; background: transparent; padding-left: 0; }
.bubble-toolcall { background: #fff4e5; color: #a5600a; font-family: ui-monospace, Menlo, Consolas, monospace; font-size: 13px; }
.send-form { display: flex; gap: 6px; margin-top: 18px; }
.send-form input[type=text] { flex: 1; min-width: 0; padding: 8px; }
button { cursor: pointer; }

@media (max-width: 700px) {
	.main { padding: 14px 16px; }
	/* 16px keeps iOS Safari from zooming the page in on focus. */
	.new-form input, .send-form input[type=text], .conv-filter { font-size: 16px; padding: 10px; }
	.new-form button, .send-form button { padding: 10px 14px; font-size: 16px; }
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
	case "reconnecting":
		return "⚠ reconnecting to server…"
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
	return htmlutil.New("div", map[string]string{"id": "sidebar", "class": "sidebar"},
		htmlutil.New("h3", nil, htmlutil.Text("Conversations")),
		htmlutil.New("input", map[string]string{
			"type": "search", "class": "conv-filter", "placeholder": "Filter conversations…",
			"data-bind": "convfilter",
		}),
		htmlutil.New("div", map[string]string{"class": "sidebar-list"}, rows...),
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
func renderMain(snap uiSnapshot) *htmlutil.Element {
	if snap.ConversationID == "" {
		return htmlutil.New("div", map[string]string{"id": "main", "class": "main"},
			htmlutil.New("p", map[string]string{"class": "placeholder"}, htmlutil.Text("Select or create a conversation from the sidebar.")),
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

	children = append(children,
		htmlutil.New("form", map[string]string{"method": "POST", "action": "/send", "class": "send-form"},
			htmlutil.New("input", map[string]string{"type": "hidden", "name": "conversation", "value": snap.ConversationID.String()}),
			htmlutil.New("input", map[string]string{"type": "text", "name": "text", "autofocus": "autofocus", "autocomplete": "off"}),
			htmlutil.New("button", map[string]string{"type": "submit"}, htmlutil.Text("Send")),
		),
	)

	return htmlutil.New("div", map[string]string{"id": "main", "class": "main"}, children...)
}
