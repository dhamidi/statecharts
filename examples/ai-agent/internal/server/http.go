package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// Server holds the HTTP handlers for the workspace server. It has no state
// of its own beyond sys: every durable fact lives in an actor.
type Server struct {
	sys *actors.System
}

// NewServerHandler returns an http.Handler exposing every endpoint the
// example's README documents, backed by sys (already Setup).
func NewServerHandler(sys *actors.System) http.Handler {
	s := &Server{sys: sys}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /conversations", s.handleListConversations)
	mux.HandleFunc("GET /directory/events", s.handleDirectoryEvents)
	mux.HandleFunc("POST /conversations", s.handleCreateConversation)
	mux.HandleFunc("GET /conversations/{id}/events", s.handleEvents)
	mux.HandleFunc("POST /conversations/{id}/messages", s.handleSendMessage)
	mux.HandleFunc("POST /conversations/{id}/tool-result", s.handleToolResult)
	return mux
}

// ensureConversation makes sure conversation id exists (Spawn is idempotent
// for an already-resident or already-known name) and is registered with
// UserActor, whether or not this is genuinely the first request to ever
// mention it -- see the package's own README for why this lazy path is
// safe to call unconditionally.
func (s *Server) ensureConversation(ctx context.Context, idStr string) (protocol.ConversationID, error) {
	id, err := protocol.NewConversationID(idStr)
	if err != nil {
		return "", err
	}
	if err := s.sys.Spawn(ctx, statecharts.Identifier(id), ConversationKind, actors.Durable()); err != nil {
		return "", err
	}
	if err := s.sys.Tell(ctx, "user", statecharts.Event{
		Name: "register", Type: statecharts.EventExternal,
		Data: &registerConversationPayload{
			TypeName: "aiagent.register_conversation",
			Value:    RegisterConversationData{ID: id, Title: "Untitled"},
		},
	}); err != nil {
		return "", err
	}
	return id, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) handleListConversations(w http.ResponseWriter, r *http.Request) {
	reply := make(chan []protocol.ConversationSummary, 1)
	if err := s.sys.Tell(r.Context(), "directory", statecharts.Event{
		Name: "list", Type: statecharts.EventExternal, Data: (chan<- []protocol.ConversationSummary)(reply),
	}); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	select {
	case items := <-reply:
		writeJSON(w, http.StatusOK, items)
	case <-r.Context().Done():
	}
}

// handleDirectoryEvents is a single, long-lived SSE stream of the workspace's
// conversation list: one "list" event with the whole list to prime a fresh
// connection, then one "conversation" event per changed entry thereafter
// (see directory.go's watchDirectory/broadcastUpsert) -- never re-sending
// the whole list just because one entry changed, so this scales with how
// much actually changed, not with how many conversations exist. A client
// process holds exactly one of these regardless of how many browser tabs it
// serves -- see internal/client's directorylink, which multiplexes this one
// upstream connection out to every browser tab's own already-open /events
// stream, so opening more tabs never opens more connections to this server.
func (s *Server) handleDirectoryEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	reply := make(chan chan protocol.ConversationSummary, 1)
	if err := s.sys.Tell(ctx, "directory", statecharts.Event{
		Name: "watch", Type: statecharts.EventExternal, Data: directoryWatchRequest{Reply: reply},
	}); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	var updates chan protocol.ConversationSummary
	select {
	case updates = <-reply:
	case <-ctx.Done():
		return
	}
	defer func() {
		_ = s.sys.Tell(context.Background(), "directory", statecharts.Event{
			Name: "unwatch", Type: statecharts.EventExternal, Data: directoryUnwatchRequest{Channel: updates},
		})
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)

	listReply := make(chan []protocol.ConversationSummary, 1)
	if err := s.sys.Tell(ctx, "directory", statecharts.Event{
		Name: "list", Type: statecharts.EventExternal, Data: (chan<- []protocol.ConversationSummary)(listReply),
	}); err == nil {
		select {
		case items := <-listReply:
			writeSSEFrame(w, sseFrame{Event: "list", Data: items})
			if flusher != nil {
				flusher.Flush()
			}
		case <-ctx.Done():
			return
		}
	}

	for {
		select {
		case cs, ok := <-updates:
			if !ok {
				return
			}
			writeSSEFrame(w, sseFrame{Event: "conversation", Data: cs})
			if flusher != nil {
				flusher.Flush()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) handleCreateConversation(w http.ResponseWriter, r *http.Request) {
	var req protocol.CreateConversationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id, err := protocol.NewConversationID(uuid.NewString())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := s.sys.Spawn(r.Context(), statecharts.Identifier(id), ConversationKind, actors.Durable()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	title := req.Title
	if title == "" {
		title = "Untitled"
	}
	if err := s.sys.Tell(r.Context(), "user", statecharts.Event{
		Name: "register", Type: statecharts.EventExternal,
		Data: &registerConversationPayload{
			TypeName: "aiagent.register_conversation",
			Value:    RegisterConversationData{ID: id, Title: title},
		},
	}); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusCreated, protocol.CreateConversationResponse{ID: id})
}

func (s *Server) handleSendMessage(w http.ResponseWriter, r *http.Request) {
	id, err := s.ensureConversation(r.Context(), r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	var req protocol.SendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.sys.Tell(r.Context(), statecharts.Identifier(id), statecharts.Event{
		Name: "user_message", Type: statecharts.EventExternal,
		Data: &userMessagePayload{TypeName: "aiagent.user_message", Value: UserMessageData{Text: req.Text}},
	}); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleToolResult(w http.ResponseWriter, r *http.Request) {
	id, err := s.ensureConversation(r.Context(), r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	var req protocol.ToolResultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.sys.Tell(r.Context(), statecharts.Identifier(id), statecharts.Event{
		Name: "tool_result", Type: statecharts.EventExternal,
		Data: &toolResultPayload{
			TypeName: "aiagent.tool_result",
			Value: ToolResultData{
				CallID: req.CallID, Output: req.Output, ExitCode: req.ExitCode, Error: req.Error,
			},
		},
	}); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	id, err := s.ensureConversation(r.Context(), r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	var tools []protocol.ToolName
	if raw := r.URL.Query().Get("tools"); raw != "" {
		for _, t := range strings.Split(raw, ",") {
			if name, err := protocol.NewToolName(strings.TrimSpace(t)); err == nil {
				tools = append(tools, name)
			}
		}
	}
	fromSeq := 0
	if lastID := r.Header.Get("Last-Event-ID"); lastID != "" {
		if n, err := strconv.Atoi(lastID); err == nil {
			fromSeq = n
		}
	}

	connName := statecharts.Identifier(uuid.NewString())
	if err := s.sys.Spawn(r.Context(), connName, ConnectionKind); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	ready := make(chan chan sseFrame, 1)
	if err := s.sys.Tell(r.Context(), connName, statecharts.Event{
		Name: "start", Type: statecharts.EventExternal,
		Data: connectionStart{ConversationID: id, Tools: tools, FromSeq: fromSeq, Ready: ready},
	}); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	var frames chan sseFrame
	select {
	case frames = <-ready:
	case <-r.Context().Done():
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}

	defer func() {
		_ = s.sys.Tell(context.Background(), connName, statecharts.Event{Name: "disconnect", Type: statecharts.EventExternal})
	}()

	for {
		select {
		case frame, ok := <-frames:
			if !ok {
				return
			}
			writeSSEFrame(w, frame)
			if flusher != nil {
				flusher.Flush()
			}
		case <-r.Context().Done():
			return
		}
	}
}

func writeSSEFrame(w http.ResponseWriter, f sseFrame) {
	if f.ID != "" {
		fmt.Fprintf(w, "id: %s\n", f.ID)
	}
	if f.Event != "" {
		fmt.Fprintf(w, "event: %s\n", f.Event)
	}
	b, err := json.Marshal(f.Data)
	if err != nil {
		return
	}
	fmt.Fprintf(w, "data: %s\n\n", b)
}
