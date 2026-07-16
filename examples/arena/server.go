package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/coder/websocket"
	"github.com/dhamidi/statecharts"
	statejson "github.com/dhamidi/statecharts/syntax/json"
)

const maxDefinitionBytes = 4 << 20

func arenaHandler(runtime *arenaRuntime) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", indexHandler)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /ws", runtime.socketHandler)
	registerAdministrationRoutes(mux, runtime)
	return mux
}

func registerAdministrationRoutes(mux *http.ServeMux, runtime *arenaRuntime) {
	mux.HandleFunc("GET /matches", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"matches": runtime.listMatches()})
	})
	mux.HandleFunc("POST /matches/{match}", func(w http.ResponseWriter, request *http.Request) {
		id := statecharts.Identifier(request.PathValue("match"))
		if err := runtime.createMatch(request.Context(), id); err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		revision, _ := runtime.system.ActorRevision(id)
		writeJSON(w, matchStatus{ID: id, Revision: revision})
	})
	mux.HandleFunc("GET /definitions/match", func(w http.ResponseWriter, request *http.Request) {
		var definition statecharts.Definition
		if revision := statecharts.RevisionID(request.URL.Query().Get("revision")); revision != "" {
			var ok bool
			definition, ok = runtime.system.Definition(matchKind, revision)
			if !ok {
				http.Error(w, "match definition revision is not retained", http.StatusNotFound)
				return
			}
		} else {
			var ok bool
			definition, _, ok = runtime.system.CurrentDefinition(matchKind)
			if !ok {
				http.Error(w, "match definition is not registered", http.StatusNotFound)
				return
			}
		}
		data, err := statejson.MarshalIndent(definition, "", "  ")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(append(data, '\n'))
	})
	mux.HandleFunc("POST /definitions/match/validate", func(w http.ResponseWriter, request *http.Request) {
		definition, err := readMatchDefinition(request)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		chart, err := statecharts.Compile(definition, runtime.match.Datamodel())
		if err == nil {
			err = chart.Prepare()
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		writeJSON(w, map[string]any{"chart_id": chart.ID(), "revision": chart.Revision()})
	})
	mux.HandleFunc("PUT /definitions/match", func(w http.ResponseWriter, request *http.Request) {
		definition, err := readMatchDefinition(request)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		revision, err := runtime.system.Publish(request.Context(), definition)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		writeJSON(w, map[string]any{"chart_id": matchKind, "revision": revision})
	})
}

func readMatchDefinition(request *http.Request) (statecharts.Definition, error) {
	data, err := io.ReadAll(io.LimitReader(request.Body, maxDefinitionBytes+1))
	if err != nil {
		return statecharts.Definition{}, fmt.Errorf("read definition: %w", err)
	}
	if len(data) > maxDefinitionBytes {
		return statecharts.Definition{}, fmt.Errorf("definition exceeds %d bytes", maxDefinitionBytes)
	}
	definition, err := statejson.Unmarshal(data)
	if err != nil {
		return statecharts.Definition{}, fmt.Errorf("decode definition: %w", err)
	}
	if definition.ID != matchKind {
		return statecharts.Definition{}, fmt.Errorf("definition chart ID %q does not match %q", definition.ID, matchKind)
	}
	return definition, nil
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (runtime *arenaRuntime) socketHandler(w http.ResponseWriter, request *http.Request) {
	match := statecharts.Identifier(request.URL.Query().Get("match"))
	if match == "" {
		match = "match.main"
	}
	if !runtime.hasMatch(match) {
		http.Error(w, "unknown match", http.StatusNotFound)
		return
	}
	player := request.URL.Query().Get("player")
	if player == "" {
		var err error
		player, err = randomIdentifier("player")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if _, err := statecharts.NewIdentifier(player); err != nil {
		http.Error(w, "invalid player", http.StatusBadRequest)
		return
	}
	connectionID, err := randomIdentifier("connection")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	outputID, err := randomIdentifier("socket")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	connection, err := websocket.Accept(w, request, nil)
	if err != nil {
		return
	}
	defer connection.CloseNow()
	connection.SetReadLimit(4096)
	done := runtime.transport.registerSocket(statecharts.Identifier(outputID), connection)
	defer runtime.transport.unregister(statecharts.Identifier(outputID))

	actorID := statecharts.Identifier(connectionID)
	if err := runtime.system.Spawn(context.Background(), actorID, connectionKind); err != nil {
		_ = connection.Close(websocket.StatusInternalError, "connection actor failed")
		return
	}
	config, err := taggedStruct(connectionConfigTag, connectionConfig{
		Match: string(match), Player: player, Name: playerName(player), Color: playerColor(player), Output: outputID,
	})
	if err != nil {
		_ = connection.Close(websocket.StatusInternalError, "connection configuration failed")
		return
	}
	if err := runtime.system.Tell(context.Background(), actorID, statecharts.Event{Name: "connection.start", Type: statecharts.EventExternal, Data: config}); err != nil {
		_ = connection.Close(websocket.StatusInternalError, "connection actor failed")
		return
	}
	defer runtime.system.Tell(context.Background(), actorID, statecharts.Event{Name: "connection.close", Type: statecharts.EventExternal})

	for {
		select {
		case <-done:
			return
		default:
		}
		messageType, data, err := connection.Read(context.Background())
		if err != nil {
			return
		}
		if messageType != websocket.MessageText {
			_ = connection.Close(websocket.StatusUnsupportedData, "text messages only")
			return
		}
		value, err := statecharts.StringValue(string(data))
		if err != nil {
			_ = connection.Close(websocket.StatusInvalidFramePayloadData, "invalid UTF-8")
			return
		}
		if err := runtime.system.Tell(context.Background(), actorID, statecharts.Event{Name: "client.message", Type: statecharts.EventExternal, Data: value}); err != nil {
			_ = connection.Close(websocket.StatusInternalError, "connection actor failed")
			return
		}
	}
}

func randomIdentifier(prefix string) (string, error) {
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate %s ID: %w", prefix, err)
	}
	return prefix + "." + hex.EncodeToString(value[:]), nil
}

func playerColor(player string) string {
	colors := []string{"#22d3ee", "#f472b6", "#fbbf24", "#a78bfa", "#4ade80", "#fb7185"}
	value := 0
	for _, character := range strings.ToLower(player) {
		value = value*31 + int(character)
	}
	if value < 0 {
		value = -value
	}
	return colors[value%len(colors)]
}

func playerName(player string) string {
	if len(player) > 6 {
		return "PLAYER " + strings.ToUpper(player[len(player)-6:])
	}
	return strings.ToUpper(player)
}
