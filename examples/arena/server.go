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
	mux.HandleFunc("GET /editor/bots", editorHandler)
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
	mux.HandleFunc("GET /bots", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"bots": runtime.listBots()})
	})
	mux.HandleFunc("POST /bots/rollout", func(w http.ResponseWriter, request *http.Request) {
		bots, err := runtime.rolloutBots(request.Context())
		if err != nil {
			writeJSONStatus(w, http.StatusUnprocessableEntity, map[string]any{"error": err.Error(), "bots": bots})
			return
		}
		writeJSON(w, map[string]any{"bots": bots})
	})
	mux.HandleFunc("POST /bots/{player}/rollout", func(w http.ResponseWriter, request *http.Request) {
		bot, err := runtime.rolloutBot(request.Context(), request.PathValue("player"))
		if err != nil {
			writeJSONStatus(w, http.StatusUnprocessableEntity, map[string]any{"error": err.Error(), "bot": bot})
			return
		}
		writeJSON(w, bot)
	})
	mux.HandleFunc("GET /definitions/bot/vocabulary", func(w http.ResponseWriter, _ *http.Request) {
		vocabulary, err := botDefinitionVocabulary()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, vocabulary)
	})
	registerDefinitionRoutes(mux, runtime, "bot", botKind, runtime.bot, validateBotDefinition, runtime.publishBotDefinition)
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

func registerDefinitionRoutes(mux *http.ServeMux, runtime *arenaRuntime, path string, kind statecharts.Identifier, source *statecharts.Chart, validate func(statecharts.Definition) error, publisher func(context.Context, statecharts.Definition) (statecharts.RevisionID, error)) {
	mux.HandleFunc("GET /definitions/"+path, func(w http.ResponseWriter, request *http.Request) {
		var definition statecharts.Definition
		var revision statecharts.RevisionID
		var ok bool
		if requested := statecharts.RevisionID(request.URL.Query().Get("revision")); requested != "" {
			revision = requested
			definition, ok = runtime.system.Definition(kind, revision)
		} else {
			definition, revision, ok = runtime.system.CurrentDefinition(kind)
		}
		if !ok {
			http.Error(w, path+" definition is not retained", http.StatusNotFound)
			return
		}
		data, err := statejson.MarshalIndent(definition, "", "  ")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Statechart-Revision", string(revision))
		_, _ = w.Write(append(data, '\n'))
	})
	handleCandidate := func(publish bool) http.HandlerFunc {
		return func(w http.ResponseWriter, request *http.Request) {
			definition, err := readDefinition(request, kind)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := validate(definition); err != nil {
				http.Error(w, err.Error(), http.StatusUnprocessableEntity)
				return
			}
			chart, err := statecharts.Compile(definition, source.Datamodel())
			if err == nil {
				err = chart.Prepare()
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnprocessableEntity)
				return
			}
			revision := chart.Revision()
			if publish {
				revision, err = publisher(request.Context(), definition)
				if err != nil {
					http.Error(w, err.Error(), http.StatusUnprocessableEntity)
					return
				}
			}
			writeJSON(w, map[string]any{"chart_id": kind, "revision": revision})
		}
	}
	mux.HandleFunc("POST /definitions/"+path+"/validate", handleCandidate(false))
	mux.HandleFunc("PUT /definitions/"+path, handleCandidate(true))
}

func validateBotDefinition(definition statecharts.Definition) error {
	var validateBlocks func([]statecharts.ExecutableBlock) error
	validateBlocks = func(blocks []statecharts.ExecutableBlock) error {
		for _, block := range blocks {
			for _, executable := range block {
				if executable.Call != nil && executable.Call.Function.Name == "arena.bot.observe" {
					args := executable.Call.Function.Args
					if len(args) != 1 || args[0].Kind != "go.literal" {
						return fmt.Errorf("bot observe requires one literal policy argument")
					}
					var policy botPolicy
					if err := decodeTaggedStruct(args[0].Data, botPolicyTag, &policy); err != nil {
						return fmt.Errorf("invalid bot policy: %w", err)
					}
					if policy.TargetPriority != "nearest" && policy.TargetPriority != "powerups" && policy.TargetPriority != "creatures" {
						return fmt.Errorf("invalid bot target_priority %q", policy.TargetPriority)
					}
					if policy.ShootRange < 0 {
						return fmt.Errorf("bot shoot_range must be non-negative")
					}
				}
				if executable.Choose != nil {
					for _, branch := range executable.Choose.Branches {
						if err := validateBlocks(branch.Actions); err != nil {
							return err
						}
					}
					if err := validateBlocks(executable.Choose.Else); err != nil {
						return err
					}
				}
				if executable.ForEach != nil {
					if err := validateBlocks(executable.ForEach.Actions); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	var visit func(statecharts.StateDefinition) error
	visit = func(state statecharts.StateDefinition) error {
		if err := validateBlocks(state.OnEntry); err != nil {
			return err
		}
		if err := validateBlocks(state.OnExit); err != nil {
			return err
		}
		if state.Initial != nil {
			if err := validateBlocks(state.Initial.Actions); err != nil {
				return err
			}
		}
		for _, transition := range state.Transitions {
			if err := validateBlocks(transition.Actions); err != nil {
				return err
			}
		}
		for _, invoke := range state.Invokes {
			if err := validateBlocks(invoke.Finalize); err != nil {
				return err
			}
		}
		for _, child := range state.Children {
			if err := visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(definition.Root)
}

type botVocabularyAction struct {
	Name        statecharts.Identifier `json:"name"`
	Version     string                 `json:"version"`
	Summary     string                 `json:"summary"`
	Example     json.RawMessage        `json:"example"`
	Constraints []string               `json:"constraints,omitempty"`
}

type botVocabularyEvent struct {
	Name    statecharts.Identifier `json:"name"`
	Summary string                 `json:"summary"`
}

type botVocabulary struct {
	Actions     []botVocabularyAction `json:"actions"`
	Events      []botVocabularyEvent  `json:"events"`
	States      []string              `json:"states"`
	Executables []string              `json:"executables"`
}

func botDefinitionVocabulary() (botVocabulary, error) {
	call := func(name statecharts.Identifier, args ...statecharts.Expression) (json.RawMessage, error) {
		executable := statecharts.NewCallExecutable(statecharts.CallDefinition{Function: statecharts.FunctionRef{Name: name, Version: "v1", Args: args}})
		return json.Marshal(executable)
	}
	start, err := call("arena.bot.start")
	if err != nil {
		return botVocabulary{}, err
	}
	policy, err := taggedStruct(botPolicyTag, defaultBotPolicy())
	if err != nil {
		return botVocabulary{}, err
	}
	observe, err := call("arena.bot.observe", statecharts.GoLiteral(policy))
	if err != nil {
		return botVocabulary{}, err
	}
	stop, err := call("arena.bot.stop")
	if err != nil {
		return botVocabulary{}, err
	}
	return botVocabulary{
		Actions: []botVocabularyAction{
			{Name: "arena.bot.start", Version: "v1", Summary: "Join the authoritative match and subscribe to snapshots.", Example: start, Constraints: []string{"Call while handling bot.start before strategy actions."}},
			{Name: "arena.bot.observe", Version: "v1", Summary: "Read the current match.snapshot and emit one ordinary player input.", Example: observe, Constraints: []string{"Requires one arena.bot-policy/v1 go.literal.", "target_priority: nearest | powerups | creatures", "shoot_range: 0 disables shooting"}},
			{Name: "arena.bot.stop", Version: "v1", Summary: "Unsubscribe and release the controller lease.", Example: stop, Constraints: []string{"Call while handling bot.stop and transition to a final state for clean rollout."}},
		},
		Events: []botVocabularyEvent{
			{Name: "bot.start", Summary: "Runtime configuration delivered once to a new controller."},
			{Name: "match.snapshot", Summary: "Authoritative world update; strategy actions inspect this event."},
			{Name: "bot.stop", Summary: "Rollout asks the old controller to release its lease and finish."},
		},
		States:      []string{"atomic", "compound", "parallel", "final", "history"},
		Executables: []string{"call", "raise", "send", "cancel", "log", "assign", "choose", "foreach", "script"},
	}, nil
}

func readMatchDefinition(request *http.Request) (statecharts.Definition, error) {
	return readDefinition(request, matchKind)
}

func readDefinition(request *http.Request, kind statecharts.Identifier) (statecharts.Definition, error) {
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
	if definition.ID != kind {
		return statecharts.Definition{}, fmt.Errorf("definition chart ID %q does not match %q", definition.ID, kind)
	}
	return definition, nil
}

func writeJSON(w http.ResponseWriter, value any) {
	writeJSONStatus(w, http.StatusOK, value)
}

func writeJSONStatus(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		// Headers have already been sent, so the connection is the only remaining
		// place to report an encoding failure.
		return
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
	spectate := request.URL.Query().Get("spectate") == "1"
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
		Match: string(match), Player: player, Name: playerName(player), Color: playerColor(player), Output: outputID, Spectate: spectate,
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
