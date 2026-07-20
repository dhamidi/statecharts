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
	mux.Handle("GET /scripts/", http.StripPrefix("/scripts/", editorEngine.ScriptHandler()))
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
	actionCapabilities, conditionCapabilities := botCapabilitySchemas()
	validateFunction := func(function statecharts.FunctionRef, capabilities map[statecharts.Identifier]botVocabularyCapability, events []statecharts.Identifier) error {
		capability, known := capabilities[function.Name]
		if !known {
			return nil
		}
		if !botCapabilitySupportsEvents(capability, events) {
			return fmt.Errorf("%s requires event %s", function.Name, strings.Join(identifiersToStrings(capability.Events), " or "))
		}
		if len(function.Args) != len(capability.Parameters) {
			return fmt.Errorf("%s requires %d argument(s), got %d", function.Name, len(capability.Parameters), len(function.Args))
		}
		for index, parameter := range capability.Parameters {
			if err := validateBotArgument(function.Args[index], parameter); err != nil {
				return fmt.Errorf("%s argument %q: %w", function.Name, parameter.Name, err)
			}
		}
		return nil
	}
	validateCondition := func(expression *statecharts.Expression, events []statecharts.Identifier) error {
		if expression == nil || expression.Kind != "go.condition" {
			return nil
		}
		function, err := goReference(*expression)
		if err != nil {
			return err
		}
		return validateFunction(function, conditionCapabilities, events)
	}
	var validateBlocks func([]statecharts.ExecutableBlock, []statecharts.Identifier) error
	validateBlocks = func(blocks []statecharts.ExecutableBlock, events []statecharts.Identifier) error {
		for _, block := range blocks {
			for _, executable := range block {
				if executable.Call != nil {
					if err := validateFunction(executable.Call.Function, actionCapabilities, events); err != nil {
						return err
					}
				}
				if executable.Choose != nil {
					for _, branch := range executable.Choose.Branches {
						if err := validateCondition(&branch.Condition, events); err != nil {
							return err
						}
						if err := validateBlocks(branch.Actions, events); err != nil {
							return err
						}
					}
					if err := validateBlocks(executable.Choose.Else, events); err != nil {
						return err
					}
				}
				if executable.ForEach != nil {
					if err := validateBlocks(executable.ForEach.Actions, events); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	var visit func(statecharts.StateDefinition) error
	visit = func(state statecharts.StateDefinition) error {
		if err := validateBlocks(state.OnEntry, nil); err != nil {
			return err
		}
		if err := validateBlocks(state.OnExit, nil); err != nil {
			return err
		}
		if state.Initial != nil {
			if err := validateBlocks(state.Initial.Actions, nil); err != nil {
				return err
			}
		}
		for _, transition := range state.Transitions {
			if err := validateCondition(transition.Condition, transition.Events); err != nil {
				return err
			}
			if err := validateBlocks(transition.Actions, transition.Events); err != nil {
				return err
			}
		}
		for _, invoke := range state.Invokes {
			if err := validateBlocks(invoke.Finalize, nil); err != nil {
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

type botVocabularyParameter struct {
	Name    string   `json:"name"`
	Label   string   `json:"label"`
	Type    string   `json:"type"`
	Default any      `json:"default"`
	Options []string `json:"options,omitempty"`
	Minimum *int     `json:"minimum,omitempty"`
}

type botVocabularyCapability struct {
	Name        statecharts.Identifier   `json:"name"`
	Version     string                   `json:"version"`
	Category    string                   `json:"category"`
	Summary     string                   `json:"summary"`
	Example     json.RawMessage          `json:"example"`
	Parameters  []botVocabularyParameter `json:"parameters"`
	Events      []statecharts.Identifier `json:"events"`
	Constraints []string                 `json:"constraints,omitempty"`
}

type botVocabularyEvent struct {
	Name    statecharts.Identifier `json:"name"`
	Summary string                 `json:"summary"`
}

type botVocabulary struct {
	Actions     []botVocabularyCapability `json:"actions"`
	Conditions  []botVocabularyCapability `json:"conditions"`
	Events      []botVocabularyEvent      `json:"events"`
	States      []string                  `json:"states"`
	Executables []string                  `json:"executables"`
}

func botDefinitionVocabulary() (botVocabulary, error) {
	_, refs := newBotBuilder()
	call := func(executable statecharts.Executable) (json.RawMessage, error) {
		return json.Marshal(executable)
	}
	condition := func(expression statecharts.Expression) (json.RawMessage, error) {
		return json.Marshal(expression)
	}
	stringLiteral := func(value string) statecharts.Expression {
		result, _ := statecharts.StringValue(value)
		return statecharts.GoLiteral(result)
	}
	integerLiteral := func(value int64) statecharts.Expression {
		return statecharts.GoLiteral(statecharts.Int64Value(value))
	}
	minimum := func(value int) *int { return &value }
	parameter := func(name, label, typ string, defaultValue any, options []string, min *int) botVocabularyParameter {
		return botVocabularyParameter{Name: name, Label: label, Type: typ, Default: defaultValue, Options: options, Minimum: min}
	}
	action := func(name statecharts.Identifier, category, summary string, events []statecharts.Identifier, executable statecharts.Executable, parameters []botVocabularyParameter, constraints ...string) (botVocabularyCapability, error) {
		if parameters == nil {
			parameters = []botVocabularyParameter{}
		}
		example, err := call(executable)
		version := ""
		if executable.Call != nil {
			version = executable.Call.Function.Version
		}
		return botVocabularyCapability{Name: name, Version: version, Category: category, Summary: summary, Example: example, Parameters: parameters, Events: events, Constraints: constraints}, err
	}
	check := func(name statecharts.Identifier, category, summary string, events []statecharts.Identifier, expression statecharts.Expression, parameters []botVocabularyParameter, constraints ...string) (botVocabularyCapability, error) {
		if parameters == nil {
			parameters = []botVocabularyParameter{}
		}
		example, err := condition(expression)
		function, referenceErr := goReference(expression)
		if err == nil {
			err = referenceErr
		}
		return botVocabularyCapability{Name: name, Version: function.Version, Category: category, Summary: summary, Example: example, Parameters: parameters, Events: events, Constraints: constraints}, err
	}
	var vocabulary botVocabulary
	var err error
	appendAction := func(capability botVocabularyCapability, capabilityErr error) {
		if err == nil {
			err = capabilityErr
		}
		vocabulary.Actions = append(vocabulary.Actions, capability)
	}
	appendCondition := func(capability botVocabularyCapability, capabilityErr error) {
		if err == nil {
			err = capabilityErr
		}
		vocabulary.Conditions = append(vocabulary.Conditions, capability)
	}
	directions := []string{actionUp, actionDown, actionLeft, actionRight}
	targets := []string{botTargetNearest, botTargetOpponent, botTargetPowerup}
	startEvents := []statecharts.Identifier{"bot.start"}
	snapshotEvents := []statecharts.Identifier{"match.snapshot"}
	stopEvents := []statecharts.Identifier{"bot.stop"}
	appendAction(action("arena.bot.start", "lifecycle", "Join the authoritative match and subscribe to snapshots.", startEvents, refs.start.Call(), nil, "Use on bot.start."))
	appendAction(action("arena.bot.move", "movement", "Move one cell in an explicit direction.", snapshotEvents, refs.move.Call(stringLiteral(actionRight)), []botVocabularyParameter{parameter("direction", "Direction", "enum", actionRight, directions, nil)}, "Use on match.snapshot."))
	appendAction(action("arena.bot.move-toward", "movement", "Pathfind one cell toward the nearest selected target.", snapshotEvents, refs.moveToward.Call(stringLiteral(botTargetPowerup)), []botVocabularyParameter{parameter("target", "Target", "enum", botTargetPowerup, targets, nil)}, "Use on match.snapshot."))
	appendAction(action("arena.bot.wander", "movement", "Choose a deterministic available direction.", snapshotEvents, refs.wander.Call(), nil, "Use on match.snapshot."))
	appendAction(action("arena.bot.shoot", "combat", "Fire in the current facing direction.", snapshotEvents, refs.shoot.Call(), nil, "Use on match.snapshot."))
	appendAction(action("arena.bot.reload", "combat", "Reload the weapon after firing.", snapshotEvents, refs.reload.Call(), nil, "Use on match.snapshot."))
	appendAction(action("arena.bot.stop", "lifecycle", "Unsubscribe and release the controller lease.", stopEvents, refs.stop.Call(), nil, "Use on bot.stop and transition to a final state."))
	appendCondition(check("arena.bot.target-exists", "sensing", "Whether a target of this kind exists.", snapshotEvents, refs.targetExists.If(stringLiteral(botTargetPowerup)), []botVocabularyParameter{parameter("target", "Target", "enum", botTargetPowerup, targets, nil)}))
	appendCondition(check("arena.bot.target-within", "sensing", "Whether the nearest selected target is within Manhattan distance.", snapshotEvents, refs.targetWithin.If(stringLiteral(botTargetOpponent), integerLiteral(5)), []botVocabularyParameter{parameter("target", "Target", "enum", botTargetOpponent, targets, nil), parameter("distance", "Distance", "integer", 5, nil, minimum(0))}))
	appendCondition(check("arena.bot.opponent-in-sights", "combat", "Whether a clear, aligned opponent is in front of the bot.", snapshotEvents, refs.opponentInSights.If(integerLiteral(8)), []botVocabularyParameter{parameter("range", "Range", "integer", 8, nil, minimum(1))}))
	appendCondition(check("arena.bot.weapon-empty", "combat", "Whether the weapon must be reloaded before firing.", snapshotEvents, refs.weaponEmpty.If(), nil))
	appendCondition(check("arena.bot.health-below", "status", "Whether health is below a threshold.", snapshotEvents, refs.healthBelow.If(integerLiteral(2)), []botVocabularyParameter{parameter("health", "Health", "integer", 2, nil, minimum(1))}))
	appendCondition(check("arena.bot.power-at-least", "status", "Whether collected power is at least a threshold.", snapshotEvents, refs.powerAtLeast.If(integerLiteral(1)), []botVocabularyParameter{parameter("power", "Power", "integer", 1, nil, minimum(0))}))
	appendCondition(check("arena.bot.tick-every", "timing", "True on every Nth authoritative match tick.", snapshotEvents, refs.tickEvery.If(integerLiteral(8)), []botVocabularyParameter{parameter("interval", "Every N ticks", "integer", 8, nil, minimum(1))}))
	if err != nil {
		return botVocabulary{}, err
	}
	vocabulary.Events = []botVocabularyEvent{
		{Name: "bot.start", Summary: "Runtime configuration delivered once to a new controller."},
		{Name: "match.snapshot", Summary: "Authoritative world update used by decision conditions and actions."},
		{Name: "bot.stop", Summary: "Rollout asks the old controller to release its lease and finish."},
	}
	vocabulary.States = []string{"atomic", "compound", "parallel", "final", "history"}
	vocabulary.Executables = []string{"call", "raise", "send", "cancel", "log", "assign", "choose", "foreach", "script"}
	return vocabulary, nil
}

func botCapabilitySchemas() (map[statecharts.Identifier]botVocabularyCapability, map[statecharts.Identifier]botVocabularyCapability) {
	vocabulary, _ := botDefinitionVocabulary()
	actions := make(map[statecharts.Identifier]botVocabularyCapability, len(vocabulary.Actions))
	conditions := make(map[statecharts.Identifier]botVocabularyCapability, len(vocabulary.Conditions))
	for _, capability := range vocabulary.Actions {
		actions[capability.Name] = capability
	}
	for _, capability := range vocabulary.Conditions {
		conditions[capability.Name] = capability
	}
	return actions, conditions
}

func botCapabilitySupportsEvents(capability botVocabularyCapability, events []statecharts.Identifier) bool {
	if len(events) == 0 {
		return false
	}
	for _, event := range events {
		found := false
		for _, supported := range capability.Events {
			if event == supported {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func identifiersToStrings(identifiers []statecharts.Identifier) []string {
	result := make([]string, len(identifiers))
	for index, identifier := range identifiers {
		result[index] = string(identifier)
	}
	return result
}

func validateBotArgument(expression statecharts.Expression, parameter botVocabularyParameter) error {
	if expression.Kind != "go.literal" {
		return fmt.Errorf("must be a literal")
	}
	switch parameter.Type {
	case "enum":
		value, ok := expression.Data.AsString()
		if !ok {
			return fmt.Errorf("must be a string")
		}
		for _, option := range parameter.Options {
			if value == option {
				return nil
			}
		}
		return fmt.Errorf("must be one of %v", parameter.Options)
	case "integer":
		value, ok := expression.Data.AsInt64()
		if !ok {
			return fmt.Errorf("must be an integer")
		}
		if parameter.Minimum != nil && value < int64(*parameter.Minimum) {
			return fmt.Errorf("must be at least %d", *parameter.Minimum)
		}
		return nil
	default:
		return fmt.Errorf("unsupported parameter type %q", parameter.Type)
	}
}

func goReferenceName(expression statecharts.Expression) (statecharts.Identifier, bool) {
	function, err := goReference(expression)
	return function.Name, err == nil
}

func goReference(expression statecharts.Expression) (statecharts.FunctionRef, error) {
	shape, ok := expression.Data.AsMap()
	if !ok {
		return statecharts.FunctionRef{}, fmt.Errorf("Go reference must be a map")
	}
	name, nameOK := shape["name"].AsString()
	version, versionOK := shape["version"].AsString()
	arguments, argumentsOK := shape["args"].AsList()
	if !nameOK || !versionOK || !argumentsOK || name == "" || version == "" {
		return statecharts.FunctionRef{}, fmt.Errorf("invalid Go reference")
	}
	function := statecharts.FunctionRef{Name: statecharts.Identifier(name), Version: version, Args: make([]statecharts.Expression, len(arguments))}
	for index, argument := range arguments {
		encoded, ok := argument.AsMap()
		if !ok {
			return statecharts.FunctionRef{}, fmt.Errorf("Go reference argument %d must be an expression", index)
		}
		kind, ok := encoded["kind"].AsString()
		if !ok || kind == "" {
			return statecharts.FunctionRef{}, fmt.Errorf("Go reference argument %d has no kind", index)
		}
		function.Args[index] = statecharts.Expression{Kind: statecharts.Identifier(kind), Data: encoded["data"].Clone()}
	}
	return function, nil
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
