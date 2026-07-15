package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/dhamidi/statecharts"
	statejson "github.com/dhamidi/statecharts/syntax/json"
)

const maxDefinitionBytes = 4 << 20

type definitionStatus struct {
	ChartID  statecharts.Identifier `json:"chart_id"`
	Revision statecharts.RevisionID `json:"revision"`
}

type chartList struct {
	Charts []definitionStatus `json:"charts"`
}

type actorStatus struct {
	ActorID   statecharts.Identifier     `json:"actor_id"`
	ChartID   statecharts.Identifier     `json:"chart_id"`
	Revision  statecharts.RevisionID     `json:"revision"`
	Resident  bool                       `json:"resident"`
	Lifecycle statecharts.ActorLifecycle `json:"lifecycle"`
}

type actorList struct {
	Actors []actorStatus `json:"actors"`
}

func registerAdministrationRoutes(mux *http.ServeMux, runtime *counterRuntime) {
	mux.HandleFunc("GET /definitions", func(w http.ResponseWriter, _ *http.Request) {
		_, revision, ok := runtime.counters.CurrentDefinition(counterKind)
		if !ok {
			http.Error(w, "counter definition is not registered", http.StatusNotFound)
			return
		}
		writeJSON(w, chartList{Charts: []definitionStatus{{ChartID: counterKind, Revision: revision}}})
	})
	mux.HandleFunc("GET /definitions/counter", func(w http.ResponseWriter, request *http.Request) {
		var definition statecharts.Definition
		if revision := statecharts.RevisionID(request.URL.Query().Get("revision")); revision != "" {
			var ok bool
			definition, ok = runtime.counters.Definition(counterKind, revision)
			if !ok {
				http.Error(w, "counter definition revision is not retained", http.StatusNotFound)
				return
			}
		} else {
			var ok bool
			definition, _, ok = runtime.counters.CurrentDefinition(counterKind)
			if !ok {
				http.Error(w, "counter definition is not registered", http.StatusNotFound)
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
	mux.HandleFunc("POST /definitions/counter/validate", func(w http.ResponseWriter, request *http.Request) {
		definition, err := readCounterDefinition(request)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		chart, err := statecharts.Compile(definition, runtime.counterChart.Datamodel())
		if err == nil {
			err = chart.Prepare()
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		writeJSON(w, definitionStatus{ChartID: chart.ID(), Revision: chart.Revision()})
	})
	mux.HandleFunc("PUT /definitions/counter", func(w http.ResponseWriter, request *http.Request) {
		definition, err := readCounterDefinition(request)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		revision, err := runtime.counters.Publish(request.Context(), definition)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		writeJSON(w, definitionStatus{ChartID: counterKind, Revision: revision})
	})
	mux.HandleFunc("GET /actors", func(w http.ResponseWriter, request *http.Request) {
		metadata, err := runtime.storage.ListNonTerminalActors(request.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result := actorList{Actors: make([]actorStatus, len(metadata))}
		for index, actor := range metadata {
			result.Actors[index] = actorStatus{ActorID: actor.ActorID, ChartID: actor.ChartID, Revision: actor.Revision, Resident: runtime.counters.IsResident(actor.ActorID), Lifecycle: actor.Lifecycle}
		}
		writeJSON(w, result)
	})
	mux.HandleFunc("GET /counters/{actorID}", func(w http.ResponseWriter, request *http.Request) {
		actorID := request.PathValue("actorID")
		projections, err := runtime.query(request.Context(), []string{actorID})
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if len(projections) != 1 {
			http.Error(w, "counter actor is not found", http.StatusNotFound)
			return
		}
		writeJSON(w, projections[0])
	})
	mux.HandleFunc("POST /actors/{actorID}", func(w http.ResponseWriter, request *http.Request) {
		actorID := statecharts.Identifier(request.PathValue("actorID"))
		if err := runtime.spawnCounter(request.Context(), actorID); err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func readCounterDefinition(request *http.Request) (statecharts.Definition, error) {
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
	if definition.ID != counterKind {
		return statecharts.Definition{}, fmt.Errorf("definition chart ID %q does not match %q", definition.ID, counterKind)
	}
	return definition, nil
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
