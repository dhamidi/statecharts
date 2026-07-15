// Package json encodes and decodes statecharts.Definition as a strict,
// human-editable JSON surface syntax. It is independent of Definition's
// internal canonical revision encoding; whitespace and presentation never
// affect revision identity.
package json

import (
	"bytes"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/dhamidi/statecharts"
)

// Error reports a JSON codec failure at a stable Definition traversal path.
type Error struct {
	Path string
	Err  error
}

func (e *Error) Error() string { return fmt.Sprintf("statecharts JSON %s: %v", e.Path, e.Err) }
func (e *Error) Unwrap() error { return e.Err }

type definitionWire struct {
	ID           statecharts.Identifier       `json:"id"`
	Name         string                       `json:"name,omitempty"`
	Datamodel    statecharts.Identifier       `json:"datamodel"`
	RevisionSalt string                       `json:"revisionSalt,omitempty"`
	DataBinding  statecharts.DataBinding      `json:"dataBinding,omitempty"`
	Data         []statecharts.DataDefinition `json:"data,omitempty"`
	Root         stateWire                    `json:"root"`
}

type stateWire struct {
	ID          statecharts.StateDefinitionID `json:"id"`
	Kind        string                        `json:"kind"`
	History     string                        `json:"history,omitempty"`
	Initial     *transitionWire               `json:"initial,omitempty"`
	OnEntry     []executableBlockWire         `json:"onEntry,omitempty"`
	OnExit      []executableBlockWire         `json:"onExit,omitempty"`
	Transitions []transitionWire              `json:"transitions,omitempty"`
	Invokes     []invokeWire                  `json:"invokes,omitempty"`
	Data        []statecharts.DataDefinition  `json:"data,omitempty"`
	DoneData    *doneDataWire                 `json:"doneData,omitempty"`
	Children    []stateWire                   `json:"children,omitempty"`
}

type transitionWire struct {
	Events    []statecharts.Identifier   `json:"events,omitempty"`
	Targets   []statecharts.Identifier   `json:"targets,omitempty"`
	Type      statecharts.TransitionType `json:"type,omitempty"`
	Condition *statecharts.Expression    `json:"condition,omitempty"`
	Actions   []executableBlockWire      `json:"actions,omitempty"`
}

type invokeWire struct {
	DefinitionID statecharts.Identifier        `json:"definitionId,omitempty"`
	ID           statecharts.Identifier        `json:"id,omitempty"`
	IDLocation   *statecharts.Expression       `json:"idLocation,omitempty"`
	Type         string                        `json:"type,omitempty"`
	TypeExpr     *statecharts.Expression       `json:"typeExpr,omitempty"`
	Src          string                        `json:"src,omitempty"`
	SrcExpr      *statecharts.Expression       `json:"srcExpr,omitempty"`
	Params       []statecharts.ParamDefinition `json:"params,omitempty"`
	Content      *statecharts.Expression       `json:"content,omitempty"`
	AutoForward  bool                          `json:"autoForward,omitempty"`
	Finalize     []executableBlockWire         `json:"finalize,omitempty"`
}

type doneDataWire struct {
	Params  []statecharts.ParamDefinition `json:"params,omitempty"`
	Content *statecharts.Expression       `json:"content,omitempty"`
}

type executableBlockWire []executableWire

type executableWire struct {
	Kind      statecharts.ExecutableKind       `json:"kind"`
	Raise     *statecharts.RaiseDefinition     `json:"raise,omitempty"`
	Send      *statecharts.SendDefinition      `json:"send,omitempty"`
	Cancel    *statecharts.CancelDefinition    `json:"cancel,omitempty"`
	Log       *statecharts.LogDefinition       `json:"log,omitempty"`
	Assign    *statecharts.AssignDefinition    `json:"assign,omitempty"`
	Choose    *chooseWire                      `json:"choose,omitempty"`
	ForEach   *forEachWire                     `json:"foreach,omitempty"`
	Script    *statecharts.ScriptDefinition    `json:"script,omitempty"`
	Call      *statecharts.CallDefinition      `json:"call,omitempty"`
	Extension *statecharts.ExtensionDefinition `json:"extension,omitempty"`
}

type chooseWire struct {
	Branches []chooseBranchWire    `json:"branches"`
	Else     []executableBlockWire `json:"else,omitempty"`
}

type chooseBranchWire struct {
	Condition statecharts.Expression `json:"condition"`
	Actions   []executableBlockWire  `json:"actions,omitempty"`
}

type forEachWire struct {
	Array   statecharts.Expression `json:"array"`
	Item    statecharts.Identifier `json:"item"`
	Index   statecharts.Identifier `json:"index,omitempty"`
	Actions []executableBlockWire  `json:"actions,omitempty"`
}

// Marshal returns deterministic compact JSON for definition.
func Marshal(definition statecharts.Definition) ([]byte, error) {
	wire, err := encodeDefinition(definition)
	if err != nil {
		return nil, err
	}
	data, err := stdjson.Marshal(wire)
	if err != nil {
		return nil, &Error{Path: "definition", Err: err}
	}
	return data, nil
}

// MarshalIndent returns deterministic JSON formatted with prefix and indent.
func MarshalIndent(definition statecharts.Definition, prefix, indent string) ([]byte, error) {
	wire, err := encodeDefinition(definition)
	if err != nil {
		return nil, err
	}
	data, err := stdjson.MarshalIndent(wire, prefix, indent)
	if err != nil {
		return nil, &Error{Path: "definition", Err: err}
	}
	return data, nil
}

// Unmarshal decodes exactly one strict JSON definition. It returns the zero
// Definition on every error; compilation and publication remain explicit
// caller steps.
func Unmarshal(data []byte) (statecharts.Definition, error) {
	if err := inspectJSONDocument(data, "definition"); err != nil {
		return statecharts.Definition{}, err
	}

	var raw any
	decoder := stdjson.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		return statecharts.Definition{}, &Error{Path: "definition", Err: err}
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("trailing JSON value")
		} else {
			err = fmt.Errorf("trailing data: %w", err)
		}
		return statecharts.Definition{}, &Error{Path: "definition", Err: err}
	}
	if err := inspectDefinition(raw, "definition"); err != nil {
		return statecharts.Definition{}, err
	}

	strict := stdjson.NewDecoder(bytes.NewReader(data))
	strict.DisallowUnknownFields()
	var wire definitionWire
	if err := strict.Decode(&wire); err != nil {
		return statecharts.Definition{}, &Error{Path: "definition", Err: err}
	}
	definition, err := decodeDefinition(wire, "definition")
	if err != nil {
		return statecharts.Definition{}, err
	}
	if err := definition.Validate(); err != nil {
		return statecharts.Definition{}, wrapDefinitionError(err)
	}
	return definition, nil
}

func inspectJSONDocument(data []byte, path string) error {
	decoder := stdjson.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := inspectJSONValue(decoder, path); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("trailing JSON value")
		} else {
			err = fmt.Errorf("trailing data: %w", err)
		}
		return &Error{Path: path, Err: err}
	}
	return nil
}

func inspectJSONValue(decoder *stdjson.Decoder, path string) error {
	token, err := decoder.Token()
	if err != nil {
		return &Error{Path: path, Err: err}
	}
	if token == nil {
		return pathError(path, "null is not valid; omit an optional field instead")
	}
	delimiter, ok := token.(stdjson.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return &Error{Path: path, Err: err}
			}
			key, ok := keyToken.(string)
			if !ok {
				return pathError(path, "object field name must be a string")
			}
			fieldPath := path + "." + key
			if _, duplicate := seen[key]; duplicate {
				return pathError(fieldPath, "duplicate field %q", key)
			}
			seen[key] = struct{}{}
			if err := inspectJSONValue(decoder, fieldPath); err != nil {
				return err
			}
		}
		if _, err := decoder.Token(); err != nil {
			return &Error{Path: path, Err: err}
		}
	case '[':
		for index := 0; decoder.More(); index++ {
			if err := inspectJSONValue(decoder, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
		if _, err := decoder.Token(); err != nil {
			return &Error{Path: path, Err: err}
		}
	default:
		return pathError(path, "unexpected JSON delimiter %q", delimiter)
	}
	return nil
}

func encodeDefinition(definition statecharts.Definition) (definitionWire, error) {
	if err := definition.Validate(); err != nil {
		return definitionWire{}, wrapDefinitionError(err)
	}
	root, err := encodeState(definition.Root)
	if err != nil {
		return definitionWire{}, err
	}
	return definitionWire{
		ID: definition.ID, Name: definition.Name, Datamodel: definition.Datamodel,
		RevisionSalt: definition.RevisionSalt, DataBinding: definition.DataBinding,
		Data: definition.Data, Root: root,
	}, nil
}

func encodeState(state statecharts.StateDefinition) (stateWire, error) {
	result := stateWire{ID: state.ID, Kind: state.Kind.String(), Data: state.Data}
	if state.Kind == statecharts.KindHistory {
		result.History = state.History.String()
	}
	if state.Initial != nil {
		value, err := encodeTransition(*state.Initial)
		if err != nil {
			return stateWire{}, err
		}
		result.Initial = &value
	}
	var err error
	if result.OnEntry, err = encodeBlocks(state.OnEntry); err != nil {
		return stateWire{}, err
	}
	if result.OnExit, err = encodeBlocks(state.OnExit); err != nil {
		return stateWire{}, err
	}
	if state.Transitions != nil {
		result.Transitions = make([]transitionWire, len(state.Transitions))
		for i := range state.Transitions {
			result.Transitions[i], err = encodeTransition(state.Transitions[i])
			if err != nil {
				return stateWire{}, err
			}
		}
	}
	if state.Invokes != nil {
		result.Invokes = make([]invokeWire, len(state.Invokes))
		for i := range state.Invokes {
			result.Invokes[i], err = encodeInvoke(state.Invokes[i])
			if err != nil {
				return stateWire{}, err
			}
		}
	}
	if state.DoneData != nil {
		result.DoneData = &doneDataWire{Params: state.DoneData.Params, Content: state.DoneData.Content}
	}
	if state.Children != nil {
		result.Children = make([]stateWire, len(state.Children))
		for i := range state.Children {
			result.Children[i], err = encodeState(state.Children[i])
			if err != nil {
				return stateWire{}, err
			}
		}
	}
	return result, nil
}

func encodeTransition(transition statecharts.TransitionDefinition) (transitionWire, error) {
	actions, err := encodeBlocks(transition.Actions)
	if err != nil {
		return transitionWire{}, err
	}
	return transitionWire{Events: transition.Events, Targets: transition.Targets, Type: transition.Type, Condition: transition.Condition, Actions: actions}, nil
}

func encodeInvoke(invoke statecharts.InvokeDefinition) (invokeWire, error) {
	finalize, err := encodeBlocks(invoke.Finalize)
	if err != nil {
		return invokeWire{}, err
	}
	return invokeWire{
		DefinitionID: invoke.DefinitionID, ID: invoke.ID, IDLocation: invoke.IDLocation,
		Type: invoke.Type, TypeExpr: invoke.TypeExpr, Src: invoke.Src, SrcExpr: invoke.SrcExpr,
		Params: invoke.Params, Content: invoke.Content, AutoForward: invoke.AutoForward, Finalize: finalize,
	}, nil
}

func encodeBlocks(blocks []statecharts.ExecutableBlock) ([]executableBlockWire, error) {
	if blocks == nil {
		return nil, nil
	}
	result := make([]executableBlockWire, len(blocks))
	for i, block := range blocks {
		result[i] = make(executableBlockWire, len(block))
		for j := range block {
			value, err := encodeExecutable(block[j])
			if err != nil {
				return nil, err
			}
			result[i][j] = value
		}
	}
	return result, nil
}

func encodeExecutable(executable statecharts.Executable) (executableWire, error) {
	result := executableWire{Kind: executable.Kind, Raise: executable.Raise, Send: executable.Send, Cancel: executable.Cancel, Log: executable.Log, Assign: executable.Assign, Script: executable.Script, Call: executable.Call, Extension: executable.Extension}
	if executable.Choose != nil {
		choose := chooseWire{Branches: make([]chooseBranchWire, len(executable.Choose.Branches))}
		var err error
		for i, branch := range executable.Choose.Branches {
			choose.Branches[i].Condition = branch.Condition
			choose.Branches[i].Actions, err = encodeBlocks(branch.Actions)
			if err != nil {
				return executableWire{}, err
			}
		}
		choose.Else, err = encodeBlocks(executable.Choose.Else)
		if err != nil {
			return executableWire{}, err
		}
		result.Choose = &choose
	}
	if executable.ForEach != nil {
		actions, err := encodeBlocks(executable.ForEach.Actions)
		if err != nil {
			return executableWire{}, err
		}
		result.ForEach = &forEachWire{Array: executable.ForEach.Array, Item: executable.ForEach.Item, Index: executable.ForEach.Index, Actions: actions}
	}
	return result, nil
}

func decodeDefinition(wire definitionWire, path string) (statecharts.Definition, error) {
	root, err := decodeState(wire.Root, path+".root")
	if err != nil {
		return statecharts.Definition{}, err
	}
	return statecharts.Definition{ID: wire.ID, Name: wire.Name, Datamodel: wire.Datamodel, RevisionSalt: wire.RevisionSalt, DataBinding: wire.DataBinding, Data: wire.Data, Root: root}, nil
}

func decodeState(wire stateWire, path string) (statecharts.StateDefinition, error) {
	kind, err := parseStateKind(wire.Kind, path+".kind")
	if err != nil {
		return statecharts.StateDefinition{}, err
	}
	history, err := parseHistoryKind(wire.History, kind, path+".history")
	if err != nil {
		return statecharts.StateDefinition{}, err
	}
	result := statecharts.StateDefinition{ID: wire.ID, Kind: kind, History: history, Data: wire.Data}
	if wire.Initial != nil {
		value, err := decodeTransition(*wire.Initial, path+".initial")
		if err != nil {
			return statecharts.StateDefinition{}, err
		}
		result.Initial = &value
	}
	if result.OnEntry, err = decodeBlocks(wire.OnEntry, path+".onEntry"); err != nil {
		return statecharts.StateDefinition{}, err
	}
	if result.OnExit, err = decodeBlocks(wire.OnExit, path+".onExit"); err != nil {
		return statecharts.StateDefinition{}, err
	}
	if wire.Transitions != nil {
		result.Transitions = make([]statecharts.TransitionDefinition, len(wire.Transitions))
		for i := range wire.Transitions {
			result.Transitions[i], err = decodeTransition(wire.Transitions[i], fmt.Sprintf("%s.transitions[%d]", path, i))
			if err != nil {
				return statecharts.StateDefinition{}, err
			}
		}
	}
	if wire.Invokes != nil {
		result.Invokes = make([]statecharts.InvokeDefinition, len(wire.Invokes))
		for i := range wire.Invokes {
			result.Invokes[i], err = decodeInvoke(wire.Invokes[i], fmt.Sprintf("%s.invokes[%d]", path, i))
			if err != nil {
				return statecharts.StateDefinition{}, err
			}
		}
	}
	if wire.DoneData != nil {
		result.DoneData = &statecharts.DoneDataDefinition{Params: wire.DoneData.Params, Content: wire.DoneData.Content}
	}
	if wire.Children != nil {
		result.Children = make([]statecharts.StateDefinition, len(wire.Children))
		for i := range wire.Children {
			result.Children[i], err = decodeState(wire.Children[i], fmt.Sprintf("%s.children[%d]", path, i))
			if err != nil {
				return statecharts.StateDefinition{}, err
			}
		}
	}
	return result, nil
}

func decodeTransition(wire transitionWire, path string) (statecharts.TransitionDefinition, error) {
	actions, err := decodeBlocks(wire.Actions, path+".actions")
	if err != nil {
		return statecharts.TransitionDefinition{}, err
	}
	return statecharts.TransitionDefinition{Events: wire.Events, Targets: wire.Targets, Type: wire.Type, Condition: wire.Condition, Actions: actions}, nil
}

func decodeInvoke(wire invokeWire, path string) (statecharts.InvokeDefinition, error) {
	finalize, err := decodeBlocks(wire.Finalize, path+".finalize")
	if err != nil {
		return statecharts.InvokeDefinition{}, err
	}
	return statecharts.InvokeDefinition{DefinitionID: wire.DefinitionID, ID: wire.ID, IDLocation: wire.IDLocation, Type: wire.Type, TypeExpr: wire.TypeExpr, Src: wire.Src, SrcExpr: wire.SrcExpr, Params: wire.Params, Content: wire.Content, AutoForward: wire.AutoForward, Finalize: finalize}, nil
}

func decodeBlocks(blocks []executableBlockWire, path string) ([]statecharts.ExecutableBlock, error) {
	if blocks == nil {
		return nil, nil
	}
	result := make([]statecharts.ExecutableBlock, len(blocks))
	for i, block := range blocks {
		result[i] = make(statecharts.ExecutableBlock, len(block))
		for j := range block {
			value, err := decodeExecutable(block[j], fmt.Sprintf("%s[%d][%d]", path, i, j))
			if err != nil {
				return nil, err
			}
			result[i][j] = value
		}
	}
	return result, nil
}

func decodeExecutable(wire executableWire, path string) (statecharts.Executable, error) {
	result := statecharts.Executable{Kind: wire.Kind, Raise: wire.Raise, Send: wire.Send, Cancel: wire.Cancel, Log: wire.Log, Assign: wire.Assign, Script: wire.Script, Call: wire.Call, Extension: wire.Extension}
	if wire.Choose != nil {
		choose := statecharts.ChooseDefinition{Branches: make([]statecharts.ChooseBranchDefinition, len(wire.Choose.Branches))}
		var err error
		for i, branch := range wire.Choose.Branches {
			choose.Branches[i].Condition = branch.Condition
			choose.Branches[i].Actions, err = decodeBlocks(branch.Actions, fmt.Sprintf("%s.choose.branches[%d].actions", path, i))
			if err != nil {
				return statecharts.Executable{}, err
			}
		}
		choose.Else, err = decodeBlocks(wire.Choose.Else, path+".choose.else")
		if err != nil {
			return statecharts.Executable{}, err
		}
		result.Choose = &choose
	}
	if wire.ForEach != nil {
		actions, err := decodeBlocks(wire.ForEach.Actions, path+".foreach.actions")
		if err != nil {
			return statecharts.Executable{}, err
		}
		result.ForEach = &statecharts.ForEachDefinition{Array: wire.ForEach.Array, Item: wire.ForEach.Item, Index: wire.ForEach.Index, Actions: actions}
	}
	return result, nil
}

func parseStateKind(value, path string) (statecharts.StateKind, error) {
	switch value {
	case "atomic":
		return statecharts.KindAtomic, nil
	case "compound":
		return statecharts.KindCompound, nil
	case "parallel":
		return statecharts.KindParallel, nil
	case "final":
		return statecharts.KindFinal, nil
	case "history":
		return statecharts.KindHistory, nil
	default:
		return 0, &Error{Path: path, Err: fmt.Errorf("unknown state kind %q", value)}
	}
}

func parseHistoryKind(value string, kind statecharts.StateKind, path string) (statecharts.HistoryKind, error) {
	if kind != statecharts.KindHistory {
		if value != "" {
			return 0, &Error{Path: path, Err: fmt.Errorf("history is only valid on history states")}
		}
		return statecharts.Shallow, nil
	}
	switch value {
	case "", "shallow":
		return statecharts.Shallow, nil
	case "deep":
		return statecharts.Deep, nil
	default:
		return 0, &Error{Path: path, Err: fmt.Errorf("unknown history kind %q", value)}
	}
}

func wrapDefinitionError(err error) error {
	var definitionErr *statecharts.DefinitionError
	if errors.As(err, &definitionErr) {
		return &Error{Path: definitionErr.Path, Err: definitionErr.Err}
	}
	return &Error{Path: "definition", Err: err}
}

func pathError(path, format string, args ...any) error {
	return &Error{Path: path, Err: fmt.Errorf(format, args...)}
}

func asObject(value any, path string) (map[string]any, error) {
	result, ok := value.(map[string]any)
	if !ok {
		return nil, pathError(path, "must be an object")
	}
	return result, nil
}

func array(value any, path string) ([]any, error) {
	result, ok := value.([]any)
	if !ok {
		return nil, pathError(path, "must be an array")
	}
	return result, nil
}

func checkAllowed(object map[string]any, path string, allowed ...string) error {
	set := make(map[string]struct{}, len(allowed))
	for _, field := range allowed {
		set[field] = struct{}{}
	}
	keys := make([]string, 0, len(object))
	for field := range object {
		keys = append(keys, field)
	}
	sort.Strings(keys)
	for _, field := range keys {
		if _, ok := set[field]; !ok {
			return pathError(path+"."+field, "unknown field %q", field)
		}
	}
	return nil
}

func inspectStringFields(object map[string]any, path string, fields ...string) error {
	for _, field := range fields {
		value, present := object[field]
		if !present {
			continue
		}
		if _, ok := value.(string); !ok {
			return pathError(path+"."+field, "must be a string")
		}
	}
	return nil
}

func inspectBoolFields(object map[string]any, path string, fields ...string) error {
	for _, field := range fields {
		value, present := object[field]
		if !present {
			continue
		}
		if _, ok := value.(bool); !ok {
			return pathError(path+"."+field, "must be a boolean")
		}
	}
	return nil
}

func inspectStringListField(object map[string]any, field, path string) error {
	value, present := object[field]
	if !present {
		return nil
	}
	items, err := array(value, path+"."+field)
	if err != nil {
		return err
	}
	for i, item := range items {
		if _, ok := item.(string); !ok {
			return pathError(fmt.Sprintf("%s.%s[%d]", path, field, i), "must be a string")
		}
	}
	return nil
}

func inspectStateID(value any, path string) error {
	object, err := asObject(value, path)
	if err != nil {
		return err
	}
	if err := checkAllowed(object, path, "value", "generated"); err != nil {
		return err
	}
	if err := inspectStringFields(object, path, "value"); err != nil {
		return err
	}
	return inspectBoolFields(object, path, "generated")
}

func inspectDefinition(value any, path string) error {
	object, err := asObject(value, path)
	if err != nil {
		return err
	}
	if err := checkAllowed(object, path, "id", "name", "datamodel", "revisionSalt", "dataBinding", "data", "root"); err != nil {
		return err
	}
	if err := inspectStringFields(object, path, "id", "name", "datamodel", "revisionSalt", "dataBinding"); err != nil {
		return err
	}
	if err := inspectDataList(object["data"], path+".data"); err != nil {
		return err
	}
	return inspectState(object["root"], path+".root")
}

func inspectState(value any, path string) error {
	object, err := asObject(value, path)
	if err != nil {
		return err
	}
	if err := checkAllowed(object, path, "id", "kind", "history", "initial", "onEntry", "onExit", "transitions", "invokes", "data", "doneData", "children"); err != nil {
		return err
	}
	if err := inspectStateID(object["id"], path+".id"); err != nil {
		return err
	}
	if err := inspectStringFields(object, path, "kind", "history"); err != nil {
		return err
	}
	kind, _ := object["kind"].(string)
	if _, err := parseStateKind(kind, path+".kind"); err != nil {
		return err
	}
	if history, present := object["history"]; present {
		historyString, ok := history.(string)
		if !ok {
			return pathError(path+".history", "must be a string")
		}
		parsed, _ := parseStateKind(kind, path+".kind")
		if _, err := parseHistoryKind(historyString, parsed, path+".history"); err != nil {
			return err
		}
	}
	if err := inspectOptional(object, "initial", path, inspectTransition); err != nil {
		return err
	}
	if err := inspectBlocksField(object, "onEntry", path); err != nil {
		return err
	}
	if err := inspectBlocksField(object, "onExit", path); err != nil {
		return err
	}
	if err := inspectListField(object, "transitions", path, inspectTransition); err != nil {
		return err
	}
	if err := inspectListField(object, "invokes", path, inspectInvoke); err != nil {
		return err
	}
	if err := inspectDataList(object["data"], path+".data"); err != nil {
		return err
	}
	if err := inspectOptional(object, "doneData", path, inspectDoneData); err != nil {
		return err
	}
	return inspectListField(object, "children", path, inspectState)
}

func inspectDataList(value any, path string) error {
	if value == nil {
		return nil
	}
	items, err := array(value, path)
	if err != nil {
		return err
	}
	for i, item := range items {
		itemPath := fmt.Sprintf("%s[%d]", path, i)
		object, err := asObject(item, itemPath)
		if err != nil {
			return err
		}
		if err := checkAllowed(object, itemPath, "id", "source", "expr", "content"); err != nil {
			return err
		}
		if err := inspectStringFields(object, itemPath, "id", "source"); err != nil {
			return err
		}
		if err := inspectOptional(object, "expr", itemPath, inspectExpression); err != nil {
			return err
		}
		if err := inspectOptional(object, "content", itemPath, inspectExpression); err != nil {
			return err
		}
	}
	return nil
}

func inspectTransition(value any, path string) error {
	object, err := asObject(value, path)
	if err != nil {
		return err
	}
	if err := checkAllowed(object, path, "events", "targets", "type", "condition", "actions"); err != nil {
		return err
	}
	if err := inspectStringListField(object, "events", path); err != nil {
		return err
	}
	if err := inspectStringListField(object, "targets", path); err != nil {
		return err
	}
	if err := inspectStringFields(object, path, "type"); err != nil {
		return err
	}
	if err := inspectOptional(object, "condition", path, inspectExpression); err != nil {
		return err
	}
	return inspectBlocksField(object, "actions", path)
}

func inspectInvoke(value any, path string) error {
	object, err := asObject(value, path)
	if err != nil {
		return err
	}
	if err := checkAllowed(object, path, "definitionId", "id", "idLocation", "type", "typeExpr", "src", "srcExpr", "params", "content", "autoForward", "finalize"); err != nil {
		return err
	}
	if err := inspectStringFields(object, path, "definitionId", "id", "type", "src"); err != nil {
		return err
	}
	if err := inspectBoolFields(object, path, "autoForward"); err != nil {
		return err
	}
	for _, field := range []string{"idLocation", "typeExpr", "srcExpr", "content"} {
		if err := inspectOptional(object, field, path, inspectExpression); err != nil {
			return err
		}
	}
	if err := inspectParams(object["params"], path+".params"); err != nil {
		return err
	}
	return inspectBlocksField(object, "finalize", path)
}

func inspectDoneData(value any, path string) error {
	object, err := asObject(value, path)
	if err != nil {
		return err
	}
	if err := checkAllowed(object, path, "params", "content"); err != nil {
		return err
	}
	if err := inspectParams(object["params"], path+".params"); err != nil {
		return err
	}
	return inspectOptional(object, "content", path, inspectExpression)
}

func inspectParams(value any, path string) error {
	if value == nil {
		return nil
	}
	items, err := array(value, path)
	if err != nil {
		return err
	}
	for i, item := range items {
		itemPath := fmt.Sprintf("%s[%d]", path, i)
		object, err := asObject(item, itemPath)
		if err != nil {
			return err
		}
		if err := checkAllowed(object, itemPath, "name", "expr", "location"); err != nil {
			return err
		}
		if err := inspectStringFields(object, itemPath, "name"); err != nil {
			return err
		}
		if err := inspectOptional(object, "expr", itemPath, inspectExpression); err != nil {
			return err
		}
		if err := inspectOptional(object, "location", itemPath, inspectExpression); err != nil {
			return err
		}
	}
	return nil
}

func inspectExpression(value any, path string) error {
	object, err := asObject(value, path)
	if err != nil {
		return err
	}
	if err := checkAllowed(object, path, "kind", "data"); err != nil {
		return err
	}
	if err := inspectStringFields(object, path, "kind"); err != nil {
		return err
	}
	return inspectValue(object["data"], path+".data")
}

func inspectValue(value any, path string) error {
	object, err := asObject(value, path)
	if err != nil {
		return err
	}
	if err := checkAllowed(object, path, "version", "kind", "bool", "string", "number", "list", "map", "tag", "payload"); err != nil {
		return err
	}
	if version, present := object["version"]; present {
		if _, ok := version.(stdjson.Number); !ok {
			return pathError(path+".version", "must be a number")
		}
	}
	if err := inspectStringFields(object, path, "kind", "string", "number", "tag"); err != nil {
		return err
	}
	if err := inspectBoolFields(object, path, "bool"); err != nil {
		return err
	}
	if list, ok := object["list"]; ok {
		items, err := array(list, path+".list")
		if err != nil {
			return err
		}
		for i, item := range items {
			if err := inspectValue(item, fmt.Sprintf("%s.list[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	if mapped, ok := object["map"]; ok {
		values, err := asObject(mapped, path+".map")
		if err != nil {
			return err
		}
		keys := make([]string, 0, len(values))
		for key := range values {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := inspectValue(values[key], fmt.Sprintf("%s.map[%q]", path, key)); err != nil {
				return err
			}
		}
	}
	if payload, ok := object["payload"]; ok {
		if err := inspectValue(payload, path+".payload"); err != nil {
			return err
		}
	}
	encoded, err := stdjson.Marshal(value)
	if err != nil {
		return pathError(path, "%v", err)
	}
	var canonical statecharts.Value
	if err := stdjson.Unmarshal(encoded, &canonical); err != nil {
		return pathError(path, "%v", err)
	}
	return nil
}

func inspectBlocksField(object map[string]any, field, path string) error {
	value, ok := object[field]
	if !ok || value == nil {
		return nil
	}
	blocks, err := array(value, path+"."+field)
	if err != nil {
		return err
	}
	for i, block := range blocks {
		blockPath := fmt.Sprintf("%s.%s[%d]", path, field, i)
		items, err := array(block, blockPath)
		if err != nil {
			return err
		}
		for j, item := range items {
			if err := inspectExecutable(item, fmt.Sprintf("%s[%d]", blockPath, j)); err != nil {
				return err
			}
		}
	}
	return nil
}

func inspectExecutable(value any, path string) error {
	object, err := asObject(value, path)
	if err != nil {
		return err
	}
	payloads := []string{"raise", "send", "cancel", "log", "assign", "choose", "foreach", "script", "call", "extension"}
	allowed := append([]string{"kind"}, payloads...)
	if err := checkAllowed(object, path, allowed...); err != nil {
		return err
	}
	kind, ok := object["kind"].(string)
	if !ok {
		return pathError(path+".kind", "must be a string")
	}
	known := false
	count := 0
	present := ""
	for _, payload := range payloads {
		if payload == kind {
			known = true
		}
		if _, ok := object[payload]; ok {
			count++
			present = payload
		}
	}
	if !known {
		return pathError(path+".kind", "unknown executable kind %q", kind)
	}
	if count != 1 || present != kind {
		return pathError(path, "executable kind %q requires exactly its matching payload", kind)
	}
	payload := object[kind]
	switch kind {
	case "raise":
		return inspectExpressionFields(payload, path+".raise", []string{"event", "eventExpr", "data"}, []string{"eventExpr", "data"}, "event")
	case "send":
		return inspectExpressionFields(payload, path+".send", []string{"event", "eventExpr", "target", "targetExpr", "type", "typeExpr", "id", "idLocation", "delay", "delayExpr", "params", "content"}, []string{"eventExpr", "targetExpr", "typeExpr", "idLocation", "delayExpr", "content"}, "event", "target", "type", "id", "delay")
	case "cancel":
		return inspectExpressionFields(payload, path+".cancel", []string{"sendID", "sendIDExpr"}, []string{"sendIDExpr"}, "sendID")
	case "log":
		return inspectExpressionFields(payload, path+".log", []string{"label", "labelExpr", "expr"}, []string{"labelExpr", "expr"}, "label")
	case "assign":
		return inspectExpressionFields(payload, path+".assign", []string{"location", "expr"}, []string{"location", "expr"})
	case "script":
		return inspectExpressionFields(payload, path+".script", []string{"expr"}, []string{"expr"})
	case "choose":
		return inspectChoose(payload, path+".choose")
	case "foreach":
		return inspectForEach(payload, path+".foreach")
	case "call":
		return inspectCall(payload, path+".call")
	case "extension":
		object, err := asObject(payload, path+".extension")
		if err != nil {
			return err
		}
		if err := checkAllowed(object, path+".extension", "namespace", "name", "data"); err != nil {
			return err
		}
		if err := inspectStringFields(object, path+".extension", "namespace", "name"); err != nil {
			return err
		}
		return inspectValue(object["data"], path+".extension.data")
	}
	return nil
}

func inspectExpressionFields(value any, path string, allowed, expressionFields []string, stringFields ...string) error {
	object, err := asObject(value, path)
	if err != nil {
		return err
	}
	if err := checkAllowed(object, path, allowed...); err != nil {
		return err
	}
	if err := inspectStringFields(object, path, stringFields...); err != nil {
		return err
	}
	for _, field := range expressionFields {
		if err := inspectOptional(object, field, path, inspectExpression); err != nil {
			return err
		}
	}
	if params, ok := object["params"]; ok {
		if err := inspectParams(params, path+".params"); err != nil {
			return err
		}
	}
	return nil
}

func inspectChoose(value any, path string) error {
	object, err := asObject(value, path)
	if err != nil {
		return err
	}
	if err := checkAllowed(object, path, "branches", "else"); err != nil {
		return err
	}
	if branches, ok := object["branches"]; ok {
		items, err := array(branches, path+".branches")
		if err != nil {
			return err
		}
		for i, item := range items {
			branchPath := fmt.Sprintf("%s.branches[%d]", path, i)
			branch, err := asObject(item, branchPath)
			if err != nil {
				return err
			}
			if err := checkAllowed(branch, branchPath, "condition", "actions"); err != nil {
				return err
			}
			if err := inspectExpression(branch["condition"], branchPath+".condition"); err != nil {
				return err
			}
			if err := inspectBlocksField(branch, "actions", branchPath); err != nil {
				return err
			}
		}
	}
	return inspectBlocksField(object, "else", path)
}

func inspectForEach(value any, path string) error {
	object, err := asObject(value, path)
	if err != nil {
		return err
	}
	if err := checkAllowed(object, path, "array", "item", "index", "actions"); err != nil {
		return err
	}
	if err := inspectStringFields(object, path, "item", "index"); err != nil {
		return err
	}
	if err := inspectExpression(object["array"], path+".array"); err != nil {
		return err
	}
	return inspectBlocksField(object, "actions", path)
}

func inspectCall(value any, path string) error {
	object, err := asObject(value, path)
	if err != nil {
		return err
	}
	if err := checkAllowed(object, path, "function"); err != nil {
		return err
	}
	function, err := asObject(object["function"], path+".function")
	if err != nil {
		return err
	}
	if err := checkAllowed(function, path+".function", "name", "version", "args"); err != nil {
		return err
	}
	if err := inspectStringFields(function, path+".function", "name", "version"); err != nil {
		return err
	}
	if args, ok := function["args"]; ok {
		items, err := array(args, path+".function.args")
		if err != nil {
			return err
		}
		for i, item := range items {
			if err := inspectExpression(item, fmt.Sprintf("%s.function.args[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func inspectOptional(object map[string]any, field, path string, inspect func(any, string) error) error {
	value, ok := object[field]
	if !ok || value == nil {
		return nil
	}
	return inspect(value, path+"."+field)
}

func inspectListField(object map[string]any, field, path string, inspect func(any, string) error) error {
	value, ok := object[field]
	if !ok || value == nil {
		return nil
	}
	items, err := array(value, path+"."+field)
	if err != nil {
		return err
	}
	for i, item := range items {
		if err := inspect(item, fmt.Sprintf("%s.%s[%d]", path, field, i)); err != nil {
			return err
		}
	}
	return nil
}
