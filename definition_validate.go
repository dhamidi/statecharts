package statecharts

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// DefinitionError reports a structural definition error at a stable public
// traversal path.
type DefinitionError struct {
	Path string
	Err  error
}

func (e *DefinitionError) Error() string {
	return fmt.Sprintf("statecharts: definition %s: %v", e.Path, e.Err)
}

func (e *DefinitionError) Unwrap() error { return e.Err }

func definitionError(path, format string, args ...any) error {
	return &DefinitionError{Path: path, Err: fmt.Errorf(format, args...)}
}

// Validate performs syntax-neutral structural validation. Datamodel-specific
// expression checking belongs to Datamodel compilation, not this method.
// Validation assigns generated IDs only on a private clone.
func (d Definition) Validate() error {
	_, err := normalizeDefinition(d)
	return err
}

type definitionStateNode struct {
	state  *StateDefinition
	parent *definitionStateNode
	path   string
}

type definitionValidator struct {
	definition *Definition
	byID       map[Identifier]*definitionStateNode
	invokeIDs  map[Identifier]string
	dataIDs    map[Identifier]string
}

func normalizeDefinition(input Definition) (Definition, error) {
	definition := input.Clone()
	validator := definitionValidator{
		definition: &definition,
		byID:       make(map[Identifier]*definitionStateNode),
		invokeIDs:  make(map[Identifier]string),
		dataIDs:    make(map[Identifier]string),
	}
	if err := validator.validateHeader(); err != nil {
		return Definition{}, err
	}
	if definition.DataBinding == "" {
		definition.DataBinding = DataBindingEarly
	}
	if err := validator.validateDataDefinitions(definition.Data, "definition.data"); err != nil {
		return Definition{}, err
	}
	reserved := make(map[Identifier]bool)
	collectReservedStateIDs(&definition.Root, reserved)
	nextGenerated := 0
	if err := assignGeneratedStateIDs(&definition.Root, "root", reserved, &nextGenerated); err != nil {
		return Definition{}, err
	}
	normalizeImplicitInitials(&definition.Root)
	normalizeTransitionTypes(&definition.Root)
	root, err := validator.collectStates(&definition.Root, nil, "root")
	if err != nil {
		return Definition{}, err
	}
	if err := validator.validateState(root, true); err != nil {
		return Definition{}, err
	}
	return definition, nil
}

func (v *definitionValidator) validateHeader() error {
	if err := validatePlainIdentifier(v.definition.ID); err != nil {
		return definitionError("definition.id", "%v", err)
	}
	if err := validateUTF8(v.definition.Name); err != nil {
		return definitionError("definition.name", "%v", err)
	}
	if err := validatePlainIdentifier(v.definition.Datamodel); err != nil {
		return definitionError("definition.datamodel", "%v", err)
	}
	if err := validateUTF8(v.definition.RevisionSalt); err != nil {
		return definitionError("definition.revisionSalt", "%v", err)
	}
	if v.definition.DataBinding != "" && v.definition.DataBinding != DataBindingEarly && v.definition.DataBinding != DataBindingLate {
		return definitionError("definition.dataBinding", "invalid data binding %q", v.definition.DataBinding)
	}
	return nil
}

func (v *definitionValidator) validateDataDefinitions(definitions []DataDefinition, path string) error {
	for i := range definitions {
		definition := &definitions[i]
		itemPath := fmt.Sprintf("%s[%d]", path, i)
		if err := validatePlainIdentifier(definition.ID); err != nil {
			return definitionError(itemPath+".id", "%v", err)
		}
		if previous, exists := v.dataIDs[definition.ID]; exists {
			return definitionError(itemPath+".id", "duplicate data ID %q (already declared at %s)", definition.ID, previous)
		}
		v.dataIDs[definition.ID] = itemPath + ".id"
		initializerCount := 0
		if definition.Source != "" {
			initializerCount++
		}
		if definition.Expr != nil {
			initializerCount++
		}
		if definition.Content != nil {
			initializerCount++
		}
		if initializerCount > 1 {
			return definitionError(itemPath, "data declaration contains %d initializers; want at most one", initializerCount)
		}
		if err := validateUTF8(definition.Source); err != nil {
			return definitionError(itemPath+".source", "%v", err)
		}
		if definition.Expr != nil {
			if err := v.validateExpression(definition.Expr, itemPath+".expr"); err != nil {
				return err
			}
		}
		if definition.Content != nil {
			if err := v.validateExpression(definition.Content, itemPath+".content"); err != nil {
				return err
			}
		}
	}
	return nil
}

func collectReservedStateIDs(state *StateDefinition, reserved map[Identifier]bool) {
	if state.ID.Value != "" {
		reserved[state.ID.Value] = true
	}
	for i := range state.Children {
		collectReservedStateIDs(&state.Children[i], reserved)
	}
}

func assignGeneratedStateIDs(state *StateDefinition, path string, reserved map[Identifier]bool, next *int) error {
	if state.ID.Value == "" {
		if !state.ID.Generated {
			return definitionError(path+".id", "state ID is empty and is not marked generated")
		}
		for {
			*next++
			candidate := Identifier(fmt.Sprintf("state.%d", *next))
			if !reserved[candidate] {
				state.ID.Value = candidate
				reserved[candidate] = true
				break
			}
		}
	}
	for i := range state.Children {
		if err := assignGeneratedStateIDs(&state.Children[i], fmt.Sprintf("%s.children[%d]", path, i), reserved, next); err != nil {
			return err
		}
	}
	return nil
}

func normalizeTransitionTypes(state *StateDefinition) {
	if state.Initial != nil && state.Initial.Type == "" {
		state.Initial.Type = TransitionExternal
	}
	for i := range state.Transitions {
		if state.Transitions[i].Type == "" {
			state.Transitions[i].Type = TransitionExternal
		}
	}
	for i := range state.Children {
		normalizeTransitionTypes(&state.Children[i])
	}
}

func normalizeImplicitInitials(state *StateDefinition) {
	if state.Kind == KindCompound && state.Initial == nil {
		for i := range state.Children {
			if state.Children[i].Kind != KindHistory {
				state.Initial = &TransitionDefinition{Targets: []Identifier{state.Children[i].ID.Value}}
				break
			}
		}
	}
	for i := range state.Children {
		normalizeImplicitInitials(&state.Children[i])
	}
}

func (v *definitionValidator) collectStates(state *StateDefinition, parent *definitionStateNode, path string) (*definitionStateNode, error) {
	if err := validatePlainIdentifier(state.ID.Value); err != nil {
		return nil, definitionError(path+".id", "%v", err)
	}
	if previous := v.byID[state.ID.Value]; previous != nil {
		return nil, definitionError(path+".id", "duplicate state ID %q (already declared at %s.id)", state.ID.Value, previous.path)
	}
	node := &definitionStateNode{state: state, parent: parent, path: path}
	v.byID[state.ID.Value] = node
	for i := range state.Children {
		if _, err := v.collectStates(&state.Children[i], node, fmt.Sprintf("%s.children[%d]", path, i)); err != nil {
			return nil, err
		}
	}
	return node, nil
}

func (v *definitionValidator) validateState(node *definitionStateNode, root bool) error {
	state := node.state
	path := node.path
	if state.Kind > KindHistory {
		return definitionError(path+".kind", "invalid state kind %d", state.Kind)
	}
	if state.Kind == KindHistory && state.History > Deep {
		return definitionError(path+".history", "invalid history kind %d", state.History)
	}
	if state.Kind != KindHistory && state.History != Shallow {
		return definitionError(path+".history", "history kind is allowed only on history states")
	}
	if root && (state.Kind == KindCompound || state.Kind == KindParallel) && (len(state.OnEntry) != 0 || len(state.OnExit) != 0 || len(state.Invokes) != 0) {
		if len(state.OnEntry) != 0 {
			return definitionError(path+".onEntry", "document root must not have entry content")
		}
		if len(state.OnExit) != 0 {
			return definitionError(path+".onExit", "document root must not have exit content")
		}
		return definitionError(path+".invokes", "document root must not have invokes")
	}
	if state.Kind != KindFinal && state.DoneData != nil {
		return definitionError(path+".doneData", "done data is allowed only on final states")
	}
	if root && len(state.Data) != 0 {
		return definitionError(path+".data", "document-level data belongs in Definition.Data")
	}
	if (state.Kind == KindFinal || state.Kind == KindHistory) && len(state.Data) != 0 {
		return definitionError(path+".data", "%s state must not contain data declarations", state.Kind)
	}
	if !root {
		if err := v.validateDataDefinitions(state.Data, path+".data"); err != nil {
			return err
		}
	}

	switch state.Kind {
	case KindAtomic:
		if state.Initial != nil {
			return definitionError(path+".initial", "atomic state must not have a default transition")
		}
		if len(state.Children) != 0 {
			return definitionError(path+".children", "atomic state must not have children")
		}
	case KindCompound:
		if len(v.realDefinitionChildren(node)) == 0 {
			return definitionError(path+".children", "compound state has no children")
		}
		if state.Initial == nil || len(state.Initial.Targets) == 0 {
			return definitionError(path+".initial", "compound state has no initial target")
		}
	case KindParallel:
		if state.Initial != nil {
			return definitionError(path+".initial", "parallel state must not have a default transition")
		}
		children := v.realDefinitionChildren(node)
		if len(children) == 0 {
			return definitionError(path+".children", "parallel state has no children")
		}
		for _, child := range children {
			if child.state.Kind == KindFinal {
				return definitionError(child.path+".kind", "parallel state must not contain a final child")
			}
		}
	case KindFinal:
		if state.Initial != nil {
			return definitionError(path+".initial", "final state must not have a default transition")
		}
		if len(state.Children) != 0 {
			return definitionError(path+".children", "final state must not have children")
		}
		if len(state.Transitions) != 0 {
			return definitionError(path+".transitions", "final state must not have transitions")
		}
		if len(state.Invokes) != 0 {
			return definitionError(path+".invokes", "final state must not have invokes")
		}
	case KindHistory:
		if node.parent == nil {
			return definitionError(path, "history state must have a parent")
		}
		if state.Initial == nil || len(state.Initial.Targets) == 0 {
			return definitionError(path+".initial", "history state has no default target")
		}
		if len(state.Children) != 0 || len(state.OnEntry) != 0 || len(state.OnExit) != 0 || len(state.Transitions) != 0 || len(state.Invokes) != 0 || state.DoneData != nil {
			return definitionError(path, "history state contains ordinary state content")
		}
	}

	if state.Initial != nil {
		if err := v.validateTransition(state.Initial, path+".initial", true); err != nil {
			return err
		}
		if root && (len(state.Initial.Actions) != 0 || state.Initial.Type == TransitionInternal) {
			return definitionError(path+".initial", "document root initial transition may only specify targets")
		}
		for i, targetID := range state.Initial.Targets {
			target := v.byID[targetID]
			targetPath := fmt.Sprintf("%s.initial.targets[%d]", path, i)
			if state.Kind == KindCompound && (target == nil || !isDefinitionDescendant(target, node)) {
				return definitionError(targetPath, "initial target %q is not a descendant of state %q", targetID, state.ID.Value)
			}
			if state.Kind == KindHistory && target != nil {
				if !isDefinitionDescendant(target, node.parent) {
					return definitionError(targetPath, "history default target %q is outside parent %q", targetID, node.parent.state.ID.Value)
				}
				if state.History == Shallow && target.parent != node.parent {
					return definitionError(targetPath, "shallow history target %q is not an immediate child", targetID)
				}
			}
		}
	}

	if err := v.validateBlocks(state.OnEntry, path+".onEntry", false); err != nil {
		return err
	}
	if err := v.validateBlocks(state.OnExit, path+".onExit", false); err != nil {
		return err
	}
	for i := range state.Transitions {
		if err := v.validateTransition(&state.Transitions[i], fmt.Sprintf("%s.transitions[%d]", path, i), false); err != nil {
			return err
		}
	}
	for i := range state.Invokes {
		if err := v.validateInvoke(&state.Invokes[i], fmt.Sprintf("%s.invokes[%d]", path, i), state.ID.Value); err != nil {
			return err
		}
	}
	if state.DoneData != nil {
		if err := v.validateDoneData(state.DoneData, path+".doneData"); err != nil {
			return err
		}
	}
	for i := range state.Children {
		child := v.byID[state.Children[i].ID.Value]
		if err := v.validateState(child, false); err != nil {
			return err
		}
	}
	return nil
}

func (v *definitionValidator) validateTransition(transition *TransitionDefinition, path string, defaultTransition bool) error {
	if transition.Type != TransitionExternal && transition.Type != TransitionInternal {
		return definitionError(path+".type", "invalid transition type %q", transition.Type)
	}
	if defaultTransition && (len(transition.Events) != 0 || transition.Condition != nil) {
		return definitionError(path, "default transition must be eventless and unconditional")
	}
	if !defaultTransition && len(transition.Events) == 0 && transition.Condition == nil && len(transition.Targets) == 0 {
		return definitionError(path, "transition has no event, condition, or target")
	}
	for i, event := range transition.Events {
		if err := validateEventDescriptor(event); err != nil {
			return definitionError(fmt.Sprintf("%s.events[%d]", path, i), "%v", err)
		}
	}
	if transition.Condition != nil {
		if err := v.validateExpression(transition.Condition, path+".condition"); err != nil {
			return err
		}
	}
	if err := v.validateBlocks(transition.Actions, path+".actions", false); err != nil {
		return err
	}
	if err := v.validateTargetSet(transition.Targets, path+".targets"); err != nil {
		return err
	}
	return nil
}

func (v *definitionValidator) validateInvoke(invoke *InvokeDefinition, path string, owner Identifier) error {
	if invoke.ID != "" && invoke.IDLocation != nil {
		return definitionError(path+".idLocation", "invoke id and idLocation are mutually exclusive")
	}
	if invoke.ID != "" {
		if err := validatePlainIdentifier(invoke.ID); err != nil {
			return definitionError(path+".id", "%v", err)
		}
		if previous, exists := v.invokeIDs[invoke.ID]; exists {
			return definitionError(path+".id", "duplicate invoke ID %q (already declared at %s)", invoke.ID, previous)
		}
		v.invokeIDs[invoke.ID] = fmt.Sprintf("state %q", owner)
	}
	if invoke.IDLocation != nil {
		if err := v.validateExpression(invoke.IDLocation, path+".idLocation"); err != nil {
			return err
		}
	}
	if invoke.Type != "" && invoke.TypeExpr != nil {
		return definitionError(path+".typeExpr", "invoke type and typeExpr are mutually exclusive")
	}
	if err := validateUTF8(invoke.Type); err != nil {
		return definitionError(path+".type", "%v", err)
	}
	if invoke.TypeExpr != nil {
		if err := v.validateExpression(invoke.TypeExpr, path+".typeExpr"); err != nil {
			return err
		}
	}
	if invoke.Src != "" && invoke.SrcExpr != nil {
		return definitionError(path+".srcExpr", "invoke src and srcExpr are mutually exclusive")
	}
	if err := validateUTF8(invoke.Src); err != nil {
		return definitionError(path+".src", "%v", err)
	}
	if invoke.SrcExpr != nil {
		if err := v.validateExpression(invoke.SrcExpr, path+".srcExpr"); err != nil {
			return err
		}
	}
	if len(invoke.Params) != 0 && invoke.Content != nil {
		return definitionError(path+".content", "invoke params and whole-payload content are mutually exclusive")
	}
	if err := v.validateParams(invoke.Params, path+".params"); err != nil {
		return err
	}
	if invoke.Content != nil {
		if err := v.validateExpression(invoke.Content, path+".content"); err != nil {
			return err
		}
	}
	return v.validateBlocks(invoke.Finalize, path+".finalize", true)
}

func (v *definitionValidator) validateDoneData(done *DoneDataDefinition, path string) error {
	if len(done.Params) != 0 && done.Content != nil {
		return definitionError(path+".content", "done-data params and whole-payload content are mutually exclusive")
	}
	if err := v.validateParams(done.Params, path+".params"); err != nil {
		return err
	}
	if done.Content != nil {
		return v.validateExpression(done.Content, path+".content")
	}
	return nil
}

func (v *definitionValidator) validateParams(params []ParamDefinition, path string) error {
	seen := make(map[Identifier]bool)
	for i := range params {
		paramPath := fmt.Sprintf("%s[%d]", path, i)
		if err := validatePlainIdentifier(params[i].Name); err != nil {
			return definitionError(paramPath+".name", "%v", err)
		}
		if seen[params[i].Name] {
			return definitionError(paramPath+".name", "duplicate parameter %q", params[i].Name)
		}
		seen[params[i].Name] = true
		if (params[i].Expr == nil) == (params[i].Location == nil) {
			return definitionError(paramPath, "parameter requires exactly one of expr and location")
		}
		if params[i].Expr != nil {
			if err := v.validateExpression(params[i].Expr, paramPath+".expr"); err != nil {
				return err
			}
		}
		if params[i].Location != nil {
			if err := v.validateExpression(params[i].Location, paramPath+".location"); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *definitionValidator) validateBlocks(blocks []ExecutableBlock, path string, finalize bool) error {
	for i, block := range blocks {
		if len(block) == 0 {
			return definitionError(fmt.Sprintf("%s[%d]", path, i), "executable block is empty")
		}
		for j := range block {
			if err := v.validateExecutable(&block[j], fmt.Sprintf("%s[%d][%d]", path, i, j), finalize); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *definitionValidator) validateExecutable(executable *Executable, path string, finalize bool) error {
	count := 0
	for _, present := range []bool{
		executable.Raise != nil, executable.Send != nil, executable.Cancel != nil,
		executable.Log != nil, executable.Assign != nil, executable.Choose != nil,
		executable.ForEach != nil, executable.Script != nil, executable.Call != nil,
		executable.Extension != nil,
	} {
		if present {
			count++
		}
	}
	if count != 1 {
		return definitionError(path, "executable union contains %d payloads; want exactly one", count)
	}
	if finalize && (executable.Kind == ExecutableRaise || executable.Kind == ExecutableSend || executable.Kind == ExecutableCancel) {
		return definitionError(path, "%s is not allowed in invoke finalize content", executable.Kind)
	}
	switch executable.Kind {
	case ExecutableRaise:
		if executable.Raise == nil {
			return definitionError(path, "raise kind does not match union payload")
		}
		return v.validateRaise(executable.Raise, path+".raise")
	case ExecutableSend:
		if executable.Send == nil {
			return definitionError(path, "send kind does not match union payload")
		}
		return v.validateSend(executable.Send, path+".send")
	case ExecutableCancel:
		if executable.Cancel == nil {
			return definitionError(path, "cancel kind does not match union payload")
		}
		return v.validateCancel(executable.Cancel, path+".cancel")
	case ExecutableLog:
		if executable.Log == nil {
			return definitionError(path, "log kind does not match union payload")
		}
		return v.validateLog(executable.Log, path+".log")
	case ExecutableAssign:
		if executable.Assign == nil {
			return definitionError(path, "assign kind does not match union payload")
		}
		if err := v.validateExpression(&executable.Assign.Location, path+".assign.location"); err != nil {
			return err
		}
		return v.validateExpression(&executable.Assign.Expr, path+".assign.expr")
	case ExecutableChoose:
		if executable.Choose == nil {
			return definitionError(path, "choose kind does not match union payload")
		}
		if len(executable.Choose.Branches) == 0 {
			return definitionError(path+".choose.branches", "choose requires at least one branch")
		}
		for i := range executable.Choose.Branches {
			branchPath := fmt.Sprintf("%s.choose.branches[%d]", path, i)
			if err := v.validateExpression(&executable.Choose.Branches[i].Condition, branchPath+".condition"); err != nil {
				return err
			}
			if err := v.validateBlocks(executable.Choose.Branches[i].Actions, branchPath+".actions", finalize); err != nil {
				return err
			}
		}
		return v.validateBlocks(executable.Choose.Else, path+".choose.else", finalize)
	case ExecutableForEach:
		if executable.ForEach == nil {
			return definitionError(path, "foreach kind does not match union payload")
		}
		if err := v.validateExpression(&executable.ForEach.Array, path+".foreach.array"); err != nil {
			return err
		}
		if err := validatePlainIdentifier(executable.ForEach.Item); err != nil {
			return definitionError(path+".foreach.item", "%v", err)
		}
		if executable.ForEach.Index != "" {
			if err := validatePlainIdentifier(executable.ForEach.Index); err != nil {
				return definitionError(path+".foreach.index", "%v", err)
			}
			if executable.ForEach.Index == executable.ForEach.Item {
				return definitionError(path+".foreach.index", "index and item bindings must differ")
			}
		}
		return v.validateBlocks(executable.ForEach.Actions, path+".foreach.actions", finalize)
	case ExecutableScript:
		if executable.Script == nil {
			return definitionError(path, "script kind does not match union payload")
		}
		return v.validateExpression(&executable.Script.Expr, path+".script.expr")
	case ExecutableCall:
		if executable.Call == nil {
			return definitionError(path, "call kind does not match union payload")
		}
		return v.validateFunctionRef(&executable.Call.Function, path+".call.function")
	case ExecutableExtension:
		if executable.Extension == nil {
			return definitionError(path, "extension kind does not match union payload")
		}
		if executable.Extension.Namespace == "" {
			return definitionError(path+".extension.namespace", "extension namespace is empty")
		}
		if err := validateUTF8(executable.Extension.Namespace); err != nil {
			return definitionError(path+".extension.namespace", "%v", err)
		}
		if executable.Extension.Name == "" {
			return definitionError(path+".extension.name", "extension name is empty")
		}
		if err := validateUTF8(executable.Extension.Name); err != nil {
			return definitionError(path+".extension.name", "%v", err)
		}
		if _, err := executable.Extension.Data.MarshalBinary(); err != nil {
			return definitionError(path+".extension.data", "%v", err)
		}
		return nil
	default:
		return definitionError(path+".kind", "unknown executable kind %q", executable.Kind)
	}
}

func (v *definitionValidator) validateRaise(raise *RaiseDefinition, path string) error {
	if (raise.Event == "") == (raise.EventExpr == nil) {
		return definitionError(path, "raise requires exactly one of event and eventExpr")
	}
	if raise.Event != "" {
		if err := validatePlainIdentifier(raise.Event); err != nil {
			return definitionError(path+".event", "%v", err)
		}
	}
	if raise.EventExpr != nil {
		if err := v.validateExpression(raise.EventExpr, path+".eventExpr"); err != nil {
			return err
		}
	}
	if raise.Data != nil {
		return v.validateExpression(raise.Data, path+".data")
	}
	return nil
}

func (v *definitionValidator) validateSend(send *SendDefinition, path string) error {
	if (send.Event == "") == (send.EventExpr == nil) {
		return definitionError(path, "send requires exactly one of event and eventExpr")
	}
	if send.Event != "" {
		if err := validatePlainIdentifier(send.Event); err != nil {
			return definitionError(path+".event", "%v", err)
		}
	}
	if send.EventExpr != nil {
		if err := v.validateExpression(send.EventExpr, path+".eventExpr"); err != nil {
			return err
		}
	}
	if send.Target != "" && send.TargetExpr != nil {
		return definitionError(path+".targetExpr", "send target and targetExpr are mutually exclusive")
	}
	if err := validateUTF8(send.Target); err != nil {
		return definitionError(path+".target", "%v", err)
	}
	if send.TargetExpr != nil {
		if err := v.validateExpression(send.TargetExpr, path+".targetExpr"); err != nil {
			return err
		}
	}
	if send.Type != "" && send.TypeExpr != nil {
		return definitionError(path+".typeExpr", "send type and typeExpr are mutually exclusive")
	}
	if err := validateUTF8(send.Type); err != nil {
		return definitionError(path+".type", "%v", err)
	}
	if send.TypeExpr != nil {
		if err := v.validateExpression(send.TypeExpr, path+".typeExpr"); err != nil {
			return err
		}
	}
	if send.ID != "" && send.IDLocation != nil {
		return definitionError(path+".idLocation", "send id and idLocation are mutually exclusive")
	}
	if send.ID != "" {
		if err := validatePlainIdentifier(send.ID); err != nil {
			return definitionError(path+".id", "%v", err)
		}
	}
	if send.IDLocation != nil {
		if err := v.validateExpression(send.IDLocation, path+".idLocation"); err != nil {
			return err
		}
	}
	if send.Delay != "" && send.DelayExpr != nil {
		return definitionError(path+".delayExpr", "send delay and delayExpr are mutually exclusive")
	}
	if err := validateUTF8(send.Delay); err != nil {
		return definitionError(path+".delay", "%v", err)
	}
	if send.DelayExpr != nil {
		if err := v.validateExpression(send.DelayExpr, path+".delayExpr"); err != nil {
			return err
		}
	}
	if len(send.Params) != 0 && send.Content != nil {
		return definitionError(path+".content", "send params and whole-payload content are mutually exclusive")
	}
	if err := v.validateParams(send.Params, path+".params"); err != nil {
		return err
	}
	if send.Content != nil {
		return v.validateExpression(send.Content, path+".content")
	}
	return nil
}

func (v *definitionValidator) validateCancel(cancel *CancelDefinition, path string) error {
	if (cancel.SendID == "") == (cancel.SendIDExpr == nil) {
		return definitionError(path, "cancel requires exactly one of sendID and sendIDExpr")
	}
	if cancel.SendID != "" {
		if err := validatePlainIdentifier(cancel.SendID); err != nil {
			return definitionError(path+".sendID", "%v", err)
		}
	}
	if cancel.SendIDExpr != nil {
		return v.validateExpression(cancel.SendIDExpr, path+".sendIDExpr")
	}
	return nil
}

func (v *definitionValidator) validateLog(log *LogDefinition, path string) error {
	if log.Label != "" && log.LabelExpr != nil {
		return definitionError(path+".labelExpr", "log label and labelExpr are mutually exclusive")
	}
	if err := validateUTF8(log.Label); err != nil {
		return definitionError(path+".label", "%v", err)
	}
	if log.LabelExpr != nil {
		if err := v.validateExpression(log.LabelExpr, path+".labelExpr"); err != nil {
			return err
		}
	}
	if log.Expr != nil {
		return v.validateExpression(log.Expr, path+".expr")
	}
	return nil
}

func (v *definitionValidator) validateFunctionRef(function *FunctionRef, path string) error {
	if err := validatePlainIdentifier(function.Name); err != nil {
		return definitionError(path+".name", "%v", err)
	}
	if function.Version == "" {
		return definitionError(path+".version", "function version is empty")
	}
	if err := validateUTF8(function.Version); err != nil {
		return definitionError(path+".version", "%v", err)
	}
	for i := range function.Args {
		if err := v.validateExpression(&function.Args[i], fmt.Sprintf("%s.args[%d]", path, i)); err != nil {
			return err
		}
	}
	return nil
}

func (v *definitionValidator) validateExpression(expression *Expression, path string) error {
	if err := validatePlainIdentifier(expression.Kind); err != nil {
		return definitionError(path+".kind", "%v", err)
	}
	if _, err := expression.Data.MarshalBinary(); err != nil {
		return definitionError(path+".data", "%v", err)
	}
	return nil
}

func (v *definitionValidator) validateTargetSet(targets []Identifier, path string) error {
	seen := make(map[Identifier]bool)
	nodes := make([]*definitionStateNode, 0, len(targets))
	for i, targetID := range targets {
		targetPath := fmt.Sprintf("%s[%d]", path, i)
		if err := validatePlainIdentifier(targetID); err != nil {
			return definitionError(targetPath, "%v", err)
		}
		if seen[targetID] {
			return definitionError(targetPath, "target %q occurs more than once", targetID)
		}
		seen[targetID] = true
		target := v.byID[targetID]
		if target == nil {
			return definitionError(targetPath, "target state %q does not exist", targetID)
		}
		nodes = append(nodes, target)
	}
	for i, left := range nodes {
		for _, right := range nodes[i+1:] {
			if isDefinitionDescendant(left, right) || isDefinitionDescendant(right, left) {
				return definitionError(path, "targets %q and %q have an ancestor/descendant relationship", left.state.ID.Value, right.state.ID.Value)
			}
		}
	}
	if len(nodes) == 0 {
		return nil
	}
	atoms := make(map[*definitionStateNode]bool)
	for _, node := range nodes {
		if err := v.collectDefaultAtoms(node, atoms, make(map[*definitionStateNode]bool)); err != nil {
			return definitionError(path, "%v", err)
		}
	}
	if len(atoms) == 0 {
		return definitionError(path, "target does not expand to an atomic state")
	}
	atomicNodes := make([]*definitionStateNode, 0, len(atoms))
	for atom := range atoms {
		atomicNodes = append(atomicNodes, atom)
	}
	for i, left := range atomicNodes {
		for _, right := range atomicNodes[i+1:] {
			lca := leastCommonDefinitionAncestor(left, right)
			if lca == nil || lca.state.Kind != KindParallel {
				return definitionError(path, "states %q and %q cannot be active together", left.state.ID.Value, right.state.ID.Value)
			}
		}
	}
	return nil
}

func (v *definitionValidator) collectDefaultAtoms(node *definitionStateNode, atoms, visiting map[*definitionStateNode]bool) error {
	if visiting[node] {
		return fmt.Errorf("default target cycle through state %q", node.state.ID.Value)
	}
	if node.state.Kind == KindAtomic || node.state.Kind == KindFinal {
		atoms[node] = true
		return nil
	}
	visiting[node] = true
	defer delete(visiting, node)
	switch node.state.Kind {
	case KindCompound, KindHistory:
		if node.state.Initial == nil || len(node.state.Initial.Targets) == 0 {
			return fmt.Errorf("state %q has no default target", node.state.ID.Value)
		}
		for _, targetID := range node.state.Initial.Targets {
			target := v.byID[targetID]
			if target == nil {
				return fmt.Errorf("state %q has unresolved default target %q", node.state.ID.Value, targetID)
			}
			if err := v.collectDefaultAtoms(target, atoms, visiting); err != nil {
				return err
			}
		}
	case KindParallel:
		for _, child := range v.realDefinitionChildren(node) {
			if err := v.collectDefaultAtoms(child, atoms, visiting); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *definitionValidator) realDefinitionChildren(node *definitionStateNode) []*definitionStateNode {
	children := make([]*definitionStateNode, 0, len(node.state.Children))
	for i := range node.state.Children {
		child := v.byID[node.state.Children[i].ID.Value]
		if child != nil && child.state.Kind != KindHistory {
			children = append(children, child)
		}
	}
	return children
}

func isDefinitionDescendant(candidate, ancestor *definitionStateNode) bool {
	if ancestor == nil {
		return false
	}
	for current := candidate.parent; current != nil; current = current.parent {
		if current == ancestor {
			return true
		}
	}
	return false
}

func leastCommonDefinitionAncestor(left, right *definitionStateNode) *definitionStateNode {
	ancestors := make(map[*StateDefinition]*definitionStateNode)
	for current := left; current != nil; current = current.parent {
		ancestors[current.state] = current
	}
	for current := right; current != nil; current = current.parent {
		if ancestor := ancestors[current.state]; ancestor != nil {
			return ancestor
		}
	}
	return nil
}

func validatePlainIdentifier(id Identifier) error {
	if _, err := NewIdentifier(string(id)); err != nil {
		return err
	}
	value := string(id)
	if strings.HasPrefix(value, "#") || value == "*" || strings.HasSuffix(value, ".") || strings.HasSuffix(value, ".*") {
		return fmt.Errorf("invalid plain identifier %q", id)
	}
	return nil
}

func validateUTF8(value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("value is not valid UTF-8")
	}
	return nil
}
