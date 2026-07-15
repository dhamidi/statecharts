package scxml

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"strings"

	"github.com/dhamidi/statecharts"
)

type definitionDecoder struct {
	expressions statecharts.TextExpressionCodec
}

func (d *definitionDecoder) definition(root *element) (statecharts.Definition, error) {
	if err := requireElement(root, scxmlNamespace, "scxml", "definition"); err != nil {
		return statecharts.Definition{}, err
	}
	allowed := plainAttr("version", "initial", "name", "datamodel", "binding")
	allowed = append(allowed,
		xml.Name{Space: extensionNamespace, Local: "definition-id"},
		xml.Name{Space: extensionNamespace, Local: "revision-salt"},
		xml.Name{Space: extensionNamespace, Local: "root-id"},
	)
	if err := root.checkAttrs("definition", allowed...); err != nil {
		return statecharts.Definition{}, err
	}
	version, ok := root.attr("", "version")
	if !ok || version != "1.0" {
		return statecharts.Definition{}, root.error("definition.version", "version must be %q", "1.0")
	}
	definition := statecharts.Definition{}
	definition.ID = statecharts.Identifier(attrValue(root, extensionNamespace, "definition-id"))
	definition.Name = attrValue(root, "", "name")
	definition.Datamodel = statecharts.Identifier(attrValue(root, "", "datamodel"))
	if definition.Datamodel == "" {
		definition.Datamodel = "go"
	}
	definition.RevisionSalt = attrValue(root, extensionNamespace, "revision-salt")
	if binding, found := root.attr("", "binding"); found {
		definition.DataBinding = statecharts.DataBinding(binding)
	}
	if strings.TrimSpace(root.text.String()) != "" {
		return statecharts.Definition{}, root.error("definition", "unexpected text content")
	}
	var stateElements []*element
	seenDatamodel := false
	for _, child := range root.children {
		switch {
		case isElement(child, scxmlNamespace, "datamodel"):
			if seenDatamodel {
				return statecharts.Definition{}, child.error("definition.data", "duplicate datamodel element")
			}
			seenDatamodel = true
			data, err := d.dataDefinitions(child, "definition.data")
			if err != nil {
				return statecharts.Definition{}, err
			}
			definition.Data = data
		case isStateElement(child):
			stateElements = append(stateElements, child)
		default:
			return statecharts.Definition{}, child.error("definition", "unknown child element {%s}%s", child.name.Space, child.name.Local)
		}
	}
	if len(stateElements) == 0 {
		return statecharts.Definition{}, root.error("definition.root", "missing root state")
	}
	if len(stateElements) == 1 {
		state, err := d.state(stateElements[0], "root")
		if err != nil {
			return statecharts.Definition{}, err
		}
		definition.Root = state
		if definition.ID == "" {
			definition.ID = state.ID.Value
			if definition.ID == "" {
				definition.ID = "scxml.document"
			}
		}
		if initial, found := root.attr("", "initial"); found && initial != "" && initial != string(state.ID.Value) {
			return statecharts.Definition{}, root.error("definition.root.id", "SCXML initial %q does not select root state %q", initial, state.ID.Value)
		}
		return definition, nil
	}

	rootID := statecharts.Identifier(attrValue(root, extensionNamespace, "root-id"))
	if rootID == "" {
		rootID = generatedRootID(stateElements)
	}
	definition.Root = statecharts.StateDefinition{
		ID:   statecharts.StateDefinitionID{Value: rootID, Generated: true},
		Kind: statecharts.KindCompound,
	}
	if initial, found := root.attr("", "initial"); found && initial != "" {
		definition.Root.Initial = &statecharts.TransitionDefinition{Targets: splitIdentifiers(initial)}
	}
	for i, child := range stateElements {
		state, err := d.state(child, fmt.Sprintf("root.children[%d]", i))
		if err != nil {
			return statecharts.Definition{}, err
		}
		definition.Root.Children = append(definition.Root.Children, state)
	}
	if definition.ID == "" {
		definition.ID = rootID
	}
	return definition, nil
}

func generatedRootID(states []*element) statecharts.Identifier {
	used := make(map[statecharts.Identifier]bool)
	var collect func(*element)
	collect = func(item *element) {
		if id, found := item.attr("", "id"); found {
			used[statecharts.Identifier(id)] = true
		}
		for _, child := range item.children {
			if isStateElement(child) {
				collect(child)
			}
		}
	}
	for _, state := range states {
		collect(state)
	}
	for index := 0; ; index++ {
		candidate := statecharts.Identifier("scxml.root")
		if index > 0 {
			candidate = statecharts.Identifier(fmt.Sprintf("scxml.root.%d", index))
		}
		if !used[candidate] {
			return candidate
		}
	}
}

func (d *definitionDecoder) dataDefinitions(container *element, path string) ([]statecharts.DataDefinition, error) {
	if err := container.checkAttrs(path); err != nil {
		return nil, err
	}
	if strings.TrimSpace(container.text.String()) != "" {
		return nil, container.error(path, "unexpected text content")
	}
	var result []statecharts.DataDefinition
	for _, child := range container.children {
		itemPath := fmt.Sprintf("%s[%d]", path, len(result))
		if err := requireElement(child, scxmlNamespace, "data", itemPath); err != nil {
			return nil, err
		}
		allowed := plainAttr("id", "src", "expr")
		allowed = append(allowed, xml.Name{Space: extensionNamespace, Local: "contentexpr"})
		if err := child.checkAttrs(itemPath, allowed...); err != nil {
			return nil, err
		}
		value := statecharts.DataDefinition{ID: statecharts.Identifier(attrValue(child, "", "id")), Source: attrValue(child, "", "src")}
		if text, found := child.attr("", "expr"); found {
			expression, err := d.expression(child, statecharts.TextExpressionValue, text, itemPath+".expr")
			if err != nil {
				return nil, err
			}
			value.Expr = &expression
		}
		if text, found := child.attr(extensionNamespace, "contentexpr"); found {
			expression, err := d.expression(child, statecharts.TextExpressionValue, text, itemPath+".content")
			if err != nil {
				return nil, err
			}
			value.Content = &expression
		}
		if strings.TrimSpace(child.text.String()) != "" {
			return nil, child.error(itemPath, "unexpected text content")
		}
		if len(child.children) != 0 {
			return nil, child.children[0].error(itemPath+".content", "inline data content is not representable as a model expression; use stc:contentexpr")
		}
		result = append(result, value)
	}
	return result, nil
}

func (d *definitionDecoder) state(item *element, path string) (statecharts.StateDefinition, error) {
	if !isStateElement(item) {
		return statecharts.StateDefinition{}, item.error(path, "expected state element")
	}
	allowed := plainAttr("id")
	if item.name.Local != "history" {
		allowed = append(allowed, xml.Name{Local: "initial"})
	}
	allowed = append(allowed, xml.Name{Space: extensionNamespace, Local: "generated"})
	if item.name.Local == "history" {
		allowed = append(allowed, xml.Name{Local: "type"})
	}
	if err := item.checkAttrs(path, allowed...); err != nil {
		return statecharts.StateDefinition{}, err
	}
	_, hasID := item.attr("", "id")
	generated := !hasID
	if text, found := item.attr(extensionNamespace, "generated"); found {
		var err error
		generated, err = parseBooleanText(item, path+".id.generated", text)
		if err != nil {
			return statecharts.StateDefinition{}, err
		}
	}
	state := statecharts.StateDefinition{ID: statecharts.StateDefinitionID{Value: statecharts.Identifier(attrValue(item, "", "id")), Generated: generated}}
	switch item.name.Local {
	case "parallel":
		state.Kind = statecharts.KindParallel
	case "final":
		state.Kind = statecharts.KindFinal
	case "history":
		state.Kind = statecharts.KindHistory
		if kind, found := item.attr("", "type"); found {
			switch kind {
			case "shallow":
				state.History = statecharts.Shallow
			case "deep":
				state.History = statecharts.Deep
			default:
				return statecharts.StateDefinition{}, item.error(path+".history", "invalid history type %q", kind)
			}
		}
	default:
		state.Kind = statecharts.KindAtomic
	}
	if initial, found := item.attr("", "initial"); found {
		state.Initial = &statecharts.TransitionDefinition{Targets: splitIdentifiers(initial)}
	}
	if strings.TrimSpace(item.text.String()) != "" {
		return statecharts.StateDefinition{}, item.error(path, "unexpected text content")
	}
	seenDatamodel, seenInitial, seenDoneData := false, state.Initial != nil, false
	for _, child := range item.children {
		switch {
		case isElement(child, scxmlNamespace, "datamodel"):
			if seenDatamodel {
				return statecharts.StateDefinition{}, child.error(path+".data", "duplicate datamodel element")
			}
			seenDatamodel = true
			data, err := d.dataDefinitions(child, path+".data")
			if err != nil {
				return statecharts.StateDefinition{}, err
			}
			state.Data = data
		case isElement(child, scxmlNamespace, "initial"):
			if seenInitial {
				return statecharts.StateDefinition{}, child.error(path+".initial", "duplicate initial transition")
			}
			seenInitial = true
			initial, err := d.initial(child, path+".initial")
			if err != nil {
				return statecharts.StateDefinition{}, err
			}
			state.Initial = &initial
		case isElement(child, scxmlNamespace, "onentry"):
			block, err := d.directBlock(child, fmt.Sprintf("%s.onEntry[%d]", path, len(state.OnEntry)))
			if err != nil {
				return statecharts.StateDefinition{}, err
			}
			state.OnEntry = append(state.OnEntry, block)
		case isElement(child, scxmlNamespace, "onexit"):
			block, err := d.directBlock(child, fmt.Sprintf("%s.onExit[%d]", path, len(state.OnExit)))
			if err != nil {
				return statecharts.StateDefinition{}, err
			}
			state.OnExit = append(state.OnExit, block)
		case isElement(child, scxmlNamespace, "transition"):
			transitionPath := fmt.Sprintf("%s.transitions[%d]", path, len(state.Transitions))
			if state.Kind == statecharts.KindHistory {
				transitionPath = path + ".initial"
			}
			transition, err := d.transition(child, transitionPath)
			if err != nil {
				return statecharts.StateDefinition{}, err
			}
			if state.Kind == statecharts.KindHistory {
				if state.Initial != nil {
					return statecharts.StateDefinition{}, child.error(path+".initial", "duplicate history default transition")
				}
				state.Initial = &transition
			} else {
				state.Transitions = append(state.Transitions, transition)
			}
		case isElement(child, scxmlNamespace, "invoke"):
			invoke, err := d.invoke(child, fmt.Sprintf("%s.invokes[%d]", path, len(state.Invokes)))
			if err != nil {
				return statecharts.StateDefinition{}, err
			}
			state.Invokes = append(state.Invokes, invoke)
		case isElement(child, scxmlNamespace, "donedata"):
			if seenDoneData {
				return statecharts.StateDefinition{}, child.error(path+".doneData", "duplicate donedata element")
			}
			seenDoneData = true
			done, err := d.doneData(child, path+".doneData")
			if err != nil {
				return statecharts.StateDefinition{}, err
			}
			state.DoneData = &done
		case isStateElement(child):
			nested, err := d.state(child, fmt.Sprintf("%s.children[%d]", path, len(state.Children)))
			if err != nil {
				return statecharts.StateDefinition{}, err
			}
			state.Children = append(state.Children, nested)
		default:
			return statecharts.StateDefinition{}, child.error(path, "unknown child element {%s}%s", child.name.Space, child.name.Local)
		}
	}
	if item.name.Local == "state" && len(state.Children) > 0 {
		state.Kind = statecharts.KindCompound
	}
	return state, nil
}

func (d *definitionDecoder) initial(item *element, path string) (statecharts.TransitionDefinition, error) {
	if err := item.checkAttrs(path); err != nil {
		return statecharts.TransitionDefinition{}, err
	}
	if strings.TrimSpace(item.text.String()) != "" || len(item.children) != 1 {
		return statecharts.TransitionDefinition{}, item.error(path, "initial must contain exactly one transition")
	}
	return d.transition(item.children[0], path)
}

func (d *definitionDecoder) transition(item *element, path string) (statecharts.TransitionDefinition, error) {
	if err := requireElement(item, scxmlNamespace, "transition", path); err != nil {
		return statecharts.TransitionDefinition{}, err
	}
	if err := item.checkAttrs(path, plainAttr("event", "target", "type", "cond")...); err != nil {
		return statecharts.TransitionDefinition{}, err
	}
	result := statecharts.TransitionDefinition{
		Events:  splitIdentifiers(attrValue(item, "", "event")),
		Targets: splitIdentifiers(attrValue(item, "", "target")),
	}
	if value, found := item.attr("", "type"); found {
		result.Type = statecharts.TransitionType(value)
	}
	if text, found := item.attr("", "cond"); found {
		condition, err := d.expression(item, statecharts.TextExpressionCondition, text, path+".condition")
		if err != nil {
			return statecharts.TransitionDefinition{}, err
		}
		result.Condition = &condition
	}
	blocks, err := d.blocks(item, path+".actions")
	if err != nil {
		return statecharts.TransitionDefinition{}, err
	}
	result.Actions = blocks
	return result, nil
}

func (d *definitionDecoder) directBlock(item *element, path string) (statecharts.ExecutableBlock, error) {
	if err := item.checkAttrs(path); err != nil {
		return nil, err
	}
	if strings.TrimSpace(item.text.String()) != "" {
		return nil, item.error(path, "unexpected text content")
	}
	block := make(statecharts.ExecutableBlock, 0, len(item.children))
	for i, child := range item.children {
		executable, err := d.executable(child, fmt.Sprintf("%s[%d]", path, i))
		if err != nil {
			return nil, err
		}
		block = append(block, executable)
	}
	return block, nil
}

func (d *definitionDecoder) blocks(item *element, path string) ([]statecharts.ExecutableBlock, error) {
	if strings.TrimSpace(item.text.String()) != "" {
		return nil, item.error(path, "unexpected text content")
	}
	var result []statecharts.ExecutableBlock
	var direct statecharts.ExecutableBlock
	for _, child := range item.children {
		if isElement(child, extensionNamespace, "block") {
			if direct != nil {
				return nil, child.error(path, "cannot mix direct executable content and stc:block")
			}
			block, err := d.directBlock(child, fmt.Sprintf("%s[%d]", path, len(result)))
			if err != nil {
				return nil, err
			}
			result = append(result, block)
			continue
		}
		if len(result) > 0 {
			return nil, child.error(path, "cannot mix stc:block and direct executable content")
		}
		executable, err := d.executable(child, fmt.Sprintf("%s[0][%d]", path, len(direct)))
		if err != nil {
			return nil, err
		}
		direct = append(direct, executable)
	}
	if direct != nil {
		result = []statecharts.ExecutableBlock{direct}
	}
	return result, nil
}

func (d *definitionDecoder) executable(item *element, path string) (statecharts.Executable, error) {
	switch {
	case isElement(item, scxmlNamespace, "raise"):
		allowed := plainAttr("event")
		allowed = append(allowed,
			xml.Name{Space: extensionNamespace, Local: "eventexpr"},
			xml.Name{Space: extensionNamespace, Local: "dataexpr"},
		)
		if err := leaf(item, path, allowed...); err != nil {
			return statecharts.Executable{}, err
		}
		value := statecharts.RaiseDefinition{Event: statecharts.Identifier(attrValue(item, "", "event"))}
		var err error
		if value.EventExpr, err = d.optionalExpression(item, extensionNamespace, "eventexpr", statecharts.TextExpressionEvent, path+".eventExpr"); err != nil {
			return statecharts.Executable{}, err
		}
		if value.Data, err = d.optionalExpression(item, extensionNamespace, "dataexpr", statecharts.TextExpressionValue, path+".data"); err != nil {
			return statecharts.Executable{}, err
		}
		return statecharts.NewRaiseExecutable(value), nil
	case isElement(item, scxmlNamespace, "send"):
		if err := item.checkAttrs(path, plainAttr("event", "eventexpr", "target", "targetexpr", "type", "typeexpr", "id", "idlocation", "delay", "delayexpr")...); err != nil {
			return statecharts.Executable{}, err
		}
		value := statecharts.SendDefinition{
			Event: statecharts.Identifier(attrValue(item, "", "event")), Target: attrValue(item, "", "target"), Type: attrValue(item, "", "type"),
			ID: statecharts.Identifier(attrValue(item, "", "id")), Delay: attrValue(item, "", "delay"),
		}
		var err error
		for _, expression := range []struct {
			name string
			role statecharts.TextExpressionRole
			path string
			to   **statecharts.Expression
		}{
			{"eventexpr", statecharts.TextExpressionEvent, ".eventExpr", &value.EventExpr},
			{"targetexpr", statecharts.TextExpressionTarget, ".targetExpr", &value.TargetExpr},
			{"typeexpr", statecharts.TextExpressionType, ".typeExpr", &value.TypeExpr},
			{"idlocation", statecharts.TextExpressionLocation, ".idLocation", &value.IDLocation},
			{"delayexpr", statecharts.TextExpressionDelay, ".delayExpr", &value.DelayExpr},
		} {
			*expression.to, err = d.optionalExpression(item, "", expression.name, expression.role, path+expression.path)
			if err != nil {
				return statecharts.Executable{}, err
			}
		}
		value.Params, value.Content, err = d.paramsAndContent(item, path)
		if err != nil {
			return statecharts.Executable{}, err
		}
		return statecharts.NewSendExecutable(value), nil
	case isElement(item, scxmlNamespace, "cancel"):
		if err := leaf(item, path, plainAttr("sendid", "sendidexpr")...); err != nil {
			return statecharts.Executable{}, err
		}
		value := statecharts.CancelDefinition{SendID: statecharts.Identifier(attrValue(item, "", "sendid"))}
		var err error
		value.SendIDExpr, err = d.optionalExpression(item, "", "sendidexpr", statecharts.TextExpressionSendID, path+".sendIDExpr")
		if err != nil {
			return statecharts.Executable{}, err
		}
		return statecharts.NewCancelExecutable(value), nil
	case isElement(item, scxmlNamespace, "log"):
		allowed := plainAttr("label", "expr")
		allowed = append(allowed, xml.Name{Space: extensionNamespace, Local: "labelexpr"})
		if err := leaf(item, path, allowed...); err != nil {
			return statecharts.Executable{}, err
		}
		value := statecharts.LogDefinition{Label: attrValue(item, "", "label")}
		var err error
		value.LabelExpr, err = d.optionalExpression(item, extensionNamespace, "labelexpr", statecharts.TextExpressionLabel, path+".labelExpr")
		if err != nil {
			return statecharts.Executable{}, err
		}
		value.Expr, err = d.optionalExpression(item, "", "expr", statecharts.TextExpressionValue, path+".expr")
		if err != nil {
			return statecharts.Executable{}, err
		}
		return statecharts.NewLogExecutable(value), nil
	case isElement(item, scxmlNamespace, "assign"):
		if err := leaf(item, path, plainAttr("location", "expr")...); err != nil {
			return statecharts.Executable{}, err
		}
		location, err := d.requiredExpression(item, "location", statecharts.TextExpressionLocation, path+".location")
		if err != nil {
			return statecharts.Executable{}, err
		}
		expression, err := d.requiredExpression(item, "expr", statecharts.TextExpressionValue, path+".expr")
		if err != nil {
			return statecharts.Executable{}, err
		}
		return statecharts.NewAssignExecutable(statecharts.AssignDefinition{Location: location, Expr: expression}), nil
	case isElement(item, scxmlNamespace, "if"):
		return d.choose(item, path)
	case isElement(item, scxmlNamespace, "foreach"):
		if err := item.checkAttrs(path, plainAttr("array", "item", "index")...); err != nil {
			return statecharts.Executable{}, err
		}
		array, err := d.requiredExpression(item, "array", statecharts.TextExpressionArray, path+".array")
		if err != nil {
			return statecharts.Executable{}, err
		}
		actions, err := d.blocks(item, path+".actions")
		if err != nil {
			return statecharts.Executable{}, err
		}
		return statecharts.NewForEachExecutable(statecharts.ForEachDefinition{Array: array, Item: statecharts.Identifier(attrValue(item, "", "item")), Index: statecharts.Identifier(attrValue(item, "", "index")), Actions: actions}), nil
	case isElement(item, scxmlNamespace, "script"):
		if err := item.checkAttrs(path); err != nil {
			return statecharts.Executable{}, err
		}
		if len(item.children) != 0 {
			return statecharts.Executable{}, item.error(path, "script cannot contain child elements")
		}
		expression, err := d.expression(item, statecharts.TextExpressionScript, item.text.String(), path+".expr")
		if err != nil {
			return statecharts.Executable{}, err
		}
		return statecharts.NewScriptExecutable(statecharts.ScriptDefinition{Expr: expression}), nil
	case isElement(item, extensionNamespace, "call"):
		return d.call(item, path)
	case isElement(item, extensionNamespace, "extension"):
		return d.extension(item, path)
	default:
		return statecharts.Executable{}, item.error(path, "unknown executable element {%s}%s", item.name.Space, item.name.Local)
	}
}

func (d *definitionDecoder) choose(item *element, path string) (statecharts.Executable, error) {
	if err := item.checkAttrs(path, plainAttr("cond")...); err != nil {
		return statecharts.Executable{}, err
	}
	if strings.TrimSpace(item.text.String()) != "" {
		return statecharts.Executable{}, item.error(path, "unexpected text content")
	}
	condition, err := d.requiredExpression(item, "cond", statecharts.TextExpressionCondition, path+".branches[0].condition")
	if err != nil {
		return statecharts.Executable{}, err
	}
	value := statecharts.ChooseDefinition{Branches: []statecharts.ChooseBranchDefinition{{Condition: condition}}}
	segmentStart := 0
	inElse := false
	for _, child := range item.children {
		switch {
		case isElement(child, scxmlNamespace, "elseif"):
			if inElse {
				return statecharts.Executable{}, child.error(path, "elseif cannot follow else")
			}
			current := len(value.Branches) - 1
			actions, err := d.blocksFromChildren(item, item.children[segmentStart:indexOfChild(item.children, child)], fmt.Sprintf("%s.branches[%d].actions", path, current))
			if err != nil {
				return statecharts.Executable{}, err
			}
			value.Branches[current].Actions = actions
			branchPath := fmt.Sprintf("%s.branches[%d]", path, len(value.Branches))
			if err := leaf(child, branchPath, plainAttr("cond")...); err != nil {
				return statecharts.Executable{}, err
			}
			condition, err := d.requiredExpression(child, "cond", statecharts.TextExpressionCondition, branchPath+".condition")
			if err != nil {
				return statecharts.Executable{}, err
			}
			value.Branches = append(value.Branches, statecharts.ChooseBranchDefinition{Condition: condition})
			segmentStart = indexOfChild(item.children, child) + 1
		case isElement(child, scxmlNamespace, "else"):
			if inElse {
				return statecharts.Executable{}, child.error(path+".else", "duplicate else")
			}
			inElse = true
			current := len(value.Branches) - 1
			actions, err := d.blocksFromChildren(item, item.children[segmentStart:indexOfChild(item.children, child)], fmt.Sprintf("%s.branches[%d].actions", path, current))
			if err != nil {
				return statecharts.Executable{}, err
			}
			value.Branches[current].Actions = actions
			if err := child.checkAttrs(path + ".else"); err != nil {
				return statecharts.Executable{}, err
			}
			if strings.TrimSpace(child.text.String()) != "" || len(child.children) != 0 {
				return statecharts.Executable{}, child.error(path+".else", "else marker must be empty")
			}
			segmentStart = indexOfChild(item.children, child) + 1
		default:
			// Executable content is decoded when its branch segment closes.
		}
	}
	remaining := item.children[segmentStart:]
	if inElse {
		actions, err := d.blocksFromChildren(item, remaining, path+".else")
		if err != nil {
			return statecharts.Executable{}, err
		}
		if actions == nil {
			actions = []statecharts.ExecutableBlock{}
		}
		value.Else = actions
	} else {
		current := len(value.Branches) - 1
		actions, err := d.blocksFromChildren(item, remaining, fmt.Sprintf("%s.branches[%d].actions", path, current))
		if err != nil {
			return statecharts.Executable{}, err
		}
		value.Branches[current].Actions = actions
	}
	return statecharts.NewChooseExecutable(value), nil
}

func (d *definitionDecoder) blocksFromChildren(parent *element, children []*element, path string) ([]statecharts.ExecutableBlock, error) {
	container := &element{name: parent.name, attrs: parent.attrs, children: children, line: parent.line, column: parent.column}
	return d.blocks(container, path)
}

func indexOfChild(children []*element, target *element) int {
	for i, child := range children {
		if child == target {
			return i
		}
	}
	return -1
}

func (d *definitionDecoder) call(item *element, path string) (statecharts.Executable, error) {
	if err := item.checkAttrs(path, plainAttr("name", "version")...); err != nil {
		return statecharts.Executable{}, err
	}
	if strings.TrimSpace(item.text.String()) != "" {
		return statecharts.Executable{}, item.error(path, "unexpected text content")
	}
	function := statecharts.FunctionRef{Name: statecharts.Identifier(attrValue(item, "", "name")), Version: attrValue(item, "", "version")}
	for _, child := range item.children {
		argumentPath := fmt.Sprintf("%s.function.args[%d]", path, len(function.Args))
		if err := requireElement(child, extensionNamespace, "arg", argumentPath); err != nil {
			return statecharts.Executable{}, err
		}
		if err := leaf(child, argumentPath, plainAttr("expr")...); err != nil {
			return statecharts.Executable{}, err
		}
		expression, err := d.requiredExpression(child, "expr", statecharts.TextExpressionValue, argumentPath)
		if err != nil {
			return statecharts.Executable{}, err
		}
		function.Args = append(function.Args, expression)
	}
	return statecharts.NewCallExecutable(statecharts.CallDefinition{Function: function}), nil
}

func (d *definitionDecoder) extension(item *element, path string) (statecharts.Executable, error) {
	if err := leaf(item, path, plainAttr("namespace", "name", "data")...); err != nil {
		return statecharts.Executable{}, err
	}
	wire, err := base64.RawStdEncoding.DecodeString(attrValue(item, "", "data"))
	if err != nil {
		return statecharts.Executable{}, item.error(path+".data", "invalid canonical Value encoding: %v", err)
	}
	var value statecharts.Value
	if err := value.UnmarshalText(wire); err != nil {
		return statecharts.Executable{}, item.error(path+".data", "invalid canonical Value: %v", err)
	}
	return statecharts.NewExtensionExecutable(statecharts.ExtensionDefinition{Namespace: attrValue(item, "", "namespace"), Name: attrValue(item, "", "name"), Data: value}), nil
}

func (d *definitionDecoder) invoke(item *element, path string) (statecharts.InvokeDefinition, error) {
	allowed := plainAttr("id", "idlocation", "type", "typeexpr", "src", "srcexpr", "autoforward")
	allowed = append(allowed, xml.Name{Space: extensionNamespace, Local: "definition-id"})
	if err := item.checkAttrs(path, allowed...); err != nil {
		return statecharts.InvokeDefinition{}, err
	}
	value := statecharts.InvokeDefinition{
		DefinitionID: statecharts.Identifier(attrValue(item, extensionNamespace, "definition-id")),
		ID:           statecharts.Identifier(attrValue(item, "", "id")), Type: attrValue(item, "", "type"), Src: attrValue(item, "", "src"),
	}
	var err error
	if text, found := item.attr("", "autoforward"); found {
		value.AutoForward, err = parseBooleanText(item, path+".autoForward", text)
		if err != nil {
			return statecharts.InvokeDefinition{}, err
		}
	}
	for _, expression := range []struct {
		name string
		role statecharts.TextExpressionRole
		path string
		to   **statecharts.Expression
	}{
		{"idlocation", statecharts.TextExpressionLocation, ".idLocation", &value.IDLocation},
		{"typeexpr", statecharts.TextExpressionType, ".typeExpr", &value.TypeExpr},
		{"srcexpr", statecharts.TextExpressionValue, ".srcExpr", &value.SrcExpr},
	} {
		*expression.to, err = d.optionalExpression(item, "", expression.name, expression.role, path+expression.path)
		if err != nil {
			return statecharts.InvokeDefinition{}, err
		}
	}
	if strings.TrimSpace(item.text.String()) != "" {
		return statecharts.InvokeDefinition{}, item.error(path, "unexpected text content")
	}
	seenContent, seenFinalize := false, false
	for _, child := range item.children {
		switch {
		case isElement(child, scxmlNamespace, "param"):
			param, err := d.param(child, fmt.Sprintf("%s.params[%d]", path, len(value.Params)))
			if err != nil {
				return statecharts.InvokeDefinition{}, err
			}
			value.Params = append(value.Params, param)
		case isElement(child, scxmlNamespace, "content"):
			if seenContent {
				return statecharts.InvokeDefinition{}, child.error(path+".content", "duplicate content")
			}
			seenContent = true
			content, err := d.content(child, path+".content")
			if err != nil {
				return statecharts.InvokeDefinition{}, err
			}
			value.Content = &content
		case isElement(child, scxmlNamespace, "finalize"):
			if seenFinalize {
				return statecharts.InvokeDefinition{}, child.error(path+".finalize", "duplicate finalize")
			}
			seenFinalize = true
			if err := child.checkAttrs(path + ".finalize"); err != nil {
				return statecharts.InvokeDefinition{}, err
			}
			blocks, err := d.blocks(child, path+".finalize")
			if err != nil {
				return statecharts.InvokeDefinition{}, err
			}
			value.Finalize = blocks
		default:
			return statecharts.InvokeDefinition{}, child.error(path, "unknown invoke child {%s}%s", child.name.Space, child.name.Local)
		}
	}
	return value, nil
}

func (d *definitionDecoder) doneData(item *element, path string) (statecharts.DoneDataDefinition, error) {
	if err := item.checkAttrs(path); err != nil {
		return statecharts.DoneDataDefinition{}, err
	}
	params, content, err := d.paramsAndContent(item, path)
	return statecharts.DoneDataDefinition{Params: params, Content: content}, err
}

func (d *definitionDecoder) paramsAndContent(item *element, path string) ([]statecharts.ParamDefinition, *statecharts.Expression, error) {
	if strings.TrimSpace(item.text.String()) != "" {
		return nil, nil, item.error(path, "unexpected text content")
	}
	var params []statecharts.ParamDefinition
	var content *statecharts.Expression
	for _, child := range item.children {
		switch {
		case isElement(child, scxmlNamespace, "param"):
			param, err := d.param(child, fmt.Sprintf("%s.params[%d]", path, len(params)))
			if err != nil {
				return nil, nil, err
			}
			params = append(params, param)
		case isElement(child, scxmlNamespace, "content"):
			if content != nil {
				return nil, nil, child.error(path+".content", "duplicate content")
			}
			expression, err := d.content(child, path+".content")
			if err != nil {
				return nil, nil, err
			}
			content = &expression
		default:
			return nil, nil, child.error(path, "unknown child {%s}%s", child.name.Space, child.name.Local)
		}
	}
	return params, content, nil
}

func (d *definitionDecoder) param(item *element, path string) (statecharts.ParamDefinition, error) {
	if err := leaf(item, path, plainAttr("name", "expr", "location")...); err != nil {
		return statecharts.ParamDefinition{}, err
	}
	value := statecharts.ParamDefinition{Name: statecharts.Identifier(attrValue(item, "", "name"))}
	var err error
	value.Expr, err = d.optionalExpression(item, "", "expr", statecharts.TextExpressionValue, path+".expr")
	if err != nil {
		return statecharts.ParamDefinition{}, err
	}
	value.Location, err = d.optionalExpression(item, "", "location", statecharts.TextExpressionLocation, path+".location")
	return value, err
}

func (d *definitionDecoder) content(item *element, path string) (statecharts.Expression, error) {
	if err := requireElement(item, scxmlNamespace, "content", path); err != nil {
		return statecharts.Expression{}, err
	}
	if err := leaf(item, path, plainAttr("expr")...); err != nil {
		return statecharts.Expression{}, err
	}
	return d.requiredExpression(item, "expr", statecharts.TextExpressionValue, path)
}

func (d *definitionDecoder) requiredExpression(item *element, name string, role statecharts.TextExpressionRole, path string) (statecharts.Expression, error) {
	text, found := item.attr("", name)
	if !found {
		return statecharts.Expression{}, item.error(path, "missing %q expression attribute", name)
	}
	return d.expression(item, role, text, path)
}

func (d *definitionDecoder) optionalExpression(item *element, namespace, name string, role statecharts.TextExpressionRole, path string) (*statecharts.Expression, error) {
	text, found := item.attr(namespace, name)
	if !found {
		return nil, nil
	}
	expression, err := d.expression(item, role, text, path)
	if err != nil {
		return nil, err
	}
	return &expression, nil
}

func (d *definitionDecoder) expression(item *element, role statecharts.TextExpressionRole, text, path string) (statecharts.Expression, error) {
	if d.expressions == nil {
		return statecharts.Expression{}, item.error(path, "expression requires a datamodel TextExpressionCodec")
	}
	expression, err := d.expressions.ParseExpression(role, text)
	if err != nil {
		return statecharts.Expression{}, item.error(path, "%v", err)
	}
	return expression.Clone(), nil
}

func requireElement(item *element, namespace, local, path string) error {
	if !isElement(item, namespace, local) {
		return item.error(path, "expected {%s}%s, got {%s}%s", namespace, local, item.name.Space, item.name.Local)
	}
	return nil
}

func isElement(item *element, namespace, local string) bool {
	return item.name.Space == namespace && item.name.Local == local
}

func isStateElement(item *element) bool {
	if item.name.Space != scxmlNamespace {
		return false
	}
	switch item.name.Local {
	case "state", "parallel", "final", "history":
		return true
	default:
		return false
	}
}

func leaf(item *element, path string, attrs ...xml.Name) error {
	if err := item.checkAttrs(path, attrs...); err != nil {
		return err
	}
	if strings.TrimSpace(item.text.String()) != "" || len(item.children) != 0 {
		return item.error(path, "element must not contain text or child elements")
	}
	return nil
}

func attrValue(item *element, namespace, name string) string {
	value, _ := item.attr(namespace, name)
	return value
}

func splitIdentifiers(text string) []statecharts.Identifier {
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return nil
	}
	result := make([]statecharts.Identifier, len(fields))
	for i, field := range fields {
		result[i] = statecharts.Identifier(field)
	}
	return result
}

func parseBooleanText(item *element, path, text string) (bool, error) {
	switch text {
	case "true", "1":
		return true, nil
	case "false", "0":
		return false, nil
	default:
		return false, item.error(path, "invalid boolean %q", text)
	}
}
