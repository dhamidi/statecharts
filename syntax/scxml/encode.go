package scxml

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/dhamidi/statecharts"
)

type definitionEncoder struct {
	writer      xmlWriter
	expressions statecharts.TextExpressionCodec
}

func (e *definitionEncoder) definition(definition statecharts.Definition) error {
	e.writer.header()
	e.writer.start("scxml",
		attribute{"xmlns", scxmlNamespace},
		attribute{"xmlns:stc", extensionNamespace},
		attribute{"version", "1.0"},
		attribute{"initial", string(definition.Root.ID.Value)},
		attribute{"name", definition.Name},
		attribute{"datamodel", string(definition.Datamodel)},
		attribute{"binding", string(definition.DataBinding)},
		attribute{"stc:definition-id", string(definition.ID)},
		attribute{"stc:revision-salt", definition.RevisionSalt},
	)
	if err := e.dataDefinitions(definition.Data, "definition.data"); err != nil {
		return err
	}
	if err := e.state(definition.Root, "root"); err != nil {
		return err
	}
	e.writer.end()
	return nil
}

func (e *definitionEncoder) dataDefinitions(definitions []statecharts.DataDefinition, path string) error {
	if len(definitions) == 0 {
		return nil
	}
	e.writer.start("datamodel")
	for i, definition := range definitions {
		itemPath := fmt.Sprintf("%s[%d]", path, i)
		attrs := []attribute{{"id", string(definition.ID)}, {"src", definition.Source}}
		if definition.Expr != nil {
			text, err := e.expression(statecharts.TextExpressionValue, *definition.Expr, itemPath+".expr")
			if err != nil {
				return err
			}
			attrs = append(attrs, expressionAttribute("expr", text))
		}
		if definition.Content != nil {
			text, err := e.expression(statecharts.TextExpressionValue, *definition.Content, itemPath+".content")
			if err != nil {
				return err
			}
			attrs = append(attrs, expressionAttribute("stc:contentexpr", text))
		}
		e.writer.start("data", attrs...)
		e.writer.end()
	}
	e.writer.end()
	return nil
}

func (e *definitionEncoder) state(state statecharts.StateDefinition, path string) error {
	name := "state"
	switch state.Kind {
	case statecharts.KindParallel:
		name = "parallel"
	case statecharts.KindFinal:
		name = "final"
	case statecharts.KindHistory:
		name = "history"
	}
	attrs := []attribute{
		{"id", string(state.ID.Value)},
		{"stc:generated", boolText(state.ID.Generated)},
	}
	if state.Kind == statecharts.KindHistory {
		attrs = append(attrs, attribute{"type", state.History.String()})
	}
	if state.Kind != statecharts.KindHistory && initialAttributeEligible(state.Initial) {
		attrs = append(attrs, attribute{"initial", string(state.Initial.Targets[0])})
	}
	e.writer.start(name, attrs...)
	if err := e.dataDefinitions(state.Data, path+".data"); err != nil {
		return err
	}
	if state.Initial != nil && (state.Kind == statecharts.KindHistory || !initialAttributeEligible(state.Initial)) {
		if state.Kind != statecharts.KindHistory {
			e.writer.start("initial")
		}
		if err := e.transition(*state.Initial, path+".initial"); err != nil {
			return err
		}
		if state.Kind != statecharts.KindHistory {
			e.writer.end()
		}
	}
	if err := e.actionContainers("onentry", state.OnEntry, path+".onEntry"); err != nil {
		return err
	}
	if err := e.actionContainers("onexit", state.OnExit, path+".onExit"); err != nil {
		return err
	}
	for i, transition := range state.Transitions {
		if err := e.transition(transition, fmt.Sprintf("%s.transitions[%d]", path, i)); err != nil {
			return err
		}
	}
	for i, invoke := range state.Invokes {
		if err := e.invoke(invoke, fmt.Sprintf("%s.invokes[%d]", path, i)); err != nil {
			return err
		}
	}
	if state.DoneData != nil {
		if err := e.doneData(*state.DoneData, path+".doneData"); err != nil {
			return err
		}
	}
	for i, child := range state.Children {
		if err := e.state(child, fmt.Sprintf("%s.children[%d]", path, i)); err != nil {
			return err
		}
	}
	e.writer.end()
	return nil
}

func initialAttributeEligible(initial *statecharts.TransitionDefinition) bool {
	return initial != nil && len(initial.Targets) == 1 && len(initial.Events) == 0 && initial.Type == "" && initial.Condition == nil && len(initial.Actions) == 0
}

func (e *definitionEncoder) transition(transition statecharts.TransitionDefinition, path string) error {
	attrs := []attribute{
		{"event", joinIdentifiers(transition.Events)},
		{"target", joinIdentifiers(transition.Targets)},
	}
	if transition.Type != "" {
		attrs = append(attrs, attribute{"type", string(transition.Type)})
	}
	if transition.Condition != nil {
		text, err := e.expression(statecharts.TextExpressionCondition, *transition.Condition, path+".condition")
		if err != nil {
			return err
		}
		attrs = append(attrs, expressionAttribute("cond", text))
	}
	e.writer.start("transition", attrs...)
	if err := e.blocks(transition.Actions, path+".actions"); err != nil {
		return err
	}
	e.writer.end()
	return nil
}

func (e *definitionEncoder) actionContainers(name string, blocks []statecharts.ExecutableBlock, path string) error {
	for i, block := range blocks {
		e.writer.start(name)
		if err := e.block(block, fmt.Sprintf("%s[%d]", path, i)); err != nil {
			return err
		}
		e.writer.end()
	}
	return nil
}

func (e *definitionEncoder) blocks(blocks []statecharts.ExecutableBlock, path string) error {
	if len(blocks) == 1 {
		return e.block(blocks[0], path+"[0]")
	}
	for i, block := range blocks {
		e.writer.start("stc:block")
		if err := e.block(block, fmt.Sprintf("%s[%d]", path, i)); err != nil {
			return err
		}
		e.writer.end()
	}
	return nil
}

func (e *definitionEncoder) block(block statecharts.ExecutableBlock, path string) error {
	for i, executable := range block {
		if err := e.executable(executable, fmt.Sprintf("%s[%d]", path, i)); err != nil {
			return err
		}
	}
	return nil
}

func (e *definitionEncoder) executable(executable statecharts.Executable, path string) error {
	switch executable.Kind {
	case statecharts.ExecutableRaise:
		value := executable.Raise
		attrs := []attribute{{"event", string(value.Event)}}
		if err := e.expressionAttr(&attrs, "stc:eventexpr", statecharts.TextExpressionEvent, value.EventExpr, path+".eventExpr"); err != nil {
			return err
		}
		if err := e.expressionAttr(&attrs, "stc:dataexpr", statecharts.TextExpressionValue, value.Data, path+".data"); err != nil {
			return err
		}
		e.writer.start("raise", attrs...)
		e.writer.end()
	case statecharts.ExecutableSend:
		value := executable.Send
		attrs := []attribute{{"event", string(value.Event)}, {"target", value.Target}, {"type", value.Type}, {"id", string(value.ID)}, {"delay", value.Delay}}
		for _, item := range []struct {
			name string
			role statecharts.TextExpressionRole
			expr *statecharts.Expression
			path string
		}{
			{"eventexpr", statecharts.TextExpressionEvent, value.EventExpr, ".eventExpr"},
			{"targetexpr", statecharts.TextExpressionTarget, value.TargetExpr, ".targetExpr"},
			{"typeexpr", statecharts.TextExpressionType, value.TypeExpr, ".typeExpr"},
			{"idlocation", statecharts.TextExpressionLocation, value.IDLocation, ".idLocation"},
			{"delayexpr", statecharts.TextExpressionDelay, value.DelayExpr, ".delayExpr"},
		} {
			if err := e.expressionAttr(&attrs, item.name, item.role, item.expr, path+item.path); err != nil {
				return err
			}
		}
		e.writer.start("send", attrs...)
		if err := e.params(value.Params, path+".params"); err != nil {
			return err
		}
		if value.Content != nil {
			if err := e.content(*value.Content, path+".content"); err != nil {
				return err
			}
		}
		e.writer.end()
	case statecharts.ExecutableCancel:
		value := executable.Cancel
		attrs := []attribute{{"sendid", string(value.SendID)}}
		if err := e.expressionAttr(&attrs, "sendidexpr", statecharts.TextExpressionSendID, value.SendIDExpr, path+".sendIDExpr"); err != nil {
			return err
		}
		e.writer.start("cancel", attrs...)
		e.writer.end()
	case statecharts.ExecutableLog:
		value := executable.Log
		attrs := []attribute{{"label", value.Label}}
		if err := e.expressionAttr(&attrs, "stc:labelexpr", statecharts.TextExpressionLabel, value.LabelExpr, path+".labelExpr"); err != nil {
			return err
		}
		if err := e.expressionAttr(&attrs, "expr", statecharts.TextExpressionValue, value.Expr, path+".expr"); err != nil {
			return err
		}
		e.writer.start("log", attrs...)
		e.writer.end()
	case statecharts.ExecutableAssign:
		value := executable.Assign
		location, err := e.expression(statecharts.TextExpressionLocation, value.Location, path+".location")
		if err != nil {
			return err
		}
		expr, err := e.expression(statecharts.TextExpressionValue, value.Expr, path+".expr")
		if err != nil {
			return err
		}
		e.writer.start("assign", expressionAttribute("location", location), expressionAttribute("expr", expr))
		e.writer.end()
	case statecharts.ExecutableChoose:
		value := executable.Choose
		for i, branch := range value.Branches {
			branchPath := fmt.Sprintf("%s.branches[%d]", path, i)
			condition, err := e.expression(statecharts.TextExpressionCondition, branch.Condition, branchPath+".condition")
			if err != nil {
				return err
			}
			if i == 0 {
				e.writer.start("if", expressionAttribute("cond", condition))
			} else {
				e.writer.start("elseif", expressionAttribute("cond", condition))
				e.writer.end()
			}
			if err := e.blocks(branch.Actions, branchPath+".actions"); err != nil {
				return err
			}
		}
		if value.Else != nil {
			e.writer.start("else")
			e.writer.end()
			if err := e.blocks(value.Else, path+".else"); err != nil {
				return err
			}
		}
		e.writer.end()
	case statecharts.ExecutableForEach:
		value := executable.ForEach
		array, err := e.expression(statecharts.TextExpressionArray, value.Array, path+".array")
		if err != nil {
			return err
		}
		e.writer.start("foreach", expressionAttribute("array", array), attribute{"item", string(value.Item)}, attribute{"index", string(value.Index)})
		if err := e.blocks(value.Actions, path+".actions"); err != nil {
			return err
		}
		e.writer.end()
	case statecharts.ExecutableScript:
		text, err := e.expression(statecharts.TextExpressionScript, executable.Script.Expr, path+".expr")
		if err != nil {
			return err
		}
		e.writer.start("script")
		e.writer.text(text)
		e.writer.end()
	case statecharts.ExecutableCall:
		value := executable.Call.Function
		e.writer.start("stc:call", attribute{"name", string(value.Name)}, attribute{"version", value.Version})
		for i, argument := range value.Args {
			text, err := e.expression(statecharts.TextExpressionValue, argument, fmt.Sprintf("%s.function.args[%d]", path, i))
			if err != nil {
				return err
			}
			e.writer.start("stc:arg", expressionAttribute("expr", text))
			e.writer.end()
		}
		e.writer.end()
	case statecharts.ExecutableExtension:
		value := executable.Extension
		wire, err := value.Data.MarshalText()
		if err != nil {
			return &Error{Path: path + ".data", Err: err}
		}
		e.writer.start("stc:extension", attribute{"namespace", value.Namespace}, attribute{"name", value.Name}, attribute{"data", base64.RawStdEncoding.EncodeToString(wire)})
		e.writer.end()
	default:
		return &Error{Path: path, Err: fmt.Errorf("unsupported executable kind %q", executable.Kind)}
	}
	return nil
}

func (e *definitionEncoder) invoke(invoke statecharts.InvokeDefinition, path string) error {
	attrs := []attribute{
		{"stc:definition-id", string(invoke.DefinitionID)},
		{"id", string(invoke.ID)},
		{"type", invoke.Type},
		{"src", invoke.Src},
		{"autoforward", boolText(invoke.AutoForward)},
	}
	for _, item := range []struct {
		name string
		role statecharts.TextExpressionRole
		expr *statecharts.Expression
		path string
	}{
		{"idlocation", statecharts.TextExpressionLocation, invoke.IDLocation, ".idLocation"},
		{"typeexpr", statecharts.TextExpressionType, invoke.TypeExpr, ".typeExpr"},
		{"srcexpr", statecharts.TextExpressionValue, invoke.SrcExpr, ".srcExpr"},
	} {
		if err := e.expressionAttr(&attrs, item.name, item.role, item.expr, path+item.path); err != nil {
			return err
		}
	}
	e.writer.start("invoke", attrs...)
	if err := e.params(invoke.Params, path+".params"); err != nil {
		return err
	}
	if invoke.Content != nil {
		if err := e.content(*invoke.Content, path+".content"); err != nil {
			return err
		}
	}
	if invoke.Finalize != nil {
		e.writer.start("finalize")
		if err := e.blocks(invoke.Finalize, path+".finalize"); err != nil {
			return err
		}
		e.writer.end()
	}
	e.writer.end()
	return nil
}

func (e *definitionEncoder) doneData(done statecharts.DoneDataDefinition, path string) error {
	e.writer.start("donedata")
	if err := e.params(done.Params, path+".params"); err != nil {
		return err
	}
	if done.Content != nil {
		if err := e.content(*done.Content, path+".content"); err != nil {
			return err
		}
	}
	e.writer.end()
	return nil
}

func (e *definitionEncoder) params(params []statecharts.ParamDefinition, path string) error {
	for i, param := range params {
		itemPath := fmt.Sprintf("%s[%d]", path, i)
		attrs := []attribute{{"name", string(param.Name)}}
		if err := e.expressionAttr(&attrs, "expr", statecharts.TextExpressionValue, param.Expr, itemPath+".expr"); err != nil {
			return err
		}
		if err := e.expressionAttr(&attrs, "location", statecharts.TextExpressionLocation, param.Location, itemPath+".location"); err != nil {
			return err
		}
		e.writer.start("param", attrs...)
		e.writer.end()
	}
	return nil
}

func (e *definitionEncoder) content(expression statecharts.Expression, path string) error {
	text, err := e.expression(statecharts.TextExpressionValue, expression, path)
	if err != nil {
		return err
	}
	e.writer.start("content", expressionAttribute("expr", text))
	e.writer.end()
	return nil
}

func (e *definitionEncoder) expressionAttr(attrs *[]attribute, name string, role statecharts.TextExpressionRole, expression *statecharts.Expression, path string) error {
	if expression == nil {
		return nil
	}
	text, err := e.expression(role, *expression, path)
	if err != nil {
		return err
	}
	*attrs = append(*attrs, expressionAttribute(name, text))
	return nil
}

func (e *definitionEncoder) expression(role statecharts.TextExpressionRole, expression statecharts.Expression, path string) (string, error) {
	if e.expressions == nil {
		return "", &Error{Path: path, Err: fmt.Errorf("expression requires a datamodel TextExpressionCodec")}
	}
	text, err := e.expressions.FormatExpression(role, expression.Clone())
	if err != nil {
		return "", &Error{Path: path, Err: err}
	}
	if err := validateXMLString(text); err != nil {
		return "", &Error{Path: path, Err: err}
	}
	return text, nil
}

func joinIdentifiers(values []statecharts.Identifier) string {
	result := make([]string, len(values))
	for i, value := range values {
		result[i] = string(value)
	}
	return strings.Join(result, " ")
}
