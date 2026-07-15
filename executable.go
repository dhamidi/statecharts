package statecharts

import "time"

// ExecutableKind identifies one first-class executable-content node.
type ExecutableKind string

const (
	ExecutableRaise     ExecutableKind = "raise"
	ExecutableSend      ExecutableKind = "send"
	ExecutableCancel    ExecutableKind = "cancel"
	ExecutableLog       ExecutableKind = "log"
	ExecutableAssign    ExecutableKind = "assign"
	ExecutableChoose    ExecutableKind = "choose"
	ExecutableForEach   ExecutableKind = "foreach"
	ExecutableScript    ExecutableKind = "script"
	ExecutableCall      ExecutableKind = "call"
	ExecutableExtension ExecutableKind = "extension"
)

// Executable is a closed tagged union. Exactly one payload pointer must be
// present and it must match Kind. Constructors below create that outer union;
// Definition.Validate validates both the union and its payload.
type Executable struct {
	Kind      ExecutableKind       `json:"kind"`
	Raise     *RaiseDefinition     `json:"raise,omitempty"`
	Send      *SendDefinition      `json:"send,omitempty"`
	Cancel    *CancelDefinition    `json:"cancel,omitempty"`
	Log       *LogDefinition       `json:"log,omitempty"`
	Assign    *AssignDefinition    `json:"assign,omitempty"`
	Choose    *ChooseDefinition    `json:"choose,omitempty"`
	ForEach   *ForEachDefinition   `json:"foreach,omitempty"`
	Script    *ScriptDefinition    `json:"script,omitempty"`
	Call      *CallDefinition      `json:"call,omitempty"`
	Extension *ExtensionDefinition `json:"extension,omitempty"`
}

// ExecutableBlock is one ordered executable-content block. Blocks remain
// distinct because an evaluation error aborts only its containing block.
type ExecutableBlock []Executable

// RaiseDefinition raises an internal event. Exactly one of Event and
// EventExpr is required. Data, when present, computes its canonical payload.
type RaiseDefinition struct {
	Event     Identifier  `json:"event,omitempty"`
	EventExpr *Expression `json:"eventExpr,omitempty"`
	Data      *Expression `json:"data,omitempty"`
}

// SendDefinition describes one send operation. Static and expression forms
// of each field are mutually exclusive. Content is one whole payload and is
// mutually exclusive with Params.
type SendDefinition struct {
	Event      Identifier        `json:"event,omitempty"`
	EventExpr  *Expression       `json:"eventExpr,omitempty"`
	Target     string            `json:"target,omitempty"`
	TargetExpr *Expression       `json:"targetExpr,omitempty"`
	Type       string            `json:"type,omitempty"`
	TypeExpr   *Expression       `json:"typeExpr,omitempty"`
	ID         Identifier        `json:"id,omitempty"`
	IDLocation *Expression       `json:"idLocation,omitempty"`
	Delay      string            `json:"delay,omitempty"`
	DelayExpr  *Expression       `json:"delayExpr,omitempty"`
	Params     []ParamDefinition `json:"params,omitempty"`
	Content    *Expression       `json:"content,omitempty"`
}

// CancelDefinition cancels a delayed send selected by either a static ID or
// a datamodel expression.
type CancelDefinition struct {
	SendID     Identifier  `json:"sendID,omitempty"`
	SendIDExpr *Expression `json:"sendIDExpr,omitempty"`
}

// LogDefinition records one diagnostic value. Label and LabelExpr are
// mutually exclusive; Expr may be omitted to log canonical null.
type LogDefinition struct {
	Label     string      `json:"label,omitempty"`
	LabelExpr *Expression `json:"labelExpr,omitempty"`
	Expr      *Expression `json:"expr,omitempty"`
}

// AssignDefinition evaluates Expr and stores it at the datamodel Location.
type AssignDefinition struct {
	Location Expression `json:"location"`
	Expr     Expression `json:"expr"`
}

// ChooseDefinition evaluates branches in order and executes the first true
// branch, or Else when none match.
type ChooseDefinition struct {
	Branches []ChooseBranchDefinition `json:"branches"`
	Else     []ExecutableBlock        `json:"else,omitempty"`
}

// ChooseBranchDefinition is one condition and its ordered action blocks.
type ChooseBranchDefinition struct {
	Condition Expression        `json:"condition"`
	Actions   []ExecutableBlock `json:"actions,omitempty"`
}

// ForEachDefinition iterates Array with datamodel-owned Item and optional
// Index bindings.
type ForEachDefinition struct {
	Array   Expression        `json:"array"`
	Item    Identifier        `json:"item"`
	Index   Identifier        `json:"index,omitempty"`
	Actions []ExecutableBlock `json:"actions,omitempty"`
}

// ScriptDefinition executes one datamodel-owned script expression.
type ScriptDefinition struct {
	Expr Expression `json:"expr"`
}

// CallDefinition invokes one named, versioned host behavior.
type CallDefinition struct {
	Function FunctionRef `json:"function"`
}

// ExtensionDefinition preserves executable content owned by an extension.
// Namespace and Name identify the extension; Data is its canonical payload.
type ExtensionDefinition struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Data      Value  `json:"data"`
}

// NewRaiseExecutable returns a raise executable with a valid outer union.
func NewRaiseExecutable(value RaiseDefinition) Executable {
	return Executable{Kind: ExecutableRaise, Raise: &value}
}

// NewSendExecutable returns a send executable with a valid outer union.
func NewSendExecutable(value SendDefinition) Executable {
	return Executable{Kind: ExecutableSend, Send: &value}
}

// NewCancelExecutable returns a cancel executable with a valid outer union.
func NewCancelExecutable(value CancelDefinition) Executable {
	return Executable{Kind: ExecutableCancel, Cancel: &value}
}

// NewLogExecutable returns a log executable with a valid outer union.
func NewLogExecutable(value LogDefinition) Executable {
	return Executable{Kind: ExecutableLog, Log: &value}
}

// NewAssignExecutable returns an assignment executable with a valid outer union.
func NewAssignExecutable(value AssignDefinition) Executable {
	return Executable{Kind: ExecutableAssign, Assign: &value}
}

// NewChooseExecutable returns a conditional executable with a valid outer union.
func NewChooseExecutable(value ChooseDefinition) Executable {
	return Executable{Kind: ExecutableChoose, Choose: &value}
}

// NewForEachExecutable returns an iteration executable with a valid outer union.
func NewForEachExecutable(value ForEachDefinition) Executable {
	return Executable{Kind: ExecutableForEach, ForEach: &value}
}

// NewScriptExecutable returns a model-script executable with a valid outer union.
func NewScriptExecutable(value ScriptDefinition) Executable {
	return Executable{Kind: ExecutableScript, Script: &value}
}

// NewCallExecutable returns a host-function call executable with a valid outer union.
func NewCallExecutable(value CallDefinition) Executable {
	return Executable{Kind: ExecutableCall, Call: &value}
}

// NewExtensionExecutable returns an extension executable with a valid outer union.
func NewExtensionExecutable(value ExtensionDefinition) Executable {
	return Executable{Kind: ExecutableExtension, Extension: &value}
}

// Raise creates executable content that raises an internal event. An optional
// expression computes its payload.
func Raise(event Identifier, data ...Expression) Executable {
	definition := RaiseDefinition{Event: event}
	if len(data) > 0 {
		value := data[0].Clone()
		definition.Data = &value
	}
	return NewRaiseExecutable(definition)
}

// SendOption configures serializable send executable content.
type SendOption func(*SendDefinition)

// SendTarget sets a static send target.
func SendTarget(target string) SendOption {
	return func(send *SendDefinition) { send.Target = target }
}

// SendType sets the static I/O processor type.
func SendType(typ string) SendOption {
	return func(send *SendDefinition) { send.Type = typ }
}

// SendID sets the author-visible send ID.
func SendID(id Identifier) SendOption {
	return func(send *SendDefinition) { send.ID = id }
}

// SendIDLocation stores a generated send ID at a model-owned location.
func SendIDLocation(location Expression) SendOption {
	return func(send *SendDefinition) {
		value := location.Clone()
		send.IDLocation = &value
	}
}

// SendDelay delays delivery by a static duration.
func SendDelay(delay time.Duration) SendOption {
	return func(send *SendDefinition) { send.Delay = delay.String() }
}

// SendContent sets the whole event payload expression.
func SendContent(content Expression) SendOption {
	return func(send *SendDefinition) {
		value := content.Clone()
		send.Content = &value
	}
}

// SendParams sets named event payload expressions.
func SendParams(params ...ParamDefinition) SendOption {
	return func(send *SendDefinition) { send.Params = cloneParams(params) }
}

// Send creates serializable event-delivery executable content.
func Send(event Identifier, options ...SendOption) Executable {
	definition := SendDefinition{Event: event}
	for _, option := range options {
		if option != nil {
			option(&definition)
		}
	}
	return NewSendExecutable(definition)
}

// CancelSend creates executable content that cancels a delayed send by ID.
func CancelSend(sendID Identifier) Executable {
	return NewCancelExecutable(CancelDefinition{SendID: sendID})
}

// LogValue creates diagnostic logging executable content.
func LogValue(label string, expression Expression) Executable {
	value := expression.Clone()
	return NewLogExecutable(LogDefinition{Label: label, Expr: &value})
}

func (e Executable) clone() Executable {
	clone := Executable{Kind: e.Kind}
	if e.Raise != nil {
		value := *e.Raise
		value.EventExpr = cloneExpression(value.EventExpr)
		value.Data = cloneExpression(value.Data)
		clone.Raise = &value
	}
	if e.Send != nil {
		value := *e.Send
		value.EventExpr = cloneExpression(value.EventExpr)
		value.TargetExpr = cloneExpression(value.TargetExpr)
		value.TypeExpr = cloneExpression(value.TypeExpr)
		value.IDLocation = cloneExpression(value.IDLocation)
		value.DelayExpr = cloneExpression(value.DelayExpr)
		value.Params = cloneParams(value.Params)
		value.Content = cloneExpression(value.Content)
		clone.Send = &value
	}
	if e.Cancel != nil {
		value := *e.Cancel
		value.SendIDExpr = cloneExpression(value.SendIDExpr)
		clone.Cancel = &value
	}
	if e.Log != nil {
		value := *e.Log
		value.LabelExpr = cloneExpression(value.LabelExpr)
		value.Expr = cloneExpression(value.Expr)
		clone.Log = &value
	}
	if e.Assign != nil {
		value := *e.Assign
		value.Location = value.Location.Clone()
		value.Expr = value.Expr.Clone()
		clone.Assign = &value
	}
	if e.Choose != nil {
		value := *e.Choose
		if value.Branches != nil {
			value.Branches = append([]ChooseBranchDefinition(nil), value.Branches...)
			for i := range value.Branches {
				value.Branches[i].Condition = value.Branches[i].Condition.Clone()
				value.Branches[i].Actions = cloneBlocks(value.Branches[i].Actions)
			}
		}
		value.Else = cloneBlocks(value.Else)
		clone.Choose = &value
	}
	if e.ForEach != nil {
		value := *e.ForEach
		value.Array = value.Array.Clone()
		value.Actions = cloneBlocks(value.Actions)
		clone.ForEach = &value
	}
	if e.Script != nil {
		value := *e.Script
		value.Expr = value.Expr.Clone()
		clone.Script = &value
	}
	if e.Call != nil {
		value := *e.Call
		value.Function = value.Function.clone()
		clone.Call = &value
	}
	if e.Extension != nil {
		value := *e.Extension
		value.Data = value.Data.Clone()
		clone.Extension = &value
	}
	return clone
}

func cloneBlocks(blocks []ExecutableBlock) []ExecutableBlock {
	if blocks == nil {
		return nil
	}
	clones := make([]ExecutableBlock, len(blocks))
	for i, block := range blocks {
		if block == nil {
			continue
		}
		clones[i] = make(ExecutableBlock, len(block))
		for j := range block {
			clones[i][j] = block[j].clone()
		}
	}
	return clones
}

func cloneParams(params []ParamDefinition) []ParamDefinition {
	if params == nil {
		return nil
	}
	clones := make([]ParamDefinition, len(params))
	for i := range params {
		clones[i] = params[i].clone()
	}
	return clones
}
