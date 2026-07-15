package statecharts

// Expression is syntax-neutral, datamodel-owned program data. Kind selects
// an expression form understood by a Datamodel; Data contains that form's
// canonical operands or source. Structural validation checks only that Kind
// and Data are well formed. A datamodel validates their meaning at compile
// time.
type Expression struct {
	Kind Identifier `json:"kind"`
	Data Value      `json:"data"`
}

// Clone returns an independently editable expression.
func (e Expression) Clone() Expression {
	e.Data = e.Data.Clone()
	return e
}

func cloneExpression(expression *Expression) *Expression {
	if expression == nil {
		return nil
	}
	clone := expression.Clone()
	return &clone
}

// FunctionRef identifies host behavior without storing a Go function. Name
// and Version are stable definition data; Args are datamodel expressions
// evaluated according to the operation that owns the reference.
type FunctionRef struct {
	Name    Identifier   `json:"name"`
	Version string       `json:"version"`
	Args    []Expression `json:"args,omitempty"`
}

func (r FunctionRef) clone() FunctionRef {
	r.Args = cloneExpressions(r.Args)
	return r
}

func cloneExpressions(expressions []Expression) []Expression {
	if expressions == nil {
		return nil
	}
	clones := make([]Expression, len(expressions))
	for i := range expressions {
		clones[i] = expressions[i].Clone()
	}
	return clones
}

// ParamDefinition binds one named invocation or done-data parameter. Exactly
// one of Expr and Location must be present. Location lets the selected
// datamodel read a value from one of its own locations without exposing that
// representation to the interpreter.
type ParamDefinition struct {
	Name     Identifier  `json:"name"`
	Expr     *Expression `json:"expr,omitempty"`
	Location *Expression `json:"location,omitempty"`
}

func (p ParamDefinition) clone() ParamDefinition {
	p.Expr = cloneExpression(p.Expr)
	p.Location = cloneExpression(p.Location)
	return p
}
