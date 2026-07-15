// Package scxml encodes and decodes statecharts.Definition using the W3C
// State Chart XML surface syntax. It is an optional interoperability adapter:
// compilation and publication remain explicit statecharts package operations.
package scxml

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/dhamidi/statecharts"
)

const (
	scxmlNamespace     = "http://www.w3.org/2005/07/scxml"
	extensionNamespace = "https://statecharts.dev/ns/definition"
)

// Error reports an XML codec failure at a stable Definition traversal path.
// Line and Column identify the source position for decode failures and are
// zero for encode failures.
type Error struct {
	Path   string
	Line   int
	Column int
	Err    error
}

func (e *Error) Error() string {
	position := ""
	if e.Line > 0 {
		position = fmt.Sprintf(" at %d:%d", e.Line, e.Column)
	}
	return fmt.Sprintf("statecharts SCXML %s%s: %v", e.Path, position, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// Option configures SCXML encoding and decoding.
type Option func(*options)

type options struct {
	expressions statecharts.TextExpressionCodec
}

// WithTextExpressionCodec supplies the selected datamodel's text bridge.
// It is required whenever a Definition contains expressions.
func WithTextExpressionCodec(codec statecharts.TextExpressionCodec) Option {
	return func(options *options) { options.expressions = codec }
}

func applyOptions(values []Option) options {
	var result options
	for _, option := range values {
		if option != nil {
			option(&result)
		}
	}
	return result
}

// Marshal returns deterministic compact SCXML for definition.
func Marshal(definition statecharts.Definition, options ...Option) ([]byte, error) {
	return marshal(definition, "", applyOptions(options))
}

// MarshalIndent returns deterministic SCXML indented by indent.
func MarshalIndent(definition statecharts.Definition, indent string, options ...Option) ([]byte, error) {
	return marshal(definition, indent, applyOptions(options))
}

func marshal(definition statecharts.Definition, indent string, options options) ([]byte, error) {
	if err := definition.Validate(); err != nil {
		return nil, wrapDefinitionError(err)
	}
	encoder := definitionEncoder{
		writer:      xmlWriter{indent: indent},
		expressions: options.expressions,
	}
	if err := encoder.definition(definition); err != nil {
		return nil, err
	}
	if encoder.writer.err != nil {
		return nil, encoder.writer.err
	}
	return encoder.writer.bytes(), nil
}

// Unmarshal decodes exactly one strict SCXML document. An omitted datamodel
// selects the library's default "go" model. It returns the zero Definition on
// every error and never compiles or publishes the result.
func Unmarshal(data []byte, options ...Option) (statecharts.Definition, error) {
	root, err := parseDocument(data)
	if err != nil {
		return statecharts.Definition{}, err
	}
	decoder := definitionDecoder{expressions: applyOptions(options).expressions}
	definition, err := decoder.definition(root)
	if err != nil {
		return statecharts.Definition{}, err
	}
	if err := definition.Validate(); err != nil {
		return statecharts.Definition{}, wrapDefinitionError(err)
	}
	return definition, nil
}

func wrapDefinitionError(err error) error {
	var definitionError *statecharts.DefinitionError
	if errors.As(err, &definitionError) {
		return &Error{Path: definitionError.Path, Err: definitionError.Err}
	}
	return &Error{Path: "definition", Err: err}
}

type element struct {
	name     xml.Name
	attrs    []xml.Attr
	children []*element
	text     strings.Builder
	line     int
	column   int
}

func parseDocument(data []byte) (*element, error) {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	decoder.Strict = true
	var stack []*element
	var root *element
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			line, column := decoder.InputPos()
			if syntax, ok := err.(*xml.SyntaxError); ok {
				line = syntax.Line
			}
			return nil, &Error{Path: currentPath(stack), Line: line, Column: column, Err: err}
		}
		switch token := token.(type) {
		case xml.StartElement:
			line, column := decoder.InputPos()
			item := &element{name: token.Name, attrs: append([]xml.Attr(nil), token.Attr...), line: line, column: column}
			for _, attr := range item.attrs {
				if err := validateXMLString(attr.Value); err != nil {
					return nil, &Error{Path: currentPath(stack), Line: line, Column: column, Err: fmt.Errorf("attribute %q: %w", attr.Name.Local, err)}
				}
			}
			if err := rejectDuplicateAttributes(item, currentPath(stack)); err != nil {
				return nil, err
			}
			if len(stack) == 0 {
				if root != nil {
					return nil, item.error("definition", "multiple root elements")
				}
				root = item
			} else {
				stack[len(stack)-1].children = append(stack[len(stack)-1].children, item)
			}
			stack = append(stack, item)
		case xml.EndElement:
			stack = stack[:len(stack)-1]
		case xml.CharData:
			if err := validateXMLString(string(token)); err != nil {
				line, column := decoder.InputPos()
				return nil, &Error{Path: currentPath(stack), Line: line, Column: column, Err: err}
			}
			if len(stack) == 0 {
				if strings.TrimSpace(string(token)) != "" {
					line, column := decoder.InputPos()
					return nil, &Error{Path: "definition", Line: line, Column: column, Err: fmt.Errorf("text outside root element")}
				}
			} else {
				stack[len(stack)-1].text.Write(token)
			}
		case xml.Directive:
			line, column := decoder.InputPos()
			return nil, &Error{Path: currentPath(stack), Line: line, Column: column, Err: fmt.Errorf("XML directives are not supported")}
		}
	}
	if root == nil {
		return nil, &Error{Path: "definition", Err: fmt.Errorf("empty document")}
	}
	return root, nil
}

func currentPath(stack []*element) string {
	if len(stack) == 0 {
		return "definition"
	}
	return "definition." + stack[len(stack)-1].name.Local
}

func rejectDuplicateAttributes(item *element, path string) error {
	seen := make(map[xml.Name]bool, len(item.attrs))
	for _, attr := range item.attrs {
		if seen[attr.Name] {
			return item.error(path, "duplicate attribute %q", attr.Name.Local)
		}
		seen[attr.Name] = true
	}
	return nil
}

func (e *element) error(path, format string, args ...any) error {
	return &Error{Path: path, Line: e.line, Column: e.column, Err: fmt.Errorf(format, args...)}
}

func (e *element) attr(namespace, name string) (string, bool) {
	for _, attr := range e.attrs {
		if attr.Name.Space == namespace && attr.Name.Local == name {
			return attr.Value, true
		}
	}
	return "", false
}

func (e *element) checkAttrs(path string, allowed ...xml.Name) error {
	for _, attr := range e.attrs {
		if attr.Name.Space == "xmlns" || (attr.Name.Space == "" && attr.Name.Local == "xmlns") {
			continue
		}
		found := false
		for _, candidate := range allowed {
			if attr.Name == candidate {
				found = true
				break
			}
		}
		if !found {
			return e.error(path, "unknown attribute %q", attr.Name.Local)
		}
	}
	return nil
}

func plainAttr(names ...string) []xml.Name {
	result := make([]xml.Name, len(names))
	for i, name := range names {
		result[i] = xml.Name{Local: name}
	}
	return result
}

type attribute struct {
	name  string
	value string
}

type xmlWriter struct {
	buffer bytes.Buffer
	indent string
	depth  int
	open   []string
	nested []bool
	err    error
}

func (w *xmlWriter) bytes() []byte { return append([]byte(nil), w.buffer.Bytes()...) }

func (w *xmlWriter) header() {
	w.buffer.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
}

func (w *xmlWriter) lineBreak() {
	if w.indent == "" {
		return
	}
	w.buffer.WriteByte('\n')
	w.buffer.WriteString(strings.Repeat(w.indent, w.depth))
}

func (w *xmlWriter) start(name string, attrs ...attribute) {
	if len(w.nested) > 0 {
		w.nested[len(w.nested)-1] = true
	}
	w.lineBreak()
	w.buffer.WriteByte('<')
	w.buffer.WriteString(name)
	for _, attr := range attrs {
		force := strings.HasPrefix(attr.name, "\x00")
		if attr.value == "" && !force {
			continue
		}
		if err := validateXMLString(attr.value); err != nil && w.err == nil {
			w.err = &Error{Path: "definition", Err: fmt.Errorf("attribute %q: %w", strings.TrimPrefix(attr.name, "\x00"), err)}
		}
		w.buffer.WriteByte(' ')
		w.buffer.WriteString(strings.TrimPrefix(attr.name, "\x00"))
		w.buffer.WriteString(`="`)
		_ = xml.EscapeText(&w.buffer, []byte(attr.value))
		w.buffer.WriteByte('"')
	}
	w.buffer.WriteByte('>')
	w.open = append(w.open, name)
	w.nested = append(w.nested, false)
	w.depth++
}

func expressionAttribute(name, value string) attribute {
	if value == "" {
		name = "\x00" + name
	}
	return attribute{name, value}
}

func (w *xmlWriter) text(text string) {
	if err := validateXMLString(text); err != nil && w.err == nil {
		w.err = &Error{Path: "definition", Err: err}
	}
	_ = xml.EscapeText(&w.buffer, []byte(text))
}

func (w *xmlWriter) end() {
	w.depth--
	name := w.open[len(w.open)-1]
	w.open = w.open[:len(w.open)-1]
	nested := w.nested[len(w.nested)-1]
	w.nested = w.nested[:len(w.nested)-1]
	if nested {
		w.lineBreak()
	}
	w.buffer.WriteString("</")
	w.buffer.WriteString(name)
	w.buffer.WriteByte('>')
}

func boolText(value bool) string {
	if value {
		return "true"
	}
	return ""
}

func validateXMLString(value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("text is not valid UTF-8")
	}
	for _, char := range value {
		if char == '\t' || char == '\n' || char == '\r' ||
			(char >= 0x20 && char <= 0xd7ff) ||
			(char >= 0xe000 && char <= 0xfffd) ||
			(char >= 0x10000 && char <= 0x10ffff) {
			continue
		}
		return fmt.Errorf("character U+%04X is not allowed in XML 1.0", char)
	}
	return nil
}
