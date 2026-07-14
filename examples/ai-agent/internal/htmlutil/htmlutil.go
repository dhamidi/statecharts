// Package htmlutil is a minimal, dependency-free way to build HTML: no
// template engine, no third-party library, just a small tree of elements
// that escape themselves when written out.
package htmlutil

import (
	"fmt"
	"html"
	"io"
	"sort"
)

// HTMLElement is anything that can write itself out as HTML.
type HTMLElement interface {
	WriteHTML(w io.Writer) error
}

// Element is a plain HTML element: a tag, its attributes, and its children.
type Element struct {
	Tag      string
	Attrs    map[string]string
	Children []HTMLElement
}

// New builds an Element. attrs may be nil.
func New(tag string, attrs map[string]string, children ...HTMLElement) *Element {
	return &Element{Tag: tag, Attrs: attrs, Children: children}
}

// WriteHTML implements HTMLElement, escaping every attribute value.
func (e *Element) WriteHTML(w io.Writer) error {
	if _, err := fmt.Fprintf(w, "<%s", e.Tag); err != nil {
		return err
	}
	keys := make([]string, 0, len(e.Attrs))
	for k := range e.Attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if _, err := fmt.Fprintf(w, ` %s="%s"`, k, html.EscapeString(e.Attrs[k])); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(w, ">"); err != nil {
		return err
	}
	if isVoidTag(e.Tag) {
		return nil
	}
	for _, c := range e.Children {
		if err := c.WriteHTML(w); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(w, "</%s>", e.Tag)
	return err
}

func isVoidTag(tag string) bool {
	switch tag {
	case "br", "hr", "img", "input", "meta", "link":
		return true
	default:
		return false
	}
}

// Text is HTML-escaped text content.
type Text string

// WriteHTML implements HTMLElement.
func (t Text) WriteHTML(w io.Writer) error {
	_, err := io.WriteString(w, html.EscapeString(string(t)))
	return err
}

// Raw is unescaped HTML, for a caller that has already produced safe markup
// (e.g. nesting a rendered fragment). Used sparingly and only for trusted,
// locally-generated content -- never user input.
type Raw string

// WriteHTML implements HTMLElement.
func (r Raw) WriteHTML(w io.Writer) error {
	_, err := io.WriteString(w, string(r))
	return err
}
