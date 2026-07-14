package main

import (
	"bytes"
	_ "embed"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dhamidi/statecharts/actors"
)

func renderCounterBox(p projection) HTMLElement {
	c := colorValues[p.Color]
	residencyClass := "is-paged-out"
	if p.Resident {
		residencyClass = "is-resident"
	}
	endpoint := "/counters/" + p.Name + "/increment"
	return New("button", map[string]string{
		"type":          "button",
		"class":         "counter " + residencyClass,
		"style":         "background:" + c.background + ";color:" + c.foreground,
		"data-on:click": "@post('" + endpoint + "')",
		"aria-label":    "Increment " + p.Name,
		"title":         "Increment " + p.Name,
	},
		New("span", map[string]string{"class": "counter-header"},
			New("strong", nil, Text(p.Name)), renderResidency(p.Resident)),
		renderCounterValue(p.Value),
	)
}

func renderResidency(resident bool) HTMLElement {
	label, class := "paged out", "nonresident"
	if resident {
		label, class = "resident", "resident"
	}
	return New("span", map[string]string{"class": "residency " + class}, Text(label))
}

func renderCounterValue(value int) HTMLElement {
	return New("data", map[string]string{"value": strconv.Itoa(value)}, Text(strconv.Itoa(value)))
}

func renderConnectionStatus(status string) HTMLElement {
	return New("div", map[string]string{"class": "status " + status},
		New("b", nil, Text("Counter server")), Text(status))
}

func renderSummary(ps []projection) HTMLElement {
	resident := 0
	for _, p := range ps {
		if p.Resident {
			resident++
		}
	}
	return New("section", map[string]string{"class": "summary"},
		Text(fmt.Sprintf("%d durable counters · %d resident · memory limit 3", len(ps), resident)))
}

func renderHeader() HTMLElement {
	return New("header", map[string]string{"class": "page-header"},
		New("p", nil, Text("STATECHART ACTORS / LIVE PROJECTION")),
		New("h1", nil, Text("Durable color counters")),
		New("p", map[string]string{"class": "hint"}, Text("Select a counter to increment it")),
	)
}

func renderDashboard(status string, ps []projection) HTMLElement {
	boxes := make([]HTMLElement, 0, len(ps))
	for _, p := range ps {
		boxes = append(boxes, renderCounterBox(p))
	}
	return New("main", map[string]string{"id": "dashboard"},
		renderHeader(), renderConnectionStatus(status), renderSummary(ps),
		New("section", map[string]string{"class": "grid"}, boxes...),
	)
}

func renderString(e HTMLElement) string {
	var b bytes.Buffer
	_ = e.WriteHTML(&b)
	return b.String()
}

const pageCSS = `*{box-sizing:border-box}body{margin:0;background:#f4f4f5;font:16px system-ui;color:#18181b}main{max-width:1100px;margin:auto;padding:2rem}.page-header{border-bottom:3px solid #18181b;margin-bottom:1rem;position:relative}.page-header p{font:700 .75rem ui-monospace,monospace;letter-spacing:.12em;margin:0}.page-header h1{font-size:2.4rem;margin:.3rem 0 1rem}.page-header .hint{position:absolute;right:0;bottom:1.2rem;font-weight:500;letter-spacing:0;text-transform:none}.status,.summary{padding:.65rem .8rem;border:1px solid #18181b;margin-bottom:.7rem;display:flex;justify-content:space-between}.status{background:#86efac}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:1px;background:#18181b;border:1px solid #18181b}.counter{appearance:none;border:0;border-radius:0;padding:1.2rem;min-height:155px;display:flex;flex-direction:column;justify-content:space-between;text-align:left;font:inherit;cursor:pointer}.counter:hover,.counter:focus-visible{outline:4px solid #18181b;outline-offset:-7px}.counter:active{transform:translate(2px,2px)}.counter.is-paged-out{filter:saturate(.2);opacity:.5}.counter-header{display:flex;justify-content:space-between;align-items:start}.counter strong{text-transform:uppercase;letter-spacing:.08em}.counter data{font:700 3rem ui-monospace,monospace}.residency{font:700 .65rem ui-monospace,monospace;text-transform:uppercase;padding:.2rem .3rem;border:1px solid currentColor}.nonresident{opacity:.65}@media(max-width:700px){main{padding:1rem}.page-header .hint{position:static;margin-bottom:1rem}.page-header h1{font-size:2rem}}`

//go:embed datastar.js
var datastarJS []byte

func renderPage() HTMLElement {
	return New("html", nil,
		New("head", nil,
			New("meta", map[string]string{"charset": "utf-8"}),
			New("meta", map[string]string{"name": "viewport", "content": "width=device-width"}),
			New("title", nil, Text("Durable counters")),
			New("script", map[string]string{"type": "module", "src": "/datastar.js"}),
			New("style", nil, Raw(pageCSS)),
		),
		New("body", map[string]string{"data-init": "@get('/ui/events')"},
			renderDashboard("starting", nil),
		),
	)
}

func datastarHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	_, _ = w.Write(datastarJS)
}

func pageHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, "<!doctype html>")
	_ = renderPage().WriteHTML(w)
}

func datastarPatch(elementHTML string) string {
	var b strings.Builder
	b.WriteString("event: datastar-patch-elements\n")
	for _, line := range strings.Split(elementHTML, "\n") {
		b.WriteString("data: elements ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	return b.String()
}

func serverBrowserEvents(sys *actors.System, hub *projectionHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		changes, unsubscribe := hub.subscribe()
		defer unsubscribe()
		send := func() bool {
			html := renderString(renderDashboard("online", residentSnapshot(sys, hub.snapshot(len(colors)))))
			_, err := fmt.Fprint(w, datastarPatch(html))
			f.Flush()
			return err == nil
		}
		if !send() {
			return
		}
		tick := time.NewTicker(15 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-changes:
				if !send() {
					return
				}
			case <-tick.C:
				fmt.Fprint(w, ": keepalive\n\n")
				f.Flush()
			}
		}
	}
}
