package inspectorhttp

import (
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	statecharts "github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/inspector"
)

func TestEmbeddedUIBrowserInteractions(t *testing.T) {
	if os.Getenv("STATECHARTS_BROWSER_TEST") != "1" {
		t.Skip("set STATECHARTS_BROWSER_TEST=1 to run the agent-browser interaction suite")
	}
	if _, err := exec.LookPath("agent-browser"); err != nil {
		t.Skip("agent-browser is not installed")
	}

	received := make(chan statecharts.Event, 4)
	handler, _, _ := testHandler(t, received,
		inspector.WithAuthorizer(inspector.AllowAll()),
		inspector.WithRingSize(8),
	)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	session := "statecharts-inspector-test-" + strconv.Itoa(os.Getpid())
	browser := func(arguments ...string) string {
		t.Helper()
		args := append([]string{"--session", session}, arguments...)
		command := exec.CommandContext(t.Context(), "agent-browser", args...)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("agent-browser %s: %v\n%s", strings.Join(arguments, " "), err, output)
		}
		return strings.TrimSpace(string(output))
	}
	t.Cleanup(func() {
		command := exec.Command("agent-browser", "--session", session, "close")
		_ = command.Run()
	})
	waitFor := func(expression string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			if browser("eval", expression) == "true" {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("browser condition did not become true: %s", expression)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	browser("open", server.URL+"/")
	waitFor(`document.querySelector('button[data-actor-id="actor"]') !== null`)
	browser("select", `select[aria-label="residency"]`, "resident")
	waitFor(`document.querySelectorAll('.directory button[data-actor-id="actor"]').length === 1`)
	browser("fill", `input[aria-label="kind"]`, "http-test")
	browser("click", "actor-directory .toolbar button")
	waitFor(`document.querySelectorAll('.directory button[data-actor-id="actor"]').length === 1`)
	browser("click", `button[data-actor-id="actor"]`)
	waitFor(`document.querySelector('.facts dd')?.textContent === 'actor'`)

	browser("fill", `event-form input[aria-label="Event name"]`, "message")
	browser("click", `event-form button[type="submit"]`)
	waitFor(`document.querySelector('event-form [aria-live]')?.textContent === 'Accepted. Not retried.'`)
	waitFor(`document.querySelectorAll('.live-history .live').length > 0 && document.querySelectorAll('.transition.selected').length === 1`)
	select {
	case event := <-received:
		if event.Name != "message" || event.Type != statecharts.EventExternal {
			t.Fatalf("browser command delivered %#v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("browser command did not reach the chart")
	}

	browser("eval", `document.querySelector('inspector-app').onGap({Sequence:999,Kind:'gap',Reason:'browser test gap',Dropped:2}); true`)
	waitFor(`document.querySelector('.live-history .gap')?.textContent.includes('browser test gap') === true`)
	browser("select", `event-form select[aria-label="Value kind"]`, "map")
	browser("click", "event-form .value-editor > button")
	waitFor(`document.querySelector('event-form input[aria-label="Map key"]') !== null`)

	browser("set", "viewport", "390", "844")
	if got := browser("eval", `document.documentElement.scrollWidth === innerWidth`); got != "true" {
		t.Fatalf("mobile inspector has document-level horizontal overflow: %s", got)
	}
	browser("reload")
	waitFor(`document.querySelector('button[data-actor-id="actor"]') !== null`)
	browser("wait", "300")
	select {
	case duplicate := <-received:
		t.Fatalf("browser reconnect repeated command %#v", duplicate)
	default:
	}

	if output := browser("errors"); output != "" {
		t.Fatalf("browser reported errors:\n%s", output)
	}
}
