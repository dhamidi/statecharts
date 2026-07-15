package ecmascript

import (
	"strings"
	"testing"
	"time"

	"github.com/dhamidi/statecharts"
)

func testProgram(configuration config) *program {
	return &program{
		owner:       &programOwner{},
		config:      configuration,
		expressions: map[string]*compiledExpression{},
		functions:   map[string]*compiledExpression{},
		data:        map[statecharts.Identifier]*compiledExpression{},
	}
}

func TestInBindingDelegatesToExecutionContext(t *testing.T) {
	session, err := newSession(testProgram(config{evaluationTimeout: time.Second}))
	if err != nil {
		t.Fatalf("newSession: %v", err)
	}
	defer session.Close()
	result, err := session.evaluate(statecharts.ExecContext{}, `In("missing")`)
	if err != nil {
		t.Fatalf("In: %v", err)
	}
	if result != false {
		t.Fatalf("In result = %#v, want false", result)
	}
}

func TestSessionCloseIsIdempotentAndRejectsEvaluation(t *testing.T) {
	session, err := newSession(testProgram(config{evaluationTimeout: time.Second}))
	if err != nil {
		t.Fatalf("newSession: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := session.evaluate(statecharts.ExecContext{}, "1"); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("evaluation after Close error = %v, want closed", err)
	}
}

func TestInterruptSignalStopsCurrentAndFutureEvaluation(t *testing.T) {
	interrupt := make(chan struct{})
	session, err := newSession(testProgram(config{evaluationTimeout: time.Second, interrupt: interrupt}))
	if err != nil {
		t.Fatalf("newSession: %v", err)
	}
	defer session.Close()
	done := make(chan error, 1)
	go func() {
		_, err := session.evaluate(statecharts.ExecContext{}, `for (;;) {}`)
		done <- err
	}()
	time.Sleep(10 * time.Millisecond)
	close(interrupt)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("interrupted evaluation returned nil error")
		}
	case <-time.After(time.Second):
		t.Fatal("interrupt did not stop evaluation")
	}
	if _, err := session.evaluate(statecharts.ExecContext{}, "1"); err == nil || !strings.Contains(err.Error(), "interrupted") {
		t.Fatalf("future evaluation error = %v, want interrupted", err)
	}
}
