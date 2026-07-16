package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/dhamidi/statecharts"
)

const socketIOProcessor statecharts.Identifier = "arena.websocket"

type socketOutput struct {
	frames chan []byte
	cancel context.CancelFunc
	done   chan error
}

type socketTransport struct {
	mu      sync.RWMutex
	outputs map[statecharts.Identifier]*socketOutput
}

func newSocketTransport() *socketTransport {
	return &socketTransport{outputs: map[statecharts.Identifier]*socketOutput{}}
}

func (transport *socketTransport) factory() statecharts.IOProcessor {
	return &socketBinding{transport: transport}
}

func (transport *socketTransport) registerTest(id statecharts.Identifier) <-chan []byte {
	output := &socketOutput{frames: make(chan []byte, 64), done: make(chan error, 1)}
	transport.mu.Lock()
	transport.outputs[id] = output
	transport.mu.Unlock()
	return output.frames
}

func (transport *socketTransport) registerSocket(id statecharts.Identifier, connection *websocket.Conn) <-chan error {
	ctx, cancel := context.WithCancel(context.Background())
	output := &socketOutput{frames: make(chan []byte, 64), cancel: cancel, done: make(chan error, 1)}
	transport.mu.Lock()
	transport.outputs[id] = output
	transport.mu.Unlock()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case frame := <-output.frames:
				writeCtx, stop := context.WithTimeout(ctx, 5*time.Second)
				err := connection.Write(writeCtx, websocket.MessageText, frame)
				stop()
				if err != nil {
					select {
					case output.done <- err:
					default:
					}
					return
				}
			}
		}
	}()
	return output.done
}

func (transport *socketTransport) unregister(id statecharts.Identifier) {
	transport.mu.Lock()
	output := transport.outputs[id]
	delete(transport.outputs, id)
	transport.mu.Unlock()
	if output != nil && output.cancel != nil {
		output.cancel()
	}
}

func (transport *socketTransport) send(ctx context.Context, request statecharts.SendRequest) error {
	if request.Type != socketIOProcessor {
		return fmt.Errorf("arena websocket transport does not support %q", request.Type)
	}
	text, ok := request.Data.AsString()
	if !ok {
		return fmt.Errorf("arena websocket frame is not text")
	}
	transport.mu.RLock()
	output := transport.outputs[request.Target]
	transport.mu.RUnlock()
	if output == nil {
		return fmt.Errorf("arena websocket capability %q is not attached", request.Target)
	}
	frame := []byte(text)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case output.frames <- frame:
		return nil
	default:
		return fmt.Errorf("arena websocket capability %q is backpressured", request.Target)
	}
}

type socketBinding struct {
	transport  *socketTransport
	dispatcher statecharts.Dispatcher
}

func (binding *socketBinding) Attach(dispatcher statecharts.Dispatcher) {
	binding.dispatcher = dispatcher
}

func (binding *socketBinding) Send(ctx context.Context, request statecharts.SendRequest) error {
	return binding.transport.send(ctx, request)
}
