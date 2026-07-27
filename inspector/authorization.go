package inspector

import (
	"context"
	"errors"

	"github.com/dhamidi/statecharts/actors"
)

// Operation identifies an inspector capability for authorization.
type Operation string

const (
	OperationListSystems    Operation = "list_systems"
	OperationListActors     Operation = "list_actors"
	OperationReadActor      Operation = "read_actor"
	OperationReadDefinition Operation = "read_definition"
	OperationReadHistory    Operation = "read_history"
	OperationSubscribe      Operation = "subscribe"
	OperationSendEvent      Operation = "send_event"
)

// ErrUnauthorized is returned for every denied request, without exposing the
// authorizer's error.
var ErrUnauthorized = errors.New("inspector: unauthorized")

// AuthorizationRequest describes the exact resource and operation requested.
type AuthorizationRequest struct {
	Operation Operation
	System    string
	ActorID   actors.ActorID
}

// Authorizer decides whether a request is allowed. Returning any error denies it.
type Authorizer interface {
	Authorize(context.Context, AuthorizationRequest) error
}

// AuthorizerFunc adapts a function to Authorizer. A nil function denies.
type AuthorizerFunc func(context.Context, AuthorizationRequest) error

func (f AuthorizerFunc) Authorize(ctx context.Context, request AuthorizationRequest) error {
	if f == nil {
		return ErrUnauthorized
	}
	return f(ctx, request)
}

// AllowAll returns an authorizer which permits every operation.
func AllowAll() Authorizer {
	return AuthorizerFunc(func(context.Context, AuthorizationRequest) error { return nil })
}
