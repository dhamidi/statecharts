package inspector

import (
	"context"
	"errors"
	"testing"
)

func TestNilAndPanickingAuthorizersDeny(t *testing.T) {
	for _, authorizer := range []Authorizer{AuthorizerFunc(nil), AuthorizerFunc(func(context.Context, AuthorizationRequest) error { panic("secret") })} {
		s := New(WithAuthorizer(authorizer))
		_, err := s.Systems(context.Background())
		if !errors.Is(err, ErrUnauthorized) || err.Error() != ErrUnauthorized.Error() {
			t.Fatalf("authorization error = %v", err)
		}
	}
}

func TestInvalidConfigurationIsNormalized(t *testing.T) {
	s := New(WithAuthorizer(AllowAll()), WithRingSize(-1), WithSourceBuffer(-1), WithClock(nil))
	if s.cfg.ringSize != 1024 || s.cfg.sourceBuffer != 256 || s.cfg.clock == nil {
		t.Fatalf("config was not normalized: %#v", s.cfg)
	}
	var sub Subscription
	sub.Close()
}
