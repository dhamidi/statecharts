// Package llmgenai implements llm.Provider against a real model via
// google.golang.org/genai. It is a separate package from internal/llm so
// that package's Provider interface and EchoProvider don't drag in the
// genai SDK as a dependency for callers who never select --llm=genai.
package llmgenai

import (
	"context"
	"fmt"

	"google.golang.org/genai"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/llm"
)

// Provider drives a real model through google.golang.org/genai, streaming
// thinking/text/tool_call chunks identically in shape to EchoProvider --
// see internal/llm.Provider's own doc comment.
type Provider struct {
	client *genai.Client
	model  string
}

// New constructs a Provider for model (e.g. "gemini-2.5-flash"), backed by
// the Gemini API using apiKey (see GEMINI_API_KEY in the example's README).
func New(ctx context.Context, apiKey, model string) (*Provider, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("llmgenai: empty API key")
	}
	if model == "" {
		return nil, fmt.Errorf("llmgenai: empty model")
	}
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey:  apiKey,
		Backend: genai.BackendGeminiAPI,
	})
	if err != nil {
		return nil, fmt.Errorf("llmgenai: new client: %w", err)
	}
	return &Provider{client: client, model: model}, nil
}

// Generate implements llm.Provider.
func (p *Provider) Generate(ctx context.Context, req llm.GenerateRequest, emit func(llm.Chunk)) error {
	contents := make([]*genai.Content, 0, len(req.History))
	for _, m := range req.History {
		role := genai.Role(genai.RoleUser)
		switch m.Role {
		case llm.RoleAssistant:
			role = genai.RoleModel
		case llm.RoleTool:
			// Represented as ordinary context, not a real FunctionResponse
			// part: llm.Message (this package's shared, provider-agnostic
			// shape) carries only text, not the original call's name/id --
			// a deliberate simplification, since this example's own
			// EchoProvider needs nothing richer either.
			contents = append(contents, genai.NewContentFromText("Tool result: "+m.Text, genai.RoleUser))
			continue
		}
		contents = append(contents, genai.NewContentFromText(m.Text, role))
	}

	var tools []*genai.Tool
	if len(req.Tools) > 0 {
		decls := make([]*genai.FunctionDeclaration, len(req.Tools))
		for i, t := range req.Tools {
			decls[i] = &genai.FunctionDeclaration{
				Name:        t.Name,
				Description: t.Description,
				Parameters: &genai.Schema{
					Type: genai.TypeObject,
					Properties: map[string]*genai.Schema{
						"command": {Type: genai.TypeString, Description: "the shell command to run"},
					},
					Required: []string{"command"},
				},
			}
		}
		tools = []*genai.Tool{{FunctionDeclarations: decls}}
	}

	cfg := &genai.GenerateContentConfig{
		Tools:          tools,
		ThinkingConfig: &genai.ThinkingConfig{IncludeThoughts: true},
	}

	for resp, err := range p.client.Models.GenerateContentStream(ctx, p.model, contents, cfg) {
		if err != nil {
			return fmt.Errorf("llmgenai: stream: %w", err)
		}
		for _, cand := range resp.Candidates {
			if cand.Content == nil {
				continue
			}
			for _, part := range cand.Content.Parts {
				switch {
				case part.FunctionCall != nil:
					emit(llm.Chunk{Kind: "tool_call", ToolCall: llm.ToolCall{
						ID: part.FunctionCall.ID, Name: part.FunctionCall.Name, Args: part.FunctionCall.Args,
					}})
				case part.Thought:
					emit(llm.Chunk{Kind: "thinking", TextDelta: part.Text})
				case part.Text != "":
					emit(llm.Chunk{Kind: "text", TextDelta: part.Text})
				}
			}
		}
	}
	return nil
}
