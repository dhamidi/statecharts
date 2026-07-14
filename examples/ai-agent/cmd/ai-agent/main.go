// Command ai-agent is a single binary demonstrating github.com/dhamidi/statecharts
// end to end: a durable, recoverable multi-conversation AI agent workspace,
// its client, and everything in between. See the example's README for the
// full story and a manual recovery test script.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/sqllog/sqlite3"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/client"
	"github.com/dhamidi/statecharts/examples/ai-agent/internal/llm"
	"github.com/dhamidi/statecharts/examples/ai-agent/internal/llmgenai"
	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
	"github.com/dhamidi/statecharts/examples/ai-agent/internal/server"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "serve":
			if err := runServe(os.Args[2:]); err != nil {
				log.Fatal(err)
			}
			return
		case "connect":
			if err := runConnect(os.Args[2:]); err != nil {
				log.Fatal(err)
			}
			return
		}
	}
	if err := runEmbedded(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func chooseProvider(ctx context.Context, choice, model string) (llm.Provider, error) {
	switch choice {
	case "", "echo":
		return llm.EchoProvider{}, nil
	case "genai":
		if model == "" {
			return nil, fmt.Errorf("--llm-model is required for --llm=genai")
		}
		apiKey := os.Getenv("GEMINI_API_KEY")
		if apiKey == "" {
			return nil, fmt.Errorf("GEMINI_API_KEY must be set for --llm=genai")
		}
		return llmgenai.New(ctx, apiKey, model)
	default:
		return nil, fmt.Errorf("unknown --llm=%q (want echo or genai)", choice)
	}
}

func parseTools(raw string) []protocol.ToolName {
	if raw == "" {
		return nil
	}
	var tools []protocol.ToolName
	for _, t := range strings.Split(raw, ",") {
		if name, err := protocol.NewToolName(strings.TrimSpace(t)); err == nil {
			tools = append(tools, name)
		}
	}
	return tools
}

func openLog(dbPath string) (*sqlite3.Storage, error) {
	return sqlite3.Open(dbPath)
}

// startServer wires up and starts the workspace server's actors.System and
// HTTP listener on addr, returning the base URL clients should use and a
// shutdown func. It blocks until the workspace is fully set up (every
// chart registered, singletons spawned, DirectoryActor primed) before the
// listener ever accepts a connection.
func startServer(ctx context.Context, addr, dbPath, llmChoice, llmModel string) (baseURL string, shutdown func(context.Context) error, err error) {
	logStore, err := openLog(dbPath)
	if err != nil {
		return "", nil, err
	}
	provider, err := chooseProvider(ctx, llmChoice, llmModel)
	if err != nil {
		return "", nil, err
	}
	clock := statecharts.NewRealClock()
	sys, _ := server.NewSystem(logStore, clock, statecharts.NoopLogger, provider)
	if err := server.Setup(ctx, sys, clock); err != nil {
		return "", nil, err
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", nil, fmt.Errorf("listen %q: %w", addr, err)
	}
	httpSrv := &http.Server{Handler: server.NewServerHandler(sys)}
	go func() {
		if err := httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("ai-agent serve: %v", err)
		}
	}()

	base := "http://" + ln.Addr().String()
	fmt.Printf("ai-agent: serving on %s\n", base)

	return base, func(shutdownCtx context.Context) error {
		httpErr := httpSrv.Shutdown(shutdownCtx)
		systemErr := sys.Stop(shutdownCtx)
		closeErr := logStore.Close()
		return errors.Join(httpErr, systemErr, closeErr)
	}, nil
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "address to listen on")
	dbPath := fs.String("db", "data/ai-agent.db", "sqlite database path")
	llmChoice := fs.String("llm", "echo", "LLM provider: echo or genai")
	llmModel := fs.String("llm-model", "", "model name (required for --llm=genai)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	_, shutdown, err := startServer(ctx, *addr, *dbPath, *llmChoice, *llmModel)
	if err != nil {
		return err
	}
	<-ctx.Done()
	fmt.Println("ai-agent: shutting down")
	return shutdown(context.Background())
}

func runConnect(args []string) error {
	fs := flag.NewFlagSet("connect", flag.ExitOnError)
	serverAddr := fs.String("server", "http://127.0.0.1:8080", "workspace server address")
	conversation := fs.String("conversation", "", "conversation id to open (optional)")
	tools := fs.String("tools", "", "comma-separated tool names this client executes")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sys := client.NewSystem(statecharts.NewRealClock())
	if err := client.Setup(ctx, sys, *serverAddr, parseTools(*tools), protocol.ConversationID(*conversation)); err != nil {
		return err
	}

	<-ctx.Done()
	fmt.Println("ai-agent: shutting down")
	return sys.Stop(context.Background())
}

func runEmbedded(args []string) error {
	fs := flag.NewFlagSet("ai-agent", flag.ExitOnError)
	tools := fs.String("tools", "", "comma-separated tool names this client executes")
	llmChoice := fs.String("llm", "echo", "LLM provider: echo or genai")
	llmModel := fs.String("llm-model", "", "model name (required for --llm=genai)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverAddr, shutdownServer, err := startServer(ctx, "127.0.0.1:0", "data/ai-agent-workspace.db", *llmChoice, *llmModel)
	if err != nil {
		return err
	}

	sys := client.NewSystem(statecharts.NewRealClock())
	if err := client.Setup(ctx, sys, serverAddr, parseTools(*tools), ""); err != nil {
		return err
	}

	<-ctx.Done()
	fmt.Println("ai-agent: shutting down")
	if err := sys.Stop(context.Background()); err != nil {
		log.Printf("ai-agent: client shutdown: %v", err)
	}
	return shutdownServer(context.Background())
}
