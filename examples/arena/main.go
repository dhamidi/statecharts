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
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "serve" {
		log.Fatal("usage: arena serve [--addr :8080] [--bots 3] [--tick 150ms]")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runServe(ctx, os.Args[2:]); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

func runServe(ctx context.Context, arguments []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	address := flags.String("addr", ":8080", "listen address")
	bots := flags.Int("bots", 3, "number of AI creatures")
	tick := flags.Duration("tick", defaultTickInterval, "authoritative simulation interval")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if *bots < 0 || *bots > 20 {
		return fmt.Errorf("bots must be between 0 and 20")
	}
	if *tick <= 0 {
		return fmt.Errorf("tick must be positive")
	}
	runtime, err := setupArena(ctx, runtimeOptions{TickInterval: *tick, Bots: *bots})
	if err != nil {
		return err
	}
	defer runtime.stop(context.Background())
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		return err
	}
	server := &http.Server{
		Handler:           arenaHandler(runtime),
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	log.Printf("arena listening on %s with %d bots", listener.Addr(), *bots)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
