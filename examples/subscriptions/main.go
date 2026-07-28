package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dhamidi/statecharts/inspector"
	inspectorhttp "github.com/dhamidi/statecharts/inspector/http"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "serve" {
		log.Fatal("usage: subscriptions serve --addr :8080 --db subscriptions.db")
	}
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "listen address")
	path := fs.String("db", "subscriptions.db", "SQLite path")
	fs.Parse(os.Args[2:])
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	r, err := openRuntime(ctx, *path)
	if err != nil {
		log.Fatal(err)
	}
	defer r.store.Close()
	ins := inspector.New(inspector.WithAuthorizer(inspector.AllowAll()))
	defer ins.Close()
	if err = ins.RegisterSystem("subscriptions", r.system); err != nil {
		log.Fatal(err)
	}
	srv := &http.Server{Addr: *addr, Handler: handler(r, inspectorhttp.NewHandler(ins)), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		c, x := context.WithTimeout(context.Background(), 10*time.Second)
		defer x()
		srv.Shutdown(c)
	}()
	log.Printf("subscriptions listening on %s", *addr)
	err = srv.ListenAndServe()
	r.close(context.Background())
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
