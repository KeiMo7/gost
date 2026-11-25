// Package local contains host-local integration tests for the redundant-chain
// topology. Unlike tests/e2e (which requires Docker via testcontainers), these
// tests use real loopback listeners only and run under a plain `go test`,
// including -race.
package local

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	// Register the built-in connectors/dialers/handlers/listeners so node and
	// service parsing works in-process, mirroring cmd/gost/register.go.
	_ "github.com/go-gost/x/connector/http"
	_ "github.com/go-gost/x/connector/socks/v5"
	_ "github.com/go-gost/x/connector/tcp"
	_ "github.com/go-gost/x/dialer/tcp"
	_ "github.com/go-gost/x/handler/http"
	_ "github.com/go-gost/x/listener/tcp"
	xlogger "github.com/go-gost/x/logger"

	"github.com/go-gost/core/logger"
)

func init() {
	// The x dialers/connectors use logger.Default() directly; install a
	// quiet default once so in-process node building never hits a nil logger.
	logger.SetDefault(testLogger())
}

// testLogger returns a discarding logger so tests stay quiet while preserving
// the real logging code path (state-transition logs are exercised).
func testLogger() logger.Logger {
	return xlogger.NewLogger(xlogger.LevelOption(logger.FatalLevel))
}

// startTCPListener opens a TCP listener on a random loopback port and returns
// its address and a close function. The optional handler echoes accepted
// connections; when nil the listener simply accepts and closes.
func startTCPListener(t *testing.T, respond bool) (addr string, closeFn func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if respond {
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				go func(c net.Conn) {
					defer c.Close()
					_, _ = io.Copy(c, c) // echo
				}(conn)
			}
		}()
	} else {
		go func() {
			for {
				if _, err := ln.Accept(); err != nil {
					return
				}
			}
		}()
	}
	return ln.Addr().String(), func() { ln.Close() }
}

// startHTTPEcho starts an HTTP server on a random loopback port that answers
// 200 with a fixed body, returning its base URL and close function.
func startHTTPEcho(t *testing.T) (addr string, closeFn func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "hello-gost")
	})
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	return ln.Addr().String(), func() {
		ctx, cancel := contextWithTimeout(2 * time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = ln.Close()
	}
}

// freeAddr returns a loopback address that currently has nothing listening,
// so connecting to it fails with connection refused deterministically.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func contextWithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}
