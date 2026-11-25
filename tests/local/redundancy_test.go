package local

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-gost/gost/internal/topology"
)

// buildManager parses the given -F values, builds node configs and installs a
// manager via the production node builder, returning the manager and chain.
func buildManager(t *testing.T, args []string) *topology.Manager {
	t.Helper()
	decl, err := topology.ParseChainDecl(args)
	if err != nil {
		t.Fatalf("parse declaration: %v", err)
	}
	nodeCfgs, err := topology.BuildNodeConfigs(decl)
	if err != nil {
		t.Fatalf("build node configs: %v", err)
	}
	mgr := topology.NewManager(topology.DefaultNodeBuilder, "chain-0", testLogger())
	if err := mgr.Apply(decl, nodeCfgs, "cli"); err != nil {
		t.Fatalf("apply topology: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	return mgr
}

// dialThrough routes one TCP connection through the managed chain to target.
func dialThrough(t *testing.T, mgr *topology.Manager, target string) (net.Conn, error) {
	t.Helper()
	ch := mgr.Chainer()
	if ch == nil {
		t.Fatal("no chainer installed")
	}
	route := ch.Route(context.Background(), "tcp", target)
	if route == nil {
		t.Fatal("nil route")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return route.Dial(ctx, "tcp", target)
}

// TestFailover_LiveAndRefused: one live and one connection-refused candidate
// in the same segment; every request must succeed and only reach the live one.
func TestFailover_LiveAndRefused(t *testing.T) {
	liveAddr, closeLive := startTCPListener(t, true)
	defer closeLive()
	deadAddr := freeAddr(t)

	mgr := buildManager(t, []string{
		fmt.Sprintf("tcp://%s|tcp://%s", deadAddr, liveAddr),
	})

	for i := range 20 {
		conn, err := dialThrough(t, mgr, liveAddr)
		if err != nil {
			t.Fatalf("request %d failed despite a live candidate: %v", i, err)
		}
		conn.Close()
	}

	summary, ok := mgr.Summary()
	if !ok {
		t.Fatal("no summary")
	}
	var deadStatus topology.CandidateStatus
	for _, seg := range summary.Segments {
		for _, c := range seg.Candidates {
			if c.Addr == deadAddr {
				deadStatus = c
			}
		}
	}
	if deadStatus.Fails == 0 {
		t.Fatal("dead candidate should have recorded failures")
	}
	if deadStatus.State != topology.StateIsolated && deadStatus.State != topology.StateProbing {
		t.Fatalf("dead candidate should be isolated/probing, got %s", deadStatus.State)
	}
}

// TestWeightedDistribution: fixed request count, weights reflected in
// selection share within tolerance; a backup candidate receives no traffic
// while primaries are healthy (covers the zero-traffic-candidate intent).
func TestWeightedDistribution(t *testing.T) {
	a, closeA := startTCPListener(t, true)
	defer closeA()
	b, closeB := startTCPListener(t, true)
	defer closeB()
	c, closeC := startTCPListener(t, true)
	defer closeC()

	mgr := buildManager(t, []string{
		fmt.Sprintf("tcp://%s?weight=3|tcp://%s?weight=1|tcp://%s?backup=true", a, b, c),
	})

	const n = 400
	hits := map[string]int{}
	for range n {
		ch := mgr.Chainer()
		route := ch.Route(context.Background(), "tcp", a)
		// Count the candidate the request actually starts on.
		hits[route.Nodes()[0].Addr]++
		conn, err := route.Dial(context.Background(), "tcp", a)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		conn.Close()
	}

	if hits[c] != 0 {
		t.Fatalf("backup candidate received %d requests while primaries healthy", hits[c])
	}
	if got := hits[a]; got < int(float64(n)*0.75*0.8) || got > int(float64(n)*0.75*1.2) {
		t.Fatalf("weight 3 share out of tolerance: %d/%d", got, n)
	}
	if got := hits[b]; got < int(float64(n)*0.25*0.8) || got > int(float64(n)*0.25*1.2) {
		t.Fatalf("weight 1 share out of tolerance: %d/%d", got, n)
	}
}

// TestIsolationAndRecovery: after maxFails consecutive failures the candidate
// is isolated and not selected; once the listener returns and the isolation
// window elapses, it auto-recovers without restart or manual intervention.
func TestIsolationAndRecovery(t *testing.T) {
	// Start with no listener: both addresses refused.
	addr1 := freeAddr(t)
	liveAddr, closeLive := startTCPListener(t, true)
	defer closeLive()

	mgr := buildManager(t, []string{
		// A short-interval TCP probe drives half-open recovery automatically:
		// once the listener returns, the next successful probe clears the
		// isolation without relying on live traffic reaching the candidate.
		fmt.Sprintf("tcp://%s?maxFails=2&failTimeout=400ms&probe=%s&probeInterval=100ms|tcp://%s", addr1, addr1, liveAddr),
	})

	// Drive failures against addr1 until it isolates.
	isolated := false
	for range 30 {
		_, _ = dialThrough(t, mgr, liveAddr)
		summary, _ := mgr.Summary()
		for _, seg := range summary.Segments {
			for _, c := range seg.Candidates {
				if c.Addr == addr1 && c.State == topology.StateIsolated {
					isolated = true
				}
			}
		}
		if isolated {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !isolated {
		t.Fatal("candidate never isolated after reaching maxFails")
	}

	// While isolated it must not be selected first.
	for range 10 {
		route := mgr.Chainer().Route(context.Background(), "tcp", liveAddr)
		if route.Nodes()[0].Addr == addr1 {
			t.Fatal("isolated candidate was selected")
		}
	}

	// Bring addr1 up: open a real echo listener on the same port.
	ln, err := net.Listen("tcp", addr1)
	if err != nil {
		t.Fatalf("re-listen on %s: %v", addr1, err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) { defer conn.Close(); _, _ = conn.Write([]byte("ok")) }(c)
		}
	}()
	defer ln.Close()

	// Wait for the isolation window to elapse and a successful recovery.
	deadline := time.Now().Add(5 * time.Second)
	recovered := false
	for time.Now().Before(deadline) {
		// Trigger real traffic so the half-open trial can succeed.
		if conn, err := dialThrough(t, mgr, liveAddr); err == nil {
			conn.Close()
		}
		summary, _ := mgr.Summary()
		for _, seg := range summary.Segments {
			for _, c := range seg.Candidates {
				if c.Addr == addr1 && c.State == topology.StateActive && c.Fails == 0 {
					recovered = true
				}
			}
		}
		if recovered {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !recovered {
		t.Fatal("candidate did not auto-recover after isolation window + listener restore")
	}
}

// TestReloadPreservesIsolation: re-applying an unchanged topology keeps the
// isolation state and failure counters; replacing the candidate set resets
// state (new set => new instances).
func TestReloadPreservesIsolation(t *testing.T) {
	deadAddr := freeAddr(t)
	liveAddr, closeLive := startTCPListener(t, true)
	defer closeLive()

	args := []string{fmt.Sprintf("tcp://%s?maxFails=1&failTimeout=10s|tcp://%s", deadAddr, liveAddr)}
	mgr := buildManager(t, args)

	// Trip the dead candidate into isolation.
	for range 10 {
		_, _ = dialThrough(t, mgr, liveAddr)
	}
	summary, _ := mgr.Summary()
	var before topology.CandidateStatus
	for _, seg := range summary.Segments {
		for _, c := range seg.Candidates {
			if c.Addr == deadAddr {
				before = c
			}
		}
	}
	if before.State != topology.StateIsolated {
		t.Fatalf("setup: expected isolated, got %s fails=%d", before.State, before.Fails)
	}

	// Reload with an identical declaration: state must be preserved.
	decl, err := topology.ParseChainDecl(args)
	if err != nil {
		t.Fatal(err)
	}
	nodeCfgs, err := topology.BuildNodeConfigs(decl)
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.Apply(decl, nodeCfgs, "cli"); err != nil {
		t.Fatalf("reload apply: %v", err)
	}
	summary2, _ := mgr.Summary()
	for _, seg := range summary2.Segments {
		for _, c := range seg.Candidates {
			if c.Addr == deadAddr {
				if c.State != topology.StateIsolated || c.Fails != before.Fails {
					t.Fatalf("state lost across reload: %s fails=%d (want %s fails=%d)",
						c.State, c.Fails, before.State, before.Fails)
				}
			}
		}
	}

	// Reload with a brand-new candidate set: state must reset.
	newDead := freeAddr(t)
	newArgs := []string{fmt.Sprintf("tcp://%s?maxFails=1&failTimeout=10s|tcp://%s", newDead, liveAddr)}
	decl2, err := topology.ParseChainDecl(newArgs)
	if err != nil {
		t.Fatal(err)
	}
	cfgs2, err := topology.BuildNodeConfigs(decl2)
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.Apply(decl2, cfgs2, "cli"); err != nil {
		t.Fatalf("reload apply new set: %v", err)
	}
	summary3, _ := mgr.Summary()
	for _, seg := range summary3.Segments {
		for _, c := range seg.Candidates {
			if c.Addr == newDead && c.State != topology.StateActive {
				t.Fatalf("new candidate set must start ACTIVE, got %s", c.State)
			}
		}
	}
}

// TestNoCandidateAvailableIsDistinguishable: with every candidate isolated
// and no half-open slot, the route returns ErrNoCandidateAvailable, not a
// low-level dial error, and does so promptly.
func TestNoCandidateAvailableIsDistinguishable(t *testing.T) {
	d1 := freeAddr(t)
	d2 := freeAddr(t)

	mgr := buildManager(t, []string{
		fmt.Sprintf("tcp://%s?maxFails=1&failTimeout=30s|tcp://%s?maxFails=1&failTimeout=30s", d1, d2),
	})

	// Exhaust both candidates into isolation.
	for range 20 {
		_, err := dialThrough(t, mgr, d1)
		if err != nil && errors.Is(err, topology.ErrNoCandidateAvailable) {
			break
		}
	}

	start := time.Now()
	_, err := dialThrough(t, mgr, d1)
	elapsed := time.Since(start)

	if !errors.Is(err, topology.ErrNoCandidateAvailable) {
		t.Fatalf("want ErrNoCandidateAvailable, got %v", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("no-candidate error took too long: %v", elapsed)
	}
}

// TestParallelSelectionIsRaceFree hammers selection and state transitions
// concurrently; run under -race to catch data races deterministically.
func TestParallelSelectionIsRaceFree(t *testing.T) {
	a, closeA := startTCPListener(t, true)
	defer closeA()
	b, closeB := startTCPListener(t, true)
	defer closeB()

	mgr := buildManager(t, []string{
		fmt.Sprintf("tcp://%s?weight=2|tcp://%s?weight=1", a, b),
	})

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				route := mgr.Chainer().Route(context.Background(), "tcp", a)
				_ = route.Nodes()
				_, _ = mgr.Summary()
				if conn, err := dialThrough(t, mgr, a); err == nil {
					conn.Close()
				}
			}
		}()
	}
	wg.Wait()
}

// TestExportRoundTrip: the topology summary is a single-source, credential
// free rendering; ensure no userinfo leaks and all candidates are represented.
func TestExportRoundTripAndCredentialMasking(t *testing.T) {
	live, closeLive := startTCPListener(t, true)
	defer closeLive()
	dead := freeAddr(t)

	mgr := buildManager(t, []string{
		fmt.Sprintf("tcp://user:secret@%s|tcp://admin:pw@%s?backup=true", live, dead),
	})
	summary, ok := mgr.Summary()
	if !ok {
		t.Fatal("no summary")
	}
	rendered := summary.RenderText()
	if strings.Contains(rendered, "secret") || strings.Contains(rendered, "pw") ||
		strings.Contains(rendered, "user@") || strings.Contains(rendered, "admin@") {
		t.Fatalf("credentials leaked into summary:\n%s", rendered)
	}
	if len(summary.Segments) != 1 || len(summary.Segments[0].Candidates) != 2 {
		t.Fatalf("summary shape wrong: %+v", summary.Segments)
	}

	// Re-loading the same declaration converges to the same candidate set.
	decl, _ := topology.ParseChainDecl([]string{
		fmt.Sprintf("tcp://user:secret@%s|tcp://admin:pw@%s?backup=true", live, dead),
	})
	cfgs, err := topology.BuildNodeConfigs(decl)
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.Apply(decl, cfgs, "cli"); err != nil {
		t.Fatalf("reload: %v", err)
	}
	again, _ := mgr.Summary()
	if len(again.Segments[0].Candidates) != 2 {
		t.Fatal("reloaded topology did not converge to the same candidate count")
	}
}

// TestInvalidInputs exercises every documented rejection path.
func TestInvalidInputs(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"zero weight", []string{"tcp://127.0.0.1:1?weight=0|tcp://127.0.0.1:2"}, "weight"},
		{"negative weight", []string{"tcp://127.0.0.1:1?weight=-1|tcp://127.0.0.1:2"}, "weight"},
		{"non-integer weight", []string{"tcp://127.0.0.1:1?weight=x|tcp://127.0.0.1:2"}, "weight"},
		{"oversize weight", []string{"tcp://127.0.0.1:1?weight=99999|tcp://127.0.0.1:2"}, "weight"},
		{"empty segment", []string{"|"}, "empty"},
		{"duplicate candidate", []string{"tcp://127.0.0.1:1|tcp://127.0.0.1:1"}, "duplicate"},
		{"comma mixing", []string{"tcp://127.0.0.1:1,127.0.0.1:2|tcp://127.0.0.1:3"}, "comma"},
		{"all backups", []string{"tcp://127.0.0.1:1?backup=true|tcp://127.0.0.1:2?backup=true"}, "primary"},
		{"bad backup bool", []string{"tcp://127.0.0.1:1|tcp://127.0.0.1:2?backup=maybe"}, "backup"},
		{"bad duration", []string{"tcp://127.0.0.1:1|tcp://127.0.0.1:2?failTimeout=soon"}, "failTimeout"},
		{"non-positive maxFails", []string{"tcp://127.0.0.1:1|tcp://127.0.0.1:2?maxFails=0"}, "maxFails"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := topology.ParseChainDecl(tc.args)
			if err == nil {
				t.Fatalf("expected error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.want)
			}
			// Error messages must never carry credentials.
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("credential in error: %v", err)
			}
		})
	}
}
