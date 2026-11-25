package topology

import (
	"strings"
	"testing"
	"time"
)

func TestParseChainDecl_LegacyNotGrouped(t *testing.T) {
	d, err := ParseChainDecl([]string{"socks5://127.0.0.1:1080"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Grouped {
		t.Fatal("single URL must not be grouped")
	}
	if len(d.Segments) != 0 {
		t.Fatalf("legacy decl should have no parsed segments, got %d", len(d.Segments))
	}
}

func TestParseChainDecl_GroupedSerialAndParallel(t *testing.T) {
	d, err := ParseChainDecl([]string{
		"socks5://127.0.0.1:1080|socks5://127.0.0.1:1081",
		"socks5://127.0.0.1:2080",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !d.Grouped {
		t.Fatal("expected grouped")
	}
	if len(d.Segments) != 2 {
		t.Fatalf("want 2 serial segments, got %d", len(d.Segments))
	}
	if len(d.Segments[0].Members) != 2 || len(d.Segments[1].Members) != 1 {
		t.Fatalf("parallel/serial shapes wrong: %+v", d.Segments)
	}
}

func TestParseChainDecl_WeightsAndBackup(t *testing.T) {
	d, err := ParseChainDecl([]string{
		"http://127.0.0.1:1?weight=3|http://127.0.0.1:2?weight=1|http://127.0.0.1:3?backup=true",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	seg := d.Segments[0]
	if seg.Members[0].Weight != 3 || seg.Members[1].Weight != 1 {
		t.Fatalf("weights not parsed: %+v", seg.Members)
	}
	if !seg.Members[2].Backup {
		t.Fatal("third candidate should be backup")
	}
	// Reserved keys must be stripped from the downstream URL.
	for _, c := range seg.Members {
		if strings.Contains(c.RawURL, "weight") || strings.Contains(c.RawURL, "backup") {
			t.Fatalf("reserved key leaked into RawURL: %s", c.RawURL)
		}
	}
	// Credentials must not survive into identity/RawURL.
	d2, err := ParseChainDecl([]string{"http://user:secret@127.0.0.1:1|http://127.0.0.1:2"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(d2.Segments[0].Members[0].RawURL, "secret") {
		t.Fatal("credential leaked into RawURL")
	}
	if d2.Segments[0].Members[0].Addr != "127.0.0.1:1" {
		t.Fatalf("addr mismatch: %s", d2.Segments[0].Members[0].Addr)
	}
}

func TestParseChainDecl_InvalidWeights(t *testing.T) {
	cases := map[string]string{
		"zero":        "http://127.0.0.1:1?weight=0|http://127.0.0.1:2",
		"negative":    "http://127.0.0.1:1?weight=-2|http://127.0.0.1:2",
		"non-integer": "http://127.0.0.1:1?weight=abc|http://127.0.0.1:2",
		"too-large":   "http://127.0.0.1:1?weight=10001|http://127.0.0.1:2",
	}
	for name, arg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseChainDecl([]string{arg}); err == nil {
				t.Fatalf("expected rejection for %s", name)
			}
		})
	}
}

func TestParseChainDecl_EmptySegmentAndDuplicates(t *testing.T) {
	if _, err := ParseChainDecl([]string{"|"}); err == nil {
		t.Fatal("empty segment must be rejected")
	}
	if _, err := ParseChainDecl([]string{"http://127.0.0.1:1|http://127.0.0.1:1"}); err == nil {
		t.Fatal("duplicate candidate must be rejected")
	}
}

func TestParseChainDecl_CommaInsideGroupRejected(t *testing.T) {
	_, err := ParseChainDecl([]string{"http://127.0.0.1:1,127.0.0.1:2|http://127.0.0.1:3"})
	if err == nil || !strings.Contains(err.Error(), "comma") {
		t.Fatalf("expected comma-mixing error, got %v", err)
	}
}

func TestParseChainDecl_SegmentKeyOnNonFirstCandidate(t *testing.T) {
	_, err := ParseChainDecl([]string{
		"http://127.0.0.1:1|http://127.0.0.1:2?strategy=round",
	})
	if err == nil || !strings.Contains(err.Error(), "segment-level") {
		t.Fatalf("expected segment-level key error, got %v", err)
	}
}

func TestParseChainDecl_AllBackupsRejected(t *testing.T) {
	_, err := ParseChainDecl([]string{
		"http://127.0.0.1:1?backup=true|http://127.0.0.1:2?backup=true",
	})
	if err == nil || !strings.Contains(err.Error(), "primary") {
		t.Fatalf("expected primary-required error, got %v", err)
	}
}

func TestParseChainDecl_MaxFailoverExceedsCandidates(t *testing.T) {
	_, err := ParseChainDecl([]string{
		"http://127.0.0.1:1?maxFailover=5|http://127.0.0.1:2",
	})
	if err == nil {
		t.Fatal("maxFailover above candidate count must be rejected")
	}
}

func TestParseChainDecl_Defaults(t *testing.T) {
	d, err := ParseChainDecl([]string{"http://127.0.0.1:1|http://127.0.0.1:2"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	seg := d.Segments[0]
	if seg.Strategy != StrategyRandom {
		t.Fatalf("default strategy = %q", seg.Strategy)
	}
	if seg.MaxFails != DefaultMaxFails || seg.FailTimeout != DefaultFailTimeout {
		t.Fatal("default fail policy not applied")
	}
	if seg.MaxFailover != 2 || seg.FailoverTimeout != DefaultFailoverTimeout {
		t.Fatal("default failover policy not applied")
	}
}

func TestBuildChainConfig_Restartable(t *testing.T) {
	args := []string{
		"http://127.0.0.1:1?weight=2|http://127.0.0.1:2?backup=true",
		"http://127.0.0.1:3",
	}
	decl, err := ParseChainDecl(args)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	nodeCfgs, err := BuildNodeConfigs(decl)
	if err != nil {
		t.Fatalf("build nodes: %v", err)
	}
	chainCfg := BuildChainConfig(decl, nodeCfgs)
	if chainCfg == nil || chainCfg.Name != "chain-0" {
		t.Fatalf("unexpected chain config: %+v", chainCfg)
	}
	if len(chainCfg.Hops) != 2 {
		t.Fatalf("want 2 hops (serial segments), got %d", len(chainCfg.Hops))
	}
	if got := len(chainCfg.Hops[0].Nodes); got != 2 {
		t.Fatalf("segment 0 want 2 candidate nodes, got %d", got)
	}
	if len(chainCfg.Hops[1].Nodes) != 1 {
		t.Fatalf("segment 1 want 1 node, got %d", len(chainCfg.Hops[1].Nodes))
	}
	// Role/weight must round-trip through optional metadata only.
	roles := map[string]string{}
	for _, n := range chainCfg.Hops[0].Nodes {
		roles[n.Addr] = n.Metadata["gost.topology.role"].(string)
	}
	if roles["127.0.0.1:1"] != "primary" || roles["127.0.0.1:2"] != "backup" {
		t.Fatalf("role metadata round-trip wrong: %v", roles)
	}
}

func TestCandidateStateMachine_IsolationAndRecovery(t *testing.T) {
	decl := &CandidateDecl{Addr: "127.0.0.1:1"}
	rt := newCandidateRuntime(decl, nil, 2, 50*time.Millisecond, nil)

	now := time.Now()
	if ok, _ := rt.available(now); !ok {
		t.Fatal("fresh candidate must be available")
	}
	rt.recordFailure(now)
	if rt.state != StateActive {
		t.Fatalf("one failure must keep ACTIVE, got %s", rt.state)
	}
	rt.recordFailure(now)
	if rt.state != StateIsolated {
		t.Fatalf("threshold reached must isolate, got %s", rt.state)
	}
	if ok, _ := rt.available(now); ok {
		t.Fatal("isolated candidate must not be available within window")
	}

	// Window elapses -> half-open trial.
	later := now.Add(60 * time.Millisecond)
	ok, st := rt.available(later)
	if !ok || st != StateProbing {
		t.Fatalf("expected PROBING trial, got ok=%v state=%s", ok, st)
	}
	// Trial failure re-arms isolation with a fresh window.
	rt.recordFailure(later)
	if rt.state != StateIsolated {
		t.Fatalf("failed trial must re-isolate, got %s", rt.state)
	}
	// Next window -> trial succeeds -> ACTIVE with counters cleared.
	recoverAt := later.Add(60 * time.Millisecond)
	if ok, _ := rt.available(recoverAt); !ok {
		t.Fatal("expected second half-open trial")
	}
	rt.recordSuccess(recoverAt)
	if rt.state != StateActive || rt.fails != 0 {
		t.Fatalf("successful trial must recover with cleared counters, got state=%s fails=%d", rt.state, rt.fails)
	}
}

func TestSmoothWRR_WeightedDistribution(t *testing.T) {
	decl := &ChainDecl{
		Grouped: true,
		Segments: []*SegmentDecl{{
			Index: 0, Strategy: StrategyRandom, MaxFailover: 3,
			Members: []*CandidateDecl{
				{Index: 0, Addr: "a", Weight: 3},
				{Index: 1, Addr: "b", Weight: 1},
			},
		}},
	}
	seg := &segment{
		name:    "segment-0",
		decl:    decl.Segments[0],
		current: make([]int, 2),
		cands: []*candidateRuntime{
			newCandidateRuntime(decl.Segments[0].Members[0], nil, 1, time.Minute, nil),
			newCandidateRuntime(decl.Segments[0].Members[1], nil, 1, time.Minute, nil),
		},
	}
	seg.decl.MaxFailover = 2

	// Count the head selection across requests: smooth WRR's long-run head
	// ratio must match the configured weights.
	counts := map[string]int{}
	for range 400 {
		plan := seg.failoverPlan(time.Now())
		counts[plan[0].decl.Addr]++
	}
	// 3:1 ratio -> expect roughly 300/100 with tolerance.
	if got := counts["a"]; got < 270 || got > 330 {
		t.Fatalf("weighted share out of tolerance: a=%d (want ~300)", got)
	}
	if got := counts["b"]; got < 70 || got > 130 {
		t.Fatalf("weighted share out of tolerance: b=%d (want ~100)", got)
	}

	// The full failover ordering must contain both candidates exactly once.
	plan := seg.failoverPlan(time.Now())
	if len(plan) != 2 {
		t.Fatalf("failover plan length = %d, want 2", len(plan))
	}
	seen := map[string]bool{}
	for _, c := range plan {
		seen[c.decl.Addr] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("failover plan missing candidates: %v", seen)
	}
}

func TestPlan_BackupOnlyWhenPrimariesDown(t *testing.T) {
	segDecl := &SegmentDecl{
		Index: 0, Strategy: StrategyFIFO, MaxFailover: 2, MaxFails: 1,
		FailTimeout: time.Minute,
		Members: []*CandidateDecl{
			{Index: 0, Addr: "primary"},
			{Index: 1, Addr: "bk", Backup: true},
		},
	}
	primary := newCandidateRuntime(segDecl.Members[0], nil, 1, time.Minute, nil)
	backup := newCandidateRuntime(segDecl.Members[1], nil, 1, time.Minute, nil)
	seg := &segment{name: "s", decl: segDecl, current: make([]int, 2), cands: []*candidateRuntime{primary, backup}}

	now := time.Now()
	if plan := seg.plan(now); plan[0].decl.Addr != "primary" {
		t.Fatal("primary must be selected while healthy")
	}
	primary.recordFailure(now) // isolate primary (maxFails=1)
	plan := seg.plan(now)
	if plan[0].decl.Addr != "bk" {
		t.Fatal("backup must take over once primary is isolated")
	}
}
