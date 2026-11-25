package topology

import (
	"context"
	"sync"
	"time"

	corechain "github.com/go-gost/core/chain"
	"github.com/go-gost/core/logger"
)

// CandidateState is the lifecycle state of a redundant-chain candidate.
type CandidateState string

const (
	// StateActive: eligible for selection.
	StateActive CandidateState = "ACTIVE"
	// StateIsolated: temporarily excluded after maxFails consecutive failures.
	StateIsolated CandidateState = "ISOLATED"
	// StateProbing: half-open after the isolation window elapsed; exactly one
	// trial connection/probe is allowed before the state resolves.
	StateProbing CandidateState = "PROBING"
)

// candidateRuntime is the mutable health state of one candidate plus the
// immutable chain.Node used to carry traffic. All state transitions happen
// under mu so parallel requests observe a deterministic state and the race
// detector stays quiet.
type candidateRuntime struct {
	decl *CandidateDecl
	node *corechain.Node

	// fingerprint is the candidate identity used for state reuse on reload.
	fingerprint string

	// Effective policy (segment defaults overlaid by candidate overrides).
	maxFails    int
	failTimeout time.Duration

	mu         sync.Mutex
	state      CandidateState
	fails      int64
	isolatedAt time.Time
	// probing is set while a half-open trial is in flight so that only one
	// request probes at a time.
	probing bool
	log     logger.Logger

	// probeCancel stops the optional active-probe goroutine. Empty ProbeAddr
	// means no goroutine is started; recovery then relies on traffic trials.
	probeCancel context.CancelFunc
}

func newCandidateRuntime(decl *CandidateDecl, node *corechain.Node, segMaxFails int, segFailTimeout time.Duration, log logger.Logger) *candidateRuntime {
	c := &candidateRuntime{
		decl:        decl,
		node:        node,
		maxFails:    segMaxFails,
		failTimeout: segFailTimeout,
		state:       StateActive,
		log:         log,
	}
	if decl.MaxFails > 0 {
		c.maxFails = decl.MaxFails
	}
	if decl.FailTimeout > 0 {
		c.failTimeout = decl.FailTimeout
	}
	return c
}

// available reports whether the candidate may receive traffic right now,
// advancing the state machine from ISOLATED to PROBING when the isolation
// window has elapsed. Callers that receive true own the trial slot when the
// returned state is PROBING.
func (c *candidateRuntime) available(now time.Time) (bool, CandidateState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.state {
	case StateActive:
		return true, StateActive
	case StateIsolated:
		if now.Sub(c.isolatedAt) >= c.failTimeout {
			c.state = StateProbing
			c.probing = true
			c.transitionLocked(StateProbing, now)
			return true, StateProbing
		}
		return false, StateIsolated
	case StateProbing:
		// Another request owns the single half-open trial.
		if c.probing {
			return false, StateProbing
		}
		c.probing = true
		return true, StateProbing
	default:
		return false, c.state
	}
}

// recordSuccess marks a successful dial/handshake or probe. A half-open trial
// that succeeds closes the circuit and clears the failure count.
func (c *candidateRuntime) recordSuccess(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	prev := c.state
	c.fails = 0
	c.probing = false
	c.state = StateActive
	if prev != StateActive {
		c.transitionLocked(StateActive, now)
	}
}

// recordFailure accounts for one failed dial/handshake/probe. It ends a
// half-open trial (re-timing the isolation window) or increments the
// consecutive failure count, isolating the candidate once the threshold is
// reached. A single failure never isolates permanently: the window always
// elapses and re-opens the circuit.
func (c *candidateRuntime) recordFailure(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.state {
	case StateProbing:
		// Trial failed: re-arm isolation with a fresh window.
		c.probing = false
		c.state = StateIsolated
		c.isolatedAt = now
		c.fails = int64(c.maxFails)
		c.transitionLocked(StateIsolated, now)
		return
	case StateIsolated:
		// Defensive: keep the window fresh; no log spam (no transition).
		c.isolatedAt = now
		return
	}

	c.fails++
	if int(c.fails) >= c.maxFails {
		c.state = StateIsolated
		c.isolatedAt = now
		c.transitionLocked(StateIsolated, now)
	}
}

// swapNode replaces the underlying node (fresh transport and probe
// goroutine) while retaining the accumulated health state. The replaced
// node's resources are released by the old snapshot's segment close.
func (c *candidateRuntime) swapNode(node *corechain.Node) {
	c.node = node
}

// snapshot reads the state and counters under lock for the topology summary.
func (c *candidateRuntime) snapshot() candidateStatus {
	c.mu.Lock()
	defer c.mu.Unlock()

	st := candidateStatus{
		Name:          c.decl.Addr,
		Addr:          c.decl.Addr,
		Role:          roleName(c.decl.Backup),
		Weight:        effectiveWeight(c.decl),
		State:         c.state,
		Fails:         c.fails,
		Threshold:     c.maxFails,
		IsolateFor:    c.failTimeout,
		IsolatedAt:    c.isolatedAt,
		IsolatedUntil: time.Time{},
	}
	if c.state == StateIsolated || c.state == StateProbing {
		st.IsolatedUntil = c.isolatedAt.Add(c.failTimeout)
	}
	return st
}

func roleName(backup bool) string {
	if backup {
		return "backup"
	}
	return "primary"
}

// effectiveWeight returns the scheduling weight: unset means equal share (1).
func effectiveWeight(d *CandidateDecl) int {
	if d.Weight > 0 {
		return d.Weight
	}
	return 1
}

// transitionLocked emits exactly one log line per state change. Caller holds
// mu. Credentials never appear here: only the credential-free Addr is logged.
func (c *candidateRuntime) transitionLocked(to CandidateState, now time.Time) {
	if c.log == nil {
		return
	}
	c.log.Infof("candidate %s state -> %s (fails=%d threshold=%d isolateFor=%s)",
		c.decl.Addr, to, c.fails, c.maxFails, c.failTimeout)
	_ = now
}

// startProbe launches an optional active-probe loop that dials+handshakes the
// candidate on a fixed cadence and feeds outcomes into the same state machine
// as real traffic. A successful probe clears isolation (the recovery
// signature required by the design); a failed probe counts as a failure.
//
// The loop is the only recovery path that works while traffic keeps preferring
// healthy primaries (a half-open candidate otherwise receives no trial).
func (c *candidateRuntime) startProbe() {
	if c.decl.ProbeAddr == "" {
		return
	}
	interval := c.decl.ProbeInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	timeout := c.decl.ProbeTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.probeCancel = cancel

	go func() {
		// First probe immediately, then on the interval.
		c.runProbe(ctx, timeout)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.runProbe(ctx, timeout)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// runProbe performs one dial+handshake against the candidate and records the
// outcome into the state machine. It never blocks beyond timeout.
func (c *candidateRuntime) runProbe(ctx context.Context, timeout time.Duration) {
	if c.node == nil {
		return
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	tr := c.node.Options().Transport
	conn, err := tr.Dial(probeCtx, c.node.Addr)
	if err == nil {
		hc, herr := tr.Handshake(probeCtx, conn)
		if herr != nil {
			conn.Close()
			c.recordFailure(time.Now())
			return
		}
		conn = hc
		conn.Close()
		c.recordSuccess(time.Now())
		return
	}
	c.recordFailure(time.Now())
}

// stopProbe cancels the active-probe goroutine if one is running.
func (c *candidateRuntime) stopProbe() {
	if c.probeCancel != nil {
		c.probeCancel()
		c.probeCancel = nil
	}
}
