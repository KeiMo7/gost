package topology

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	corechain "github.com/go-gost/core/chain"
	corehop "github.com/go-gost/core/hop"
	"github.com/go-gost/core/logger"
)

// segment is one serial segment of a redundant chain. It implements
// corehop.Hop so it can sit inside a core chain, while additionally exposing
// an ordered failover plan for per-request candidate failover.
type segment struct {
	name string
	decl *SegmentDecl
	log  logger.Logger

	// fingerprint identifies the segment policy and candidate set so an
	// unchanged segment can be reused verbatim across reload.
	fingerprint string

	cands []*candidateRuntime

	// Deterministic scheduling state. Smooth weighted round-robin
	// (nginx algorithm) needs no randomness: parallel requests get a
	// deterministic, lock-serialized selection sequence whose long-run
	// distribution matches the configured weights exactly.
	schedMu   sync.Mutex
	current   []int // currentWeight per candidate index
	rrCounter uint64
}

func newSegment(name string, decl *SegmentDecl, cands []*candidateRuntime, log logger.Logger) *segment {
	return &segment{
		name:    name,
		decl:    decl,
		cands:   cands,
		current: make([]int, len(cands)),
		log:     log,
	}
}

func (s *segment) Nodes() []*corechain.Node {
	out := make([]*corechain.Node, 0, len(s.cands))
	for _, c := range s.cands {
		out = append(out, c.node)
	}
	return out
}

// Select implements corehop.Hop: it returns the single best available node,
// or nil when no candidate can currently take traffic.
func (s *segment) Select(ctx context.Context, opts ...corehop.SelectOption) *corechain.Node {
	plan := s.plan(time.Now())
	if len(plan) == 0 {
		return nil
	}
	return plan[0].node
}

// failoverPlan returns up to maxFailover candidates in failover order: the
// selected candidate first, then the remaining available candidates. It is
// the single decision point used by both Select() and the redundant route,
// so routing and introspection never diverge (single source of truth).
func (s *segment) failoverPlan(now time.Time) []*candidateRuntime {
	plan := s.plan(now)
	if len(plan) == 0 {
		return nil
	}
	if max := s.decl.MaxFailover; max > 0 && len(plan) > max {
		plan = plan[:max]
	}
	return plan
}

// plan builds the ordered candidate list for one request.
//
// Primary candidates are scheduled first according to the segment strategy;
// backup candidates are appended behind them and only receive traffic when no
// primary is available. Isolated candidates are excluded unless their
// isolation window has elapsed (in which case they enter the half-open
// PROBING state and become eligible as a trial).
func (s *segment) plan(now time.Time) []*candidateRuntime {
	var primaries, backups []*candidateRuntime
	for _, c := range s.cands {
		ok, _ := c.available(now)
		if !ok {
			continue
		}
		if c.decl.Backup {
			backups = append(backups, c)
		} else {
			primaries = append(primaries, c)
		}
	}

	pool := primaries
	if len(pool) == 0 {
		// All primaries down: backups take over. When primaries recover they
		// automatically win selection again on the next request.
		pool = backups
	}
	if len(pool) == 0 {
		return nil
	}

	ordered := s.order(pool)

	// Append any remaining candidates (the other tier) as later failover
	// options so a request can exhaust the whole segment before failing.
	extra := backups
	if len(primaries) == 0 {
		extra = nil
	} else {
		extra = s.order(extra)
	}
	return append(ordered, extra...)
}

// order applies the configured strategy to produce a deterministic ordering.
func (s *segment) order(pool []*candidateRuntime) []*candidateRuntime {
	if len(pool) <= 1 {
		return pool
	}
	switch s.decl.Strategy {
	case StrategyRound:
		return s.orderRoundRobin(pool)
	case StrategyFIFO:
		return pool
	default: // StrategyRandom, implemented deterministically via smooth WRR
		return s.orderSmoothWRR(pool)
	}
}

// orderRoundRobin rotates the pool by an atomic counter: no goroutine observes
// a torn sequence and equal-weight candidates share traffic evenly.
func (s *segment) orderRoundRobin(pool []*candidateRuntime) []*candidateRuntime {
	n := atomic.AddUint64(&s.rrCounter, 1) - 1
	start := int(n % uint64(len(pool)))
	out := make([]*candidateRuntime, 0, len(pool))
	for i := range pool {
		out = append(out, pool[(start+i)%len(pool)])
	}
	return out
}

// orderSmoothWRR produces a deterministic failover ordering using the nginx
// smooth weighted round-robin. The persistent current[] slots evolve across
// requests (one selection step per request, exactly as the standard
// algorithm); the remaining positions are filled by repeating the selection
// step on a local copy, so every candidate appears exactly once in the
// returned order. The long-run head-selection ratio equals
// weight[i]/sum(weight) with no randomness and no torn state under schedMu.
func (s *segment) orderSmoothWRR(pool []*candidateRuntime) []*candidateRuntime {
	s.schedMu.Lock()
	defer s.schedMu.Unlock()

	idx := make([]int, len(pool))
	total := 0
	for i, c := range pool {
		idx[i] = c.decl.Index
		total += effectiveWeight(c.decl)
	}

	// First pick: one canonical WRR step against the persistent state.
	first := s.smoothWRRStep(pool, idx, total)

	out := make([]*candidateRuntime, 0, len(pool))
	out = append(out, pool[first])

	// Remaining picks run on a local copy of the persistent state so the
	// cross-request invariant (one step per request) is preserved.
	local := append([]int(nil), s.current...)
	used := map[int]bool{first: true}
	for len(out) < len(pool) {
		pick := smoothWRRStepOn(local, pool, idx, total, used)
		used[pick] = true
		out = append(out, pool[pick])
	}
	return out
}

// smoothWRRStep performs one nginx WRR step on the persistent current slots
// and returns the chosen pool index.
func (s *segment) smoothWRRStep(pool []*candidateRuntime, idx []int, total int) int {
	best := -1
	bestWeight := 0
	for i, c := range pool {
		s.current[idx[i]] += effectiveWeight(c.decl)
		if best == -1 || s.current[idx[i]] > bestWeight {
			best = i
			bestWeight = s.current[idx[i]]
		}
	}
	s.current[idx[best]] -= total
	return best
}

// smoothWRRStepOn is the same step operating on a caller-provided state
// snapshot, skipping already-used indices to build a complete ordering.
func smoothWRRStepOn(state []int, pool []*candidateRuntime, idx []int, total int, used map[int]bool) int {
	best := -1
	bestWeight := 0
	for i, c := range pool {
		if used[i] {
			continue
		}
		state[idx[i]] += effectiveWeight(c.decl)
		if best == -1 || state[idx[i]] > bestWeight {
			best = i
			bestWeight = state[idx[i]]
		}
	}
	state[idx[best]] -= total
	return best
}

// close releases every candidate node (transport connections) and stops
// active-probe goroutines when a segment is removed during reload.
func (s *segment) close() {
	for _, c := range s.cands {
		c.stopProbe()
		if c.node != nil {
			c.node.Close()
		}
	}
}

// statuses reads a consistent summary of every candidate.
func (s *segment) statuses() []candidateStatus {
	out := make([]candidateStatus, 0, len(s.cands))
	for _, c := range s.cands {
		out = append(out, c.snapshot())
	}
	return out
}
