package topology

import (
	"fmt"
	"sync"
	"time"

	corechain "github.com/go-gost/core/chain"
	"github.com/go-gost/core/logger"
	xconfig "github.com/go-gost/x/config"
)

// NodeBuilder turns a parsed candidate node config into a live
// *corechain.Node. The host wires it using the standard x parsing stack, so
// the topology package stays free of config-building details.
type NodeBuilder func(hopName string, cfg *xconfig.NodeConfig, log logger.Logger) (*corechain.Node, error)

// Manager owns the current immutable snapshot and serializes reloads. It is
// a process-level singleton for the one CLI-managed redundant chain.
type Manager struct {
	mu       sync.RWMutex
	current  *snapshot
	builder  NodeBuilder
	log      logger.Logger
	chainKey string
}

// NewManager creates a Manager.
func NewManager(builder NodeBuilder, chainKey string, log logger.Logger) *Manager {
	return &Manager{
		builder:  builder,
		log:      log,
		chainKey: chainKey,
	}
}

// ChainKey returns the registry name under which the managed chain is
// published.
func (m *Manager) ChainKey() string { return m.chainKey }

// Current returns the active snapshot (nil before the first Apply).
func (m *Manager) Current() *snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current
}

// Active reports whether a redundant topology is currently installed.
func (m *Manager) Active() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current != nil
}

// Chainer returns the live chain for registry registration, or nil.
func (m *Manager) Chainer() corechain.Chainer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.current == nil {
		return nil
	}
	return m.current.chain
}

// Summary returns the current topology summary.
func (m *Manager) Summary() (TopologySummary, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.current == nil {
		return TopologySummary{}, false
	}
	return m.current.Summary(), true
}

// candidateFingerprint is the identity used to decide whether a candidate can
// reuse a previous runtime (preserving isolation/counters/probe state). It
// covers everything that affects the candidate's connection behavior.
func candidateFingerprint(d *CandidateDecl, cfg *xconfig.NodeConfig) string {
	connType, dialType := "", ""
	if cfg.Connector != nil {
		connType = cfg.Connector.Type
	}
	if cfg.Dialer != nil {
		dialType = cfg.Dialer.Type
	}
	return fmt.Sprintf("%s|%s|%t|%d|%d|%s|%s|%s|%s|%s",
		d.Scheme, d.Addr, d.Backup, effectiveWeight(d), d.MaxFails, d.FailTimeout,
		d.ProbeAddr, d.ProbeInterval, d.ProbeTimeout,
		connType+"|"+dialType)
}

// segmentFingerprint identifies a segment's candidate set and policy so an
// unchanged segment reuses its runtime verbatim across reload.
func segmentFingerprint(seg *SegmentDecl, nodeCfgs []*xconfig.NodeConfig) string {
	out := fmt.Sprintf("strategy=%s maxFails=%d failTimeout=%s maxFailover=%d failoverTimeout=%s;",
		seg.Strategy, seg.MaxFails, seg.FailTimeout, seg.MaxFailover, seg.FailoverTimeout)
	for i, c := range seg.Members {
		var ncfg *xconfig.NodeConfig
		if i < len(nodeCfgs) {
			ncfg = nodeCfgs[i]
		}
		out += candidateFingerprint(c, ncfg) + ";"
	}
	return out
}

// Apply builds a new snapshot from the declaration and candidate node
// configs, reusing unchanged segment runtimes (state preservation), then
// atomically swaps it in. On any build error the previous snapshot remains
// fully in service and nothing is closed (atomic reload).
//
// nodeCfgs must align 1:1 with decl segments; each inner slice aligns with
// that segment's candidates. The host produces them via the x cmd builder.
func (m *Manager) Apply(decl *ChainDecl, nodeCfgs [][]*xconfig.NodeConfig, source string) error {
	if decl == nil || !decl.Grouped {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	prev := m.current
	prevByKey := make(map[string]*candidateRuntime)
	prevSegPrint := make(map[string]string)
	if prev != nil {
		for _, seg := range prev.segments {
			prevSegPrint[seg.name] = seg.fingerprint
			for _, c := range seg.cands {
				prevByKey[seg.name+"/"+c.decl.Addr] = c
			}
		}
	}

	var (
		newSegs []*segment
		toClose []*segment
	)

	for _, segDecl := range decl.Segments {
		segName := fmt.Sprintf("segment-%d", segDecl.Index)
		segNodeCfgs := nodeCfgs[segDecl.Index]
		if len(segNodeCfgs) != len(segDecl.Members) {
			return fmt.Errorf("segment %d: candidate/node config count mismatch (%d vs %d)",
				segDecl.Index, len(segDecl.Members), len(segNodeCfgs))
		}

		print := segmentFingerprint(segDecl, segNodeCfgs)

		// Whole-segment reuse when policy and candidate set are unchanged.
		if prev != nil && prevSegPrint[segName] == print {
			if old := findSegment(prev, segName); old != nil {
				newSegs = append(newSegs, old)
				continue
			}
		}

		cands := make([]*candidateRuntime, 0, len(segDecl.Members))
		for ci, candDecl := range segDecl.Members {
			nodeCfg := segNodeCfgs[ci]
			node, err := m.builder(segName, nodeCfg, m.log)
			if err != nil {
				return fmt.Errorf("segment %d candidate %d: build node: %w", segDecl.Index, ci, err)
			}

			// Per-candidate state reuse: same identity across reload keeps the
			// isolation/counter/probe state even if a sibling changed. The new
			// node (fresh transport) replaces the old one; the probe loop keeps
			// running against the current node via swapNode.
			if reused := prevByKey[segName+"/"+candDecl.Addr]; reused != nil &&
				reused.fingerprint == candidateFingerprint(candDecl, nodeCfg) {
				reused.swapNode(node)
				cands = append(cands, reused)
			} else {
				rt := newCandidateRuntime(candDecl, node, segDecl.MaxFails, segDecl.FailTimeout, m.log)
				rt.fingerprint = candidateFingerprint(candDecl, nodeCfg)
				rt.startProbe()
				cands = append(cands, rt)
			}
		}

		seg := newSegment(segName, segDecl, cands, m.log)
		seg.fingerprint = print
		newSegs = append(newSegs, seg)
	}

	// Segments removed by the new topology are closed after the swap.
	if prev != nil {
		newNames := make(map[string]bool, len(newSegs))
		for _, s := range newSegs {
			newNames[s.name] = true
		}
		for _, s := range prev.segments {
			if !newNames[s.name] {
				toClose = append(toClose, s)
			}
		}
	}

	failoverTimeout := DefaultFailoverTimeout
	for _, s := range decl.Segments {
		if s.FailoverTimeout > failoverTimeout {
			failoverTimeout = s.FailoverTimeout
		}
	}

	chain := newRedundantChain(m.chainKey, newSegs, failoverTimeout, m.log)
	next := &snapshot{
		chainName: m.chainKey,
		source:    source,
		chain:     chain,
		segments:  newSegs,
		decl:      decl,
		builtAt:   time.Now(),
	}

	// Atomic swap first; only then release resources of removed segments.
	m.current = next
	for _, c := range toClose {
		c.close()
	}
	return nil
}

// Close shuts down the active topology and releases every candidate.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current != nil {
		m.current.chain.Close()
		m.current = nil
	}
	return nil
}

func findSegment(snap *snapshot, name string) *segment {
	for _, s := range snap.segments {
		if s.name == name {
			return s
		}
	}
	return nil
}
