package topology

import (
	"fmt"

	"github.com/go-gost/core/chain"
	"github.com/go-gost/core/logger"
	xconfig "github.com/go-gost/x/config"
	"github.com/go-gost/x/config/cmd"
	node_parser "github.com/go-gost/x/config/parsing/node"
)

// BuildNodeConfigs runs each candidate URL through the standard x cmd config
// builder and returns the per-segment NodeConfig list. Reusing the x builder
// preserves every existing per-node capability (connector/dialer selection,
// auth, TLS, resolver, hosts, bypass, interface, so_mark, ...) for redundant
// candidates, including the single-candidate serial-chain behaviors.
//
// The returned slices align 1:1 with decl.Segments and each segment's
// Candidates. Topology-reserved query keys were already stripped during
// ParseChainDecl, so they never reach the connector/dialer metadata.
func BuildNodeConfigs(decl *ChainDecl) ([][]*xconfig.NodeConfig, error) {
	if decl == nil || !decl.Grouped {
		return nil, nil
	}

	out := make([][]*xconfig.NodeConfig, len(decl.Segments))

	for si, seg := range decl.Segments {
		// One -F-equivalent per candidate: build a single-node chain config and
		// harvest its hop's sole node. This keeps parsing identical to the
		// legacy single-candidate path.
		nodeCfgs := make([]*xconfig.NodeConfig, 0, len(seg.Members))
		for ci, cand := range seg.Members {
			cfg, err := cmd.BuildConfigFromCmd(nil, []string{cand.RawURL})
			if err != nil {
				return nil, fmt.Errorf("segment %d candidate %d: %w", seg.Index, ci, err)
			}
			if len(cfg.Chains) != 1 || len(cfg.Chains[0].Hops) != 1 ||
				len(cfg.Chains[0].Hops[0].Nodes) != 1 {
				return nil, fmt.Errorf("segment %d candidate %d: unexpected cmd build result", seg.Index, ci)
			}
			nodeCfg := cfg.Chains[0].Hops[0].Nodes[0]
			nodeCfg.Name = fmt.Sprintf("node-%d", ci)

			applyProbe(nodeCfg, cand)
			nodeCfgs = append(nodeCfgs, nodeCfg)
		}
		out[si] = nodeCfgs
	}

	return out, nil
}

// applyProbe translates a candidate probe declaration into the standard node
// probe config so the existing x node probe goroutine drives half-open
// recovery. An absent probe leaves the config untouched (recovery then relies
// on real traffic trials via the state machine).
func applyProbe(nodeCfg *xconfig.NodeConfig, cand *CandidateDecl) {
	if cand.ProbeAddr == "" {
		return
	}
	if nodeCfg.Probe == nil {
		nodeCfg.Probe = &xconfig.ProbeConfig{
			Type: "tcp",
			Addr: cand.ProbeAddr,
		}
	}
	if cand.ProbeInterval > 0 {
		nodeCfg.Probe.Interval = cand.ProbeInterval
	}
	if cand.ProbeTimeout > 0 {
		nodeCfg.Probe.Timeout = cand.ProbeTimeout
	}
}

// BuildChainConfig renders the declaration as a standard x ChainConfig with
// one hop per serial segment and one node per candidate. The result is
// fully restartable: loading it through the standard parser reproduces the
// same candidate set, roles and weights. Topology-specific policy is carried
// in optional node/hop metadata under the "gost.topology.*" keys, so existing
// field names and structures remain unchanged.
func BuildChainConfig(decl *ChainDecl, nodeCfgs [][]*xconfig.NodeConfig) *xconfig.ChainConfig {
	if decl == nil || !decl.Grouped {
		return nil
	}
	chain := &xconfig.ChainConfig{
		Name: "chain-0",
		Metadata: map[string]any{
			"gost.topology.managed": true,
		},
	}
	for si, seg := range decl.Segments {
		hop := &xconfig.HopConfig{
			Name: fmt.Sprintf("hop-%d", si),
			Metadata: map[string]any{
				"gost.topology.strategy":        seg.Strategy,
				"gost.topology.maxFails":        seg.MaxFails,
				"gost.topology.failTimeout":     seg.FailTimeout.String(),
				"gost.topology.maxFailover":     seg.MaxFailover,
				"gost.topology.failoverTimeout": seg.FailoverTimeout.String(),
			},
		}
		for ci, cand := range seg.Members {
			var nodeCfg *xconfig.NodeConfig
			if si < len(nodeCfgs) && ci < len(nodeCfgs[si]) {
				nodeCfg = nodeCfgs[si][ci]
			}
			if nodeCfg == nil {
				continue
			}
			if nodeCfg.Metadata == nil {
				nodeCfg.Metadata = map[string]any{}
			}
			nodeCfg.Metadata["gost.topology.role"] = roleName(cand.Backup)
			nodeCfg.Metadata["gost.topology.weight"] = effectiveWeight(cand)
			if cand.MaxFails > 0 {
				nodeCfg.Metadata["gost.topology.maxFails"] = cand.MaxFails
			}
			if cand.FailTimeout > 0 {
				nodeCfg.Metadata["gost.topology.failTimeout"] = cand.FailTimeout.String()
			}
			hop.Nodes = append(hop.Nodes, nodeCfg)
		}
		chain.Hops = append(chain.Hops, hop)
	}
	return chain
}

// DefaultNodeBuilder is the production NodeBuilder: it resolves references and
// parses a NodeConfig into a live node through the x stack. Active probing is
// deliberately NOT started here; the topology candidateRuntime owns the probe
// loop so probe outcomes drive the same isolation/recovery state machine as
// real traffic (single source of health truth).
func DefaultNodeBuilder(hopName string, cfg *xconfig.NodeConfig, log logger.Logger) (*chain.Node, error) {
	resolved, err := node_parser.ResolveConfig(cfg)
	if err != nil {
		return nil, err
	}
	return node_parser.ParseNode(hopName, resolved, log)
}
