package main

import (
	"fmt"

	"github.com/go-gost/core/logger"
	xconfig "github.com/go-gost/x/config"
	"github.com/go-gost/x/registry"

	"github.com/go-gost/gost/internal/topology"
)

// managedChainName is the fixed name of the CLI-built chain, matching the x
// cmd builder's default ("chain-0").
const managedChainName = "chain-0"

// managedChainMarker marks the placeholder ChainConfig injected so the x
// service parser binds CLI services to chain-0. The loader skips the
// placeholder; the topology Manager owns the real registry entry.
const managedChainMarker = "gost.topology.managed"

// topologyManager is the process-wide redundant-chain manager. It is only
// active when the new grouped -F syntax is used.
var topologyManager *topology.Manager

// prepareGroupedChain validates the grouped -F declarations and, when grouped
// syntax is present, injects a placeholder ChainConfig so the x service parser
// binds CLI services to chain-0. It returns whether grouped syntax is active.
func prepareGroupedChain(cfg *xconfig.Config, nodeArgs []string) (bool, error) {
	decl, err := topology.ParseChainDecl(nodeArgs)
	if err != nil {
		return false, fmt.Errorf("chain declaration: %w", err)
	}
	if !decl.Grouped {
		return false, nil
	}

	for _, ch := range cfg.Chains {
		if ch != nil && ch.Name == managedChainName {
			return false, fmt.Errorf("chain name conflict: CLI chain %q already declared in configuration; rename one of them", managedChainName)
		}
	}

	// Build the full node configs up front so the exported config carries a
	// complete, restartable chain (one hop per segment, one node per
	// candidate) rather than an empty placeholder.
	nodeCfgs, err := topology.BuildNodeConfigs(decl)
	if err != nil {
		return false, err
	}
	managedCfg := topology.BuildChainConfig(decl, nodeCfgs)
	managedCfg.Metadata[managedChainMarker] = true
	cfg.Chains = append(cfg.Chains, managedCfg)

	// parser.Parse already built the CLI services without a chain reference
	// (the grouped -F values were withheld), so bind them to chain-0 here using
	// the same handler/listener rule the x cmd builder applies. Config-file
	// services that already name a chain are untouched.
	bindServicesToManagedChain(cfg)
	return true, nil
}

// bindServicesToManagedChain points CLI services without an explicit chain at
// the managed chain. rtcp/rudp/runix bind at the listener; the rest at the
// handler, matching the x cmd builder.
func bindServicesToManagedChain(cfg *xconfig.Config) {
	for _, svc := range cfg.Services {
		if svc == nil {
			continue
		}
		if svc.Listener != nil &&
			(svc.Listener.Type == "rtcp" || svc.Listener.Type == "rudp" || svc.Listener.Type == "runix") {
			if svc.Listener.Chain == "" {
				svc.Listener.Chain = managedChainName
			}
			continue
		}
		if svc.Handler != nil && svc.Handler.Chain == "" && svc.Handler.ChainGroup == nil {
			svc.Handler.Chain = managedChainName
		}
	}
}

// shouldSkipChain reports whether a chain config is the topology placeholder
// and therefore must not be built/registered by the standard loader (the
// Manager owns that registry entry).
func shouldSkipChain(ch *xconfig.ChainConfig) bool {
	return ch != nil && ch.Name == managedChainName &&
		ch.Metadata != nil && ch.Metadata[managedChainMarker] == true
}

// applyTopologyState builds (or re-applies) the redundant topology snapshot,
// preserving state across reloads for unchanged segments and candidates. It
// does not touch the chain registry; call publishTopology for that.
func applyTopologyState(nodeArgs []string) (bool, error) {
	decl, err := topology.ParseChainDecl(nodeArgs)
	if err != nil {
		return false, fmt.Errorf("chain declaration: %w", err)
	}
	if !decl.Grouped {
		if topologyManager != nil && topologyManager.Active() {
			if err := topologyManager.Close(); err != nil {
				return false, err
			}
		}
		return false, nil
	}

	nodeCfgs, err := topology.BuildNodeConfigs(decl)
	if err != nil {
		return false, err
	}

	log := logger.Default().WithFields(map[string]any{"kind": "topology"})
	if topologyManager == nil {
		topologyManager = topology.NewManager(topology.DefaultNodeBuilder, managedChainName, log)
	}

	// Apply first: on failure the previous snapshot keeps serving and nothing
	// is re-registered.
	if err := topologyManager.Apply(decl, nodeCfgs, "cli"); err != nil {
		return false, err
	}
	return true, nil
}

// publishTopology registers the current topology under chain-0. It must run
// after loader.Load, because the loader resets the chain registry. The
// service-side chain wrapper resolves the name lazily, so it observes the
// instance published here.
func publishTopology() error {
	if topologyManager == nil {
		return nil
	}
	chainer := topologyManager.Chainer()
	if chainer == nil {
		return fmt.Errorf("chain declaration: topology produced no chain")
	}

	if registry.ChainRegistry().IsRegistered(managedChainName) {
		registry.ChainRegistry().Unregister(managedChainName)
	}
	if err := registry.ChainRegistry().Register(managedChainName, chainer); err != nil {
		return err
	}

	printTopologySummary(logger.Default().WithFields(map[string]any{"kind": "topology"}))
	return nil
}

// applyTopology builds state and publishes the registry entry. Used by reload.
func applyTopology(nodeArgs []string) (bool, error) {
	grouped, err := applyTopologyState(nodeArgs)
	if err != nil {
		return false, err
	}
	if !grouped {
		registry.ChainRegistry().Unregister(managedChainName)
		return false, nil
	}
	if err := publishTopology(); err != nil {
		return false, err
	}
	return true, nil
}

// printTopologySummary emits the single-source topology block once at startup
// and after each successful reload.
func printTopologySummary(log logger.Logger) {
	if topologyManager == nil {
		return
	}
	summary, ok := topologyManager.Summary()
	if !ok {
		return
	}
	log.Info("\n" + summary.RenderText())
}
