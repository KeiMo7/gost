package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-gost/core/auth"
	"github.com/go-gost/core/logger"
	"github.com/go-gost/core/service"
	api_service "github.com/go-gost/x/api/service"
	xauth "github.com/go-gost/x/auth"
	"github.com/go-gost/x/config"
	"github.com/go-gost/x/config/loader"
	auth_parser "github.com/go-gost/x/config/parsing/auth"
	"github.com/go-gost/x/config/parsing/parser"
	xmetrics "github.com/go-gost/x/metrics"
	metrics "github.com/go-gost/x/metrics/service"
	xplugin "github.com/go-gost/x/plugin"
	"github.com/go-gost/x/registry"
	"github.com/judwhite/go-svc"

	"github.com/go-gost/gost/internal/topology"
)

type program struct {
	srvApi       service.Service
	srvMetrics   service.Service
	srvProfiling *http.Server

	cancel context.CancelFunc
}

func (p *program) Init(env svc.Environment) error {
	args := parser.Args{
		CfgFiles:    cfgFiles,
		Services:    services,
		Nodes:       nodes,
		Debug:       debug,
		Trace:       trace,
		ApiAddr:     apiAddr,
		MetricsAddr: metricsAddr,
	}

	// When the new grouped "-F a|b" syntax is used, the redundant-chain
	// topology manager owns the chain. The grouped -F values must not reach
	// the legacy x cmd builder (which would misread "|" as part of a URL), so
	// they are withheld from the parser and wired after loader.Load.
	if grouped, _ := topology.IsGroupedCLI(nodes); grouped {
		args.Nodes = nil
	}

	parser.Init(args)

	return nil
}

func (p *program) Start() error {
	cfg, err := parser.Parse()
	if err != nil {
		return err
	}

	// Inject the placeholder chain binding for grouped declarations before any
	// output or load so -O and service parsing see a consistent config.
	grouped, err := prepareGroupedChain(cfg, nodes)
	if err != nil {
		return err
	}

	if outputFormat != "" {
		if err := cfg.Write(os.Stdout, outputFormat); err != nil {
			return err
		}
		os.Exit(0)
	}

	config.Set(cfg)

	// Enable metrics before loading services so that listener wrappers
	// can observe the enabled state at Init time.
	if cfg.Metrics != nil && cfg.Metrics.Addr != "" {
		xmetrics.Enable(true)
	}

	// Build the redundant topology state (isolation, counters, probes) before
	// loader.Load. The registry entry itself is (re)published afterwards: the
	// loader clears the chain registry while building services, and the
	// services hold a name-based wrapper that resolves chain-0 lazily at
	// request time, so a post-load registration is what they actually observe.
	if grouped {
		if _, err := applyTopologyState(nodes); err != nil {
			return err
		}
	}

	if err := loadConfig(cfg); err != nil {
		return err
	}

	// Re-publish chain-0 after the loader's registry reset.
	if grouped {
		if err := publishTopology(); err != nil {
			return err
		}
	}

	if err := p.run(cfg); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	go p.reload(ctx)

	return nil
}

func (p *program) run(cfg *config.Config) error {
	for _, svc := range registry.ServiceRegistry().GetAll() {
		svc := svc
		go func() {
			svc.Serve()
		}()
	}

	if p.srvApi != nil {
		p.srvApi.Close()
		p.srvApi = nil
	}
	if cfg.API != nil {
		s, err := buildApiService(cfg.API)
		if err != nil {
			return err
		}

		p.srvApi = s

		go func() {
			defer s.Close()

			log := logger.Default().WithFields(map[string]any{"kind": "service", "service": "@api"})

			log.Info("listening on ", s.Addr())
			if err := s.Serve(); !errors.Is(err, http.ErrServerClosed) {
				log.Error(err)
			}
		}()
	}

	if p.srvMetrics != nil {
		p.srvMetrics.Close()
		p.srvMetrics = nil
	}
	if cfg.Metrics != nil && cfg.Metrics.Addr != "" {
		s, err := buildMetricsService(cfg.Metrics)
		if err != nil {
			return err
		}

		p.srvMetrics = s

		go func() {
			defer s.Close()

			log := logger.Default().WithFields(map[string]any{"kind": "service", "service": "@metrics"})

			log.Info("listening on ", s.Addr())
			if err := s.Serve(); !errors.Is(err, http.ErrServerClosed) {
				log.Error(err)
			}
		}()
	}

	if p.srvProfiling != nil {
		p.srvProfiling.Close()
		p.srvProfiling = nil
	}
	if cfg.Profiling != nil {
		addr := cfg.Profiling.Addr
		if addr == "" {
			addr = ":6060"
		}
		s := &http.Server{
			Addr: addr,
		}
		p.srvProfiling = s

		go func() {
			defer s.Close()

			log := logger.Default().WithFields(map[string]any{"kind": "service", "service": "@profiling"})

			log.Info("listening on ", addr)
			if err := s.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				log.Error(err)
			}
		}()
	}

	return nil
}

func (p *program) Stop() error {
	if p.cancel != nil {
		p.cancel()
	}

	for name, srv := range registry.ServiceRegistry().GetAll() {
		srv.Close()
		logger.Default().Debugf("service %s shutdown", name)
	}

	if p.srvApi != nil {
		p.srvApi.Close()
		logger.Default().Debug("service @api shutdown")
	}
	if p.srvMetrics != nil {
		p.srvMetrics.Close()
		logger.Default().Debug("service @metrics shutdown")
	}
	if p.srvProfiling != nil {
		p.srvProfiling.Close()
		logger.Default().Debug("service @profiling shutdown")
	}

	xplugin.Shutdown()

	return nil
}

func (p *program) reload(ctx context.Context) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP)

	var ticker <-chan time.Time
	if reload > 0 {
		t := time.NewTicker(reload)
		defer t.Stop()
		ticker = t.C
	}

	for {
		select {
		case <-c:
			if err := p.reloadConfig(); err != nil {
				logger.Default().Error(err)
			} else {
				logger.Default().Info("config reloaded")
			}

		case <-ticker:
			if err := p.reloadConfig(); err != nil {
				logger.Default().Errorf("auto reload: %v", err)
			} else {
				logger.Default().Debug("config auto reloaded")
			}

		case <-ctx.Done():
			return
		}
	}
}

func (p *program) reloadConfig() error {
	cfg, err := parser.Parse()
	if err != nil {
		return err
	}

	grouped, err := prepareGroupedChain(cfg, nodes)
	if err != nil {
		return err
	}

	// Build topology state first (preserves isolation/counters). On failure
	// the previous snapshot keeps serving and we leave the loader untouched.
	wasActive := topologyManager != nil && topologyManager.Active()
	if grouped || wasActive {
		if _, err := applyTopologyState(nodes); err != nil {
			return err
		}
	}

	config.Set(cfg)

	if err := loadConfig(cfg); err != nil {
		return err
	}

	// Re-publish after the loader's registry reset.
	if grouped {
		if err := publishTopology(); err != nil {
			return err
		}
	} else if wasActive {
		// Grouped syntax removed: clear the stale entry.
		registry.ChainRegistry().Unregister(managedChainName)
	}

	if err := p.run(cfg); err != nil {
		return err
	}

	return nil
}

// loadConfig loads cfg while hiding the topology placeholder chain from the
// standard loader. The Manager owns chain-0's registry entry; without this
// filter the loader would build an empty chain and fail registering the
// duplicate name.
func loadConfig(cfg *config.Config) error {
	if cfg == nil {
		return loader.Load(cfg)
	}
	original := cfg.Chains
	filtered := make([]*config.ChainConfig, 0, len(original))
	for _, ch := range original {
		if shouldSkipChain(ch) {
			continue
		}
		filtered = append(filtered, ch)
	}
	cfg.Chains = filtered
	err := loader.Load(cfg)
	cfg.Chains = original
	return err
}

func buildApiService(cfg *config.APIConfig) (service.Service, error) {
	var authers []auth.Authenticator
	if auther := auth_parser.ParseAutherFromAuth(cfg.Auth); auther != nil {
		authers = append(authers, auther)
	}
	if cfg.Auther != "" {
		authers = append(authers, registry.AutherRegistry().Get(cfg.Auther))
	}

	var auther auth.Authenticator
	if len(authers) > 0 {
		auther = xauth.AuthenticatorGroup(authers...)
	}

	network := "tcp"
	addr := cfg.Addr
	if strings.HasPrefix(addr, "unix://") {
		network = "unix"
		addr = strings.TrimPrefix(addr, "unix://")
	}
	return api_service.NewService(
		network, addr,
		api_service.PathPrefixOption(cfg.PathPrefix),
		api_service.AccessLogOption(cfg.AccessLog),
		api_service.AutherOption(auther),
	)
}

func buildMetricsService(cfg *config.MetricsConfig) (service.Service, error) {
	auther := auth_parser.ParseAutherFromAuth(cfg.Auth)
	if cfg.Auther != "" {
		auther = registry.AutherRegistry().Get(cfg.Auther)
	}

	network := "tcp"
	addr := cfg.Addr
	if strings.HasPrefix(addr, "unix://") {
		network = "unix"
		addr = strings.TrimPrefix(addr, "unix://")
	}
	return metrics.NewService(
		network, addr,
		metrics.PathOption(cfg.Path),
		metrics.AutherOption(auther),
	)
}
