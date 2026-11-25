package topology

import (
	"context"
	"io"
	"time"

	corechain "github.com/go-gost/core/chain"
	"github.com/go-gost/core/logger"
)

// redundantChain is a corechain.Chainer built from serial segments, each with
// parallel failover candidates.
type redundantChain struct {
	name     string
	segments []*segment
	log      logger.Logger

	failoverTimeout time.Duration
}

func newRedundantChain(name string, segments []*segment, failoverTimeout time.Duration, log logger.Logger) *redundantChain {
	return &redundantChain{
		name:            name,
		segments:        segments,
		log:             log,
		failoverTimeout: failoverTimeout,
	}
}

// Route constructs one failover plan per segment for the request and returns
// a redundant route. When a segment has no available candidate at all, the
// returned route still carries the empty plan so its Dial reports
// ErrNoCandidateAvailable deterministically (rather than silently falling
// back to a direct connection).
func (c *redundantChain) Route(ctx context.Context, network, address string, opts ...corechain.RouteOption) corechain.Route {
	// Route options (e.g. target host) are accepted for interface compatibility
	// and consumed by the candidate transports configured at build time.
	_ = opts

	now := time.Now()
	planned := make([]*plannedSegment, 0, len(c.segments))
	for _, seg := range c.segments {
		planned = append(planned, &plannedSegment{
			seg:   seg,
			cands: seg.failoverPlan(now),
		})
	}
	return newRedundantRoute(planned, c.failoverTimeout, c.log)
}

// Close cascades shutdown to every segment and candidate.
func (c *redundantChain) Close() error {
	for _, s := range c.segments {
		s.close()
	}
	return nil
}

var (
	_ corechain.Chainer = (*redundantChain)(nil)
	_ io.Closer         = (*redundantChain)(nil)
)
