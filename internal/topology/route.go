package topology

import (
	"context"
	"net"
	"time"

	corechain "github.com/go-gost/core/chain"
	"github.com/go-gost/core/connector"
	"github.com/go-gost/core/logger"
)

// plannedSegment is one segment's ordered failover attempt list for a single
// request. The plan is computed once at route construction so the whole
// request follows one consistent decision.
type plannedSegment struct {
	seg   *segment
	cands []*candidateRuntime
}

// redundantRoute walks serial segments; within each segment it fails over
// across the planned candidates when dial/handshake fails. It implements
// corechain.Route.
type redundantRoute struct {
	segments []*plannedSegment
	log      logger.Logger

	// failoverTimeout bounds the total failover time of the whole route so
	// that "no candidate available" is always reported within a known budget
	// instead of hanging until an outer timeout.
	failoverTimeout time.Duration
}

func newRedundantRoute(segments []*plannedSegment, failoverTimeout time.Duration, log logger.Logger) *redundantRoute {
	return &redundantRoute{
		segments:        segments,
		log:             log,
		failoverTimeout: failoverTimeout,
	}
}

func (r *redundantRoute) Nodes() []*corechain.Node {
	var out []*corechain.Node
	for _, ps := range r.segments {
		if len(ps.cands) > 0 {
			out = append(out, ps.cands[0].node)
		}
	}
	return out
}

// Dial connects through every serial segment to the final address. Each
// segment's candidates are tried in plan order; a failure marks that
// candidate and moves to the next one within the failover budget.
func (r *redundantRoute) Dial(ctx context.Context, network, address string, opts ...corechain.DialOption) (net.Conn, error) {
	// Dial options (interface/netns/sockopts/logger) are carried by each
	// candidate's node transport, which the x node builder configured from the
	// candidate URL; the route only orchestrates failover.
	_ = opts

	// Bound the entire failover sequence. The derived context is canceled on
	// return, which also unblocks any dial currently in flight.
	dialCtx, cancel := context.WithTimeout(ctx, r.failoverTimeout)
	defer cancel()

	conn, err := r.connect(dialCtx, address)
	if err != nil {
		return nil, err
	}

	// Reach the final destination through the last segment's transport.
	last := r.pickedLastNode()
	if last == nil {
		if conn != nil {
			conn.Close()
		}
		return nil, ErrNoCandidateAvailable
	}
	cc, err := last.Options().Transport.Connect(dialCtx, conn, network, address)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return cc, nil
}

// Bind establishes a reverse tunnel, applying the same per-segment failover.
func (r *redundantRoute) Bind(ctx context.Context, network, address string, opts ...corechain.BindOption) (net.Listener, error) {
	var options corechain.BindOptions
	for _, o := range opts {
		o(&options)
	}

	dialCtx, cancel := context.WithTimeout(ctx, r.failoverTimeout)
	defer cancel()

	conn, err := r.connect(dialCtx, address)
	if err != nil {
		return nil, err
	}

	last := r.pickedLastNode()
	if last == nil {
		conn.Close()
		return nil, ErrNoCandidateAvailable
	}
	ln, err := last.Options().Transport.Bind(dialCtx, conn, network, address,
		connector.BacklogBindOption(options.Backlog),
		connector.MuxBindOption(options.Mux),
		connector.UDPConnTTLBindOption(options.UDPConnTTL),
		connector.UDPDataBufferSizeBindOption(options.UDPDataBufferSize),
		connector.UDPDataQueueSizeBindOption(options.UDPDataQueueSize),
	)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return ln, nil
}

// picked is the node chosen per segment after a successful connect.
func (r *redundantRoute) pickedLastNode() *corechain.Node {
	if len(r.segments) == 0 {
		return nil
	}
	ps := r.segments[len(r.segments)-1]
	if len(ps.cands) == 0 {
		return nil
	}
	return ps.cands[0].node
}

// connect walks the serial segments, retrying each segment's candidate list
// on failure. A candidate failure marks its runtime so the isolation state
// machine can trip once the threshold is reached.
func (r *redundantRoute) connect(ctx context.Context, address string) (net.Conn, error) {
	var (
		conn net.Conn
		err  error
	)

	for si, ps := range r.segments {
		if len(ps.cands) == 0 {
			return nil, ErrNoCandidateAvailable
		}

		connected := false

		for ci, cand := range ps.cands {
			if ctx.Err() != nil {
				if conn != nil {
					conn.Close()
				}
				// Budget exhausted while candidates remained: surface the
				// distinguishable no-candidate error rather than a context
				// deadline leak.
				return nil, ErrNoCandidateAvailable
			}

			r.log.Debugf("trying segment %d candidate %d %s", ps.seg.decl.Index, ci, cand.decl.Addr)
			var cc net.Conn
			if si == 0 {
				cc, err = r.dialFirst(ctx, cand)
			} else {
				cc, err = r.dialNext(ctx, conn, cand)
			}

			if err != nil {
				cand.recordFailure(time.Now())
				r.log.Debugf("segment %d candidate %d (%s) failed: %v",
					ps.seg.decl.Index, ci, cand.decl.Addr, err)
				if cc != nil {
					cc.Close()
				}
				continue
			}

			cand.recordSuccess(time.Now())
			conn = cc
			connected = true
			break
		}

		if !connected {
			if conn != nil {
				conn.Close()
			}
			return nil, ErrNoCandidateAvailable
		}
	}

	return conn, nil
}

// dialFirst dials and handshakes the first segment's candidate.
func (r *redundantRoute) dialFirst(ctx context.Context, cand *candidateRuntime) (net.Conn, error) {
	tr := cand.node.Options().Transport
	cc, err := tr.Dial(ctx, cand.node.Addr)
	if err != nil {
		return nil, err
	}
	hc, err := tr.Handshake(ctx, cc)
	if err != nil {
		cc.Close()
		return nil, err
	}
	return hc, nil
}

// dialNext connects/handshakes a later segment through the existing conn.
func (r *redundantRoute) dialNext(ctx context.Context, conn net.Conn, cand *candidateRuntime) (net.Conn, error) {
	tr := cand.node.Options().Transport
	cc, err := tr.Connect(ctx, conn, "tcp", cand.node.Addr)
	if err != nil {
		return nil, err
	}
	hc, err := tr.Handshake(ctx, cc)
	if err != nil {
		cc.Close()
		return nil, err
	}
	return hc, nil
}
