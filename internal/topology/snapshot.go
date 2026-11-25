package topology

import (
	"fmt"
	"strings"
	"time"
)

// CandidateStatus is one row of the topology summary for a single candidate.
// It is derived from the live candidateRuntime, so the summary and the
// routing path share a single source of truth.
type CandidateStatus struct {
	Segment       string         `json:"segment" yaml:"segment"`
	Name          string         `json:"name" yaml:"name"`
	Addr          string         `json:"addr" yaml:"addr"` // credential-free
	Role          string         `json:"role" yaml:"role"` // primary|backup
	Weight        int            `json:"weight" yaml:"weight"`
	State         CandidateState `json:"state" yaml:"state"`
	Fails         int64          `json:"fails" yaml:"fails"`
	Threshold     int            `json:"maxFails" yaml:"maxFails"`
	IsolateFor    time.Duration  `json:"isolateFor" yaml:"isolateFor"`
	IsolatedAt    time.Time      `json:"isolatedAt,omitempty" yaml:"isolatedAt,omitempty"`
	IsolatedUntil time.Time      `json:"isolatedUntil,omitempty" yaml:"isolatedUntil,omitempty"`
}

type candidateStatus struct {
	Name          string
	Addr          string
	Role          string
	Weight        int
	State         CandidateState
	Fails         int64
	Threshold     int
	IsolateFor    time.Duration
	IsolatedAt    time.Time
	IsolatedUntil time.Time
}

// SegmentStatus is the summary of one serial segment.
type SegmentStatus struct {
	Name            string            `json:"name" yaml:"name"`
	Strategy        string            `json:"strategy" yaml:"strategy"`
	MaxFails        int               `json:"maxFails" yaml:"maxFails"`
	FailTimeout     time.Duration     `json:"failTimeout" yaml:"failTimeout"`
	MaxFailover     int               `json:"maxFailover" yaml:"maxFailover"`
	FailoverTimeout time.Duration     `json:"failoverTimeout" yaml:"failoverTimeout"`
	Source          string            `json:"source" yaml:"source"`
	Candidates      []CandidateStatus `json:"candidates" yaml:"candidates"`
}

// TopologySummary is the full, exportable summary of a chain snapshot.
type TopologySummary struct {
	Chain    string          `json:"chain" yaml:"chain"`
	Source   string          `json:"source" yaml:"source"`
	Segments []SegmentStatus `json:"segments" yaml:"segments"`
}

// snapshot is an immutable orchestration result. Both live routing and the
// exported summary read from this one object; there is no second assembly
// path.
type snapshot struct {
	chainName string
	source    string
	chain     *redundantChain
	segments  []*segment
	decl      *ChainDecl
	builtAt   time.Time
}

func (s *snapshot) Summary() TopologySummary {
	out := TopologySummary{
		Chain:  s.chainName,
		Source: s.source,
	}
	for _, seg := range s.segments {
		st := SegmentStatus{
			Name:            seg.name,
			Strategy:        seg.decl.Strategy,
			MaxFails:        seg.decl.MaxFails,
			FailTimeout:     seg.decl.FailTimeout,
			MaxFailover:     seg.decl.MaxFailover,
			FailoverTimeout: seg.decl.FailoverTimeout,
			Source:          s.source,
		}
		for _, cs := range seg.statuses() {
			st.Candidates = append(st.Candidates, CandidateStatus{
				Segment:       seg.name,
				Name:          cs.Name,
				Addr:          cs.Addr,
				Role:          cs.Role,
				Weight:        cs.Weight,
				State:         cs.State,
				Fails:         cs.Fails,
				Threshold:     cs.Threshold,
				IsolateFor:    cs.IsolateFor,
				IsolatedAt:    cs.IsolatedAt,
				IsolatedUntil: cs.IsolatedUntil,
			})
		}
		out.Segments = append(out.Segments, st)
	}
	return out
}

// RenderText emits the fixed-format topology block logged at startup and
// after every successful reload. It never contains credentials.
func (t TopologySummary) RenderText() string {
	var b strings.Builder
	fmt.Fprintf(&b, "topology chain %s (source=%s):\n", t.Chain, t.Source)
	for _, seg := range t.Segments {
		fmt.Fprintf(&b, "  segment %s strategy=%s maxFails=%d failTimeout=%s maxFailover=%d failoverTimeout=%s source=%s\n",
			seg.Name, seg.Strategy, seg.MaxFails, seg.FailTimeout, seg.MaxFailover, seg.FailoverTimeout, seg.Source)
		for _, c := range seg.Candidates {
			until := "-"
			if !c.IsolatedUntil.IsZero() {
				until = c.IsolatedUntil.Format(time.RFC3339)
			}
			fmt.Fprintf(&b, "    candidate %s role=%s weight=%d addr=%s state=%s fails=%d threshold=%d isolateFor=%s isolatedUntil=%s\n",
				c.Name, c.Role, c.Weight, c.Addr, c.State, c.Fails, c.Threshold, c.IsolateFor, until)
		}
	}
	return strings.TrimRight(b.String(), "\n")
}
