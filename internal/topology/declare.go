// Package topology implements explicit redundant chain semantics for CLI
// chain declarations: parallel candidates within a segment, serial segments
// within a chain, weighted and backup selection, failure isolation with
// automatic recovery, per-request failover, state-preserving reload, and a
// single-source-of-truth topology summary.
//
// The package only activates when the new grouped syntax ("a|b|c" inside an
// -F value) is used. Legacy single-candidate declarations never enter this
// package, so their runtime behavior is unchanged.
package topology

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// MaxWeight is the inclusive upper bound accepted for a candidate weight.
const MaxWeight = 10000

// Reserved query keys consumed by the topology parser. They are stripped
// from each candidate URL before the URL is handed to the x config builder,
// so the underlying connector/dialer never sees them.
const (
	keyWeight         = "weight"
	keyBackup         = "backup"
	keyMaxFails       = "maxFails"
	keyFailTimeout    = "failTimeout"
	keyProbe          = "probe"
	keyProbeInterval  = "probeInterval"
	keyProbeTimeout   = "probeTimeout"
	keyStrategy       = "strategy"
	keyMaxFailover    = "maxFailover"
	keyFailoverTimout = "failoverTimeout"
)

// candidateReservedKeys are valid on every candidate URL.
var candidateReservedKeys = map[string]struct{}{
	keyWeight:        {},
	keyBackup:        {},
	keyMaxFails:      {},
	keyFailTimeout:   {},
	keyProbe:         {},
	keyProbeInterval: {},
	keyProbeTimeout:  {},
}

// segmentOnlyKeys are only valid on the first candidate of a segment; they
// configure the segment as a whole. Seeing them on any other candidate is an
// ambiguous declaration and rejected.
var segmentOnlyKeys = map[string]struct{}{
	keyStrategy:       {},
	keyMaxFailover:    {},
	keyFailoverTimout: {},
}

// Strategy names accepted on segment declarations.
const (
	StrategyRandom = "random"
	StrategyRound  = "round"
	StrategyFIFO   = "fifo"
)

// Defaults applied when a segment omits the corresponding setting.
const (
	DefaultMaxFails        = 1
	DefaultFailTimeout     = 10 * time.Second
	DefaultFailoverTimeout = 30 * time.Second
)

// ErrNoCandidateAvailable is returned by a redundant route when every
// candidate of a segment is unavailable (isolated or exhausted) within the
// segment's failover budget. Callers can distinguish it from low-level dial
// errors with errors.Is. It never wraps credentials or candidate userinfo.
var ErrNoCandidateAvailable = errors.New("no candidate available")

// CandidateDecl is one parallel candidate inside a segment, parsed from a
// single full proxy URL within an "-F a|b|c" declaration.
type CandidateDecl struct {
	// Index is the zero-based position of the candidate within its segment.
	Index int
	// RawURL is the candidate URL with all topology-reserved query keys
	// removed. It remains parseable by the standard x cmd config builder.
	RawURL string
	// Addr is the credential-free host:port used for identity and summaries.
	Addr string
	// Scheme is the URL scheme (connector[+dialer]), part of the fingerprint.
	Scheme string

	Weight      int
	Backup      bool
	MaxFails    int
	FailTimeout time.Duration

	ProbeAddr     string
	ProbeInterval time.Duration
	ProbeTimeout  time.Duration

	// reservedSegKeys records segment-level keys seen on this candidate so
	// the parser can reject them on non-first candidates.
	segmentKeys []string
}

// SegmentDecl is one serial segment of a chain: an ordered list of parallel
// candidates plus segment-wide policy.
type SegmentDecl struct {
	// Index is the zero-based position of the segment within its chain.
	Index int
	// Raw is the original "-F" value (retained for diagnostics without
	// credentials; see Redacted()).
	Raw     string
	Members []*CandidateDecl

	Strategy        string
	MaxFails        int
	FailTimeout     time.Duration
	MaxFailover     int
	FailoverTimeout time.Duration
}

// ChainDecl is the parsed redundant chain declaration derived from all -F
// values of one invocation.
type ChainDecl struct {
	// Grouped reports whether the new "a|b" syntax was used. When false the
	// declaration must be handled by the legacy x cmd path instead.
	Grouped  bool
	Segments []*SegmentDecl
}

// IsGroupedArg reports whether raw is a new-style grouped declaration
// (contains at least one candidate separator '|').
func IsGroupedArg(raw string) bool {
	return strings.Contains(raw, "|")
}

// IsGroupedCLI reports whether any of the raw -F values uses the grouped
// syntax. It performs no validation; ParseChainDecl owns the validation and
// returns the concrete errors.
func IsGroupedCLI(nodeArgs []string) (bool, error) {
	for _, raw := range nodeArgs {
		if IsGroupedArg(raw) {
			return true, nil
		}
	}
	return false, nil
}

// ParseChainDecl parses the ordered -F values into a redundant chain
// declaration. It enforces the mixing rules and all start-up validations;
// every returned error is safe to print (candidate credentials are stripped).
func ParseChainDecl(nodeArgs []string) (*ChainDecl, error) {
	decl := &ChainDecl{}

	grouped := false
	for _, raw := range nodeArgs {
		if IsGroupedArg(raw) {
			grouped = true
			break
		}
	}
	decl.Grouped = grouped
	if !grouped {
		// Legacy path: nothing to validate here; the x cmd builder owns it.
		return decl, nil
	}

	for si, raw := range nodeArgs {
		seg, err := parseSegment(si, raw)
		if err != nil {
			return nil, err
		}
		decl.Segments = append(decl.Segments, seg)
	}

	if err := decl.validate(); err != nil {
		return nil, err
	}
	return decl, nil
}

func parseSegment(index int, raw string) (*SegmentDecl, error) {
	seg := &SegmentDecl{
		Index: index,
		Raw:   raw,
	}

	parts := strings.Split(raw, "|")
	if len(parts) == 0 {
		return nil, fmt.Errorf("segment %d: empty segment declaration", index)
	}

	seenAddr := make(map[string]struct{})

	for ci, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("segment %d: empty candidate at position %d", index, ci)
		}

		// The legacy implicit parallel syntax (comma-separated hosts) must not
		// be combined with the explicit grouped syntax: the two are different
		// expressions of redundancy and mixing them is ambiguous. Check before
		// URL parsing because a comma inside host:port makes the URL invalid.
		if host := hostPart(part); strings.Contains(host, ",") {
			return nil, fmt.Errorf("segment %d candidate %d: comma-separated multi-host syntax is not allowed inside a grouped declaration; split candidates with '|'", index, ci)
		}

		u, err := url.Parse(part)
		if err != nil {
			return nil, fmt.Errorf("segment %d candidate %d: parse url: %w", index, ci, err)
		}
		if u.Host == "" {
			return nil, fmt.Errorf("segment %d candidate %d: missing host:port", index, ci)
		}

		cand := &CandidateDecl{
			Index:  ci,
			Scheme: u.Scheme,
			Addr:   u.Host,
		}

		q := u.Query()
		for key, vals := range q {
			if len(vals) == 0 {
				continue
			}
			val := vals[0]
			if _, ok := segmentOnlyKeys[key]; ok {
				cand.segmentKeys = append(cand.segmentKeys, key)
			}
			if err := applyReservedKey(seg, cand, key, val); err != nil {
				return nil, fmt.Errorf("segment %d candidate %d: %w", index, ci, err)
			}
			if _, reserved := candidateReservedKeys[key]; reserved {
				q.Del(key)
				continue
			}
			if _, segOnly := segmentOnlyKeys[key]; segOnly {
				q.Del(key)
			}
		}

		// Rebuild the URL without reserved keys so the downstream x builder
		// only receives connector/dialer/hop metadata it understands.
		u.RawQuery = q.Encode()
		u.User = nil
		cand.RawURL = u.String()

		identity := cand.Scheme + "://" + cand.Addr
		if _, dup := seenAddr[identity]; dup {
			return nil, fmt.Errorf("segment %d: duplicate candidate %q", index, redactURL(u))
		}
		seenAddr[identity] = struct{}{}

		seg.Members = append(seg.Members, cand)
	}

	if len(seg.Members) == 0 {
		return nil, fmt.Errorf("segment %d: segment has no candidates", index)
	}

	applySegmentDefaults(seg)
	return seg, nil
}

func applyReservedKey(seg *SegmentDecl, cand *CandidateDecl, key, val string) error {
	switch key {
	case keyWeight:
		w, err := parseWeight(val)
		if err != nil {
			return fmt.Errorf("weight: %w", err)
		}
		cand.Weight = w
	case keyBackup:
		b, err := parseBoolStrict(val)
		if err != nil {
			return fmt.Errorf("backup: %w", err)
		}
		cand.Backup = b
	case keyMaxFails:
		n, err := strconv.Atoi(val)
		if err != nil || n <= 0 {
			return fmt.Errorf("maxFails must be a positive integer, got %q", val)
		}
		cand.MaxFails = n
	case keyFailTimeout:
		d, err := time.ParseDuration(val)
		if err != nil || d <= 0 {
			return fmt.Errorf("failTimeout must be a duration > 0, got %q", val)
		}
		cand.FailTimeout = d
	case keyProbe:
		// Accept both a bare host:port ("127.0.0.1:9000") and a full URL.
		raw := val
		if !strings.Contains(raw, "://") {
			raw = "tcp://" + raw
		}
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" {
			return fmt.Errorf("probe must be a valid host:port URL, got %q", val)
		}
		cand.ProbeAddr = u.Host
	case keyProbeInterval:
		d, err := time.ParseDuration(val)
		if err != nil || d <= 0 {
			return fmt.Errorf("probeInterval must be a duration > 0, got %q", val)
		}
		cand.ProbeInterval = d
	case keyProbeTimeout:
		d, err := time.ParseDuration(val)
		if err != nil || d <= 0 {
			return fmt.Errorf("probeTimeout must be a duration > 0, got %q", val)
		}
		cand.ProbeTimeout = d
	case keyStrategy:
		switch val {
		case StrategyRandom, StrategyRound, StrategyFIFO:
			seg.Strategy = val
		default:
			return fmt.Errorf("strategy must be one of %s|%s|%s, got %q",
				StrategyRandom, StrategyRound, StrategyFIFO, val)
		}
	case keyMaxFailover:
		n, err := strconv.Atoi(val)
		if err != nil || n <= 0 {
			return fmt.Errorf("maxFailover must be a positive integer, got %q", val)
		}
		seg.MaxFailover = n
	case keyFailoverTimout:
		d, err := time.ParseDuration(val)
		if err != nil || d <= 0 {
			return fmt.Errorf("failoverTimeout must be a duration > 0, got %q", val)
		}
		seg.FailoverTimeout = d
	default:
		// Unknown keys are ordinary node/connector/dialer metadata and are
		// left untouched for the x builder.
	}
	return nil
}

func applySegmentDefaults(seg *SegmentDecl) {
	if seg.Strategy == "" {
		seg.Strategy = StrategyRandom
	}
	if seg.MaxFails == 0 {
		seg.MaxFails = DefaultMaxFails
	}
	if seg.FailTimeout == 0 {
		seg.FailTimeout = DefaultFailTimeout
	}
	if seg.FailoverTimeout == 0 {
		seg.FailoverTimeout = DefaultFailoverTimeout
	}
	if seg.MaxFailover == 0 {
		seg.MaxFailover = len(seg.Members)
	}
}

// validate runs cross-candidate and cross-segment checks.
func (d *ChainDecl) validate() error {
	if !d.Grouped {
		return nil
	}
	if len(d.Segments) == 0 {
		return errors.New("empty chain declaration")
	}
	for _, seg := range d.Segments {
		if len(seg.Members) == 0 {
			return fmt.Errorf("segment %d: segment has no candidates", seg.Index)
		}
		// Segment-level keys are only legal on the first candidate.
		for _, cand := range seg.Members[1:] {
			if len(cand.segmentKeys) > 0 {
				return fmt.Errorf("segment %d candidate %d: segment-level key(s) %s only allowed on the first candidate",
					seg.Index, cand.Index, strings.Join(cand.segmentKeys, ","))
			}
		}
		if seg.MaxFailover > len(seg.Members) {
			return fmt.Errorf("segment %d: maxFailover %d exceeds candidate count %d",
				seg.Index, seg.MaxFailover, len(seg.Members))
		}

		primaries := 0
		for _, cand := range seg.Members {
			if !cand.Backup {
				primaries++
			}
		}
		if primaries == 0 {
			return fmt.Errorf("segment %d: every candidate is a backup; at least one primary candidate is required",
				seg.Index)
		}
	}
	return nil
}

// parseWeight enforces the weight contract: positive integer within
// [1, MaxWeight]. Zero is explicitly rejected (see design doc §2.1).
func parseWeight(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("weight must be an integer, got %q", s)
	}
	if n <= 0 {
		return 0, fmt.Errorf("weight must be > 0 (use backup=true for a standby candidate), got %d", n)
	}
	if n > MaxWeight {
		return 0, fmt.Errorf("weight %d exceeds maximum %d", n, MaxWeight)
	}
	return n, nil
}

func parseBoolStrict(s string) (bool, error) {
	switch s {
	case "true", "True", "TRUE", "1":
		return true, nil
	case "false", "False", "FALSE", "0":
		return false, nil
	default:
		return false, fmt.Errorf("expected a boolean, got %q", s)
	}
}

// redactURL strips userinfo for safe inclusion in diagnostics and summaries.

// redactURL strips userinfo for safe inclusion in diagnostics and summaries.
func redactURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	c := *u
	c.User = nil
	return c.String()
}

// hostPart extracts the host portion (between scheme:// and the first / or ?)
// of a raw candidate URL without fully parsing it, so a comma inside the host
// can be rejected before the stricter URL parser reports a confusing port
// error.
func hostPart(raw string) string {
	s := raw
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	if i := strings.IndexAny(s, "/?"); i >= 0 {
		s = s[:i]
	}
	if i := strings.LastIndex(s, "@"); i >= 0 {
		s = s[i+1:]
	}
	return s
}
func (s *SegmentDecl) Redacted() string {
	var out []string
	for _, cand := range s.Members {
		out = append(out, cand.Scheme+"://"+cand.Addr)
	}
	return strings.Join(out, " | ")
}
