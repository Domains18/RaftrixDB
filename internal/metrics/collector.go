package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Collector holds all Prometheus metrics for a RaftrixDB node.
type Collector struct {
	// ── Raft state ────────────────────────────────────────────────────────
	RaftTerm      prometheus.Gauge
	RaftState     *prometheus.GaugeVec // labels: state={follower,candidate,leader}
	ElectionTotal prometheus.Counter
	LeaderChanges prometheus.Counter

	// ── Log metrics ───────────────────────────────────────────────────────
	CommitIndex  prometheus.Gauge
	LastApplied  prometheus.Gauge
	LogSize      prometheus.Gauge
	SnapshotSize prometheus.Gauge

	// ── Operation metrics ─────────────────────────────────────────────────
	OpsTotal      *prometheus.CounterVec   // labels: op={put,get,delete,cas}
	OpDuration    *prometheus.HistogramVec // labels: op
	OpsErrors     *prometheus.CounterVec   // labels: op

	// ── Replication metrics ───────────────────────────────────────────────
	ReplicationLatency *prometheus.HistogramVec // labels: peer
	AppendEntriesTotal *prometheus.CounterVec   // labels: peer, result={success,failure}
}

// New registers and returns all metrics under the "raftrixdb" namespace.
func New(nodeID string) *Collector {
	labels := prometheus.Labels{"node_id": nodeID}

	return &Collector{
		RaftTerm: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace:   "raftrixdb",
			Name:        "raft_term",
			Help:        "Current Raft term of this node.",
			ConstLabels: labels,
		}),

		RaftState: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   "raftrixdb",
			Name:        "raft_state",
			Help:        "Current state of the Raft node (1 = active for that state).",
			ConstLabels: labels,
		}, []string{"state"}),

		ElectionTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace:   "raftrixdb",
			Name:        "elections_total",
			Help:        "Total number of elections started by this node.",
			ConstLabels: labels,
		}),

		LeaderChanges: promauto.NewCounter(prometheus.CounterOpts{
			Namespace:   "raftrixdb",
			Name:        "leader_changes_total",
			Help:        "Total number of times this node has become leader.",
			ConstLabels: labels,
		}),

		CommitIndex: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace:   "raftrixdb",
			Name:        "commit_index",
			Help:        "Highest log index known to be committed.",
			ConstLabels: labels,
		}),

		LastApplied: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace:   "raftrixdb",
			Name:        "last_applied",
			Help:        "Highest log index applied to the state machine.",
			ConstLabels: labels,
		}),

		OpsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "raftrixdb",
			Name:        "operations_total",
			Help:        "Total number of client operations, by type.",
			ConstLabels: labels,
		}, []string{"op"}),

		OpDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   "raftrixdb",
			Name:        "operation_duration_seconds",
			Help:        "Latency of client operations from proposal to commit.",
			ConstLabels: labels,
			Buckets:     prometheus.DefBuckets,
		}, []string{"op"}),

		OpsErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "raftrixdb",
			Name:        "operation_errors_total",
			Help:        "Total number of failed client operations, by type.",
			ConstLabels: labels,
		}, []string{"op"}),

		ReplicationLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   "raftrixdb",
			Name:        "replication_latency_seconds",
			Help:        "Round-trip latency for AppendEntries RPCs to each peer.",
			ConstLabels: labels,
			Buckets:     []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"peer"}),

		AppendEntriesTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "raftrixdb",
			Name:        "append_entries_total",
			Help:        "Total AppendEntries RPCs sent, by peer and result.",
			ConstLabels: labels,
		}, []string{"peer", "result"}),
	}
}