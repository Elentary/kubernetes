# Better-Scheduler Observability

Three signal sources: profile-gated **decision logs** (why a pod landed where it did), **Kubernetes events** (preemption lifecycle, reservations), and **Prometheus metrics** (config + health). A Grafana dashboard ties them together.

## 1. Decision Logs

### 1.1 Design

Upstream kube-scheduler only reveals per-node scores and rejections at klog V(10), and verbosity is process-wide — unusable in production. There is no upstream metric or dashboard for per-pod decision explainability (scheduler metrics are health/perf only). So this fork emits the decision detail at plain `Info`, gated to one profile:

- Gate: `schedulingDecisionLogsEnabled(fwk)` → `fwk.ProfileName() == "better-scheduler"` (`pkg/scheduler/schedule_one.go`, const `detailedScoreLoggingProfile`).
- Every line carries `profile`, `decisionID`, `attempt`, and `pod` fields, so log pipelines can correlate one decision and isolate one scheduling attempt without regexes.
- Introduced in v0.6 (score lines) and v0.7 (rejection/summary lines).

### 1.2 Log line reference

All lines are structured klog (parseable with `logfmt`). `pod` renders as `<namespace>/<name>`. Shared fields on every line: `profile`, `decisionID` (the workload pod UID), and `attempt` (the 1-based `QueuedPodInfo.Attempts` for that scheduling cycle).

**Score lines** (emitted during prioritization in `schedule_one.go`):

| Message | Extra fields |
|---|---|
| `Plugin scored node for pod` | `plugin`, `node`, `score` |
| `Extender scored node for pod` | `extender`, `node`, `score` |
| `Calculated node's final score for pod` | `node`, `score` |
| `Preemption candidate scored for pod` | `node`, `score`, `score_plugin`, `score_name`, `victims`, `numPDBViolatingVictims`, GPU packing fields |
| `Preemption candidate selected for pod` | `node`, `score`, `selection_path`, `victims`, `numPDBViolatingVictims`, GPU packing fields |

**Rejection lines** (emitted during node filtering in `schedule_one.go` `findNodesThatFitPod`):

| Message | Extra fields |
|---|---|
| `Rejected node for pod` | `phase` (`PreFilter`/`Filter`/`Extender`), `node`, `status` (framework code, e.g. `Unschedulable`), `plugin`, `reason`, `synthetic` |
| `Scheduling rejection reason summary for pod` | `phase`, `status`, `plugin`, `reason`, `synthetic`, `nodes` (count) |
| `Scheduling decision summary for pod` | counters below |

`synthetic=true` marks PreFilter prunes: those per-node lines are synthesized from the aggregate PreFilter result (all nodes get the same status), not from evaluating each node.

**Decision summary counters** (one line per scheduling attempt):

| Field | Meaning |
|---|---|
| `cluster_nodes` | Nodes in the scheduler snapshot |
| `prefilter_candidate_nodes` | Nodes surviving PreFilter |
| `prefilter_pruned_nodes` | Nodes removed by PreFilter |
| `evaluated_candidate_nodes` | Candidates actually run through Filter (feasible + explicitly rejected) |
| `unevaluated_candidate_nodes` | Candidates never evaluated (search stopped early once enough feasible nodes were found) |
| `filter_rejected_nodes` | Explicit Filter rejections |
| `extender_rejected_nodes` | Extender rejections |
| `feasible_nodes` | Nodes eligible for scoring |
| `scored_nodes` | Nodes scored (0 when only one feasible node — scoring is skipped) |

When PreFilter rejects the pod outright (e.g. guarantee exceeded), a summary line is still emitted with all candidate counters at 0 and `prefilter_pruned_nodes = cluster_nodes`.

Interpretation caveats:

- A node not appearing in rejections is **not** necessarily feasible — check `unevaluated_candidate_nodes`.
- The three rejection categories are disjoint: prefilter-pruned nodes never reach Filter; extender rejections exclude filter-rejected nodes.

### 1.3 Querying (Loki/LogQL)

The dashboard queries follow this pattern (scheduler pods live in the scheduler's own namespace):

```logql
{namespace="better-scheduler", pod=~"$scheduler_pod"}
  |= `profile="better-scheduler"`
  |= `Calculated node's final score for pod`
  | logfmt
  | pod_extracted = `team-a/my-pod-abc123`
  | attempt = `${attempt}`
```

Gotchas learned the hard way:

- Quote match strings with **backticks** in LogQL to avoid escaping the inner `"` characters.
- After `| logfmt`, the log's own `pod` field is renamed to **`pod_extracted`** because `pod` collides with the stream label. Filter workload pods on `pod_extracted`, not `pod`.
- Derived dashboard panels should also filter on **`attempt = `${attempt}`** to avoid blending multiple retries in one chart.
- Numeric fields (`score`, the summary counters, `attempt`) are usable as metrics via `| unwrap score` etc.

## 2. Kubernetes Events

Preemption lifecycle events on the preemptor (action `NamespaceResourceGuaranteePostFilter`; full table in [architecture.md](architecture.md) §5.4): `...PreemptionStarted` / `Waiting` / `NotHelpful` / `NoCandidate` / `Error`. Victims get `NamespaceResourceGuaranteePreempted`. Reservation events on the holder pod: `NamespaceResourceGuaranteeNodeReserved` / `...ReservationReleased` / `...ReservationCleanedUp`.

Event notes embed `decisionID=<preemptor pod UID>` for correlation and are bounded to the 1024-character event limit (victim lists become `...(+N more)`).

## 3. Metrics (Prometheus)

Plugin-exported metrics are ALPHA. Config and policy metrics are registered in `namespaceresourceguarantee/metrics.go`:

- `scheduler_namespace_resource_guarantee_quota{profile, namespace, resource, unit}` — configured guarantee (`unit`: `millicore` | `byte` | `unit`).
- `scheduler_namespace_resource_guarantee_protected_priority_class_info{profile, priority_class}` — constant 1.
- `scheduler_namespace_resource_guarantee_prefilter_decisions_total{profile, namespace, tier, result}` — protected-pod guarantee checks; `result` is `allowed`, `quota_exceeded`, or `error`. `allowed` means only that the guarantee check passed.
- `scheduler_namespace_resource_guarantee_quota_exceeded_total{profile, namespace, tier, resource}` — the resource that rejected a protected-pod guarantee check.
- `scheduler_namespace_resource_guarantee_preemption_outcomes_total{profile, namespace, tier, outcome}` — PostFilter outcomes: `ineligible`, `started`, `waiting`, `not_helpful`, `no_candidate`, or `error`.
- `scheduler_namespace_resource_guarantee_victim_deletions_total{profile, namespace, tier}` — successful victim deletion requests, attributed to the preemptor namespace and tier.

Reservation metrics are registered in `nominatednodereservation/metrics.go`:

- `scheduler_nominated_node_reservations_active` — current process-local reservations.
- `scheduler_nominated_node_reservation_transitions_total{action, reason}` — bounded reservation creation, release, and stale-cleanup transitions.
- `scheduler_nominated_node_reservation_blocked_pods_total{profile}` — scheduling attempts blocked to protect a reservation.

The ServiceMonitor adds its own `namespace` target label. With the current Prometheus label-conflict behavior, the plugin's metric label named `namespace` is stored as `exported_namespace`; dashboards must query that label.

Useful upstream scheduler metrics (filter by `profile="better-scheduler"` where the label exists):

- `scheduler_plugin_execution_duration_seconds_bucket{plugin, extension_point}` — e.g. custom-plugin p95; this metric has no profile label.
- `scheduler_framework_extension_point_duration_seconds_bucket{extension_point, profile}`.
- `scheduler_schedule_attempts_total{result, profile}` — scheduled / unschedulable / error rates.
- `scheduler_pending_pods{queue}` — active / backoff / unschedulable / gated.
- `scheduler_preemption_attempts_total` — eligible preemption attempts; ordinary Pods rejected by the custom PostFilter are excluded.

There is deliberately **no** per-Pod or per-node Prometheus metric (label cardinality would explode); individual decisions live in logs.

## 4. Grafana Dashboard

Operational metrics: `releases/better-scheduler/grafana-scheduler-dashboard.json` — "Better Scheduler" (uid `better-scheduler`). It uses Prometheus for guarantee usage, policy outcomes, scheduling health, and scheduler internals.

Decision investigation: `releases/better-scheduler/grafana-decision-dashboard.json` — "Better Scheduler Decision Explainability" (uid `better-scheduler-decision-explainability`). It needs one Loki and one Prometheus datasource.

Usage model: enter a workload namespace + pod name, get the full story of its scheduling decision.

Template variables:

| Variable | Source | Purpose |
|---|---|---|
| `loki_ds`, `prom_ds` | datasource pickers | — |
| `workload_ns`, `workload_pod` | textboxes | The pod being investigated |
| `final_node` | Prometheus `label_values(kube_pod_info{namespace="$workload_ns", pod="$workload_pod"}, node)` | Where the pod actually landed; pins the breakdown panels |
| `scheduler_pod` | Loki `label_values({namespace="better-scheduler"}, pod)` | Which scheduler replica(s) to read logs from |
| `attempt` | textbox variable set manually to the scheduling attempt number you want to inspect | Scopes derived panels to one scheduling attempt |

Panels:

- **Decision Trace** (logs) — normal scheduling, rejection, and preemption decision log lines for the pod, raw across attempts. Use it to identify the attempt number you want in derived panels.
- **Final Node Scores** (bargauge) — `max by (node)` over `Calculated node's final score for pod`, filtered to `attempt = $attempt`.
- **Final Node Plugin / Extender Breakdown** (bargauges) — per-plugin/per-extender scores filtered to `node = $final_node` and `attempt = $attempt`.
- **Scored Candidate Count** (stat) — filtered to `attempt = $attempt`.
- **Actual Bound Node** (table, Prometheus `kube_pod_info`).
- **Decision Summary Counts** (bargauge) — the summary counters via `last_over_time ... | unwrap <counter>`, filtered to `attempt = $attempt`.
- **Why Nodes Dropped Out** (donut) — prefilter-pruned vs filter-rejected vs extender-rejected vs unevaluated for `attempt = $attempt`.
- **Latest Rejection Reasons** (bargauge) — `topk(12, ...)` over the rejection summary for `attempt = $attempt`, labeled `phase / plugin / reason`.
- **Rejected Nodes Drilldown** (logs) — raw `Rejected node for pod` lines for `attempt = $attempt`.
- **Preemption Candidate Scores** (bargauge) — protected-GPU preemption candidate packing scores for `attempt = $attempt`.
- **Selected Preemption Candidate** (logs) — the winning nominated node and victim set for `attempt = $attempt`.
- **Health row** (Prometheus) — Score plugin p95, extension-point p95, schedule attempts by result, pending pods by queue.

Prerequisites: scheduler logs shipped to Loki with a `namespace` label; `kube-state-metrics` (`kube_pod_info`) in Prometheus.
