# Better-Scheduler History (v0.1 → v0.7)

Full narrative of how the project evolved, reconstructed from git history and the working-session logs (April–July 2026). Each release lists: the problem that prompted it, what was built, and the outcome. Real cluster/workload names are genericized throughout.

Release ledger: `releases/better-scheduler/releases.csv`. Base: Kubernetes `v1.32.4` (commit `59526cd4867`), branch `CLOUD-0-better-scheduler`.

## Foundation (April 2026, pre-v0.1)

The project began with a complete architecture brief (multi-tenant GPU fairness, ~300 shared GPUs, teams as namespaces) and an already-prototyped PreFilter-only plugin. Decisions locked at this stage, most of which still stand:

- Two-tier model (guaranteed + normal); an intermediate "borrowed" tier explicitly rejected.
- Scheduler plugin, not ResourceQuota (need Pending-not-rejected semantics and native preemption flow).
- Snapshot-lister accounting, no informers, no incremental index (premature optimization avoided at ~50 GPU nodes scale).
- In-tree fork build (not a sidecar/extender); scheduler version must match the cluster.
- Admission webhook (separate system) owns priority-assignment policy and MIG rules; the scheduler owns only guarantee enforcement.
- Fragmentation accepted in v1: guarantee means guaranteed share, not instant placement. (This came back later — see v0.5.)
- A single canonical design doc kept in sync with every change (originally `SITE_SCHEDULER_DESIGN.md` → `NAMESPACE_RESOURCE_GUARANTEE_DESIGN.md` → now this doc tree).

Commits:

- `742763452d2` — rename `SiteScheduler` → `NamespaceResourceGuarantee` end-to-end (plugin, args types, registry, validation, codegen). The scheduler/profile name `better-scheduler` and the plugin name diverge intentionally: the plugin name describes the policy, the profile name the product.
- `f6fb4ef913d` — unrelated build fix: rsync container IP detection in `build/common.sh` for newer Docker engines (kept as a separate commit deliberately).

At this stage the plugin was PreFilter + EnqueueExtensions only and counted a single configured GPU resource.

## v0.1 — 2026-04-06, `57d4142defc` — release workflow

- Problem: needed repeatable self-releases of the forked scheduler to a private ECR repo.
- Built: `hack/release-better-scheduler.sh`, the runbook `docs/releases/better-scheduler.md`, and the CSV ledger. Decisions: tag scheme `v<k8s>-bs-v<sub>` with subversions independent across upstream versions; no auto-deploy step; amd64 only; rely on Docker's ECR credential helper; annotated git tags carry the image digest.
- Also produced at rollout time (outside the repo): the `KubeSchedulerConfiguration`, the `guaranteed` PriorityClass (value 1000000, `PreemptLowerPriority`), and a cluster smoke-test bundle (health, preemption, over-guarantee-Pending, namespace isolation).

## v0.2 — 2026-05-04, `f4595b426cd` — cpu/memory guarantees

- Problem: the v0.1 plugin was GPU-only (`gpuResourceName` + integer guarantees); teams also needed cpu/memory guarantees, and cpu required millicore-precision accounting.
- Built: `NamespaceResourceGuaranteeArgs` reshaped to `namespaceGuarantees: map[namespace]ResourceList` supporting `cpu`, `memory`, and extended scalar resources, each enforced independently; validation rules (non-negative, integers for extended resources); millicore accounting for cpu.
- This is the config schema still in use.

## v0.3 — 2026-05-05, `10a987bd2fc` — namespace-aware preemption

- Problem: with stock `DefaultPreemption`, victim selection ignored namespace-level fairness — a guaranteed pod's preemption cost fell on arbitrary victims (newest pods first) rather than on the namespaces borrowing the most opportunistic capacity.
- Built: the plugin took over PostFilter (`DefaultPreemption` disabled in the profile) implementing `preemption.Interface` with custom victim ordering: priority first, then namespace preemptible-usage of the node's deficient resources (heaviest borrower evicted first), then preemptor's own namespace first, then start time. Per-victim preemption events with `decisionID` added.

## v0.3.1 — 2026-05-14, `afe076663b4` (commit message just "test") — preemption decision events

- Despite the throwaway message, this commit is substantive: it introduced the classified preemption lifecycle events (`NamespaceResourceGuaranteePreemptionStarted/Waiting/NotHelpful/NoCandidate/Error` with `decisionID`/`phase`/`nominatedNode`/`victims` notes, `classifyPreemptionEvent`) replacing a single generic event, plus a large test suite (~320 test lines).
- Lesson recorded: it also demonstrates why commit messages matter — the ledger is the only place marking this as a release.

## v0.3.2 — 2026-05-19, `d8ab4e07f08` — config metrics

- Problem: no way to see the effective guarantees without reading the ConfigMap.
- Built: `metrics.go` — `scheduler_namespace_resource_guarantee_quota` and `..._protected_priority_class_info` gauges exported at plugin construction.

## v0.4 — 2026-06-09, `f072c5990b1` — NominatedNodeReservation

- Problem (the nominated-node race): after preemption, `status.nominatedNodeName` reserves nothing — while victims terminate, any other pod (especially from the other scheduler profile) could bind to the freed node, forcing the guaranteed pod to preempt again.
- Built: the `NominatedNodeReservation` plugin (Filter + PostBind) with a process-wide in-memory reservation store; the guarantee plugin's PostFilter creates/releases reservations (`syncNominatedNodeReservation`); stale reservations self-clean.
- Rollout decision made alongside: run **one** scheduler process with **two profiles** (`better-scheduler` + a profile literally named `default-scheduler` replacing the stock scheduler), because the store is per-process — the reservation Filter must cover all pods, and a separately running stock scheduler would defeat it. A mutating webhook routes guaranteed pods to the profile.
- `828f6c2f2e9` is the ledger commit recording the release.

## Interlude — 2026-07-01 — client-side throttling episode

- The scheduler logged `Waited for 10.9s due to client-side throttling ... POST .../events`. Root cause: event bursts from a very large, churny namespace (~3200 pods, a crash-looping startup probe among them) against the kube-scheduler default client limits (`qps: 50, burst: 100` — `clientConnection` was omitted from the config).
- Fix: explicit `clientConnection: {qps: 100, burst: 200}` in the ConfigMap + rollout. No code change; captured in [operations.md](operations.md) §4.2.

## v0.5 — 2026-07-02, `1f374fb3573` — GPU packing

- Problem: fragmentation bit in practice. 1-GPU guaranteed pods were spread (default scoring) one-per-8-GPU-node; since guaranteed pods can never be evicted, an 8-GPU guaranteed pod became permanently unschedulable despite plenty of free GPUs.
- Built: `Score` on the guarantee plugin — pack guaranteed GPU pods onto nodes with the most existing *guaranteed* GPU usage (`(usage + incoming) * 100 / allocatable`), deliberately scoped to `nvidia.com/gpu` only and blind to normal pods' GPU usage (normal pods can be preempted away, so they shouldn't attract guaranteed pods). Enabled at weight 100.
- Explicitly preventive: existing fragmentation can only drain naturally.
- Release incident (harmless but confusing): the v0.5 binary logs `version="v1.32.4-bs-v0.4-2-g..."` — the release script tags git *after* building, so `git describe` embeds the previous tag. Still unfixed; see [known-issues.md](known-issues.md) #1.

## Unreleased fix — 2026-07-03, `d100345e408` — bounded event notes

- Production incident: a preemption with dozens of victims produced a `PreemptionStarted` event note over 1024 characters; the API server rejected the event (`can have at most 1024 characters`), losing the observability signal.
- Built: victim lists in event notes are summarized (`pod-1,...(+N more)`) and all preemption notes pass a truncation guard. Regression test included. Shipped inside v0.6.

## v0.6 — 2026-07-03, `98bf4cff4fa` — score decision logs

- Problem: "for each pod I want to fully understand why this node was chosen — candidates, per-candidate scores, score components." Investigation confirmed upstream has no such metric and only exposes scores at process-wide V(10) — too noisy, and not scoppable to one profile via verbosity.
- Decision: emit the three score log lines (`Plugin scored node for pod`, `Extender scored node for pod`, `Calculated node's final score for pod`) at plain Info, gated on `ProfileName() == "better-scheduler"`, each with a `profile` field for log-pipeline filtering. Rejected alternatives: global `-v=10` (floods), a Prometheus metric (per-pod/per-node cardinality).
- The Grafana dashboard (`releases/better-scheduler/grafana-decision-dashboard.json`) was built alongside: Loki for decision logs (backtick-quoted LogQL matchers, `pod_extracted` after `logfmt`), Prometheus for health panels, bargauge score breakdowns pinned to the pod's actual bound node via `kube_pod_info`.

## v0.7 — 2026-07-06, `22dfb9910d3` — rejection decision logs

- Problem: scores explain the winner among feasible nodes, but not "which nodes were considered, which were infeasible and why, how many were scored".
- Design constraint taken seriously: the scheduler stops filtering early once enough feasible nodes are found, so "not feasible" ≠ "rejected" — the logs distinguish prefilter-pruned, filter/extender-rejected, and unevaluated nodes (`synthetic=true` marks synthesized PreFilter per-node lines).
- Built: `Rejected node for pod` (per node), `Scheduling rejection reason summary for pod` (aggregated), `Scheduling decision summary for pod` (per-attempt counters) — same profile gate. Explicit scope cut by the user: no new Prometheus metric. Dashboard extended with rejection panels afterwards.
- Released from a clean temporary worktree (main tree carried the uncommitted dashboard/ledger files).

## Unreleased — 2026-07-10 — attempt-aware decision logs

- Problem: per-attempt decision logs existed, but Grafana panels using `last_over_time` / `max_over_time` over a time window could silently blend multiple scheduling attempts for the same pod. In particular, rejection-reason charts could display reasons from an older attempt that no longer applied to the latest retry.
- Built: every decision log line now carries `decisionID=<pod UID>` and `attempt=<QueuedPodInfo.Attempts>`. `decisionID` matches the existing preemption-event correlation key; `attempt` lets LogQL scope derived panels to the latest scheduling attempt while keeping the raw trace multi-attempt. The dashboard gained a hidden `latest_attempt` variable and all derived Loki panels now filter on it.

## Cross-cutting: the protected → guaranteed rename

- Code and config API say "protected" (`protectedPriorityClassName`); the operational concept was renamed to **guaranteed** during initial user-facing rollout (PriorityClass `guaranteed`, "guaranteed tier"), and downstream application code migrated identifiers accordingly (e.g. `use_protected_quota` → `use_guaranteed_quota`). The code-level "protected" naming was deliberately left alone.

## Current state (as of 2026-07-09)

- Deployed: v0.7 concepts released; branch `CLOUD-0-better-scheduler` at `22dfb9910d3` + this documentation.
- Uncommitted at time of writing: ledger rows for v0.6/v0.7 in `releases/better-scheduler/releases.csv` and the Grafana dashboard JSON (see [known-issues.md](known-issues.md)).
