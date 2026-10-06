# Kubernetes 1.33.13 upgrade audit

## Source and migration boundary

The source branch was `CLOUD-0-better-scheduler` at `73b657ffb02`, based on
Kubernetes v1.32.4 (`59526cd4867447956156ae3a602fcbac10a2c335`). The new branch is
`CLOUD-0-better-scheduler-1.33`, based on official v1.33.13
(`c029d48d28322ad0369aabdcf8b656fd3195cd30`). All 30 custom patches replayed
unchanged; the 31st patch (`f6fb4ef913d`, Docker rsync IP detection) was already
present upstream. No textual conflicts occurred. API adaptation follows the
rebase in a separate commit. Existing release tags and ledger entries retain
their original meaning and original commit IDs.

This audit compares the actual v1.32.4 and v1.33.13 source trees, not just the
v1.32.0-to-v1.33.0 release notes. Sources:

- [1.33 changelog](https://github.com/kubernetes/kubernetes/blob/v1.33.13/CHANGELOG/CHANGELOG-1.33.md)
- [exact upstream comparison](https://github.com/kubernetes/kubernetes/compare/v1.32.4...v1.33.13)
- [1.33.12-to-1.33.13 changes](https://github.com/kubernetes/kubernetes/compare/v1.33.12...v1.33.13)

The changelog shipped at the v1.33.13 tag ends at v1.33.12. The final patch was
therefore checked through its commit diff: it updates build images/toolchain to
Go 1.25.11 and fixes an endpoint-controller panic; it does not modify scheduler
code. The module's minimum Go version is 1.24; release builds use the upstream
pinned Go 1.25.11 image.

## Scheduling changes and disposition

| Upstream change | Interaction with this fork | Disposition and verification |
|---|---|---|
| [Score receives NodeInfo, #130537](https://github.com/kubernetes/kubernetes/pull/130537) | Custom GPU packing previously accepted a node name and fetched a second snapshot entry. | Adapt Score and its callers to use the supplied NodeInfo directly. Regression verifies scoring without a framework handle. |
| [Async preemption defaults to true, #130550](https://github.com/kubernetes/kubernetes/pull/130550) | PostFilter can return a nomination before victim deletion/metrics finish. PreEnqueue gates an in-flight preemptor; reservations must already protect the nominated node. | Keep upstream default and evaluator lifecycle. Run RDMA placement/PDB/exhaustive-search and event tests with both gate values; wait for actual deletion completion. Run race tests. Candidate traces are protected by mutex and used during synchronous evaluation/logging; the async victim callback does not read the deleted trace. |
| Preemption evaluator now preserves its pluginName | Older upstream evaluator hardcoded DefaultPreemption, even for custom evaluators. | Retain upstream fix: activation, event attribution and metrics use NamespaceResourceGuarantee. Preserve custom PDB-first ordering and managed-namespace victim restrictions. |
| [In-place resize defaults to true, #130905](https://github.com/kubernetes/kubernetes/pull/130905), [resize conditions, #130733](https://github.com/kubernetes/kubernetes/pull/130733) | Effective CPU/memory requests can differ from desired requests. | Existing helper already passes UseStatusResources and PodLevelResources gates. Retain upstream helper, including allocated-resource maxima and infeasible-resize conditions. New test covers pending/completed downsize, status-only queue events, and absent status resources. |
| [Resource-helper nil panic fix, #133285](https://github.com/kubernetes/kubernetes/pull/133285) | Custom quota and packing calculations call the same upstream helper. | Preserve patched helper, including nil Resources checks for containers/sidecars; exercise helper and plugin tests. |
| [Pop from backoff when activeQ is empty, #130772](https://github.com/kubernetes/kubernetes/pull/130772) | Changes retry timing, not priority ordering or quota admission. | Keep default true and upstream split between unschedulable and error backoff queues. Exercise queue unit/integration tests. This does not solve starvation while activeQ remains continuously populated. |
| [Retry after PreBind/Bind failure, #130189](https://github.com/kubernetes/kubernetes/pull/130189) | Custom schedule_one.go instrumentation and RDMA retry wrap the same scheduling cycle. | Keep upstream Done-before-PreBind and error retry behavior. Preserve bounded RDMA retry and single-snapshot reuse. Run scheduler/bind/plugin integration tests. |
| [Nomination-change rescheduling, #129058](https://github.com/kubernetes/kubernetes/pull/129058) | Intersects reservation cleanup and waiting preemptors. | Preserve upstream nomination events. Existing reservation lifecycle tests cover bind, holder removal/replacement/movement and dry-run non-mutation; RDMA tests cover in-flight waiting. |
| [NodeInfo request caching, #129635](https://github.com/kubernetes/kubernetes/pull/129635) | Victim removal during dry runs modifies cloned NodeInfo. | Preserve upstream caching and Snapshot/AddPod/RemovePod behavior; no custom mutation of cached requests. Verify packing, victims, resource helper and preemption tests. |
| [Sidecars GA, #129731](https://github.com/kubernetes/kubernetes/pull/129731), [sidecar hostPorts, #133390](https://github.com/kubernetes/kubernetes/pull/133390) | Requests and node feasibility include restartable init containers. | Retain upstream sidecar accounting and NodePorts filtering. Do not set SidecarContainers=false. |
| DRA v1beta2, prioritized requests, device taints, device-allocation race fixes | GPU guarantees currently account for extended-resource requests, not DRA ResourceClaims. | Preserve upstream DRA changes and tests. Do not reinterpret DRA devices as nvidia.com/gpu quota. No new DRA policy is enabled by this migration. |
| [StorageCapacityScoring replaces VolumeCapacityPriority, #128184](https://github.com/kubernetes/kubernetes/pull/128184), mutable CSI limits | May change storage feasibility or scoring if explicitly enabled. | Keep upstream defaults (new alpha scoring disabled). Remove obsolete VolumeCapacityPriority settings from any deployment config before rollout; do not silently enable its replacement, which prefers the opposite capacity direction. |
| Cache metric rename; [old scheduling latency metric removed, #128906](https://github.com/kubernetes/kubernetes/pull/128906) | Bundled dashboard used scheduler_scheduler_cache_size. | Switch to scheduler_cache_size. Bundled dashboards do not reference removed pod_scheduling_duration_seconds. Keep custom metric names/labels. |

SchedulerQueueingHints was already enabled by default in 1.32; it is not a new
default in this upgrade. Existing custom config schemas, tier semantics, quota
values, admission-assigned classification and RDMA preference are preserved.
Generated deepcopy/default/conversion/OpenAPI output was regenerated on the new
base and produced no additional changes.

In-place resize of already running Pods is not quota admission by this scheduler:
PreFilter only controls new placement attempts. Effective request accounting is
preserved, but admission/reclaimer policy for live resize remains owned by those
components. Reservations remain process-local and do not survive leader failover.
These existing boundaries are not expanded by the upgrade.

## Release and rollout procedure

Release tag: `v1.33.13-bs-v0.1`, platform `linux/amd64`, repository:
`767397673936.dkr.ecr.us-east-2.amazonaws.com/better-scheduler`.

The helper now embeds the intended version before building and checks container
`--version` and architecture before pushing. It loads the upstream image tar after the build removes local tags. It still creates the annotated tag
only after the image push. The ledger records the immutable digest.

Before deploying:

1. Save the currently deployed image digest and scheduler ConfigMap. Decode the
   actual config with the new binary, including all profiles and feature gates.
   Documentation examples are not production quota values.
2. Complete the API-server upgrade to 1.33 before starting the 1.33 scheduler.
   [Version-skew policy](https://kubernetes.io/releases/version-skew-policy/)
   forbids a scheduler newer than an API server it can contact.
3. Deploy the new image by digest using the existing leader-election identity.
   Do not run an independent competing scheduler for the same profiles; every
   profile needs the reservation Filter/PostBind hooks.
4. Compare scheduling attempts, queue depths, pending Pod age, scheduling latency,
   errors, preemption outcomes, quota decisions and reservation transitions with
   a pre-upgrade sample. Check ordinary, guaranteed, semi-guaranteed and RDMA
   workloads, including quota-blocked workloads that become eligible.
5. If scheduling stalls, quota rules change unexpectedly, or new crashes/errors
   appear, restore the saved scheduler image and config. A 1.32 scheduler may run
   against a 1.33 API server during this rollback; this is not a control-plane
   downgrade. Watch in-flight nominations because local reservations reset.

Publishing and local validation do not establish production rollout acceptance.
No live cluster deployment is part of this release.

## Validation results (2026-10-06)

Passed before publication:

- `go test ./pkg/scheduler/... ./cmd/kube-scheduler/app/...`, both with Go 1.24.10
  and the release toolchain Go 1.25.11.
- `go test -race` for NamespaceResourceGuarantee, NominatedNodeReservation,
  framework/preemption and pkg/scheduler.
- Full `go test -p 2 -timeout 20m ./test/integration/scheduler/...` with etcd
  3.5.24: scheduler, bind, eventhandler, extender, filters, plugins, preemption,
  queueing, former queue, QueueingHints, scoring, serving and taint suites.
- `go test k8s.io/component-helpers/resource`.
- Explicit `TestReleasedImage` against the built container and an isolated 1.33.13
  API server: first protected Pod binds, second is quota-blocked, deleting the
  first requeues and binds the second. This verifies scheduling, not kubelet/GPU
  device execution; the synthetic node has no kubelet.
- Code generation (deepcopy, defaults, conversions, OpenAPI), JSON parsing,
  shell syntax and `git diff --check`.

Reproduce the container check with:

```bash
BETTER_SCHEDULER_TEST_IMAGE=registry.k8s.io/kube-scheduler-amd64:v1.33.13-bs-v0.1 \
PATH="$PWD/third_party/etcd:$PATH" \
go test -count=1 -v ./test/integration/scheduler/betterimage
```

Performance comparison used the same host and Go 1.24.10 for both branches,
upstream SchedulingBasic with 5,000 nodes, 1,000 initial Pods and 10,000 measured
Pods, QueueingHints enabled, three sequential repetitions per branch:

| Metric | 1.32.4 custom | 1.33.13 custom |
|---|---:|---:|
| Median of mean throughput, Pods/s | 1182.056 | 1308.437 |
| Median of mean scheduling attempt, ms | 2.587 | 2.647 |

Throughput did not regress. Mean attempt latency varied over 2.536–2.740 ms in
1.32 and 2.561–2.698 ms in 1.33; the ranges overlap. These short synthetic runs
use the default upstream profile, so they do not establish production custom
plugin throughput or GPU workload performance. Custom policy behavior is covered
by the regression tests and container quota smoke test. A separate upstream queue
microbenchmark was excluded from performance conclusions because its fixture
reports missing in-flight Pods under 1.33; queue correctness is covered by the
passing unit and integration suites.

Artifacts from this run are retained locally under
`_output/better-scheduler-validation/1.33.13/`. The image remains a local build
until AWS authentication permits the release helper to push it and record an ECR
digest. No release tag or ledger row should be created before that push succeeds.
