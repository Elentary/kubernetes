# Kubernetes 1.34.12 upgrade audit

## Base and conflict record

Source: `CLOUD-0-better-scheduler-1.33` at `dad0cb551094d02e8aa0b5d18bd10f9e8fcc3dd2`.
Destination: `CLOUD-0-better-scheduler-1.34`, rebased onto official v1.34.12
(`26c89157669bdc7e3657302bc1abde758507095b`). All 33 fork commits were replayed.
The 1.32 and 1.33 branches, existing tags and historical release ledger entries
were preserved.

On 2026-10-06 both [stable-1.34.txt](https://dl.k8s.io/release/stable-1.34.txt)
and the [published release](https://github.com/kubernetes/kubernetes/releases/tag/v1.34.12)
identified 1.34.12 as the latest stable patch (published 2026-09-23).

Five source commits needed manual conflict resolution, in nine distinct files:

| Commit on 1.33 branch | Conflicts and cause | Resolution |
|---|---|---|
| `13dd9e9b085` — rename plugin to NamespaceResourceGuarantee | Internal/v1 config registrations and validation: upstream appended DynamicResourcesArgs at the same locations. | Retain both DynamicResources and NamespaceResourceGuarantee registrations/validators. |
| `92da76b90aa` — CPU/memory guarantees | Both generated deepcopy files: core/v1 and meta/v1 import aliases collided after upstream added Duration fields. | Preserve both sets of types; regenerate deepcopy/defaults/conversions/OpenAPI with 1.34 generators. |
| `d80fab3ead1` — decision rejection logs | schedule_one.go: upstream moved framework types and renamed its framework handle. | Retain rejection logs and counts, use the new public types and the correct handle. |
| `deda3b5ee3d` — score preemption candidates | CycleState, preemption evaluator and default-preemption tests: public interfaces replaced concrete types; upstream expanded custom-victim-selection tests. | Retain upstream tests and evaluator fixes, plus our preemptor-aware scoring with PDB safety first. Migrate observability state to the public CycleState interface. |
| `c2222b4235c` — RDMA fallback | CycleState cloning and scheduling cycle: upstream uses private state fields, new interfaces and feature-aware failure nomination. | Retain upstream clone behavior; put custom flags into cloneable StateData. Preserve one retry/one snapshot and upstream failure nomination semantics in the extracted scheduling helper. |

The subsequent adaptation commit is `74ce7613814`. No upstream version of an
entire conflicted file was discarded. Semantic regressions were checked after
the textual rebase, including conflicts that Git auto-merged without noticing
outdated Go symbols. The range-diff and rebase log are retained with validation
artifacts.

## Upstream changes relevant to this fork

Sources: [1.34 changelog](https://github.com/kubernetes/kubernetes/blob/v1.34.12/CHANGELOG/CHANGELOG-1.34.md)
and the [exact 1.33.13-to-1.34.12 comparison](https://github.com/kubernetes/kubernetes/compare/v1.33.13...v1.34.12).
The changelog at the target tag ends at 1.34.11. The final patch was also inspected
through its commit diff: among other fixes, it preserves the DRA per-claim
integer-overflow fix (`8e2d815bf17`), Windows subPath protection and StatefulSet
ControllerRevision restore fix. This is not an upgrade merely to 1.34.0.

| Change | Effect and adaptation |
|---|---|
| Framework extraction: [CycleState #131887](https://github.com/kubernetes/kubernetes/pull/131887), [Status #132087](https://github.com/kubernetes/kubernetes/pull/132087), [events #132190](https://github.com/kubernetes/kubernetes/pull/132190), [NodeInfo/PodInfo #132457](https://github.com/kubernetes/kubernetes/pull/132457) | Import public interfaces from k8s.io/kube-scheduler/framework. Use NodeInfo/PodInfo/resource getters. Internal constructors, framework handles and our helpers remain in pkg/scheduler/framework. RDMA retry and dry-run flags now live in cloneable StateData; no extension to the public upstream CycleState interface is needed. |
| [PreFilter receives NodeInfos #130720](https://github.com/kubernetes/kubernetes/pull/130720) | Update plugin signature and direct test calls. Quota usage still considers the whole snapshot, not just candidate nodes. Framework test fixtures must provide the shared snapshot even with no registered PreFilter plugins. |
| [PodLevelResources beta/default-on #132999](https://github.com/kubernetes/kubernetes/pull/132999) | CPU/memory may be specified in spec.resources. Existing helper usage already accounts for this when enabled. Add regression for both incoming and bound pod-level requests and image smoke tests with empty container requests. No quota schema or limits change. |
| [QueueingHints GA #131973](https://github.com/kubernetes/kubernetes/pull/131973) | Gate is locked true in 1.34; remove any explicit false setting before startup. Preserve custom delete/scale-down hints. Upstream backoff now distinguishes unsuccessful attempts from pending work; retain preemption retry fixes and test the queue. This does not provide fairness when activeQ remains full of high-priority quota-blocked Pods. |
| [Async API dispatcher #132886](https://github.com/kubernetes/kubernetes/pull/132886), [default disabled again #134401](https://github.com/kubernetes/kubernetes/pull/134401) | The final patch defaults SchedulerAsyncAPICalls=false because of its interaction with async preemption under API-server load. Keep that default; do not infer default-on from the initial 1.34 notes. New cache constructors receive the dispatcher argument; unit fixtures pass nil. Preserve upstream APICacher support in preemption nomination cleanup. |
| [PreBindPreFlight #133021](https://github.com/kubernetes/kubernetes/pull/133021), [nomination retention #133276](https://github.com/kubernetes/kubernetes/pull/133276), [clear nomination on bind #132443](https://github.com/kubernetes/kubernetes/pull/132443) | NominatedNodeNameForExpectation is alpha/default-off. Preserve upstream binding and feature-aware failure behavior. Our RDMA helper must call newFailureNominatingInfo instead of unconditionally clearing nomination; test both modes for no-node and snapshot errors. Custom reservations are created only from our successful preemption result, not every external nomination. |
| [Same-name Pod replacement fix #138435](https://github.com/kubernetes/kubernetes/pull/138435) | Preserve UID checks and queue cleanup in handleSchedulingFailure. A recreated Pod must not inherit stale in-flight state or receive status updates from an old attempt. |
| [DRA GA #132706](https://github.com/kubernetes/kubernetes/pull/132706), [FilterTimeout #132033](https://github.com/kubernetes/kubernetes/pull/132033) | DynamicResourceAllocation is now default-on, with resource.k8s.io/v1 informers; filter search has a configurable 10-second default timeout. Keep its registration, defaults and validation when resolving conflicts. Verify deployment RBAC for ResourceClaims, ResourceSlices and DeviceClasses. DRA allocations via ResourceClaims are not converted into our extended-resource quota/packing policy. |
| [DRA-backed extended resources #130653](https://github.com/kubernetes/kubernetes/pull/130653) | DRAExtendedResource remains alpha/default-off. Enabling it requires a separate review of GPU/RDMA discovery and packing; our NodeHasResource classification uses advertised Node capacity/allocatable. |
| [matchLabelKeys selector merge #129874](https://github.com/kubernetes/kubernetes/pull/129874) | API server merges topology-spread selectors; scheduler no longer supplies the old fallback. Finish scheduling pending Pods created on 1.32 with matchLabelKeys before advancing from 1.33 to 1.34. Verify topology spread and affinity on actual workload templates. |
| [Old cache metric removed #131425](https://github.com/kubernetes/kubernetes/pull/131425) | Our 1.33 branch already migrated the bundled dashboard to scheduler_cache_size. No new rename is required; external dashboards may still use the removed metric. |

Async preemption and in-place resize were already default-on in 1.33. They are
not newly enabled by this migration. Default async API calls, DRA extended
resources and external nomination expectation are not enabled by our changes.
Release builds use upstream's pinned Go 1.26.5; go.mod requires Go 1.25.0.

## Accounting boundary outside the scheduler

Read-only inspection of the local entitlement sources found:

- Webhook `internal/accounting/accounting.go` and reclaimer
  `internal/reclaim/controller.go` call PodRequests with default options from
  component-helpers v0.32.4. That helper already reads pod-level CPU/memory in
  spec.resources. There is no evidence here of a CREATE-time pod-level accounting
  bypass just because of the old library version.
- Webhook `internal/admission/server.go` returns Allowed for non-CREATE operations.
  Consequently, UPDATE via pods/resize is not re-admitted by this webhook. The
  scheduler's PreFilter also does not re-admit a bound Pod on resize.
- These two components do not enable UseStatusResources, while the scheduler does
  when resize is enabled. During a pending downsize their accounting may diverge
  from the resources still allocated on the node. This is an existing 1.33-era
  limitation, not fixed by the 1.34 scheduler rebase.

This inspection used local source, not a verified deployed digest. Those
repositories were not modified. Before permitting live resize of protected
workloads, validate or implement a consistent admission/accounting policy for
pods/resize across the components.

## Rollout checks

1. Upgrade every API server reachable by the scheduler to 1.34 before deploying
   the 1.34 scheduler; retain the saved 1.33 scheduler digest/config for rollback.
   See [version-skew policy](https://kubernetes.io/releases/version-skew-policy/).
2. Validate the real ConfigMap, flags and RBAC. Remove SchedulerQueueingHints=false,
   leave SchedulerAsyncAPICalls at its default false unless separately tested,
   and verify all DRA informer permissions/cache synchronization. The image smoke
   test uses test-server credentials and does not validate production RBAC.
3. Keep NamespaceResourceGuarantee PreEnqueue and reservation Filter/PostBind in
   all required profiles. Keep the existing leader-election identity; do not run
   independent schedulers competing for the same profiles/nodes.
4. Check protected/semi-protected quotas (including pod-level CPU/memory), victim
   restrictions and PDB preference, ordinary-before-RDMA placement and fallback.
   Test victim deletion failures, long termination, preemptor deletion and leader
   failover; process-local reservations are still lost on restart.
5. Measure queue depths, Pod age, attempts/s, latency, errors and reservation
   cleanup with many quota-blocked high-priority Pods plus fitting lower-priority
   Pods. Repeated upstream-profile benchmarks are not production-policy load tests.
6. Verify matchLabelKeys/topology spread on old and newly created Pods. Do not
   introduce DRA-backed GPU quota policy or live resize without the separate
   checks above.

No live cluster deployment or sibling-repository changes are part of this release.

## Validation

Passed on 2026-10-06:

- Full scheduler and scheduler-app unit tests with Go 1.25.0 and the pinned release
  toolchain Go 1.26.5; component-helpers/resource tests with Go 1.26.5.
- Race tests for the custom plugins, framework state, preemption and scheduler.
- Full `go test -p 2 -timeout 25m ./test/integration/scheduler/...`, including
  preemption, nomination, both queue suites, bind, filters, scoring and serving.
- Generated deepcopy/default/conversion/OpenAPI code, shell syntax, dashboard JSON
  parsing and `git diff --check`.
- Built linux/amd64 container on Go 1.26.5. Explicit image smoke on an isolated
  1.34.12 API server with both container-level and pod-level CPU requests: bind a
  fitting Pod, reject the next by quota, delete the first and verify requeue/bind
  of the second. The test does not run a kubelet or GPU device plugin.

Reproduce image verification with:

```bash
BETTER_SCHEDULER_TEST_IMAGE=registry.k8s.io/kube-scheduler-amd64:v1.34.12-bs-v0.1 \
PATH="$PWD/third_party/etcd:$PATH" \
go test -count=1 -v ./test/integration/scheduler/betterimage
```

Custom regressions cover pod-level incoming/bound accounting, pending/completed
resize requests, interface-compatible CycleState clone isolation, failure
nomination in both feature modes, async/sync preemption, PDB-first selection,
managed-namespace victims, exhaustive ordinary-node search and bounded RDMA retry.

Performance measurements use SchedulingBasic, 5,000 nodes, 1,000 initial Pods and
10,000 measured Pods, QueueingHints enabled, Go 1.25.0 on the same host. Each group
contains three sequential samples. These are the default upstream profiles;
custom quota plugins are tested separately, not benchmarked here.

| Build | Median mean throughput, Pods/s | Median mean scheduling attempt, ms |
|---|---:|---:|
| 1.33.13 custom | 1330.288 | 2.733 |
| 1.34.12 custom, initial | 1182.848 | 2.771 |
| 1.34.12 pristine upstream | 1182.918 | 2.756 |
| 1.34.12 custom, repeated after integration suite | 1180.872 | 2.686 |

The roughly 11% throughput difference versus 1.33 is reproducible in this short
synthetic workload. The custom 1.34 build matches pristine upstream 1.34 (median
throughput within 0.2% on the repeat). The measurements do not show an additional
penalty from our fork. They do not isolate the cause of the cross-version gap:
upstream defaults and benchmark harness changed, and the one-second collector
includes a partial final interval. Scheduling-attempt latency ranges overlap.
Do not describe this as a production performance improvement or proof of equal
production capacity; compare your actual profiles and API-server load on rollout.

Validation logs, raw benchmark samples, range-diff and conflict log are retained
locally in `_output/better-scheduler-validation/1.34.12/`.
