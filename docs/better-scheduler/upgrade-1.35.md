# Kubernetes 1.35.9 upgrade audit

## Base and conflict record

Source: `CLOUD-0-better-scheduler-1.34` at
`dc0e47bce67ab5f03fbdb5a09cfa5ae84be5046c`.
Destination: `codex/better-scheduler-1.35`, rebased onto official `v1.35.9`
(`329000efd0b26bd78155e51e4df449130f0fca00`). All 36 fork commits were replayed.
The existing 1.32, 1.33 and 1.34 branches, release tags and ledger entries were
preserved. Work was performed in a separate worktree.

On 2026-10-22, [stable-1.35.txt](https://dl.k8s.io/release/stable-1.35.txt) and the
[published release](https://github.com/kubernetes/kubernetes/releases/tag/v1.35.9)
identified 1.35.9 as the latest stable patch (published 2026-09-23).

Five source commits needed manual conflict resolution, in seven distinct files:

| Commit on the 1.34 branch | Conflict and cause | Resolution |
|---|---|---|
| `641a5fdffba` — rename plugin to NamespaceResourceGuarantee | Generated OpenAPI changed its model-name keys; upstream added GangScheduling at the same registry insertion point. | Retain upstream model-name generation and both plugin registrations. Regenerate the custom schema and its `OpenAPIModelName` method. |
| `24e45400c52` — decision logs | Upstream removed an obsolete scoring-log constant from the block where our profile constant was added. | Retain our logging profile constant and upstream removal. |
| `28be7cb38cf` — decision rejection logs | `findNodesThatFitPod` now returns batching hints and signatures in addition to nodes/diagnosis/error. | Preserve our rejection logs and all five upstream return values. |
| `f0fcb51e27c` — RDMA fallback | Framework plugin interfaces and PostFilterResult moved into the public staging module; nominated-node evaluation gained a batching hint. | Move our existing bounded-retry field with PostFilterResult; keep internal configuration validation. Apply ordinary-node preference to both nominations and node hints. |
| `74ce7613814` — 1.34 framework adaptation | Prior type migration and nomination-failure logic conflicted with the next upstream migration in scheduler and preemption tests. | Retain upstream interface definitions, tests and new nomination clearing; migrate custom types/calls and update our failure regressions. |

Textual conflict resolution alone was insufficient. The follow-up adaptation
also fixed auto-merged references to moved interfaces, updated test call
signatures, and propagated the new pod-level resize accounting option.
The adaptation commit is `584ace7ac2a`. All 48 custom test functions inherited
from the 1.34 branch and all 50 upstream test functions in touched test files
remain present.

## Upstream scheduling review

Reviewed the [1.35 changelog](https://github.com/kubernetes/kubernetes/blob/release-1.35/CHANGELOG/CHANGELOG-1.35.md),
the [exact base-to-target source comparison](https://github.com/kubernetes/kubernetes/compare/v1.34.12...v1.35.9),
and the inventory of 96 non-merge commits touching `pkg/scheduler` in that range. The changelog
inside the target tag stops at 1.35.8, so the final patch was also checked through
its source diff. It includes the Go 1.26.8 build update, a container-resize
resource-update fix and DRA per-claim integer-overflow protection.

| Change | Effect on this fork and adaptation |
|---|---|
| [Framework interface extraction #133172](https://github.com/kubernetes/kubernetes/pull/133172) | Handle, Plugin interfaces, PostFilterResult, scoring/nomination types, Extender, listers and the Parallelizer interface now live in `k8s.io/kube-scheduler/framework`. Migrate custom plugin signatures and tests. Keep concrete internal constructors and configuration validation internal. Our existing `RetryScheduling` field follows PostFilterResult into its new package; the cycle still permits only one immediate retry on the same snapshot. |
| [Opportunistic batching #135231](https://github.com/kubernetes/kubernetes/pull/135231) | Beta/default-on. The runtime requires SignPlugin from every PreFilter/Filter/PreScore/Score plugin before reusing scheduling results. NamespaceResourceGuarantee and NominatedNodeReservation intentionally do not implement it, so profiles containing either disable batching. Test this with a batchable upstream control and each custom plugin independently. Guard the RDMA path against node hints bypassing the ordinary-node phase. Other compatible profiles retain upstream batching. |
| [Nomination expectation beta #135103](https://github.com/kubernetes/kubernetes/pull/135103), [failure clearing #135007](https://github.com/kubernetes/kubernetes/pull/135007) | `NominatedNodeNameForExpectation` is default-on; API-server clearing after binding is also default-on. Unlike 1.34's optional retention, 1.35 clears failed nominations on no-node, snapshot, no-PostFilter and binding errors. Preserve this in our extracted scheduling helper and update tests. Successful preemption can still nominate its selected node; a PostFilter result remains authoritative. External nomination is not a durable reservation. Our store is populated only by our own successful preemption. |
| [Async preemption #134730](https://github.com/kubernetes/kubernetes/pull/134730), [long binding #134294](https://github.com/kubernetes/kubernetes/pull/134294), [lost wakeup #140637](https://github.com/kubernetes/kubernetes/pull/140637) | Preserve tracking of the last victim while deletion is in flight, informer-based completion checks, skipping already terminating victims, and periodic wildcard re-evaluation of gated Pods. Our ExecuteCandidate path uses the same upstream preparation functions and PreEnqueue evaluator. Keep PreEnqueue enabled wherever custom preemption is used. |
| [Async API regression #135904](https://github.com/kubernetes/kubernetes/pull/135904) | The final 1.35 patch defaults `SchedulerAsyncAPICalls=false` after a 1.35.0 API-throttling performance regression. Keep the final default. Initial-release notes are insufficient to determine the effective setting. |
| [Pod-level resize #132919](https://github.com/kubernetes/kubernetes/pull/132919) | Container-level resize is GA/locked true. Pod-level resize is a separate alpha/default-off feature dependent on NodeDeclaredFeatures and PodLevelResources. Pass `InPlacePodLevelResourcesVerticalScalingEnabled` to our PodRequests helper, matching NodeResourcesFit/cache accounting. Test pending CPU/memory downsizes and status-only scale-down queue hints with the gate on/off. This does not implement resize admission for bound Pods. |
| [DRA locked on #134452](https://github.com/kubernetes/kubernetes/pull/134452), [prioritized-list scoring #134711](https://github.com/kubernetes/kubernetes/pull/134711) | DRA core can no longer be disabled. Retain resource informers and default plugin registration. Prioritized claims now affect node scoring (DynamicResources has default weight 2); claim allocations still are not our GPU/RDMA extended-resource quota. Verify custom scheduler RBAC/cache sync. |
| [DRA binding timeout #134905](https://github.com/kubernetes/kubernetes/pull/134905), [allocation race #136567](https://github.com/kubernetes/kubernetes/pull/136567) | Preserve the new DynamicResources `bindingTimeout` argument and allocation race fixes. The timeout defaults to 10 minutes when both device binding conditions and device status are enabled. Binding conditions remain alpha/default-off. No new DRA gates are enabled by this rebase. |
| [DRA-backed resource quotas #134210](https://github.com/kubernetes/kubernetes/pull/134210), [partitionable devices #134189](https://github.com/kubernetes/kubernetes/pull/134189) | DRAExtendedResource remains alpha/default-off. Standard ResourceQuota support does not make our custom quota/packing or Node capacity classification DRA-aware. Partitionable-device ResourceSlices have an incompatible alpha schema transition; review separately if that feature is used. |
| [CSI driver presence #135012](https://github.com/kubernetes/kubernetes/pull/135012) | With the new `VolumeLimitScaling` alpha gate enabled, Filter rejects a node missing a registered CSI driver in its CSINode. The gate defaults false, so this is not a default placement change. Preserve the upstream checks and verify CSINode registration before opting in. |
| [GangScheduling #134722](https://github.com/kubernetes/kubernetes/pull/134722), [NodeDeclaredFeatures #133389](https://github.com/kubernetes/kubernetes/pull/133389), [numeric tolerations #134665](https://github.com/kubernetes/kubernetes/pull/134665) | Preserve registration and upstream filters. These features default off. Per-Pod quota/preemption/reservation is not a group-atomic guarantee; enabling gang scheduling requires separate policy and load testing. |
| [Recreated-Pod queue fix #138434](https://github.com/kubernetes/kubernetes/pull/138434), [deleted binding Pod #134157](https://github.com/kubernetes/kubernetes/pull/134157) | Preserve queue UID cleanup and removal of deleted Pods from the assumed cache. No custom identity checks were relaxed. The recreated-Pod fix was already present in the 1.34.12 branch. |
| [OpenAPI model names #131755](https://github.com/kubernetes/kubernetes/pull/131755) | Regenerate with the 1.35 generators, including the new model-name method for NamespaceResourceGuaranteeArgs. The v1 configuration shape and existing quota values remain compatible. |

The release build uses upstream's pinned Go 1.26.8 image; go.mod requires Go
1.25.0. The Go compatibility defaults advance from 1.24 to 1.25, which can affect
container CPU parallelism. Performance comparisons must control the toolchain
and GOMAXPROCS.

Upstream also replaced its rsync/build-image wrapper with a direct source bind
mount into kube-cross. Our Docker daemon sees the shared workspace but has no
`/etc/localtime` at the client's path. Adapt the timezone mount to copy the file
into `_output` first, keeping build logs in the client's timezone without
depending on the daemon's `/etc`. This is a build portability fix.

## Accounting and deployment boundary

The scheduler correctly accounts for effective container requests and, when its
alpha gate is enabled, pod-level status/allocated requests. A bound Pod's resize
does not pass through scheduler PreFilter and does not trigger relocation or our
quota preemption. Full resize admission remains a separate task for the
entitlement webhook/reclaimer. Read-only reinspection on 2026-10-22 confirmed
that local webhook validation/mutation and both webhook manifest rules are
CREATE-only; webhook and reclaimer use PodRequests with default options, without
status/allocated accounting. These are local source observations, not a check of
deployed digests. No sibling repository was modified.

Reservations remain process-local and are shared only among profiles of this
scheduler process. Restart/leader failover loses them. Nomination expectation
does not make them durable. This limitation already existed on 1.34.
Clearing the Pod's nominatedNodeName alone does not clear our reservation store:
release still follows the custom PostFilter result, binding, or stale-holder
cleanup. In particular, verify recovery when a reserved preemptor later fails
before PostFilter; an empty nomination is not itself treated as stale. This is
an existing lifecycle limitation, retained rather than redesigned in this rebase.

## Required rollout checks

1. Upgrade all API servers reachable by this scheduler to 1.35 first, per the
   [version-skew policy](https://kubernetes.io/releases/version-skew-policy/).
   Retain the 1.34 scheduler digest/configuration for rollback.
2. Check real startup flags: remove explicit false values for
   `InPlacePodVerticalScaling`, `DynamicResourceAllocation`, and
   `SchedulerQueueingHints`. Keep `SchedulerAsyncAPICalls` at its final default
   false. The startup image test does not prove production RBAC or configuration.
3. Verify informer permissions for ResourceClaims/status, ResourceSlices,
   DeviceClasses and storage objects; check cache sync and scheduling readiness.
   Keep reservation Filter/PostBind in all required profiles, custom PreEnqueue,
   and the existing leader-election identity.
4. Exercise guaranteed and semi-guaranteed quotas, pod-level CPU/memory, managed
   victim namespaces, PDB preference, GPU packing, ordinary-before-RDMA search
   and fallback. Test slow/erroring victim deletion, deleted preemptors, bind
   failure, nomination cleanup and leader restart.
5. Observe queue depth/age, scheduling attempts/latency, API throttling and
   reservation cleanup under many quota-blocked high-priority Pods. Verify the
   custom profiles report batching disabled. Do not add a SignPlugin method
   without reviewing the stateful quota/reservation contract.
6. Before allowing resize of protected workloads, implement/validate admission
   and effective-request accounting across the entitlement components. The
   container resize gate can no longer serve as the off switch in 1.35.
7. Keep new alpha DRA, gang, pod-level resize and VolumeLimitScaling gates off
   until their policies and failure modes are tested. If already using alpha DRA
   partitionable devices, review the ResourceSlice migration before upgrading.
8. Cluster upgrade prerequisite outside the scheduler: [1.35 defaults
   failCgroupV1=true](https://github.com/kubernetes/kubernetes/pull/134298).
   Check node cgroup v2 readiness before upgrading kubelets, and remove any
   [obsolete --pod-infra-container-image flag](https://github.com/kubernetes/kubernetes/pull/133779).

No live cluster deployment is part of this release.

## Validation

Passed on 2026-10-22:

- Full scheduler and scheduler-app unit tests, public framework tests and
  component-helpers/resource tests on the pinned release toolchain Go 1.26.8.
- Race tests for scheduler, framework state, preemption and both custom plugins.
- Full `go test -p 2 -timeout 25m ./test/integration/scheduler/...`: 19 packages,
  including batching, filters, nomination, gang scheduling, preemption, both
  queue implementations, scoring and serving. The image test is opt-in and is
  verified separately against the built container.
- New regressions for unsigned custom profiles disabling batching, node hints
  preserving ordinary-before-RDMA placement/preemption, failure nomination
  clearing in both feature modes, and gated pod-level resize accounting/hints.
  Existing bounded-retry, single-snapshot, quota, packing, PDB and managed-victim
  tests remain in the suite.
- Regenerated deepcopy/default/conversion/OpenAPI code, including model names;
  release script shell syntax, dashboard JSON parsing and `git diff --check`.

Logs, the rebase conflict record and range-diff are retained locally in
`_output/better-scheduler-validation/1.35.9/`.

Performance comparison: SchedulingBasic, 5,000 nodes, 1,000 initial Pods and
10,000 measured Pods, QueueingHints enabled, Go 1.26.8 and GOMAXPROCS=32 on the
same host. Each group has three sequential samples; groups ran without other
build/test jobs. These are standard upstream profiles, not custom quota/RDMA
load tests.

| Build | Mean throughput samples, Pods/s | Median throughput, Pods/s | Median mean scheduling attempt, ms |
|---|---|---:|---:|
| 1.34.12 custom | 1328.720, 1383.406, 1304.837 | 1328.720 | 2.710 |
| 1.35.9 custom | 1207.752, 1521.827, 1546.534 | 1521.827 | 2.974 |
| 1.35.9 pristine upstream | 1441.667, 1516.176, 1560.646 | 1516.176 | 3.101 |

Custom 1.35 median throughput is within 0.4% of pristine 1.35. The approximately
14.5% higher throughput than 1.34 accompanies approximately 9.7% higher mean
attempt latency; sample spread is substantial, particularly the first 1.35 runs.
There is no additional fork throughput penalty apparent in this workload.
These short measurements do not establish a production improvement, isolate the
cause of the version difference, or test quota contention. The one-second
collector includes a partial final interval. Validate the real profiles and
scheduler container CPU limits during rollout.

Reproduction (run each branch separately with no other benchmark/build jobs):

```bash
PATH=/workspaces/kubernetes-1.35/third_party/etcd:$PATH \
GOTOOLCHAIN=go1.26.8 GOMAXPROCS=32 \
go test ./test/integration/scheduler_perf/misc -run '^$' \
  -bench '^BenchmarkPerfScheduling/SchedulingBasic/5000Nodes_10000Pods_QueueingHintsEnabled$' \
  -benchtime=1x -count=3 -data-items-dir=/tmp/better-scheduler-perf
```
