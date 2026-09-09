# Better-Scheduler Configuration & Deployment

All values below are **generic examples** (`team-a`, `team-b`, …). The live cluster ConfigMap is the source of truth for real guarantees; do not treat numbers in this file as current production values.

## 1. Deployment Shape

- The custom `kube-scheduler` binary (built from this fork, see [operations.md](operations.md)) runs as a Deployment in its own namespace (e.g. `better-scheduler`), multiple replicas with leader election.
- The `KubeSchedulerConfiguration` lives in a ConfigMap mounted into the pods. **kube-scheduler reads its config only at startup** — after changing the ConfigMap you must roll the Deployment (`kubectl rollout restart deployment/...`).
- One scheduler **process**, two **profiles** (see below). The stock kube-scheduler must not compete for the same workloads — the `NominatedNodeReservation` store is per-process (see [architecture.md](architecture.md) §6.3).

## 2. Full Example KubeSchedulerConfiguration

```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration

leaderElection:
  leaderElect: true            # set false for a single replica
  leaseDuration: 15s
  renewDeadline: 10s
  retryPeriod: 2s
  resourceLock: leases
  resourceName: better-scheduler
  resourceNamespace: better-scheduler

clientConnection:
  qps: 100                     # kube-scheduler default is 50/100; see tuning note below
  burst: 200

profiles:
  # Profile 1: handles pods that opt in via spec.schedulerName.
  - schedulerName: better-scheduler
    plugins:
      preEnqueue:
        enabled:
          - name: NamespaceResourceGuarantee
      preFilter:
        enabled:
          - name: NamespaceResourceGuarantee
      filter:
        enabled:
          - name: NamespaceResourceGuarantee  # required when RDMA preference is enabled
          - name: NominatedNodeReservation
      postFilter:
        disabled:
          - name: DefaultPreemption          # replaced by the plugin's namespace-aware preemption
        enabled:
          - name: NamespaceResourceGuarantee
      score:
        enabled:
          - name: NamespaceResourceGuarantee
            weight: 100                       # must dominate default spreading scores
      postBind:
        enabled:
          - name: NominatedNodeReservation
    pluginConfig:
      - name: NamespaceResourceGuarantee
        args:
          apiVersion: kubescheduler.config.k8s.io/v1
          kind: NamespaceResourceGuaranteeArgs
          protectedPriorityClassName: guaranteed
          semiProtectedPriorityClassName: semi-guaranteed
          admissionAssignedTierNamespaces:
            - team-a
            - team-b
          restrictGuaranteedPreemptionToManagedNamespaces: true
          preferNonRDMANodesForGuaranteedGPU: false  # opt in after validating GPU/RDMA placement
          namespaceGuarantees:
            team-a:
              cpu: "64"
              memory: "256Gi"
              nvidia.com/gpu: "32"
            team-b:
              cpu: "48"
              nvidia.com/gpu: "24"
      # Optional but recommended: GPU bin-packing for everything this profile schedules.
      - name: NodeResourcesFit
        args:
          apiVersion: kubescheduler.config.k8s.io/v1
          kind: NodeResourcesFitArgs
          scoringStrategy:
            type: RequestedToCapacityRatio
            resources:
              - name: nvidia.com/gpu
                weight: 100
            requestedToCapacityRatio:
              shape:
                - utilization: 0
                  score: 0
                - utilization: 100
                  score: 10

  # Profile 2: replaces the stock default scheduler so the reservation store
  # covers ALL pods. A profile literally named "default-scheduler" is valid —
  # pods with empty spec.schedulerName land here.
  - schedulerName: default-scheduler
    plugins:
      preFilter:
        enabled:
          # Cap protected PriorityClasses even if a Pod is misrouted.
          - name: NamespaceResourceGuarantee
      filter:
        enabled:
          - name: NominatedNodeReservation    # REQUIRED in every profile — see caveat
      postBind:
        enabled:
          - name: NominatedNodeReservation
    pluginConfig:
      - name: NamespaceResourceGuarantee
        args:
          apiVersion: kubescheduler.config.k8s.io/v1
          kind: NamespaceResourceGuaranteeArgs
          protectedPriorityClassName: guaranteed
          semiProtectedPriorityClassName: semi-guaranteed
          admissionAssignedTierNamespaces: [team-a, team-b]
          namespaceGuarantees:
            team-a:
              cpu: "64"
              memory: "256Gi"
              nvidia.com/gpu: "32"
            team-b:
              cpu: "48"
              memory: "192Gi"
              nvidia.com/gpu: "24"
```

Key points:

- `NominatedNodeReservation` must be in `filter` + `postBind` of **both** profiles. If only the better-scheduler profile has it, default-profile pods will steal reserved nodes.
- `DefaultPreemption` is disabled only in `postFilter` of the better-scheduler profile. (Disabling it in `preEnqueue` is harmless but unnecessary — DefaultPreemption is not a PreEnqueue plugin in this fork's baseline; the plugin's own PreEnqueue handles async-preemption gating.)
- Neither custom plugin is in any default plugin set — forgetting to enable one is silent.
- Decision logs (see [observability.md](observability.md)) key off the profile name: only the profile literally named `better-scheduler` emits them (`detailedScoreLoggingProfile` in `pkg/scheduler/schedule_one.go`).

## 3. NamespaceResourceGuaranteeArgs Contract

Type: `pkg/scheduler/apis/config/types_pluginargs.go` (internal), `staging/src/k8s.io/kube-scheduler/config/v1/types_pluginargs.go` (v1). Validation: `ValidateNamespaceResourceGuaranteeArgs` in `pkg/scheduler/apis/config/validation/validation_pluginargs.go`.

| Field | Type | Semantics |
|---|---|---|
| protectedPriorityClassName | string, required | PriorityClass name identifying explicit guaranteed pods (exact match on spec.priorityClassName) |
| semiProtectedPriorityClassName | string, optional | PriorityClass for admission-assigned semi-guaranteed Pods; enables the two-tier mode |
| admissionAssignedTierNamespaces | []string, required when semi tier is set | Namespaces permitted to receive semi-guaranteed classification; each must occur in namespaceGuarantees |
| `namespaceGuarantees` | `map[namespace]ResourceList`, required | Per-namespace shared protected guarantee. A configured namespace is capped only for resources it lists; an omitted resource is uncapped. A namespace absent from the map has guarantee **0** for resources configured anywhere, so protected pods from unlisted namespaces requesting those resources cannot schedule |
| restrictGuaranteedPreemptionToManagedNamespaces | bool, optional | When true, explicit guaranteed Pods may preempt only lower-priority Pods from a managed namespace (a key in namespaceGuarantees). Defaults to false and does not affect semi-guaranteed Pods. |
| `preferNonRDMANodesForGuaranteedGPU` | bool, optional, default `false` | For explicit guaranteed GPU Pods without an RDMA request, exhaust ordinary placements without PDB violations before using RDMA nodes. Requires the plugin in PreFilter, Filter and PostFilter, with DefaultPreemption disabled. |

Validation rules (config is rejected at scheduler startup otherwise):

- `protectedPriorityClassName` must be non-empty.
- When configured, semiProtectedPriorityClassName must differ from the guaranteed class and the enabled namespace list must be non-empty, unique, and covered by namespaceGuarantees.
- `namespaceGuarantees` must be non-empty and contain at least one resource guarantee overall; namespace keys must be non-empty.
- Resources must be `cpu`, `memory`, or extended scalar resources (e.g. `nvidia.com/gpu`).
- Quantities must be `>= 0`; extended resources must be integers (`"2"` ok, `"1500m"` rejected).
- cpu is accounted in millicores internally, so fractional cpu guarantees (`"500m"`) work.

### 3.1 RDMA fallback for guaranteed GPU Pods

Set `preferNonRDMANodesForGuaranteedGPU: true` in the Better-Scheduler profile's
`NamespaceResourceGuarantee` arguments. Enable its `filter` extension as in the
example above. Startup validates the final extension list, including MultiPoint
expansion. Leave the flag false or omitted in a PreFilter-only backstop profile.

The policy applies to the configured explicit guaranteed PriorityClass when the
Pod requests `nvidia.com/gpu > 0` and `nvidia.com/rdma_shared_device_a = 0`. Effective
requests include init containers and restartable init containers. The GPU resource
does not have to appear in `namespaceGuarantees` for this preference to apply.
Semi-guaranteed, ordinary, CPU-only, GPU+RDMA and MIG-only Pods retain their existing
placement behavior.

A node is an RDMA node if either its Capacity or Allocatable advertises a positive
`nvidia.com/rdma_shared_device_a` quantity. Allocations by other Pods do not change
its classification; positive Capacity keeps it classified as RDMA when device
health temporarily reduces Allocatable to zero.

| Order | Placement |
|---|---|
| 1 | Ordinary node, without preemption |
| 2 | Ordinary node, preemption with no PDB-violating victims |
| 3 | RDMA node, without preemption |
| 4 | RDMA node, preemption with no PDB-violating victims |
| 5 | Remaining preemption candidates: fewest PDB-violating victims, then ordinary nodes, then existing GPU packing and victim tie-breakers |

An ordinary preemption with no PDB violations takes precedence over a free RDMA
node. PDB safety takes precedence over avoiding RDMA. `preemptionPolicy: Never`
skips preemption steps but still permits direct RDMA placement as a fallback.
Namespace caps, numeric priority and the managed-namespace victim restriction
remain enforced. Existing running Pods are not relocated.

Once preemption has started, the scheduler waits for the chosen node instead of
starting a second wave elsewhere. A newly free ordinary node can still be used
immediately. An invalid nomination restarts the search; binding releases the old
reservation, including when the Pod binds on a different node.

The scheduler checks all eligible nodes in each considered group. Native filtering
and preemption candidate limits cannot hide an alternative, and extender results
are included. Filter-only extenders are checked against candidate node copies with
the selected victims removed. Non-ignorable evaluation errors abort the attempt;
they are not interpreted as absence of ordinary capacity.

Fallback is one bounded retry on the same scheduler snapshot and in the same queue
attempt. Pure candidate evaluation does not delete Pods or alter nominations or
reservations. Only the chosen preemption is executed. The additional full search
can increase Filter/PostFilter latency for opted-in Pods; inspect latency and
`placement_phase` decision logs during staging validation.

## 4. Pod Opt-In and PriorityClass

Workloads opt in by setting both:

```yaml
spec:
  schedulerName: better-scheduler
  priorityClassName: guaranteed
```

- `schedulerName` alone routes the pod through the profile (getting reservation/packing behavior) without guarantee semantics.
- priorityClassName: guaranteed alone is capped by the default-profile PreFilter backstop, but it still bypasses routing and the protected preemption/reservation path; the admission webhook must prevent it.

With admission-assigned tiers enabled, the external webhook sets
semi-guaranteed for ordinary Pods that fit its conservative namespace charge,
and routes both protected classes to better-scheduler. The scheduler enforces
one shared physical cap: guaranteed plus semi-guaranteed requests cannot exceed
the namespace quota. A separate entitlement reclaimer evicts semi-guaranteed
Pods when guaranteed demand needs headroom.

Example PriorityClass:

```yaml
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: guaranteed
value: 1000000
preemptionPolicy: PreemptLowerPriority
globalDefault: false
description: >-
  Urgent priority for better-scheduler; can preempt normal jobs but remains
  capped by namespace guarantees.
```

Naming rationale (from the original design discussion): avoid `guaranteed-quota`-style names containing "quota" (implies ResourceQuota) and note that `guaranteed` intentionally echoes the tier name; the QoS class `Guaranteed` is an accepted, documented collision.

**Recommended routing webhook.** Rather than requiring users to set both fields, run a mutating admission webhook that sets `spec.schedulerName: better-scheduler` on pods whose `priorityClassName` is `guaranteed` (and that polices who may use that PriorityClass at all). The webhook is out of scope of this repo — see [architecture.md](architecture.md) §8 for the responsibility split.

## 5. Client Connection Tuning

Symptom of an undersized client rate limit (in scheduler logs):

```
request.go:...] Waited for 10.9s due to client-side throttling, not priority and fairness, request: POST:.../events
```

- kube-scheduler's effective defaults are `qps: 50, burst: 100` when `clientConnection` is omitted (the generic client-go 5/10 defaults do **not** apply).
- In large clusters with heavy event traffic, set `clientConnection.qps/burst` explicitly (e.g. 100/200) and double again only if throttling waits are actually observed.
- Remember: config change ⇒ Deployment rollout.

## 6. Log Verbosity

Decision logs need no verbosity flag (always-on Info for the `better-scheduler` profile). For everything else use the scheduler container's `--v`:

- `--v=2` — production default (includes per-candidate preemption decision lines).
- `--v=4`/`--v=5` — general scheduling decisions / verbose diagnostics.
- `--v=10` — upstream per-node scoring for **all** profiles; extremely noisy, avoid in production.
