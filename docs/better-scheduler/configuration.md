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
| `namespaceGuarantees` | `map[namespace]ResourceList`, required | Per-namespace shared protected guarantee. A namespace or resource missing from the map has guarantee **0** — protected pods from unlisted namespaces requesting a configured resource can never schedule |
| restrictGuaranteedPreemptionToManagedNamespaces | bool, optional | When true, explicit guaranteed Pods may preempt only lower-priority Pods from a managed namespace (a key in namespaceGuarantees). Defaults to false and does not affect semi-guaranteed Pods. |

Validation rules (config is rejected at scheduler startup otherwise):

- `protectedPriorityClassName` must be non-empty.
- When configured, semiProtectedPriorityClassName must differ from the guaranteed class and the enabled namespace list must be non-empty, unique, and covered by namespaceGuarantees.
- `namespaceGuarantees` must be non-empty and contain at least one resource guarantee overall; namespace keys must be non-empty.
- Resources must be `cpu`, `memory`, or extended scalar resources (e.g. `nvidia.com/gpu`).
- Quantities must be `>= 0`; extended resources must be integers (`"2"` ok, `"1500m"` rejected).
- cpu is accounted in millicores internally, so fractional cpu guarantees (`"500m"`) work.

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
