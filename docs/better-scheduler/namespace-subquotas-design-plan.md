# Namespace Subquotas: Detailed Design and Implementation Plan

- **Status:** Implemented; staging validation pending
- **Date:** 2026-09-02
- **Primary scope:** Dynamic Namespace Entitlement admission and reclaim components
- **Scheduler scope:** Configuration-only; no Better Scheduler code or API changes in the first version

## 1. Executive Summary

The simulator currently has one namespace-level resource entitlement. That
entitlement protects the simulator as a whole, but it does not control which
types of simulator tasks consume it. A single task type can therefore occupy
the entire protected namespace capacity and prevent every other task type from
receiving a predictable share.

This design adds one level of elastic subquotas below the existing namespace
entitlement:

```text
cluster capacity
└── simulator namespace entitlement
    ├── task type A subquota
    ├── task type B subquota
    └── default subquota
```

Each child subquota receives an integer percentage of every resource configured
on the parent namespace entitlement. The percentages sum to exactly 100. A
child is guaranteed the ability to reach its floor when it has demand, but it
may also borrow unused protected capacity from its siblings. Borrowing is
work-conserving: idle child capacity is not reserved on physical nodes.

The guarantee belongs to the child aggregate, not to each individual Pod. All
automatically protected simulator Pods use the existing `semi-guaranteed`
PriorityClass. When a child below its floor has pending demand and the parent is
full, a child-aware external reclaimer evicts only reclaimable worker Pods from
siblings that are above their own floors. The existing Better Scheduler
continues to enforce the parent namespace cap and to handle normal node-local
preemption.

This is intentionally a small, queue-like extension of the existing system. It
is not a general recursive queue scheduler, not a weighted fair-share
implementation, and not a replacement for Kueue, Apache YuniKorn, or Volcano.

## 2. Problem Statement

Assume the simulator namespace has a protected entitlement of 50 CPU cores and
two task types, A and B, each configured for 50%:

- A has a 25 CPU floor.
- B has a 25 CPU floor.
- If B is idle, A may use all 50 protected CPU cores.
- If B later requests 10 CPU while A is using all 50, the system must return at
  least 10 CPU from A's borrowed portion to B.
- Reclaim must never intentionally reduce A below its own 25 CPU floor.
- If A and B together exceed 50 CPU, additional Pods may still run as ordinary,
  opportunistic workload when the cluster has spare capacity.
- Workload outside the simulator must not displace automatically protected
  simulator Pods using lower Kubernetes priority while total protected
  simulator usage remains within the parent entitlement.

The current namespace-level entitlement can protect the aggregate 50 CPU but
cannot distinguish A from B. The new design must add that distinction without
turning Better Scheduler into a general hierarchical scheduler.

## 3. Goals

The first version must provide all of the following:

1. One level of child subquotas below a configured namespace entitlement.
2. Child selection from a Pod label, initially `simulator.task-type`.
3. Percentage-based child floors derived from the parent resource limits.
4. The same percentage applied to CPU, memory, GPU, and every other resource
   explicitly configured on the parent.
5. Guaranteed access to a child's floor when that child has schedulable demand,
   subject to Pod granularity, PDBs, physical capacity, and the other explicit
   limitations in this document.
6. Borrowing of unused sibling capacity up to the parent entitlement.
7. Opportunistic use above the parent entitlement with ordinary Kubernetes
   priority.
8. Reclaim only when a child below its floor has pending demand, or when the
   existing parent-level reclaimer has parent debt to repair.
9. Worker-only eviction through the Kubernetes Eviction API.
10. Accounting of every Pod belonging to a task type, including drivers and
    Redis Pods, even though only workers are reclaimable.
11. Backward compatibility for namespaces without subquota configuration.
12. A `Shadow` mode that evaluates the child policy without enforcing it.
13. Bounded-cardinality metrics and enough structured logging to explain every
    admission and reclaim decision.

## 4. Non-Goals

The following are explicitly outside the first version:

- Arbitrarily deep or recursive quota trees.
- Weighted fair sharing of excess capacity.
- Historical-usage fairness or dominant-resource fairness.
- Periodic redistribution merely to make borrowed usage look balanced.
- Gang scheduling or whole-task admission.
- Eviction or suspension of an entire simulator task as one unit.
- Scheduler-integrated destination reservation for child reclaim.
- Dynamically changing the PriorityClass of an existing Pod.
- Automatically promoting an ordinary Pod after entitlement becomes available.
- Hot reload of subquota configuration.
- A new CRD, queue API, or database model for task types.
- Changes to the simulator task API or database schema.
- Changes to the Better Scheduler plugin API or generated Kubernetes scheduler
  configuration types.
- Production rollout as part of the first validation step.
- Choosing the real task-type names or their initial percentages in code.

## 5. Terminology

### 5.1 Parent entitlement

The existing per-namespace protected resource limit configured for Dynamic
Namespace Entitlement and enforced by `NamespaceResourceGuarantee.PreFilter`.

### 5.2 Child or subquota

A named share below one parent namespace. In the simulator use case, the child
name is the normalized value of `simulator.task-type`.

### 5.3 Floor

The minimum protected capacity that a child can reclaim when it has demand. It
is derived independently for every parent resource:

```text
floor(namespace, child, resource) =
    floor(parent_limit(namespace, resource) * child_share_percent / 100)
```

### 5.4 Protected Pod

A Pod using either the `guaranteed` or `semi-guaranteed` PriorityClass and the
configured Better Scheduler profile.

### 5.5 Child claimant

A pending protected Pod whose entire request fits within the currently unused
floor of its child. Such a Pod is allowed to cause child-level reclaim.

### 5.6 Borrower

A Pod or child usage above its own floor but still inside the parent protected
entitlement.

### 5.7 Ordinary or opportunistic Pod

A Pod that retains its non-protected PriorityClass and ordinary scheduler after
both child floor and parent protected headroom are exhausted.

### 5.8 Reclaimable Pod

A Pod matching the configured `reclaimablePodSelector`. Initially this is a
worker Pod matching `mylos.pod_type=worker`.

### 5.9 Infrastructure Pod

A driver, Redis Pod, or other non-worker Pod. Infrastructure is charged to its
child but is not eligible for external child reclaim.

## 6. Current System and Constraints

This section records the current behavior that the design extends. It is
important because the minimal solution deliberately composes existing
mechanisms instead of duplicating them.

### 6.1 Better Scheduler

`NamespaceResourceGuarantee` currently recognizes two protected tiers:

- `guaranteed`, for explicitly guaranteed Pods;
- `semi-guaranteed`, for admission-assigned entitlement Pods.

Its `PreFilter` calculates bound protected Pod requests in the incoming Pod's
namespace and rejects the scheduling cycle with
`UnschedulableAndUnresolvable` when adding the Pod would exceed an explicitly
configured namespace resource limit. The Pod remains Pending; it is not rejected
at API admission.

For a namespace present in `namespaceGuarantees`, only explicitly listed
resources are capped. An omitted resource is uncapped for that namespace. That
semantics must be preserved: child floors exist only for resources explicitly
present on the parent entitlement.

The scheduler's custom preemption path considers only victims whose numeric
priority is strictly lower than the preemptor's priority. Equal-priority
`semi-guaranteed` workers therefore cannot preempt each other through native
node-local preemption. This property is central to the selected design: the
external reclaimer becomes the sole normal authority for moving protected
capacity between children.

The scheduler knows only the namespace-level aggregate. It does not know the
task-type label, child floors, or whether a `semi-guaranteed` Pod is currently a
claimant or borrower.

### 6.2 Dynamic Namespace Entitlement webhook

The current webhook is a single active admission writer with an in-memory
pending reservation ledger. For a configured namespace, it compares protected
live requests plus local pending admission reservations with the parent limit:

- if the entire incoming request fits, it assigns `semi-guaranteed` and the
  Better Scheduler;
- otherwise it leaves the Pod ordinary;
- explicit `guaranteed` Pods are preserved and charged;
- direct manual use of `semi-guaranteed` is rejected unless backed by the
  webhook's admission reservation;
- dry-run requests do not mutate accounting;
- ordinary classification is conservative while informer caches are warming.

The webhook uses Pod resource requests, not telemetry usage. Its pending ledger
prevents a concurrent admission burst from granting the same remaining
headroom more than once.

### 6.3 Namespace Entitlement reclaimer

The current external reclaimer serializes reconciliation by namespace. It
computes parent protected usage, watches for existing parent overage or a
pending `guaranteed` Pod, and evicts newest `semi-guaranteed` Pods until the full
parent resource debt is covered.

It already has several properties that must be retained:

- eviction through `policy/v1` rather than direct deletion;
- a UID precondition to avoid deleting a replacement Pod with the same name;
- PDB handling, including retry after API server `429 Too Many Requests`;
- all-resource-vector planning;
- no destination-node reservation;
- no claim that a successful eviction guarantees placement of the pending Pod.

The current planner is not child-aware and can consider any
`semi-guaranteed` Pod. The new design narrows victims to workers and protects
every child's floor.

### 6.4 Simulator Pod metadata

The simulator will place a new `simulator.task-type` label on every Pod belonging
to a task, including the driver, Redis, and workers. This propagation is an
external prerequisite and is not implemented by this design change.

The existing `mylos.pod_type=worker` label is used to recognize reclaimable
workers. The worker selector remains configurable rather than being hardcoded
in a generic entitlement component.

Simulator TaskPriority is separate from resource entitlement. Inside the parent
protected quota, all automatically protected workers use the same
`semi-guaranteed` PriorityClass. TaskPriority may continue to influence
application-side launch order and the ordinary priority retained above the
parent, but it cannot bypass child quota accounting.

## 7. Decisions Made

The following decisions are final for the first version.

| Area | Decision |
|---|---|
| Hierarchy depth | Exactly one level: namespace parent to task-type child. |
| Child identity | A new Pod label, initially `simulator.task-type`. |
| Missing or unknown label | Map to a configured default child; normalize the label in `Enforce`. |
| Share representation | Integer percentage from 1 through 100. |
| Share validation | All child shares sum to exactly 100. |
| Resource dimensions | One percentage applies to every explicitly configured parent resource. |
| Rounding | Round each child floor down to the resource's base unit; the remainder is lendable. |
| Consumption measure | Kubernetes Pod requests, not observed CPU/GPU utilization. |
| Child accounting scope | All active bound Pods, regardless of PriorityClass, plus protected pending reservations. |
| Ordinary pending Pods | Do not reserve child capacity. |
| Boundary Pod | Protected by its child floor only if the entire request fits in every child resource floor. |
| Protected child tier | The child guarantee is aggregate; automatically protected Pods use `semi-guaranteed`. |
| Borrowing | Allowed up to the parent entitlement. |
| Above-parent use | Allowed as ordinary opportunistic workload. |
| Excess fairness | First-come/work-conserving; no weighted fair sharing and no periodic rebalance. |
| Reclaim trigger | Actual pending under-floor demand, existing parent debt, or pending explicit infrastructure guarantee. |
| Reclaim unit | Individual worker Pod only. Never an entire task. |
| Victim interface | Kubernetes Eviction API with PDB and UID precondition. |
| Victim selector | Configurable Kubernetes selector; initially `mylos.pod_type=worker`. |
| Victim ordering | Newest eligible worker first; UID is the deterministic tie-breaker. |
| Claimant ordering | Oldest eligible pending Pod across children; UID is the deterministic tie-breaker. |
| Plan completeness | Do not start a plan unless the selected victim set covers the entire resource debt. |
| Driver and Redis | Count against their child, never external-reclaimer victims, and preserve explicit `guaranteed`. |
| Guaranteed worker | It may not bypass child policy; downgrade automatically. |
| Guaranteed-worker fallback | Configurable ordinary PriorityClass and scheduler; initially `simulator-workers-high` and `default-scheduler`. |
| Scheduler implementation | No Better Scheduler code/API changes in version one. |
| Reclaimer placement safety | Keep the existing external, non-placement-safe design. |
| Admission mutability | Tier is chosen at Pod creation and never changed for that Pod. |
| Shadow mode | Shadow only the child layer; keep existing parent entitlement enforcement active. |
| Configuration reload | Static; changes require rollout. |
| First environment | Validate in the staging simulator namespace before production. |
| Initial child names/shares | Operational input; not hardcoded or selected by this implementation. |

## 8. Core Policy Invariants

The implementation must preserve these invariants.

### 8.1 Parent safety

The Better Scheduler remains the scheduling-time authority for the parent
protected limit. A new child claimant may be admitted as `semi-guaranteed` even
when the parent is full, but it cannot bind until protected parent usage falls
within the parent limit.

### 8.2 Child floor safety

The external reclaimer must not intentionally select a worker victim if removing
that whole Pod would reduce the victim's child below its floor in any configured
resource dimension.

Formally, for every configured resource `r`, victim `v` from child `c` is safe
only if:

```text
scheduled_usage(c, r) - request(v, r) >= floor(c, r)
```

The check is repeated against the updated hypothetical usage after every
victim added to a plan.

### 8.3 Demand-driven reclaim

Borrowed capacity is not reclaimed merely because a sibling is below its floor.
The sibling must have an eligible pending Pod. Idle floors remain available to
borrowers.

### 8.4 Equal protected worker priority

Workers inside the parent share the `semi-guaranteed` PriorityClass. Native
Kubernetes preemption cannot use one equal-priority worker to evict another.
Child transfer is therefore driven by the child-aware reclaimer.

### 8.5 No hidden TaskPriority bypass

A reclaimable worker arriving with explicit `guaranteed` must enter the same
child/parent decision as other workers. It is converted to `semi-guaranteed`
when protected admission is allowed and to the configured ordinary fallback
when it is not.

### 8.6 Infrastructure exception

Explicitly `guaranteed` non-worker infrastructure is deliberately exempt from
worker downgrade. This is an accepted exception to strict child floor safety:
because the current scheduler is not child-aware, a higher-priority
`guaranteed` driver or Redis Pod may use node-local preemption to evict a
`semi-guaranteed` worker even when the worker's child is at its floor. The
external reclaimer itself must never make that choice.

### 8.7 Parent overflow remains ordinary

A Pod that fits neither its child floor nor parent protected headroom must not
be routed to the Better Scheduler as an ordinary Pod. It keeps, or is restored
to, an ordinary PriorityClass and the configured fallback scheduler, so it
retains normal Kubernetes scheduling and preemption behavior.

## 9. Configuration Contract

The webhook and reclaimer receive the same optional subquota policy. A proposed
wire shape is:

```yaml
apiVersion: better-scheduler.avride.ai/v1alpha1
kind: NamespaceEntitlementWebhookConfiguration

schedulerName: better-scheduler
guaranteedPriorityClassName: guaranteed
semiGuaranteedPriorityClassName: semi-guaranteed

namespaces:
  simulator-staging:
    cpu: "50"
    memory: "200Gi"
    nvidia.com/gpu: "8"

subQuotas:
  simulator-staging:
    mode: Shadow
    labelKey: simulator.task-type
    defaultSubQuota: other
    reclaimablePodSelector: mylos.pod_type=worker
    guaranteedWorkerFallback:
      priorityClassName: simulator-workers-high
      schedulerName: default-scheduler
    shares:
      type-a: 40
      type-b: 40
      other: 20
```

The values in this example are illustrative. They are not the agreed staging or
production allocation.

### 9.1 Validation rules

Configuration loading must fail before readiness if any of these checks fails:

1. Every `subQuotas` namespace exists in `namespaces`.
2. `mode` is exactly `Shadow` or `Enforce`.
3. `labelKey` is a valid non-empty Kubernetes label key.
4. `defaultSubQuota` is non-empty and exists in `shares`.
5. Every child name is a valid Kubernetes label value.
6. Every share is an integer in `[1, 100]`.
7. Shares sum to exactly 100 using overflow-safe arithmetic.
8. `reclaimablePodSelector` parses successfully and is non-empty.
9. Fallback `priorityClassName` is non-empty and is neither the configured
   `guaranteed` nor `semi-guaranteed` class.
10. Fallback `schedulerName` is non-empty and differs from the configured Better
    Scheduler name.
11. Every configured parent resource quantity remains non-negative and every
    extended scalar resource remains integral under the existing rules.

### 9.2 Backward compatibility

- If `subQuotas` is absent, both components preserve their current behavior.
- A namespace may be parent-managed without enabling child management.
- Scheduler configuration contains no child fields.
- Existing root entitlement fields retain their current schema and meaning.
- Unknown YAML fields should continue to follow the components' current parser
  behavior; the new validation must not silently accept an invalid known field.

### 9.3 Configuration consistency

The parent limits and subquota policy in the webhook and reclaimer are one
logical configuration and must be deployed together. A mismatch is unsupported
because the webhook would admit according to one floor while the reclaimer
would evict according to another.

At minimum, both components must log a deterministic digest of the effective
namespace/subquota policy at startup and expose it through their existing
configuration observability. Deployment validation must compare the two
effective policies before switching a namespace to `Enforce`.

## 10. Floor Calculation

For each configured parent resource:

- CPU uses millicores.
- Memory uses bytes.
- Extended scalar resources, including GPU, use whole units.

For parent value `P` and integer child percentage `s`:

```text
F = floor(P * s / 100)
```

Multiplication must be overflow-safe. The implementation may divide before
multiplying or use a wider/integer-safe intermediate, but it must not use
floating point.

Because every floor rounds down:

```text
sum(child floors) <= parent limit
```

The difference is unassigned protected capacity. It may be borrowed by any
child through parent headroom, but no child can reclaim it as part of its floor.

Example for three GPUs and two 50% children:

- each floor is one GPU;
- the third GPU is lendable;
- whichever child obtains it first may use it;
- the other child cannot reclaim that third GPU merely to equalize borrowed
  usage.

## 11. Child Identity and Label Normalization

The effective child is resolved as follows:

1. Read the configured `labelKey` from the incoming or observed Pod.
2. If the value is present in `shares`, use that value.
3. If the label is absent or the value is unknown, use `defaultSubQuota`.

In `Enforce`, the webhook patches an absent or unknown label to the canonical
default value. This provides stable accounting for the reclaimer and prevents
raw arbitrary values from creating unbounded metric cardinality.

In `Shadow`, the webhook uses the effective default for calculation and metrics
but does not patch the label. Shadow metrics must label such Pods with the
configured default child and record the normalization reason separately as
`missing` or `unknown`; they must never expose the raw unknown value as a metric
label.

The default child is a real child. Its percentage is part of the required 100%
sum and its Pods may borrow or be reclaimed exactly like any other child.

## 12. Accounting Model

### 12.1 Definitions

For namespace `n`, child `c`, and resource `r`, maintain these logical values:

```text
scheduled_child[n,c,r]
    Requests of active bound Pods in c, across all PriorityClasses.

pending_protected_child[n,c,r]
    Requests of active unbound guaranteed/semi-guaranteed Pods in c.

local_pending_child[n,c,r]
    Admission reservations created by this webhook process but not yet observed
    by the informer.

committed_child[n,c,r]
    scheduled_child + pending_protected_child + local_pending_child.

committed_parent_protected[n,r]
    Requests of all active protected Pods observed for the namespace plus local
    pending protected admission reservations.
```

An ordinary unbound Pod is demand, not consumption or a protected reservation,
and therefore contributes zero to `committed_child` until it binds.

### 12.2 Active Pod definition

A Pod is active for accounting when all of these are true:

- it is not being deleted;
- its phase is neither `Succeeded` nor `Failed`;
- it belongs to a configured namespace.

The implementation must transition an observed Pod between scheduled,
protected-pending, and uncounted states on informer updates without double
charging.

### 12.3 Resources included

Only resources explicitly configured on the parent namespace are calculated.
The request vector must use Kubernetes `PodRequests` semantics so that init
containers and Pod overhead are treated consistently with scheduling.

### 12.4 Pending admission lifecycle

The existing single-writer ledger remains authoritative for admissions that the
informer has not observed yet:

1. Classification creates at most one reservation for the admission key.
2. A retry or mutating-webhook reinvocation returns the same effective decision
   without duplicating the charge.
3. When the informer observes the Pod, the local charge is removed and the Pod
   enters the appropriate observed accounting bucket.
4. If the Pod is never observed, the reservation expires after the configured
   pending TTL and increments the existing expiration diagnostics.
5. Dry-run calculates the decision but creates no reservation.

Named-Pod reservations transfer to informer accounting when the Pod is observed.
For generated-name Pods, retain the existing admission-UID key strategy through
`pendingTTLSeconds`; because the final name cannot be correlated to the
admission UID, that reservation may temporarily overlap the observed Pod charge.

### 12.5 Why protected pending Pods reserve the child

Without protected pending reservations, several Pods admitted concurrently
while a child is below its floor could all receive `semi-guaranteed` and together
claim more than the floor. Counting them preserves the fit-based boundary rule.

Ordinary pending Pods are excluded because they have no protected admission and
consume no node resources. Counting them would let an arbitrarily large
ordinary queue block the child indefinitely.

## 13. Admission Decision Algorithm

### 13.1 High-level flow

For each Pod CREATE request:

```text
if namespace is not parent-managed:
    preserve existing behavior

resolve optional subquota policy

if policy is absent:
    use legacy root entitlement behavior

resolve effective child

if mode is Shadow:
    calculate child-policy result with isolated, non-enforcing shadow state
    execute legacy root entitlement behavior
    emit shadow result

if mode is Enforce:
    execute child-aware classification below
```

### 13.2 Explicit guaranteed non-worker

If the Pod does not match `reclaimablePodSelector` and arrives with
`guaranteed`:

- preserve `guaranteed` and the Better Scheduler;
- charge it to parent protected accounting;
- charge it to its effective child;
- normalize its task-type label if needed;
- do not subject it to child or parent fit classification.

This rule covers the agreed driver/Redis behavior.

### 13.3 Reclaimable worker classification

A worker's incoming PriorityClass does not grant entitlement. In particular,
incoming `guaranteed` must not short-circuit the following checks.

Let `q[r]` be the incoming request vector.

```text
child_fits = true when, for every configured resource r:
    committed_child[n,c,r] + q[r] <= floor[n,c,r]

parent_fits = true when, for every configured resource r:
    committed_parent_protected[n,r] + q[r] <= parent_limit[n,r]
```

The decision table is:

| Condition | Final tier | Final scheduler | Reason |
|---|---|---|---|
| `child_fits` | `semi-guaranteed` | Better Scheduler | `child_headroom_available` |
| not child fit, but `parent_fits` | `semi-guaranteed` | Better Scheduler | `parent_borrow_available` |
| neither fits, ordinary input | Preserve original ordinary class | Configured fallback scheduler | `parent_and_child_exhausted` |
| neither fits, incoming guaranteed worker | Configured fallback class | Configured fallback scheduler | `guaranteed_worker_downgraded` |
| cache not ready, ordinary input | Preserve ordinary scheduling | Preserve ordinary scheduler | `cache_not_ready` |
| cache not ready, incoming guaranteed worker | Configured fallback class | Configured fallback scheduler | `cache_not_ready_worker_downgraded` |

`child_fits` intentionally takes precedence over `parent_fits`. If the child
fits but the parent is full, the resulting `semi-guaranteed` Pod becomes a
pending claimant. Better Scheduler blocks it at the parent cap, and the
reclaimer decides whether safe sibling excess exists.

### 13.4 Other ordinary managed Pods

Ordinary non-worker Pods still belong to a child and count when bound. Unless
they are explicit guaranteed infrastructure, apply the same child-then-parent
fit rule. They may receive `semi-guaranteed`, but they remain non-reclaimable
unless they match the configured worker selector.

This means non-worker consumption can make a child unable to lend enough
capacity. That is intentional: the system must never delete driver/Redis merely
to satisfy a worker floor.

### 13.5 Ignored PriorityClasses

The legacy `ignoredPriorityClassNames` check must not allow a simulator worker
to bypass child policy. In an `Enforce` subquota namespace:

- a low/normal/high ordinary worker may be promoted to `semi-guaranteed` when
  child or parent capacity fits;
- if neither fits, its original ordinary PriorityClass remains;
- only the explicit guaranteed non-worker exception bypasses fit
  classification.

### 13.6 Manual semi-guaranteed protection

Keep the existing validate-after-mutate handshake:

- direct `semi-guaranteed` is accepted only if the same admission is backed by
  the webhook's pending decision;
- a reinvoked mutation remains idempotent;
- protected Pods must use the configured Better Scheduler;
- a user cannot obtain child or parent entitlement simply by spelling the
  protected PriorityClass in the submitted Pod.

### 13.7 Immutable decision consequences

The webhook never changes an existing Pod's tier after creation. Therefore:

- an ordinary Pod does not automatically become protected when another Pod
  exits;
- a `semi-guaranteed` borrower remains protected if its sibling demand later
  disappears;
- future admissions see the new accounting state and may receive a different
  decision;
- deleting and recreating a Pod causes a fresh admission decision.

This best-effort behavior is accepted for version one.

## 14. Reclaim Planning Algorithm

### 14.1 Reconciliation scope

Retain one serialized work queue key per namespace. Pod add, update, and delete
events enqueue the namespace. No cross-namespace child reclaim is introduced.

For a subquota-enabled namespace, each reconciliation builds:

- scheduled protected parent usage;
- scheduled all-tier usage per child;
- pending explicit guaranteed infrastructure Pods;
- pending `semi-guaranteed` Pods that may be child claimants;
- eligible scheduled semi-guaranteed worker victims.

### 14.2 Target selection

Reconciliation handles at most one pending target at a time.

Priority order:

1. Repair existing scheduled parent protected overage, if present.
2. Otherwise select the oldest pending explicit `guaranteed` non-worker Pod.
3. Otherwise select the oldest pending `semi-guaranteed` Pod for which the
   entire request fits its floor relative to current scheduled child usage.

For targets in the same category:

- earlier `creationTimestamp` wins;
- lexicographically smaller UID is the deterministic tie-breaker.

A pending guaranteed Pod matching the reclaimable worker selector is a policy
violation in `Enforce`: the webhook should have downgraded it. The reclaimer
must report it rather than treating it as a privileged infrastructure target.

### 14.3 Claimant eligibility

For pending semi-guaranteed target `t` in child `c`:

```text
claimant = true when, for every configured resource r:
    scheduled_child[c,r] + request(t,r) <= floor[c,r]
```

Only current scheduled usage is used here. Other pending Pods are demand, not
already consumed capacity. Because one target is processed per reconciliation,
the oldest claim progresses first; after it binds, the next reconciliation sees
the updated scheduled usage.

This also permits a Pod originally admitted as a borrower to become an effective
claimant later if churn leaves its child below the floor. Its Kubernetes tier
does not change; only the aggregate queue state changes.

### 14.4 Parent debt

For a pending target:

```text
debt[r] = max(
    scheduled_parent_protected[r] + request(target,r) - parent_limit[r],
    0,
)
```

If every debt component is zero, no external eviction is needed. The scheduler
may bind the protected Pod directly or preempt lower-priority ordinary workload
on a feasible node.

For existing parent overage:

```text
debt[r] = max(
    scheduled_parent_protected[r] - parent_limit[r],
    0,
)
```

### 14.5 Victim eligibility

A victim candidate must satisfy all of these conditions:

1. It is active and bound.
2. It uses the configured `semi-guaranteed` PriorityClass.
3. It uses the configured Better Scheduler.
4. It matches `reclaimablePodSelector`.
5. Its child is resolved through the same normalization rules as admission.
6. Its request overlaps at least one currently positive debt resource.
7. Removing the entire Pod leaves its child at or above its floor for every
   configured resource.
8. It is not already terminating.

Guaranteed Pods, drivers, Redis Pods, ordinary Pods, and semi-guaranteed
non-workers are never external-reclaimer victims.

### 14.6 Victim ordering and selection

Sort eligible candidates by:

1. newest creation timestamp first;
2. lexicographically larger UID first as a stable tie-breaker.

Walk the candidates in order. For each candidate:

1. Re-evaluate floor safety against the hypothetical child usage after all
   already selected victims.
2. Skip it if it no longer has safe releasable capacity.
3. Skip it if it contributes nothing to the remaining debt vector.
4. Otherwise append it, subtract its requests from both the remaining debt and
   hypothetical child usage, and stop once every debt component is covered.

The plan is executable only when the selected set covers the complete debt
vector. If safe workers cannot cover every resource, return an
`insufficient_safe_reclaimable_usage` result and select no victims.

This is all-or-nothing planning, not transactional eviction. Kubernetes has no
atomic multi-Pod Eviction API. Once execution starts, an early victim may be
accepted and a later victim may fail due to a concurrent PDB or API change. The
controller must stop on error and reconcile the new observed state rather than
assuming rollback.

### 14.7 Eviction execution

For every planned victim:

- send a `policy/v1` Eviction;
- include the observed Pod UID as a delete precondition;
- do not use direct delete as a fallback;
- stop the current execution on the first error;
- on `429 Too Many Requests`, preserve the current delayed retry behavior;
- rely on informer events to trigger the next reconciliation after accepted
  evictions.

### 14.8 No destination reservation

The reclaimer does not identify or reserve a destination node. It reasons only
about entitlement debt. After victims terminate, Better Scheduler independently
finds a feasible node.

Consequences:

- the target may remain Pending because of affinity, topology, taints, device
  fragmentation, or a resource not represented by the entitlement vector;
- another eligible protected Pod may consume the released parent headroom;
- a reclaim may be safe but fail to produce useful placement;
- repeated failures require operational visibility, not speculative additional
  evictions.

This risk is explicitly accepted to keep the first version small.

## 15. End-to-End Behavior

### 15.1 Child uses its floor

Given parent CPU 50 and A/B floors of 25:

- A uses 15.
- A submits a 10 CPU Pod.
- `15 + 10 <= 25`, so the Pod receives `semi-guaranteed` through child
  headroom.
- No sibling borrowing is involved.

### 15.2 Child borrows unused sibling capacity

- A uses 25.
- B uses 0.
- A submits another 15 CPU Pod.
- It does not fit A's floor, but total protected parent usage becomes 40, which
  fits 50.
- The Pod receives `semi-guaranteed` as a parent borrower.
- No reclaim occurs because B has no pending demand.

### 15.3 Sibling asks for borrowed capacity back

- A uses 50 protected CPU.
- B uses 0.
- B submits a 10 CPU Pod.
- The Pod fits B's 25 CPU floor and is admitted as `semi-guaranteed` even though
  the parent is full.
- Better Scheduler keeps B Pending because `50 + 10 > 50`.
- Reclaimer calculates 10 CPU of parent debt.
- Only A workers whose removal leaves A at or above 25 are eligible.
- Reclaimer selects newest eligible A workers until at least 10 CPU is covered.
- After the victims terminate, B can schedule inside the parent entitlement.

### 15.4 Parent is full and requesting child is already at its floor

- A uses 25 and B uses 25.
- A submits another Pod.
- The Pod fits neither A's floor nor parent protected headroom.
- It remains ordinary and uses the configured fallback scheduler.
- It may run if the cluster has spare capacity.
- A future B floor claim does not require entitlement reclaim from this Pod;
  protected B can use normal priority preemption against lower-priority ordinary
  Pods when node placement allows it.

### 15.5 Ordinary Pod survives later headroom

- A Pod was admitted ordinary because both child and parent were full.
- Another protected Pod later exits.
- The existing ordinary Pod is not promoted in place.
- A newly created Pod may receive `semi-guaranteed` using the new headroom.

This is the accepted admission-time immutability gap.

### 15.6 Boundary Pod does not fully fit

- A's CPU floor is 25.
- A currently uses 24.
- A submits a 4 CPU Pod.
- `24 + 4 > 25`, so it is not a child-floor claim.
- It may still receive `semi-guaranteed` through free parent headroom.
- If the parent is full, it remains ordinary even though A is one CPU below its
  floor.

The floor is therefore request-granular, not continuously divisible.

### 15.7 Multi-resource boundary

If a Pod fits the child CPU floor but exceeds the child GPU floor, it is not a
child-floor claim. It can be protected only by fitting the parent vector. Every
fit and victim-safety check is an all-resource conjunction.

### 15.8 Non-reclaimable infrastructure dominates a child

If driver/Redis requests consume most or all of a child floor:

- they count against the child;
- new workers may be borrowers or ordinary;
- the reclaimer cannot delete the infrastructure;
- another child's guarantee may remain temporarily unattainable if safe worker
  excess is insufficient.

This is preferred over deleting task infrastructure.

## 16. Priority and Preemption Semantics

Resource entitlement and Kubernetes priority remain separate concepts.

### 16.1 Inside the simulator parent

- Automatically protected workers are `semi-guaranteed`.
- Equal semi-guaranteed workers cannot natively preempt one another.
- The reclaimer transfers child capacity by eviction and protects sibling
  floors.
- Guaranteed infrastructure is numerically above semi-guaranteed workers and
  retains native preemption capability.

### 16.2 Outside the simulator parent

- Workload with numeric priority below `semi-guaranteed` may be preempted by a
  protected simulator Pod when native node feasibility allows it.
- Workload with higher numeric priority, such as the existing development or
  system-critical classes, remains able to preempt simulator Pods according to
  Kubernetes priority rules.
- Namespace entitlement is not an absolute override of global priority.

### 16.3 Why under-floor workers do not use `guaranteed`

Giving under-floor workers the real `guaranteed` PriorityClass would let native
node-local preemption select any lower-priority semi-guaranteed sibling on a
candidate node. The current scheduler has no child policy and could reduce the
victim sibling below its floor.

Preventing that would require duplicating child configuration in Better
Scheduler and modifying victim eligibility, scheduler APIs, validation,
generated code, and preemption tests. The selected aggregate-guarantee model
avoids those changes and has one clear child-reclaim authority.

## 17. Shadow and Enforce Modes

### 17.1 Shadow

`Shadow` applies only to the new child layer:

- run effective-child resolution;
- calculate child floors and child/parent fit;
- calculate whether a worker would be downgraded;
- calculate child-aware reclaim targets and victims;
- maintain an isolated hypothetical pending ledger so concurrent Shadow
  decisions do not repeatedly consume the same simulated child headroom;
- emit logs and metrics for the hypothetical result;
- do not normalize task-type labels;
- do not change a Pod because of child policy;
- do not issue child-policy evictions.

The existing root Dynamic Namespace Entitlement remains active. Root admission
mutations and existing root reclaim behavior continue normally.

The hypothetical Shadow ledger must never feed the enforced root accounting or
change an admission response. It is reconciled or expired independently and is
allowed to reset on process restart because Shadow has no enforcement authority.

### 17.2 Enforce

`Enforce` activates:

- canonical label normalization;
- child-first admission decisions;
- guaranteed-worker downgrade;
- child-aware target selection;
- worker-only, floor-safe eviction.

Mode changes require a component rollout.

## 18. Observability

Observability must answer four questions quickly:

1. How much is each child guaranteed?
2. How much is it using and reserving?
3. Which child is borrowing?
4. Why was a Pod protected, left ordinary, or involved in reclaim?

### 18.1 Webhook metrics

Add bounded metrics equivalent to:

```text
namespace_entitlement_webhook_subquota_resource_floor
  {namespace, subquota, resource, unit}

namespace_entitlement_webhook_subquota_resource_scheduled_usage
  {namespace, subquota, resource, unit}

namespace_entitlement_webhook_subquota_resource_pending_usage
  {namespace, subquota, resource, unit}

namespace_entitlement_webhook_subquota_resource_borrowed_usage
  {namespace, subquota, resource, unit}

namespace_entitlement_webhook_subquota_decisions_total
  {namespace, subquota, mode, tier, reason, action}

namespace_entitlement_webhook_subquota_label_normalizations_total
  {namespace, reason}
```

`borrowed_usage` is `max(committed_child - floor, 0)` per resource. Metric
labels must use only configured child values. Pod name, task ID, user, raw
unknown task type, and node name are prohibited as Prometheus labels.

### 18.2 Reclaimer metrics

Add bounded metrics equivalent to:

```text
namespace_entitlement_reclaimer_subquota_debt
  {namespace, target_subquota, resource, unit}

namespace_entitlement_reclaimer_subquota_plans_total
  {namespace, mode, reason, result}

namespace_entitlement_reclaimer_subquota_evictions_total
  {namespace, victim_subquota, result}

namespace_entitlement_reclaimer_subquota_policy_violations_total
  {namespace, reason}
```

Important results include no reclaim needed, existing parent overage, pending
guaranteed infrastructure, pending child claimant, oversized request,
insufficient safe worker usage, PDB rejection, stale UID, and eviction accepted.

### 18.3 Structured logs

Admission logs should contain:

- namespace and effective child;
- label normalization reason;
- mode;
- incoming and final PriorityClass/scheduler;
- request vector;
- child floor and committed child vector;
- parent limit and committed protected vector;
- `child_fits`, `parent_fits`, decision reason, and whether a patch was emitted;
- dry-run status and admission UID.

Reclaim logs should contain:

- namespace, target Pod reference, and target child;
- plan reason and debt vector;
- each victim Pod reference and victim child;
- victim child usage before and after hypothetical removal;
- rejected-candidate reason;
- execution result.

High-cardinality identities belong in logs, not metrics.

### 18.4 Dashboard

Extend the existing entitlement dashboard with a child section:

- stacked scheduled usage by subquota;
- protected pending reservations by subquota;
- an unfilled dashed floor reference per selected child/resource;
- borrowed usage;
- Shadow versus Enforce admission decisions;
- reclaim plans and eviction outcomes;
- missing/unknown label normalization count;
- zero must remain visible so idle children and unused floors are obvious.

## 19. Failure Modes and Safe Behavior

### 19.1 Invalid configuration

The component fails startup and never becomes ready. It must not silently drop
an invalid child or renormalize percentages.

### 19.2 Webhook unavailable

Retain the existing fail-closed admission webhook policy. The API request fails
rather than allowing an unclassified protected Pod to bypass accounting.

### 19.3 Informer cache not ready

- Do not automatically grant semi-guaranteed entitlement.
- An incoming guaranteed worker must still be downgraded to ordinary fallback;
  cache unavailability must not create a bypass.
- Explicit guaranteed non-worker infrastructure retains its agreed exception.
- Emit the cache-not-ready decision reason.

### 19.4 Pending reservation expires before observation

Release the reservation, increment expiration metrics, and log enough admission
identity to diagnose an informer or API persistence issue. A later observed Pod
is accounted from informer state.

### 19.5 Reclaimer unavailable

Pending child claimants remain Pending at the parent cap. No unsafe eviction
occurs. Borrowers already running continue until the controller returns or they
terminate naturally.

### 19.6 PDB prevents eviction

Stop execution, expose the PDB/API rejection, and retry through the existing
delayed path. Do not direct-delete the Pod and do not substitute a floor-unsafe
victim.

### 19.7 Insufficient safe worker excess

Select no plan. The claimant remains Pending. Possible causes include large Pod
granularity, non-reclaimable infrastructure, PDB constraints, or all siblings
already being at/below floor.

### 19.8 Target cannot schedule after reclaim

Do not perform speculative extra reclaim. Report the persistent pending target
and allow the next observed state transition to drive reconciliation.

### 19.9 Configuration drift between components

Treat mismatched policy as an operational error. Do not enable `Enforce` until
effective parent limits, child shares, default child, selector, and mode match
between webhook and reclaimer.

### 19.10 Mislabelled worker

The system trusts the configured role selector. A worker that does not match it
may be counted but not reclaimed, and a non-worker incorrectly matching it may
become a victim. Stable, truthful Pod role labels are therefore a deployment
contract.

## 20. Concurrency and Race Handling

### 20.1 Admission concurrency

Keep one active webhook writer. Child pending reservations must be updated under
the same lock as root reservations and classification so two admissions cannot
both consume the same child or parent headroom.

### 20.2 Informer transition races

Every add/update/delete handler must compute old and new contributions and apply
their delta atomically. Required transitions include:

- local admission reservation to observed unbound protected Pod;
- unbound protected Pod to bound protected Pod;
- bound ordinary Pod appearing in child scheduled usage;
- bound Pod becoming terminal or terminating;
- task-type label normalization observed after mutation;
- request changes, although normal Pod requests are effectively immutable.

Accounting must never transiently count both a pending reservation and the
observed Pod under the same admission key.

### 20.3 Reclaimer races

The planner operates on an informer snapshot and execution happens later.
Safety mechanisms are:

- namespace-serialized reconciliation;
- UID preconditions;
- PDB-aware Eviction API;
- re-evaluation on subsequent Pod events;
- no direct delete;
- no partial plan chosen when the initial snapshot lacks enough safe capacity.

The design does not claim a globally atomic view across webhook, scheduler, and
reclaimer.

## 21. Comparison with Established Queue Schedulers

### 21.1 Kueue

Kueue ClusterQueues in a Cohort can lend unused nominal quota and reclaim it
from another ClusterQueue that is borrowing. Its classic preemption model is
the closest semantic match to this proposal. Kueue also has hierarchical
Cohorts and separate fair-sharing mechanisms based on weighted or historical
share.

References:

- [Kueue Cohorts](https://kueue.sigs.k8s.io/docs/concepts/cohort/)
- [Kueue ClusterQueue and reclaimWithinCohort](https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/)
- [Kueue Preemption](https://kueue.sigs.k8s.io/docs/concepts/preemption/)
- [Kueue Fair Sharing](https://kueue.sigs.k8s.io/docs/concepts/fair_sharing/)

The selected design borrows Kueue's nominal-quota/borrowing/reclaim distinction
but does not adopt its Workload and queue lifecycle. Introducing Kueue would
require changing how simulator jobs or Pods enter admission and would be a much
larger migration than extending the already deployed entitlement components.

### 21.2 Apache YuniKorn

The product referred to during discussion as “Apache Unicorn” is Apache
YuniKorn. YuniKorn's hierarchical queues have guaranteed and maximum resources,
recursive parent usage, and explicit preemption laws. The most relevant laws
are:

- a queue below its guarantee may trigger reclaim;
- a queue at or above its guarantee may not use reclaim merely to grow;
- a victim must come from a queue above its guarantee;
- removing the victim must not leave that queue below its guarantee;
- equal-priority cross-queue preemption is allowed because queue policy, rather
  than Kubernetes priority alone, authorizes the transfer.

References:

- [YuniKorn Queue Configuration](https://yunikorn.apache.org/docs/user_guide/queue_config/)
- [YuniKorn Resource Quota Management](https://yunikorn.apache.org/docs/user_guide/resource_quota_management/)
- [YuniKorn Preemption Design](https://yunikorn.apache.org/docs/next/design/preemption/)

The proposed reclaimer follows these floor-safety laws. It uses explicit
Eviction because native Kubernetes preemption does not allow equal-priority
semi-guaranteed workers to preempt one another. Adopting YuniKorn itself would
replace the scheduling model and is outside the minimal scope.

### 21.3 Volcano

Volcano supports hierarchical queues with `deserved`, `guarantee`, and
`capability` resources and sibling-first reclaim. It is attractive for larger
batch/gang-scheduling systems but introduces a queue scheduler and job model.

References:

- [Volcano Hierarchical Queue](https://volcano.sh/docs/v1.12.0/keyfeatures/hierarchicalqueue/)
- [Volcano Capacity Plugin](https://volcano.sh/docs/userguide/user_guide_how_to_use_capacity_plugin/)

The proposed single-level tree and sibling reclaim are conceptually similar,
but intentionally stop before recursive queue traversal, gang semantics, or a
new scheduler deployment.

### 21.4 Why this is not weighted fair share

Percentages in this design are reclaimable floors, not weights for distributing
all excess capacity. Once every demanding child has reached its floor, excess
goes to whichever Pods are admitted and scheduled first. There is no attempt to
make two borrowers converge on a weighted ratio.

This is more accurately described as:

```text
hierarchical guaranteed capacity + elastic borrowing + demand-driven reclaim
```

rather than full hierarchical fair sharing.

## 22. Alternatives Considered

### 22.1 Literal guaranteed PriorityClass for under-floor workers

Rejected for version one.

It matches the wording “this Pod is guaranteed” literally, but it creates two
competing child-reclaim paths. A guaranteed worker could natively preempt a
semi-guaranteed sibling without knowing whether that sibling is at its floor.
Strict correctness would require child-aware victim filtering inside Better
Scheduler and duplicate policy configuration in the scheduler.

### 22.2 Literal guaranteed workers without scheduler changes

Rejected because it directly violates the core child floor rule during
node-local preemption.

### 22.3 Full child-aware Better Scheduler

Deferred. It could provide placement-aware reclaim and strict victim protection
for both workers and guaranteed infrastructure, but would require:

- new scheduler config API fields;
- internal and v1 types;
- validation and defaults;
- deepcopy/conversion/OpenAPI generation;
- child accounting in scheduler snapshots;
- victim filtering across every relevant preemption path;
- configuration synchronization across three components;
- substantially broader integration and release testing.

This becomes justified only if the accepted infrastructure exception or
non-placement-safe reclaim causes material operational problems.

### 22.4 Weighted fair sharing of borrowed excess

Rejected for the first version. It requires persistent or continuously computed
share state, a dominance rule for multiple resources, anti-oscillation logic,
and periodic rebalancing. None of that is necessary to prevent one task type
from monopolizing another type's floor.

### 22.5 Whole-task eviction

Rejected. The agreed reclaim unit is a worker Pod. Drivers and Redis stay alive,
and PDBs remain the Kubernetes-level disruption guard.

### 22.6 Hard caps per child

Rejected because hard caps waste idle sibling capacity and do not meet the
borrowing requirement.

### 22.7 Count actual CPU/GPU telemetry

Rejected. Metrics are delayed, noisy, and do not correspond to scheduler
capacity decisions. Requests provide deterministic admission and preemption
accounting.

## 23. Implementation Detail by Component

This section is an implementation map, not a separate milestone breakdown.

### 23.1 Webhook repository

Extend the existing configuration model with the optional `subQuotas` map and
validated compiled selectors. Derive immutable per-resource floor vectors at
startup.

Extend the accountant with:

- effective-child resolution;
- scheduled all-tier child usage;
- protected pending child usage;
- per-child local pending reservations;
- floor and borrowed snapshots;
- child-aware decision reasons;
- idempotent reservation removal on informer observation.

Extend admission mutation with:

- Shadow calculation alongside legacy root behavior;
- label normalization in Enforce;
- child-first, parent-second classification;
- worker matching before the explicit guaranteed shortcut;
- guaranteed-worker downgrade and fallback scheduler restoration;
- removal of a stale numeric `spec.priority` whenever PriorityClass changes;
- idempotent JSON patch construction for labels, priority, and scheduler.

Extend validation with:

- unchanged manual semi-guaranteed protection;
- protected scheduler validation;
- checks ensuring a guaranteed worker cannot survive Enforce because of webhook
  ordering or reinvocation.

Keep one active replica unless accounting is redesigned around a shared,
linearizable reservation store.

### 23.2 Reclaimer repository

Extend configuration with the same subquota policy and selector validation.

Refactor planning into explicit phases:

1. Build parent and child usage snapshots.
2. Classify pending targets.
3. Select one target.
4. Calculate parent debt.
5. Build the floor-safe victim set.
6. Return a complete plan or an explainable no-plan result.

Keep the pure planner independently unit-testable. Execution remains a thin
Eviction API loop over the plan.

In Shadow, calculate and report the child-aware plan but execute only the legacy
root plan. In Enforce, use the child-aware planner for both parent overage and
pending demand so root repair cannot accidentally delete infrastructure or
reduce a child below its floor.

### 23.3 Better Scheduler repository

Do not add subquota types or code paths.

For the staging rollout only:

- verify that the staging namespace has the intended parent guarantee;
- add it to `admissionAssignedTierNamespaces` when enabling root entitlement
  admission there;
- retain the existing guaranteed and semi-guaranteed PriorityClasses;
- retain the existing numeric priority ordering and preemption policies;
- run the existing focused NamespaceResourceGuarantee regression tests after
  any configuration or release update.

### 23.4 Simulator repository

No simulator implementation is part of this change. Before Enforce, the
simulator owner must guarantee:

- `simulator.task-type` exists on driver, Redis, and every worker;
- the value is stable for the Pod lifetime;
- every worker reliably matches `mylos.pod_type=worker`;
- task-type values match the configured child keys or intentionally fall into
  the default child.

The webhook handles the existing high-worker `guaranteed` override by
downgrading workers according to entitlement; no task-level database field is
required.

## 24. Test Plan

### 24.1 Configuration tests

Cover:

- no `subQuotas` preserves legacy behavior;
- unknown namespace;
- empty or malformed label key;
- missing default child;
- default absent from shares;
- zero, negative, or greater-than-100 share;
- sum below or above 100;
- invalid selector;
- protected fallback PriorityClass;
- Better Scheduler used as ordinary fallback;
- valid CPU, memory, and integral extended-resource floors;
- overflow-safe floor calculation;
- rounding residue remains lendable.

### 24.2 Webhook accounting tests

Cover:

- bound guaranteed, semi-guaranteed, ignored, and ordinary Pods all contribute
  to scheduled child usage;
- unbound protected Pods reserve child capacity;
- unbound ordinary Pods do not;
- terminal and deleting Pods do not count;
- add/update/delete transitions do not double charge;
- local pending becomes informer-observed pending without a gap or duplicate;
- admission retries are idempotent;
- generated-name admission UIDs are independent;
- pending TTL expiration releases every affected child/root resource;
- dry-run changes no state;
- missing and unknown labels map to default without raw-label metrics.

### 24.3 Webhook admission tests

Cover the complete decision matrix:

- child fit and parent fit;
- child fit with parent full;
- child over floor with parent headroom;
- both exhausted;
- one resource fits while another does not;
- exact-boundary equality;
- boundary Pod crossing the floor;
- ordinary low/normal/high inputs;
- incoming guaranteed worker protected as semi;
- incoming guaranteed worker downgraded to fallback;
- explicit guaranteed non-worker preserved;
- manual semi-guaranteed rejected;
- protected class with wrong scheduler rejected;
- cache-not-ready fail-safe behavior;
- Shadow emits hypothetical decisions but only legacy mutations;
- Enforce normalizes labels and applies child policy;
- JSON patch handling when labels or spec maps are initially absent;
- existing `spec.priority` is removed when PriorityClass changes.

### 24.4 Reclaimer planner tests

Cover:

- no debt and no target;
- existing parent overage;
- pending guaranteed infrastructure target;
- pending guaranteed worker policy violation;
- pending under-floor semi target;
- pending semi borrower that does not fit the floor;
- oldest target across multiple children;
- stable UID target tie-break;
- newest eligible victim first;
- stable UID victim tie-break;
- only workers are victims;
- guaranteed, ordinary, driver, and Redis Pods are excluded;
- all PriorityClasses count toward child scheduled usage;
- one victim safe for CPU but unsafe for memory is rejected;
- sequential victims cannot cumulatively cross a floor;
- candidates with no debt overlap are skipped;
- complete multi-resource plan;
- insufficient victims returns no plan;
- Pod granularity prevents an otherwise approximate reclaim;
- unknown/missing child uses default;
- Shadow plan differs from executed legacy plan without child eviction.

### 24.5 Reclaimer execution tests

Cover:

- `policy/v1` Eviction subresource;
- UID precondition;
- PDB `429` delayed retry;
- NotFound/stale UID behavior;
- stopping after the first execution error;
- successful eviction metrics and logs;
- no API action for incomplete or Shadow-only plans.

### 24.6 End-to-end scenarios

At minimum, validate:

1. `parent=50`, `A=25`, `B=25`; A borrows to 50, B requests 10, only A's
   excess workers are evicted, and B schedules.
2. A and B are both at 25; neither child can be a victim for sibling reclaim.
3. A is above 25 only because of ordinary Pods; a B semi-guaranteed claim uses
   native priority against ordinary workload rather than external child
   eviction.
4. A new Pod above both child and parent remains ordinary.
5. CPU fits but GPU does not; no child-floor protection is granted.
6. A PDB blocks the newest worker; the controller reports and retries without
   direct deletion.
7. Non-reclaimable infrastructure makes the desired floor temporarily
   unattainable; the claimant remains Pending without unsafe victims.
8. A guaranteed driver demonstrates the documented infrastructure exception.
9. A namespace without subquota config behaves bit-for-bit like the legacy
   system.

## 25. Rollout Plan

### 25.1 Build and release behind optional configuration

Release webhook and reclaimer versions that understand `subQuotas`, but leave
the section absent in existing environments. Verify legacy metrics and
admission/reclaim behavior before enabling a policy.

### 25.2 Prepare simulator metadata

Deploy and verify task-type label propagation. Inventory active staging Pods and
confirm:

- every task-owned Pod resolves to the intended child;
- every worker matches the reclaimable selector;
- missing/unknown traffic is understood before assigning the default share.

### 25.3 Configure staging Shadow

The agreed first target is the staging simulator namespace, currently
`web-simulation`. At the time of design analysis it had a parent CPU guarantee
configured in Better Scheduler but was not in the webhook's automatic
admission namespace set. Re-verify live configuration before rollout rather
than treating that snapshot as permanent truth.

Provide the real child names and percentages, update webhook and reclaimer
together, enable root automatic admission for staging as required, and set the
child mode to `Shadow`.

### 25.4 Shadow validation gates

Do not proceed to Enforce until all of these are true:

- configured shares sum to 100 and match the intended ownership model;
- webhook and reclaimer expose identical effective policy;
- missing/unknown label volume is expected and acceptable;
- calculated child usage matches an independent Pod request query;
- hypothetical claimants are truly under floor;
- hypothetical victims are workers and remain above floor after removal;
- no unexpected guaranteed workers or role-label gaps appear;
- at least one representative borrow-and-return exercise has been observed;
- PDB and insufficient-capacity outcomes are visible and understandable.

### 25.5 Staging Enforce

Switch both components to `Enforce` in one controlled rollout. Observe:

- admission decision counts;
- pending protected Pods by child;
- borrowed amount by child;
- reclaim debt and plan outcomes;
- eviction/PDB failures;
- scheduler parent-quota rejections;
- ordinary above-parent usage;
- time from under-floor demand to successful placement.

Rollback consists of returning the child policy to `Shadow` or removing the
optional `subQuotas` section. Existing Pods retain their immutable classes, so
rollback affects new admissions and future reclaim decisions rather than
rewriting running Pods.

### 25.6 Production

Production rollout is a separate decision after staging evidence. Re-verify
live parent limits, PriorityClasses, scheduler profiles, and component images at
that time. Do not copy point-in-time staging values into production by default.

## 26. Acceptance Criteria

The feature is accepted when all of the following hold in staging:

1. A task type cannot permanently monopolize protected parent capacity while a
   sibling below floor has an eligible pending Pod and sufficient safe worker
   excess exists.
2. Borrowing remains possible whenever sibling capacity is idle.
3. No external child reclaim reduces a victim child below its floor in any
   configured resource.
4. Only worker Pods matching the configured selector are evicted.
5. Driver and Redis requests count against their child and are never external
   victims.
6. Worker TaskPriority, ignored classes, or explicit guaranteed input cannot
   bypass child admission accounting.
7. Protected simulator usage remains capped by the existing parent scheduler
   guarantee.
8. Above-parent Pods remain ordinary and may use otherwise idle cluster
   capacity.
9. Manual assignment of `semi-guaranteed` remains blocked.
10. Shadow mode causes no child-specific mutation or eviction.
11. Namespaces without subquota configuration retain existing behavior.
12. Every no-reclaim outcome is explainable through bounded metrics and
    structured logs.

## 27. Accepted Limitations and Residual Risks

These are design properties, not hidden implementation bugs:

1. **Pod granularity:** a child can remain below floor when its next whole Pod
   does not fit, and a sibling's large worker may be impossible to evict without
   crossing its floor.
2. **Admission-time immutability:** an ordinary Pod is not promoted after churn.
3. **No destination safety:** entitlement reclaim may not make the target
   schedulable on any node.
4. **PDB authority:** a PDB can delay or prevent floor restoration.
5. **Non-reclaimable usage:** driver/Redis consumption can make a floor
   temporarily unattainable.
6. **Guaranteed infrastructure exception:** guaranteed infrastructure can use
   scheduler preemption without child floor awareness.
7. **External higher priorities:** development and system-critical priorities
   retain their existing ability to preempt simulator workload.
8. **No excess fairness:** one child may hold all currently unused sibling
   capacity until real sibling demand arrives.
9. **Single webhook writer:** horizontal active-active admission remains unsafe
   without shared reservation state.
10. **Static configuration:** share changes require rollout and affect future
    decisions; they do not immediately rewrite existing Pod tiers.
11. **Trusted labels:** inaccurate task-type or Pod-role labels produce
    inaccurate accounting or victim eligibility.
12. **Best-effort guarantee wording:** physical capacity, node constraints,
    disruption policy, and Pod shape can prevent exact floor realization.

## 28. Follow-up Triggers

The following operational evidence would justify a second-generation design:

- repeated safe-but-useless evictions due to placement constraints;
- guaranteed infrastructure frequently violating sibling floors;
- a need for more than one child hierarchy level;
- starvation or unacceptable imbalance among borrowers after every child has
  reached its floor;
- a requirement to admit or evict whole tasks atomically;
- a need for active-active admission replicas;
- frequent dynamic share changes;
- a desire to manage multiple namespaces through one shared queue tree.

At that point, compare the cost of a child-aware scheduler implementation with
a migration to Kueue, YuniKorn, or Volcano rather than continuing to grow the
external controller indefinitely.

## 29. Remaining Operational Inputs

The design and implementation behavior are complete, but rollout still requires
environment-owned data:

- the canonical task-type values;
- the integer percentage assigned to each value;
- the default child name and percentage;
- confirmation that the selected shares reflect all parent resources, not only
  CPU;
- current staging and production parent entitlement values;
- confirmation of label propagation on all Pod roles;
- the release window for Shadow and later Enforce.

These inputs belong in deployment configuration. They must not be guessed or
compiled into the components.

## 30. Point-in-Time Environment Snapshot

This appendix records facts verified during design analysis on 2026-09-02. It
explains why several decisions were made, but it is not normative configuration.
Every value must be re-verified before deployment.

### 30.1 Repository revision

The Better Scheduler checkout was:

```text
branch: CLOUD-0-better-scheduler
commit: 87f12b79348560e9c92ad28d536526b61672bdd1
```

### 30.2 Cluster and deployed component versions

The inspected Kubernetes context was `kansas-gimel`. The relevant deployments
were:

| Deployment | Replicas | Image tag |
|---|---:|---|
| `better-scheduler` | 3 | `better-scheduler:v1.32.4-bs-v0.14` |
| `namespace-entitlement-webhook` | 1 | `namespace-entitlement-webhook:0.1.3` |
| `namespace-entitlement-reclaimer` | 1 | `namespace-entitlement-controller:0.1` |

The single webhook replica agrees with the current in-memory reservation model.

### 30.3 Scheduler profiles and namespace limits

The live scheduler configuration had two profiles:

- `default-better-scheduler`, with `NominatedNodeReservation` filtering and
  post-bind handling;
- `better-scheduler`, with `NamespaceResourceGuarantee` at PreEnqueue,
  PreFilter, PostFilter, and Score, `NominatedNodeReservation`, and upstream
  `DefaultPreemption` disabled in that profile.

The simulator-related parent CPU limits were:

| Namespace | CPU parent guarantee | Automatic semi-tier admission |
|---|---:|---|
| `web-simulation` | 500 cores | Not enabled |
| `web-simulation-production` | 37,000 cores | Enabled |

At that time the webhook and reclaimer configs managed only
`web-simulation-production` at 37,000 CPU. This is why the agreed rollout starts
by wiring the staging namespace into root admission and child Shadow rather
than changing production first.

### 30.4 Relevant PriorityClasses

The live numeric ordering was:

| PriorityClass | Value | Preemption policy |
|---|---:|---|
| `system-node-critical` | 2,000,001,000 | `PreemptLowerPriority` |
| `system-cluster-critical` | 2,000,000,000 | `PreemptLowerPriority` |
| `infra-critical` | 1,000,000,000 | `PreemptLowerPriority` |
| `dev-environment` | 10,000,000 | `PreemptLowerPriority` |
| `guaranteed` | 1,000,000 | `PreemptLowerPriority` |
| `semi-guaranteed` | 500,000 | `PreemptLowerPriority` |
| `simulator-workers-high` | 0 | `PreemptLowerPriority` |
| `simulator-workers-normal` | -100 | `PreemptLowerPriority` |
| `low-priority` | -1,000 | `Never` |

This proves two important boundaries:

- simulator guarantees do not override higher numeric development or critical
  priorities;
- real `guaranteed` infrastructure can preempt semi-guaranteed workers, which
  creates the explicitly accepted infrastructure exception.

### 30.5 Existing simulator scheduling behavior

The inspected simulator source mapped TaskPriority as follows when the special
guaranteed override was disabled:

```text
LOW    -> low-priority
NORMAL -> simulator-workers-normal
HIGH   -> simulator-workers-high
```

When `use_guaranteed_quota_for_high_priority_workers` was enabled, high workers
instead used the `guaranteed` PriorityClass and Better Scheduler. That existing
path is why Enforce must classify and automatically downgrade guaranteed
workers rather than trusting the submitted class.

Driver metadata already included task ID, task priority, and scheduling-group
labels. Worker metadata included task ID and creator labels. The new
`simulator.task-type` label was not yet present in the inspected construction
path, so propagation to driver, Redis, and workers remains a hard rollout
prerequisite.

## 31. Documentation Provenance

This document records the agreed design discussion and direct inspection of the
tracked Better Scheduler source, the Dynamic Namespace Entitlement webhook, the
external reclaimer, simulator Pod construction, and the live staging
configuration available during analysis.

Pre-existing untracked RFC/design files in `docs/better-scheduler`, including
`namespace-subquotas-rfc.md`, were intentionally not used as design inputs and
were not modified.
