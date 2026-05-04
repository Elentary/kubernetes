# NamespaceResourceGuarantee Plugin Design (Protected GPU Guarantees)

## 1. Purpose

This document is the single-source handoff for the custom Kubernetes scheduler behavior we are building for multi-tenant GPU fairness.

Target environment assumptions:

- Shared GPU pool (about 300 GPUs total)
- Multiple teams mapped to namespaces
- Each team needs a guaranteed urgent GPU lane
- Teams can also run opportunistic jobs outside guarantees

Primary goals:

- A team can run urgent workloads within its guarantee
- Urgent workloads can preempt normal workloads when the cluster is full
- Teams cannot freely self-assign urgent priority
- Opportunistic usage remains available
- Keep scheduler logic simple and production-stable

## 2. Final Architecture Decision

We intentionally use a **2-tier model** (not 3-tier):

- `Protected` tier:
  - Urgent workloads only
  - Identified by one protected `PriorityClass`
  - Hard-capped by per-namespace protected resource guarantees
  - Can preempt normal pods via native Kubernetes preemption
- `Normal` tier:
  - Everything else
  - Opportunistic
  - Preemptible

Why no borrowed tier:

- No separate latency SLA right now
- No queue semantics that require an intermediate tier
- Additional complexity without operational benefit in v1

## 3. Scheduling Semantics

### 3.1 Normal pod path

- Plugin does nothing special.
- Pod follows default kube-scheduler behavior.

### 3.2 Protected pod path

At scheduling time, the `NamespaceResourceGuarantee` PreFilter:

1. Checks whether the pod is protected (`priorityClassName == protectedPriorityClassName`).
2. Computes requested amounts for configured protected resources (`cpu`, `memory`, extended scalar resources such as `nvidia.com/gpu`).
3. Computes current protected resource usage in the pod namespace from scheduler snapshot.
4. Enforces each configured resource independently: `currentUsage + requested <= namespaceGuarantee`.

If the check fails:

- Returns `UnschedulableAndUnresolvable` and pod stays pending.
- No scheduling/preemption attempt occurs for that cycle.

If the check passes:

- Pod proceeds through normal scheduling pipeline.
- If cluster capacity is tight, built-in preemption can evict lower-priority (normal) pods.

### 3.3 Protected-vs-protected behavior

Protected pods are not allowed to exceed namespace guarantee, and they do not preempt each other under current policy because protected pods share the same priority class level.

Critical SLA wording:

- Protected does **not** mean immune to node failure.
- Protected does **not** mean immune to eviction for unrelated reasons.
- Protected does **not** mean immune to admin deletion.
- Protected means protected from opportunistic/normal workload competition via priority + guarantee enforcement.

## 4. Why Scheduler Plugin (Not ResourceQuota)

`ResourceQuota` is admission-time and rejects pod creation when over limit.

We need scheduling-time behavior:

- allow pod creation
- keep over-limit protected pods pending
- allow later scheduling when usage drops
- preserve preemption flow when guarantee check passes

Therefore this policy belongs in a scheduler plugin, not quota.

## 5. Implemented Code (Current Repo State)

Plugin and tests:

- `pkg/scheduler/framework/plugins/namespaceresourceguarantee/namespaceresourceguarantee.go`
- `pkg/scheduler/framework/plugins/namespaceresourceguarantee/namespaceresourceguarantee_test.go`

Plugin registration:

- `pkg/scheduler/framework/plugins/names/names.go`
- `pkg/scheduler/framework/plugins/registry.go`

Scheduler config API wiring:

- Internal args type: `pkg/scheduler/apis/config/types_pluginargs.go`
- Internal registration: `pkg/scheduler/apis/config/register.go`
- External v1 args type: `staging/src/k8s.io/kube-scheduler/config/v1/types_pluginargs.go`
- External v1 registration: `staging/src/k8s.io/kube-scheduler/config/v1/register.go`
- Validation hook: `pkg/scheduler/apis/config/validation/validation.go`
- Validation impl/tests:
  - `pkg/scheduler/apis/config/validation/validation_pluginargs.go`
  - `pkg/scheduler/apis/config/validation/validation_pluginargs_test.go`

Generated updates:

- conversions/deepcopy/defaults/openapi for `NamespaceResourceGuaranteeArgs` were generated and committed in corresponding generated files.

Note:

- `build/common.sh` has an unrelated local change and is not part of scheduler design.

## 6. Exact Resource Accounting Rules in Implementation

Current implementation counts only:

- pods with protected `PriorityClassName`
- pods in the same namespace
- pods already assigned to a node (`spec.nodeName != ""`)
- requested amount of configured protected resources

Current implementation ignores:

- non-protected pods
- protected pods requesting 0 of configured protected resources
- unscheduled pods
- pods outside namespace

MIG handling:

- Scheduler plugin does not do MIG-specific policy.
- MIG safety/policy is expected from admission webhook.
- If guarantees include `nvidia.com/gpu`, MIG-only requests do not count toward that GPU cap.

## 7. Enqueue and Requeue Behavior

Although enforcement logic is PreFilter-only, the plugin also implements `EnqueueExtensions` to avoid stale pending pods.

Registered events:

- Pod `Delete`
- Pod `UpdatePodScaleDown`

Queueing hint behavior:

- Queue protected pending pod when relevant protected usage in its namespace decreases.
- Queue when the pending pod itself scales down GPU request.
- Skip queue when change cannot reduce relevant namespace protected usage.

This keeps pending protected pods reactive without custom watchers/informers.

## 8. Config Contract (`NamespaceResourceGuaranteeArgs`)

Fields:

- `protectedPriorityClassName` (required)
- `namespaceGuarantees` (`map[string]ResourceList` per namespace; missing namespace/resource implies guarantee `0`)

Validation rules:

- protected priority class name must be non-empty
- namespace keys must be non-empty
- resources must be `cpu`, `memory`, or extended scalar resources
- guarantee quantities must be `>= 0` (extended scalar resources must be integer quantities)

## 9. Scheduler Profile Pattern

This plugin is intended to run in a custom scheduler profile, leaving default scheduler untouched.

Example shape:

```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
  - schedulerName: site-scheduler
    plugins:
      preFilter:
        enabled:
          - name: NamespaceResourceGuarantee
    pluginConfig:
      - name: NamespaceResourceGuarantee
        args:
          apiVersion: kubescheduler.config.k8s.io/v1
          kind: NamespaceResourceGuaranteeArgs
          protectedPriorityClassName: protected
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

Pod opt-in:

- workloads that should use this behavior set `spec.schedulerName: site-scheduler`

## 10. Admission Webhook Contract (Separation of Concerns)

Webhook is responsible for:

- mapping trusted label/intent to protected priority
- blocking manual protected-priority abuse
- forbidding protected MIG workloads (if policy requires)

Scheduler plugin is responsible only for:

- enforcing per-namespace protected resource guarantees at scheduling time

## 11. Performance and Complexity Position

Current accounting strategy:

- snapshot shared lister scan at scheduling cycle
- no API calls, in-memory scheduler view
- optimized by scanning only scheduled protected pods in the scheduler snapshot

Given expected scale (about 50 GPU nodes, about 8 GPU pods/node), v1 scan cost is acceptable.

No incremental index in v1 by design (premature optimization avoided).

## 12. Fragmentation Policy

Accepted in v1:

- guarantee means guaranteed share, not guaranteed instant placement
- large pods can wait due to fragmentation

Future scoring work (optional):

- add binpacking/fragmentation-aware scoring to pack opportunistic workloads tighter

## 13. Build and Deployment Strategy

This is an in-tree kube-scheduler plugin, not a sidecar.

Required path:

1. Keep Kubernetes source fork with plugin code and API wiring.
2. Build `kube-scheduler` binary/image from same Kubernetes version as cluster.
3. Deploy separate scheduler instance/profile for `site-scheduler`.
4. Keep default scheduler profile untouched for non-opt-in workloads.

## 14. Tested Status (Current Working Tree)

Verified locally:

- `go test ./pkg/scheduler/framework/plugins/namespaceresourceguarantee`
- `go test ./pkg/scheduler/apis/config/validation -run TestValidateNamespaceResourceGuaranteeArgs`

Both pass in current tree.

## 15. Known v1 Boundaries / Non-Goals

Not implemented now:

- borrowed tier
- custom scoring / DRF / fairness model
- custom preemption logic
- informer-based dynamic policy updates (config is static in scheduler args)
- dynamic guarantee CRD
- queue-wide arbitration across namespaces
- gang scheduling

## 16. Next Session Quick Start

If starting fresh, use this file and follow:

1. Re-read sections 2, 3, 6, 8, 10 for policy contract.
2. Inspect plugin file and tests listed in section 5.
3. Validate config wiring from section 5 if making API/arg changes.
4. Run focused tests from section 14 after edits.
5. Keep webhook responsibilities separate from scheduler enforcement.

This preserves the design philosophy for v1:

- correctness over optimal packing
- simplicity over theoretical fairness
- explicit guarantees over elastic behavior
- scheduler stability over feature breadth
