# Better-Scheduler

A custom `kube-scheduler` for multi-tenant GPU fairness, built as a fork of Kubernetes (currently v1.32.4, branch `CLOUD-0-better-scheduler`). It gives each team (namespace) a hard per-namespace **guarantee** of resources (`nvidia.com/gpu`, cpu, memory) for urgent workloads: pods in the *guaranteed tier* can preempt normal workloads up to their namespace guarantee, stay Pending (not rejected) above it, and get packed tightly on GPU nodes to avoid fragmentation. Everything else schedules opportunistically as usual.

This directory is the **source of truth** for the project. The old root `NAMESPACE_RESOURCE_GUARANTEE_DESIGN.md` is a stub pointing here. Update these docs in the same change set as any behavior change.

## Documentation Map

| Doc | Read it when… |
|---|---|
| [architecture.md](architecture.md) | you need to understand what the scheduler does and why — tiers, plugins, preemption, packing, logging design |
| [configuration.md](configuration.md) | you're writing/reviewing the `KubeSchedulerConfiguration`, plugin args, PriorityClass, or deploying |
| [observability.md](observability.md) | you're debugging a scheduling decision, writing Loki/PromQL queries, or using the Grafana dashboard |
| [operations.md](operations.md) | you're cutting a release, changing config, or handling an incident |
| [history.md](history.md) | you want the why behind any version (v0.1 → v0.7), incidents included |
| [known-issues.md](known-issues.md) | before assuming something is a bug, or planning next work |
| [design-review.md](design-review.md) | you want the 2026-07 adversarial design review — findings, per-finding reasoning/steelman, mitigations, and the prioritized action plan |
| [better-scheduler-design.pdf](better-scheduler-design.pdf) | you want a single self-contained design doc to send to any developer (source: [design-doc.typ](design-doc.typ), rebuild with `typst compile --input rev=$(git rev-parse --short HEAD) design-doc.typ better-scheduler-design.pdf`) |

## Components at a Glance

| What | Where |
|---|---|
| `NamespaceResourceGuarantee` plugin (guarantee enforcement, namespace-aware preemption, GPU packing, config metrics) | `pkg/scheduler/framework/plugins/namespaceresourceguarantee/` |
| `NominatedNodeReservation` plugin (reserves freed nodes after preemption) | `pkg/scheduler/framework/plugins/nominatednodereservation/` |
| Decision/rejection logs (profile-gated explainability) | `pkg/scheduler/schedule_one.go` (`detailedScoreLoggingProfile`) |
| Plugin args API (`NamespaceResourceGuaranteeArgs`) | `pkg/scheduler/apis/config/types_pluginargs.go`, `staging/src/k8s.io/kube-scheduler/config/v1/types_pluginargs.go` |
| Args validation | `pkg/scheduler/apis/config/validation/validation_pluginargs.go` |
| Plugin registration | `pkg/scheduler/framework/plugins/registry.go`, `.../names/names.go` |
| Release script + runbook | `hack/release-better-scheduler.sh`, `docs/releases/better-scheduler.md` |
| Release ledger | `releases/better-scheduler/releases.csv` |
| Grafana dashboard | `releases/better-scheduler/grafana-decision-dashboard.json` |

## Releases

- Tag/image scheme: `v<k8s-version>-bs-v<subversion>` (e.g. `v1.32.4-bs-v0.7`), pushed to a private ECR repo; every release is a row in the ledger CSV with the immutable image digest.
- Current release: **v1.32.4-bs-v0.7** (commit `22dfb9910d3`).
- One-liner: `hack/release-better-scheduler.sh --upstream-version 1.32.4 --subversion <next> --push` — details in [operations.md](operations.md) §1.

## Working on the Code (quick start for the next session)

1. Read [architecture.md](architecture.md) §4–7 for the component you're touching; check [known-issues.md](known-issues.md) for existing caveats.
2. Key invariants to preserve:
   - Guarantee check failure must stay `UnschedulableAndUnresolvable` (Pending, no preemption attempt).
   - Only guaranteed pods trigger custom preemption/packing; normal pods must be untouched by the plugins.
   - Decision logs stay gated to the `better-scheduler` profile and at `Info` level.
   - Event notes must stay under the 1024-character event limit.
   - `NominatedNodeReservation` semantics assume a single scheduler process covering all profiles.
3. If you change `NamespaceResourceGuaranteeArgs`: update internal + v1 types, validation (+tests), and run `hack/update-codegen.sh` (conversions/deepcopy/defaults/openapi are generated).
4. Focused tests:

```bash
go test ./pkg/scheduler/framework/plugins/namespaceresourceguarantee/...
go test ./pkg/scheduler/framework/plugins/nominatednodereservation/...
go test ./pkg/scheduler/apis/config/validation -run TestValidateNamespaceResourceGuaranteeArgs
go test ./pkg/scheduler -run 'TestPrioritizeNodesDecisionLogs|TestFindNodesThatFitPodDecisionLogs'
```

5. Update the relevant doc here in the same change set; release via [operations.md](operations.md) when ready.

## Conventions

- No company-specific values in these docs (cluster names, real namespaces, guarantee numbers) — the live ConfigMap and the `scheduler_namespace_resource_guarantee_quota` metric are the source of truth for those.
- Keep unrelated changes in separate commits; write real commit messages (see [known-issues.md](known-issues.md) #7 for the cautionary tale).
- Terminology: code says "protected", operations say "guaranteed" — same concept ([architecture.md](architecture.md) §2).
