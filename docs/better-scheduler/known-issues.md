# Better-Scheduler Known Issues & Caveats

Ordered by how likely they are to bite.

## 1. Binary self-reports the wrong version

- Symptom: an image tagged `v1.32.4-bs-v0.5` logs `version="v1.32.4-bs-v0.4-2-g1f374fb..."` at startup.
- Cause: the embedded `gitVersion` comes from `git describe --tags --match='v*'` at build time (`hack/lib/version.sh`), but `hack/release-better-scheduler.sh` creates the release tag **after** building — so the newest visible tag is the previous release, plus a commit offset.
- Impact: cosmetic but confusing during rollouts. The image tag, git tag, and ledger digest are always correct — trust those.
- Fix options (not yet implemented): create the local git tag before the build and push it only after a successful image push; or inject the intended version explicitly via `KUBE_GIT_VERSION`/ldflags in the release script.

## 2. NominatedNodeReservation store is in-memory and per-process

- Reservations exist only inside one scheduler process: they don't survive restarts or leader failover, and they are invisible to any other scheduler process.
- Consequences: the Filter must be enabled in **every profile** of the (single) scheduler binary; a separately running stock kube-scheduler on the same nodes silently defeats the mechanism; after a scheduler restart mid-preemption, a freed node can be stolen once (the preemptor then re-preempts).
- Accepted as a v1 trade-off; a durable reservation (annotation/CRD-based) would be the escalation path if restarts prove painful.

## 3. Static configuration

- Guarantees live in scheduler args; every change requires a ConfigMap edit **and** a Deployment rollout (config is read at startup only). There is no CRD/informer-driven reload by design.
- A namespace missing from `namespaceGuarantees` has guarantee 0 for all configured resources: a guaranteed-priority pod from an unlisted namespace that requests a configured resource can never schedule (Pending forever with a guarantee-exceeded message). Watch for this when onboarding new teams.

## 4. MIG resources are not counted

- Only exact configured resource names count toward guarantees; `nvidia.com/mig-*` requests do not count toward a `nvidia.com/gpu` guarantee.
- MIG policy (e.g. forbidding guaranteed MIG workloads) is the admission webhook's responsibility, outside this repo. If the webhook doesn't enforce it, guaranteed MIG workloads bypass GPU guarantee accounting entirely.

## 5. Scheduler-side policing is absent by design

- Nothing in the scheduler prevents a pod from claiming the guaranteed PriorityClass; without the admission webhook, any team can use guaranteed priority (still capped by their namespace guarantee — namespaces without guarantees get 0, which is the backstop).
- Similarly, pods can set `schedulerName: better-scheduler` without the guaranteed PriorityClass and receive packing/reservation behavior only. Harmless, but know it exists.

## 6. Packing cannot repair existing fragmentation

- The v0.5 Score only prevents *new* fragmentation of guaranteed GPU pods. Nodes fragmented before v0.5 (or during any period the Score is disabled/outweighed) stay fragmented until pods churn naturally.

## 7. Commit `afe076663b4` has a useless message ("test")

- It is actually release v0.3.1 — the preemption decision events work (see [history.md](history.md)). Don't drop or squash it casually; the release ledger and git tag `v1.32.4-bs-v0.3.1` point at it.

## 8. Repo housekeeping (as of 2026-07-09)

- `releases/better-scheduler/releases.csv` has the v0.6 and v0.7 rows **uncommitted** in the working tree, and the Grafana dashboard `releases/better-scheduler/grafana-decision-dashboard.json` is **untracked**. Both should be committed so the ledger matches the pushed tags.
- The root `NAMESPACE_RESOURCE_GUARANTEE_DESIGN.md` is a pointer stub; this doc tree is the source of truth. Update these docs in the same change set as behavior changes.
- These docs snapshot the code at `v1.32.4-bs-v0.7` (commit `22dfb9910d3`).
