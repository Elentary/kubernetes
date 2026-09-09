# Better-Scheduler Operations

## 1. Releases

The step-by-step runbook is [docs/releases/better-scheduler.md](../releases/better-scheduler.md). Summary:

- One command: `hack/release-better-scheduler.sh --upstream-version 1.32.4 --subversion 0.7 --push` (without `--push` it prints the plan and exits; `--skip-tests` skips preflight tests).
- Release string `v<k8s-version>-bs-v<subversion>` (e.g. `v1.32.4-bs-v0.7`) is used for the git tag and the image tag. Subversions are independent across upstream Kubernetes versions.
- The script enforces: clean working tree, named branch (no detached HEAD), tag format, and tag uniqueness locally and on `origin`; runs preflight `go test` for the scheduler packages; builds via `make quick-release-images` (amd64 only, conformance image skipped); retags the local `registry.k8s.io/kube-scheduler-amd64:<tag>` image to the ECR repo (`767397673936.dkr.ecr.us-east-2.amazonaws.com/better-scheduler`, default in the script); pushes; captures the immutable digest; creates and pushes an annotated git tag containing the digest; appends a row to the ledger.
- Ledger: `releases/better-scheduler/releases.csv` (`release_tag, git_sha, image, image_digest, released_at_utc, released_by`). The script appends the row to your working tree — **commit it afterwards**, it is not committed automatically.
- Prerequisites: local Docker daemon, `git`/`make`/`docker`, ECR credential helper configured in Docker (the script runs no auth commands), `origin` reachable.

Practical notes from real releases:

- **Dirty tree**: the script refuses to run with uncommitted changes. Either commit exactly what you intend to release first, or release from a clean temporary worktree at the release commit (`git worktree add /tmp/bs-release <sha>` and run the script there) — done for v0.7 when the main tree carried unrelated uncommitted files. If you use a worktree, merge the new ledger row back into the main tree's `releases.csv`.
- **Version-string caveat**: the binary's logged `version=` comes from `git describe` at build time, and the script creates the release tag *after* building — so the binary self-reports the *previous* tag plus a commit offset (e.g. image `v0.5` logs `v1.32.4-bs-v0.4-2-g...`). The image tag and ledger are correct; trust those. See [known-issues.md](known-issues.md) #1.

Deploying a release: update the scheduler Deployment's image to the new tag (or better, the immutable digest from the ledger) and roll it. There is deliberately no `--deploy` in the release script.

## 2. Config Changes

- Guarantees, plugin enablement, and client tuning all live in the `KubeSchedulerConfiguration` ConfigMap ([configuration.md](configuration.md)).
- kube-scheduler reads config at startup only: **every ConfigMap change requires a Deployment rollout**.
- Config errors (validation failures in `NamespaceResourceGuaranteeArgs`) crash the scheduler at startup — watch the rollout.
- The effective config is observable without cluster access via the `scheduler_namespace_resource_guarantee_quota` metric ([observability.md](observability.md) §3).

### 2.1 Enabling RDMA fallback

1. Build the scheduler with the new configuration field. First validate in a test
   environment with both ordinary and RDMA GPU nodes: ordinary free placement,
   ordinary preemption ahead of a free RDMA node, PDB-preserving RDMA fallback,
   and completion of an existing nomination without a second eviction wave.
2. Enable `NamespaceResourceGuarantee` in the Better-Scheduler profile's `filter`
   list as well as its existing `preFilter` and `postFilter` lists. Keep
   `DefaultPreemption` disabled in that profile. Preserve reservation plugins in
   every profile.
3. Set `preferNonRDMANodesForGuaranteedGPU: true` in that profile's arguments and
   roll the Deployment. The default-profile quota backstop keeps the flag omitted
   or false.
4. Check scheduler readiness, `placement_phase` / `fallback_reason` decision logs,
   victim deletions, nominated-node reservations, and Filter/PostFilter latency.
   A free RDMA fallback must not delete any ordinary victims merely to test fit.

Rollback: set the flag to false and roll the Deployment. If rolling back to an older
binary, remove the new field first. Disabling the policy does not move bound Pods.
The repository's unit tests do not substitute for this live placement validation.

## 3. Diagnosing "Why is my pod Pending / why did it land there?"

- First stop: the Grafana decision dashboard ([observability.md](observability.md) §4) — enter namespace + pod name.
- Guaranteed pod Pending with event/status `namespace ... protected resource guarantee exceeded`: the namespace is at its guarantee. Expected behavior; the pod schedules when usage drops (deletion/scale-down re-queues it automatically).
- Guaranteed pod Pending with nominated node: preemption started; victims are terminating. Check the `NamespaceResourceGuaranteePreemption*` events (with `decisionID`) on the pod.
- Nodes rejected with `node reserved for namespace resource guarantee preemption by <ns>/<pod>`: a reservation is active for another preemptor. Stale reservations self-clean; if one appears stuck, restarting the scheduler clears the in-memory store.

## 4. Incident Playbook

### 4.1 Events rejected: `can have at most 1024 characters`

- Signature: `event_broadcaster.go:... "Server rejected event (will not retry!)" err="... message: Invalid value: ...: can have at most 1024 characters"`.
- Happened in production when a preemption evicted dozens of victims and the started-event note carried the full victim list. Fixed in code (bounded `victims=[...]` summary + truncation guard, commit `d100345e408`, in releases ≥ v0.6).
- If it reappears: some other event path is emitting an unbounded note; find the emitter and bound it the same way (`summarizeVictimKeysForEvent` / `truncatePreemptionEventNote` in the guarantee plugin are the precedent).

### 4.2 Client-side throttling

- Signature: `Waited for <seconds> due to client-side throttling, not priority and fairness, request: POST:.../events`.
- This is the scheduler's own client rate limit, not API Priority & Fairness. Typical trigger: event bursts from very large/churny namespaces (thousands of pods, crash-looping workloads).
- Diagnosis: confirm the log is from the scheduler pod; check which namespace dominates events (`kubectl get events -A --sort-by=...` or Loki); consider whether the event volume itself is a symptom (e.g. a failing startup probe on a large workload) worth fixing at the source.
- Fix: set `clientConnection.qps/burst` explicitly (see [configuration.md](configuration.md) §5) and roll the scheduler. Escalate in 2× steps only when throttling waits are actually observed.

### 4.3 Guaranteed pod unschedulable despite free GPUs (fragmentation)

- Guarantee ≠ instant placement: a large (e.g. 8-GPU) guaranteed pod can be blocked by 1-GPU guaranteed pods spread across nodes, since guaranteed pods are never evicted.
- Since v0.5 the packing Score prevents *new* fragmentation; existing fragmentation must drain naturally (or pods be moved manually with team consent).
- Verify packing is active: profile has `NamespaceResourceGuarantee` under `score` with a high weight; check `Plugin scored node for pod` lines for the plugin on GPU pods.

### 4.4 Freed capacity stolen after preemption

- Symptom: guaranteed pod preempts victims, but another pod binds to the freed node and the guaranteed pod preempts again.
- Should be prevented by `NominatedNodeReservation` since v0.4. If observed: verify the plugin is enabled in `filter`+`postBind` of **every** profile, and that no separate stock kube-scheduler is scheduling onto the same nodes (the reservation store is per-process).

## 5. Testing

- Unit tests: see [README.md](README.md) §"Working on the code" for the focused `go test` commands.
- Cluster smoke bundle: a test manifest set was created during initial rollout (scheduler health check, protected-preempts-normal, over-guarantee-stays-Pending, namespace isolation). Pattern: a tiny namespace with a small guarantee, a normal pod filling a node, then a guaranteed pod that must preempt it; and a guaranteed pod exceeding the namespace guarantee that must stay Pending with the guarantee-exceeded message.

## 6. Keeping This Documentation Honest

- These docs snapshot the code as of `v1.32.4-bs-v0.7` (commit `22dfb9910d3`). When changing scheduler behavior, update the relevant doc in the same change set — that has been the project convention since day one.
- Real cluster values (namespaces, guarantee numbers, cluster/node names) are deliberately **not** recorded here; the live ConfigMap and the config metrics are the source of truth.
