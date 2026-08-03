# Better-Scheduler Design Review — Findings, Reasoning, Mitigations

An adversarial design and implementation-decision review of better-scheduler, conducted 2026-07-13 against the code at commit `9c7c68970fe` (the v0.8 feature set, branch `CLOUD-0-better-scheduler`) and the documentation tree as of the same date. While this review was being written, v0.8 was released and v0.9 (`3961e2be095`, preemption candidate scoring) landed on the branch; all file/line references below have been updated to HEAD `c67c49f0348`. Both releases received their own analysis pass — findings [F19–F23](#f19) — and the [addendum](#addendum) records how they move the main findings. This is **not** a bug hunt in the line-by-line sense; it examines whether the design's decisions hold up, whether the docs match the code, and where a decision deserves to be revisited.

**Method.** All seven docs and the full design document (`design-doc.typ` / PDF) were read end-to-end; every claim below was then verified directly against `namespaceresourceguarantee.go`, `nominatednodereservation/{plugin,store}.go`, the `schedule_one.go` diff vs upstream `v1.32.4` (`59526cd4867`), and the relevant *upstream* framework code the design interacts with (`framework/runtime/framework.go`, `framework/preemption/preemption.go`, the scheduling queue). Where a finding depends on production behavior that static analysis cannot settle, it says so explicitly and includes a concrete way to verify.

**A note on self-correction.** Two findings changed materially during verification, and the corrections are kept visible in their *Steelman / self-challenge* sections (F3, F4). A review that never catches itself being wrong hasn't pushed hard enough; these two are also the findings where the eventual conclusion is most interesting.

**Severity scale.** *High* = can produce user-visible scheduling failures or invalidates a documented guarantee. *Medium* = wasted work, silent degradation, or a standing maintenance/operational risk. *Low* = cosmetic, cognitive, or process cost.

---

## Findings index

| ID | Title | Category | Severity | Fix effort |
|---|---|---|---|---|
| [F1](#f1) | Guarantee resource set is a cluster-wide union — cross-tenant config coupling | Design flaw | High | Small |
| [F2](#f2) | Guarantee cap unenforced in the `default-scheduler` profile | Design flaw | High | Small |
| [F3](#f3) | In-flight preemptors invisible to guarantee accounting | Design flaw | Medium-High | Medium |
| [F4](#f4) | Reservation plugin's premise misstates upstream; its distinctive effect is over-blocking | Doc/design misalignment | High (as understanding), Medium (as behavior) | Doc: small; code: medium |
| [F5](#f5) | Alternatives analysis omits the out-of-tree plugin pattern | Strategic decision | Medium (compounding) | Doc: small; migration: large |
| [F6](#f6) | Decision logging removed upstream V(10) behavior — violates R5 | Misalignment with own goals | Medium | Trivial |
| [F7](#f7) | Namespace-fair victim ordering only applies between configured namespaces | Design flaw | Medium | Small |
| [F8](#f8) | Reservation lifecycle keyed on string-matching upstream messages | Fragility by construction | Medium | Medium |
| [F9](#f9) | ~400 lines copied from DefaultPreemption to change one comparator | Maintenance liability | Medium | Medium |
| [F10](#f10) | Load-bearing config invariants enforced only by documentation | Robustness gap | Medium | Small |
| [F11](#f11) | Decision logs gated on a hardcoded profile-name literal | Code/config coupling | Low-Medium | Small |
| [F12–F18](#minor) | Minor findings (docs accuracy, metrics, packing side effects, testing, misc.) | Various | Low | Small each |
| [F19](#f19) | v0.8: release hygiene collapse — mega-commit, off-branch ledger sha, stale release docs | Process | Medium | Small |
| [F20](#f20) | v0.8: attempt-scoping shipped as a default-empty manual textbox; history.md describes a mechanism that doesn't exist | Doc-vs-artifact misalignment | Medium | Small |
| [F21](#f21) | v0.9: shared `OrderedScoreFuncs` contract silently re-defined; preemption-placement policy shift undocumented | Design decision + fork drift | Medium | Small-Medium |
| [F22](#f22) | v0.9: victim-subtraction in the candidate packing score is a tautology; log fields that can never differ | Dead-in-practice logic | Low-Medium | Small |
| [F23](#f23) | v0.9: decision-log field contract now duplicated across two packages | Maintenance liability | Low | Small |

Cross-cutting themes, the balance section (what is genuinely good), a prioritized action plan, and open questions needing production data follow the findings.

---

<a name="f1"></a>
## F1. Guarantee resource set is a cluster-wide union — one team's config silently breaks another team's scheduling

**Category:** design flaw (config semantics). **Severity:** High — produces permanently-Pending guaranteed pods, the exact failure the system promises to prevent, triggered by an unrelated team's config change. **Likelihood:** near-certain on any config where namespaces list different resource sets; the docs' own example is such a config.

### Mechanism and reasoning

The plugin derives its enforced resource set as the **union of all resources across all namespaces**:

- `configuredResources()` (`namespaceresourceguarantee.go:1286`) collects every resource name appearing under any namespace in `namespaceGuarantees` into one sorted list, stored as `pl.configuredResource`.
- `PreFilter` (`:194-214`) then enforces **every** resource in that union against the pod's namespace, and `namespaceGuaranteeValue` (`:1259`) returns `0` for any resource the namespace does not itself configure.

The namespace-level "absent ⇒ 0" rule is a deliberate, documented security backstop, and it is correct: a namespace not in the config should get nothing. But the same rule applied at the **resource** level, combined with the union, creates cross-tenant coupling with no security rationale:

> The moment *any* namespace configures a guarantee for resource X, every *other* configured namespace's guaranteed pods that request X are capped at **zero** X — i.e., they can never schedule if they request any amount of it.

This is not hypothetical. `configuration.md` §2 (and the design doc's configuration section) shows:

```yaml
namespaceGuarantees:
  team-a: {cpu: "64", memory: "256Gi", nvidia.com/gpu: "32"}
  team-b: {cpu: "48", nvidia.com/gpu: "24"}          # no memory
```

Because `team-a` configures `memory`, `memory` enters the union. Any `team-b` guaranteed pod that requests memory — which in practice is nearly every real pod — fails PreFilter with `guarantee=0` and stays Pending forever. **The documentation's own example configuration is broken under the code's semantics.** Either the live production config always specifies all three resources for every namespace (in which case the constraint was learned operationally and never written down), or this has bitten or will bite.

The deeper design point: a guarantee model in which *adding* a guarantee for one tenant is a *breaking change* for other tenants violates the principle of least surprise and makes config changes — already expensive (rollout required) — risky in a way nothing warns about. `ValidateNamespaceResourceGuaranteeArgs` checks non-negativity and integer-ness but is silent on cross-namespace resource coverage.

### Worked example

Cluster with the example config above. `team-b` submits a guaranteed pod requesting `{nvidia.com/gpu: 2, cpu: 4, memory: 16Gi}`.

1. PreFilter iterates the union `[cpu, memory, nvidia.com/gpu]`.
2. cpu: `usage + 4000m ≤ 48000m` — passes. gpu: `usage + 2 ≤ 24` — passes.
3. memory: requested `16Gi > 0`, `team-b`'s memory guarantee = **0** → `UnschedulableAndUnresolvable`, message `guarantee=0 current=0 requested=17179869184`.
4. The pod is Pending forever. The re-queue hints never help: no deletion in `team-b` can bring usage below 0. The team sees a "guarantee exceeded" message for a resource they never asked to have capped.

Note the failure mode's shape: it appears when **team-a** merges a config change, and it manifests as **team-b's** pods hanging — the worst kind of incident to attribute.

### Steelman / self-challenge

- *"Enforcing the union is the conservative, safe reading: if memory is a protected resource anywhere, an unlisted namespace shouldn't consume unbounded protected memory."* — But guaranteed pods from a *configured* namespace are already namespace-capped on the resources that namespace configures; zeroing a resource the operator never mentioned for that namespace doesn't close a security hole (the namespace was deliberately onboarded), it just encodes "unspecified means forbidden" without saying so. For *unconfigured* namespaces the zero backstop already applies wholesale.
- *"Production config probably lists all resources for every namespace, so this never fires."* — Plausible, and would explain the absence of an incident. But then the constraint is real and unenforced: the system relies on an operator convention that neither validation nor documentation states. The example config actively teaches the broken pattern.
- What would invalidate this finding: a deliberate, documented decision that "a namespace's guarantee list must be total, and partial lists mean zero" — with validation enforcing totality. No such statement exists anywhere in the doc tree.

### Mitigations and proposals

1. **Preferred (small):** extend `ValidateNamespaceResourceGuaranteeArgs` to reject configs where any namespace omits a resource that any other namespace configures ("resource coverage must be rectangular"). Startup-fail is the project's existing pattern for config errors, and it converts a silent per-pod failure into a loud rollout failure. Cost: a few lines + tests. Risk: an existing live config that relies on implicit zeros would fail the rollout — which is exactly the conversation the operator should have.
2. **Alternative (semantics change):** enforce only the resources the pod's namespace itself configures (drop the union; iterate `args.NamespaceGuarantees[pod.Namespace]` instead of `pl.configuredResource` in PreFilter). This makes partial lists mean "uncapped for unlisted resources", which matches how most people read the config, but *weakens* enforcement — probably not desirable for this system's philosophy.
3. **Regardless:** fix the example config in `configuration.md` and the design doc (give `team-b` a memory line), and add an explicit warning to `configuration.md` §3 and `known-issues.md`.

### How to verify

- Unit test: two namespaces, disjoint resource sets, guaranteed pod in each requesting the other's resource → both rejected today.
- Production: `scheduler_namespace_resource_guarantee_quota` — check whether every `namespace` label carries every `resource` label. If yes, the live config is rectangular and only the docs/validation need fixing.

---

<a name="f2"></a>
## F2. The guarantee cap is only as strong as the webhook — the `default-scheduler` profile has no enforcement

**Category:** design flaw (enforcement placement). **Severity:** High — a complete bypass of the system's core promise, reachable by omitting one field. **Likelihood:** depends entirely on an external component's correctness and availability.

### Mechanism and reasoning

The two-tier model's cap is enforced by the guarantee PreFilter, which the configuration enables **only in the `better-scheduler` profile**. The `default-scheduler` profile (configuration.md §2, design doc §Configuration) enables only the reservation plugin. Therefore:

> A pod with `priorityClassName: guaranteed` and an **empty** `schedulerName` is scheduled by the `default-scheduler` profile with stock semantics: `DefaultPreemption` is active, its priority is 1,000,000, and **no guarantee check runs**. It preempts anything, cluster-wide, with no cap.

The design doc's Security section claims defense-in-depth: *"First line: the admission webhook... Second line (this scheduler): a namespace absent from `namespaceGuarantees` has guarantee zero, so a rogue guaranteed pod from an unauthorized namespace can never schedule."* The second line is **profile-scoped and therefore hollow**: the zero-guarantee backstop only triggers for pods that opt *into* the `better-scheduler` profile. A rogue (or merely misconfigured) workload that sets the PriorityClass and *doesn't* set `schedulerName` sails past both the cap and the backstop. The scheduler-side defense covers exactly the path an abuser would not take.

The docs know the combination is dangerous — `configuration.md` §4: *"`priorityClassName: guaranteed` alone (with the default profile) gives priority-based preemption but no guarantee cap — do not allow this; the admission webhook should prevent it."* — but the mitigation lives entirely outside this repo, in a component whose `failurePolicy`, correctness, and coverage this system cannot see. The failure-modes table even analyzes "the admission webhook is down" and concludes the guarantee cap "still holds for anything that does carry the PriorityClass and reach the profile" — an accurate sentence whose qualifier ("and reach the profile") is doing all the work.

This matters more than the usual "webhooks can fail" hedge because the *whole point* of the fork is that scheduling-time enforcement is more robust than admission-time enforcement (§3 of the architecture doc makes exactly this argument against ResourceQuota). The design then leaves its own enforcement admission-gated.

### Worked example

`team-c` (no guarantees configured) creates a Deployment with `priorityClassName: guaranteed`, no `schedulerName`. The webhook is down with `failurePolicy: Ignore` (or has a namespace-selector gap, or the pod is created by a controller the webhook exempts).

1. Pod lands in the `default-scheduler` profile.
2. No guarantee PreFilter runs. The pod's priority is 1,000,000.
3. Cluster is full → `DefaultPreemption` evicts normal pods — from any team — and the pod binds.
4. Repeat at scale: `team-c` can displace the entire opportunistic tier of every other team. The audit trail is stock preemption events, not the `NamespaceResourceGuarantee*` events dashboards watch.

### Steelman / self-challenge

- *"Priority abuse is an admission-control problem; RBAC/webhooks are the Kubernetes-native place for it. The scheduler shouldn't authenticate intent."* — Agreed, and the design says so (§8, non-goal "authenticating who may use the PriorityClass"). But this finding is not about *authenticating* — it's about *capping*. The cap is the scheduler's declared job, and it currently applies in one profile out of two. Enabling the same PreFilter in the default profile authenticates nothing; it just makes the cap total.
- *"Enabling the guarantee plugin in the default profile changes stock behavior, violating R5."* — Only for pods carrying the guaranteed PriorityClass, which by the system's own definition are *not* "workloads outside the guaranteed tier". For every other pod the PreFilter is a no-op (`:180`: non-protected pods return immediately). R5 survives intact.
- *"The webhook also routes guaranteed pods to the profile, so in practice the combination never occurs."* — When the webhook works. The design's own failure-mode analysis contemplates it not working; the conclusion drawn there is too optimistic (see above).
- What would invalidate this finding: evidence that `DefaultPreemption` being active for stray guaranteed pods is *intended* as a grace path. Nothing in the docs suggests that; the docs say "do not allow this".

### Mitigations and proposals

1. **Preferred (small, config-only):** enable `NamespaceResourceGuarantee` under `preFilter` (with the same `pluginConfig` args) in the `default-scheduler` profile. Same plugin, same process, same snapshot; per-pod cost for non-protected pods is one string compare. Stray guaranteed pods then hit the namespace cap (or the zero backstop) regardless of routing. Consider also disabling `DefaultPreemption`/enabling the plugin's PostFilter there — but that is a bigger behavior change for the default profile; the PreFilter alone closes the *cap* bypass, which is the dangerous half.
2. **Also worth doing (defense in depth, small):** a validating rule in the (external) webhook is still the right first line; this proposal only ensures the scheduler's *second line* actually exists. Update the design doc's Security section either way — today it overstates the backstop.
3. **Detection until fixed:** alert on stock `Preempted` events (reason `Preempting`, not `NamespaceResourceGuaranteePreempted`) where the preemptor carries the guaranteed PriorityClass — that signature is exactly this bypass in action.

### How to verify

- Unit/integration: schedule a guaranteed-PC pod through a profile without the plugin; observe uncapped preemption (trivially true from config, but a test pins the fix).
- Production: `kubectl get pods -A -o json | jq '[.items[] | select(.spec.priorityClassName=="guaranteed" and .spec.schedulerName!="better-scheduler")] | length'` — nonzero means the bypass population exists right now.

---

<a name="f3"></a>
## F3. Guarantee accounting is blind to in-flight preemptors — concurrent admissions against the same headroom, wasted evictions

**Category:** design flaw (accounting model). **Severity:** Medium-High — no lasting guarantee violation, but real workloads get evicted for nothing, and the design promises evictions are charged fairly and purposefully. **Likelihood:** rises with preemption frequency and victim termination time; needs two guaranteed pods of the same namespace arriving within one preemption window.

### Mechanism and reasoning

`namespaceProtectedUsage` counts a pod toward its namespace's guaranteed usage only when `pod.Spec.NodeName != ""` (`scheduledProtectedResourceRequest`, `namespaceresourceguarantee.go:596-601`). This is the right conservative rule for *bound* state — but a preemptor that has passed PreFilter, **evicted victims**, and received `status.nominatedNodeName` has an empty `NodeName` while its victims terminate. During that window it is invisible to the guarantee check of every subsequent pod in its namespace.

So the guarantee admission is not serialized against the *commitment* a preemption represents, only against the eventual bind. Two pods can be admitted against the same headroom:

1. Namespace guarantee: 8 GPU, current usage 0.
2. Pod `A` (8 GPU) passes PreFilter (`0+8 ≤ 8`), finds no space, preempts victims on `node-1`, gets nominated + node reserved. Usage still counts as 0.
3. Pod `B` (8 GPU) arrives while `A`'s victims terminate. PreFilter: `0+8 ≤ 8` — **passes**. `B` preempts victims on `node-2` (it cannot use `node-1` — reserved for `A` — so it evicts a *second* set of victims) or binds to free capacity. `B` binds.
4. `A` retries (victim-deletion events requeue it): PreFilter now sees `B`'s 8 GPU bound → `8+8 > 8` → `UnschedulableAndUnresolvable`. `A` waits, correctly.

End state: the guarantee held (re-check at every attempt prevents lasting overcommit), but `A`'s victims were **evicted for nothing** — real opportunistic workloads killed, the freed capacity ultimately going to whoever schedules next. The design's §Security promise — "every eviction leaves an audit trail" and the cost is "bounded by the preemptor namespace's guarantee" — is technically kept, but the evictions attributed to `A` bought nobody anything. In a system whose victim-ordering policy exists to make evictions *fair and purposeful*, purposeless evictions are a design-level defect, not just an inefficiency.

The same blindness also means `PodEligibleToPreemptOthers` will happily start preemption for a pod whose namespace headroom was consumed between its PreFilter and its PostFilter — the check and the eviction are not atomic even within one cycle sequence.

### Steelman / self-challenge

- **Correction made during this review (previous iteration of this analysis was wrong):** an earlier draft claimed `A` would additionally hold its node reservation *indefinitely* while over-guarantee Pending, blacking out `node-1`. That is incorrect. When `A` retries and PreFilter rejects it, `schedulePod` returns a `FitError` and **PostFilter still runs** (`schedule_one.go:163-180`); the preemption evaluator finds no candidates (every node is `UnschedulableAndUnresolvable`, which `preemption.go` excludes — see `:271`), and `syncNominatedNodeReservation`'s no-candidate/not-helpful branch releases the reservation (`namespaceresourceguarantee.go:325-346`). The reservation is held only until the holder's next scheduling attempt. The wasted-eviction half of the finding survives; the stuck-node half does not.
- *"Scheduling cycles are serialized; the failure-modes table says the second pod 'sees the first as scheduled/assumed in the snapshot and fails the guarantee check cleanly'."* — True for the direct-bind path (assume happens in the cycle, synchronously). The table's claim is **incomplete for the preemption path**, where there is nothing to assume for the duration of victim termination — which is precisely when guaranteed pods cluster (they arrive under contention). The failure-modes table should be amended.
- *"Counting nominated pods toward usage risks the opposite failure: a preemptor that never binds (victim stuck terminating) blocks its namespace's other pods."* — Real trade-off, and the strongest argument for the status quo. But blocking a same-namespace sibling while a commitment is in flight is the *correct* reading of a hard cap ("your namespace has already spent this headroom"); admitting the sibling and wasting evictions is not. The stuck-preemptor case is already surfaced (`PreemptionWaiting` events) and has an operator playbook.
- What would invalidate this finding: production data showing same-namespace guaranteed pods essentially never overlap within a preemption window. Possible at current scale — but the design should not depend on arrival statistics staying friendly.

### Mitigations and proposals

1. **Preferred (medium):** in `namespaceProtectedUsage`, additionally count protected pods of the same namespace that have `status.nominatedNodeName != ""` and `spec.nodeName == ""` (visible via the nominator, `handle.NominatedPodsForNode` per node, or the pod lister). This makes admission see in-flight commitments. Care: don't double-count a pod that is both nominated and being evaluated (`pod.UID` exclusion, as upstream's `addNominatedPods` does at `framework.go:1047`).
2. **Cheaper partial (small):** release the reservation *and clear the nomination* when the holder subsequently fails PreFilter with guarantee-exceeded (the release already happens per the correction above; explicitly clearing nomination makes the state legible and frees upstream's nominated-pod accounting from reserving for a pod that cannot bind).
3. **Do regardless (doc):** amend the failure-modes row "Two guaranteed pods race for the last guarantee headroom" to cover the preemption path honestly.

### How to verify

- Unit test: guarantee 8; pod A nominated (not bound) with 8 requested; pod B PreFilter → today passes, should fail under proposal 1.
- Production signal for how often this fires today: victims with `NamespaceResourceGuaranteePreempted` events whose preemptor (by `decisionID`) later logs a guarantee-exceeded rejection before ever binding.

---

<a name="f4"></a>
## F4. The reservation plugin's stated premise is wrong for the topology it ships with; its distinctive behavior is over-blocking, and its store is less durable than the upstream mechanism it duplicates

**Category:** doc-vs-reality misalignment + questionable mechanism scope. **Severity:** High as a understanding/documentation issue (the design doc's rationale for a whole component doesn't hold as stated); Medium as behavior (the plugin's net effect may be negative in the current topology). **This finding was substantially reframed during the review — see the steelman.**

### Mechanism and reasoning

The design doc (§7.1) motivates `NominatedNodeReservation` with: *"When preemption succeeds, the preemptor only receives `status.nominatedNodeName`. Upstream treats this as a soft hint: **nothing reserves the freed capacity**."* Architecture.md §6.1 repeats it: *"Upstream considers nominated nodes only as a soft signal."*

That premise is **inaccurate for a single scheduler process** — the topology this system has run since the same release (v0.4) that introduced the plugin:

- Upstream's `RunFilterPluginsWithNominatedPods` (`framework/runtime/framework.go:974-1029`) evaluates every pod's Filter **twice** when a node has nominated pods: first with all nominated pods of **equal or greater priority** added to the node's usage and PreFilter state (`addNominatedPods`, `:1031-1057`). A node is feasible only if the pod fits *with the nominee accounted*.
- The nominator that feeds `NominatedPodsForNode` lives in the **single `SchedulingQueue`** of the scheduler process (`scheduler.go:90-91`), which **both profiles share**. A guaranteed preemptor nominated by the `better-scheduler` profile is therefore visible to, and accounted against, every pod the `default-scheduler` profile filters.

Consequence: in the one-process/two-profile deployment, a normal pod **already cannot take capacity a nominated guaranteed pod needs** — stock Kubernetes reserves it, resource-accurately, per priority. The race the plugin exists to close was real, but it was a **cross-process** race: the history records it was observed *before* v0.4, when the fork ran alongside a separate stock kube-scheduler whose queue (and therefore nominator) never contained the fork's pods — pods with a foreign `schedulerName` never enter another scheduler's queue, so their nominations are invisible to it. The v0.4 rollout made **two** changes at once: it consolidated to one process, *and* it added the reservation plugin. The consolidation alone plausibly fixed the observed race; the plugin's contribution was never isolated.

What the plugin *distinctively* does, relative to the upstream mechanism it overlaps:

1. **Blocks pods that would legitimately fit alongside the preemptor.** Upstream accounting is resource-accurate: a 2-GPU normal pod may bind next to a nominated 4-GPU preemptor on an 8-GPU node if the leftover fits. The reservation Filter rejects *every* non-holder (`plugin.go:70-102`) — including small CPU-only pods on GPU+CPU nodes, other guaranteed pods, and even above-guaranteed system-priority pods. On mixed nodes this blacks out the whole node for the victim-termination window, which the project's own failure-mode table admits can be unbounded (stuck finalizers ⇒ "operator action"). There is no TTL; staleness triggers are holder-state-driven only (`isStaleReservation`, `plugin.go:149`).
2. **Adds protection against pods with priority *above* the guaranteed class** (upstream accounts nominees only against equal-or-lower-priority pods, `framework.go:1047`). In practice that's system-critical priority classes — rare on GPU nodes, and arguably pods that *should* be able to displace a reservation.
3. **Covers windows where the nomination is cleared** (certain scheduling-cycle errors pass `clearNominatedNode`, `schedule_one.go:140,160-257`) — real but narrow.
4. **Is less durable than what upstream already has.** The store is process memory (documented). But `status.nominatedNodeName` is **API-persisted**: after a restart or failover, the new leader's queue re-learns pending pods and their nominations, and upstream accounting resumes. The plugin's protection dies with the process; the mechanism it duplicates does not. The docs present in-memory-ness as an accepted cost of the *only possible* mechanism — it is actually a regression against the built-in one.

None of this is visible in the documentation: no doc mentions that upstream Filter accounts nominated pods at all. The design doc's alternatives-and-honesty standard is high everywhere else; here, the analysis that would have questioned the component's necessity is missing.

### Worked example (the plugin costing, not saving)

`node-1`: 8 GPU, 64 CPU. Guaranteed pod `P` (4 GPU) preempts victims; node reserved. Victims get 60s grace.

- Normal pod `Q1` (2 GPU): upstream accounting would admit it once victims exit *if* 2 GPU remain after `P`'s 4 — i.e., legitimate coexistence. Reservation Filter: blocked.
- CPU-only pods `Q2..Qn` (no GPU at all): upstream would admit them freely. Reservation Filter: blocked.
- For 60+ seconds, an entire 8-GPU/64-CPU node admits nothing. Multiply by preemption frequency.

The event trail even records these as wins (`Blocking pod on reserved nominated node`, V(3)) — the dashboard cannot distinguish "blocked a real steal" from "blocked a pod upstream would have correctly co-scheduled".

### Steelman / self-challenge

- **This finding was reframed during the review.** The initial draft treated over-blocking (formerly a separate finding) as the reservation's main flaw while accepting its premise. Reading the actual upstream code (`RunFilterPluginsWithNominatedPods`) showed the premise itself fails in-process — which both strengthens the critique (the component may be redundant) and softens it (see next points).
- *"Maybe steals were observed even after consolidation."* — This is the decisive question, and static analysis cannot answer it. Upstream nominated-pod accounting has known soft edges: nomination cleared on some failure paths; the nominee itself may be re-nominated elsewhere; historical upstream bugs in this area. If post-v0.4 production data shows the reservation Filter blocking pods in situations where the nominee's resources *would not* have protected it, the plugin earns its keep. The docs record no such observation — the plugin was designed against the pre-consolidation incident.
- *"Belt and suspenders is cheap; the plugin is 300 lines and one map lookup per Filter."* — Fair for the *code* cost. The argument fails on the *behavioral* cost: the belt (over-blocking whole nodes) is not free, and per the worked example it may waste more capacity than the residual race it covers. Redundancy that changes behavior is not mere redundancy.
- *"The reservation gives deterministic, observable semantics (events, named holder) that implicit accounting doesn't."* — Genuinely true and the best defense. A reasonable end state keeps the plugin for observability and the above-priority edge but narrows its Filter (below).
- What would invalidate this finding: (a) production evidence of post-consolidation steals that nominee accounting demonstrably missed; (b) an upstream-1.32-specific defect in nominated-pod accounting that the team knew of (none is documented).

### Mitigations and proposals

1. **Doc fix (small, do first):** correct §7.1 / architecture §6.1 — state that upstream *does* account nominated pods within one process, that the observed race predates the single-process topology, and that the plugin's marginal coverage is (over-)blocking co-fitters, above-priority pods, and nomination-cleared windows. The design doc's credibility elsewhere is built on exactly this kind of honesty.
2. **Measure (small):** count `Blocking pod on reserved nominated node` occurrences (raise to V(2) or add a counter metric) and, for each, whether the blocked pod would have fit *with the nominee accounted* — that classification separates real saves from over-blocking. Decide the plugin's future on that data.
3. **If keeping it — narrow it (medium):** make the Filter resource-aware: block a non-holder only if it does **not** fit in `allocatable − (current usage + holder's requests)`, i.e., replicate nominee accounting but keyed on the reservation. This retains the deterministic reservation semantics while ending whole-node blackouts. Add a generous TTL (e.g., 15 min) as a backstop against pathological holder states.
4. **If the data says it never saves anything:** retire the plugin; keep the events by emitting them from the existing nomination path. This also deletes the F10 cross-profile invariant and most of F8's fragile string-matching.

### How to verify

- Integration test (also closes part of F17): one process, two profiles, preempt with the reservation plugin *disabled* — assert a lower-priority pod cannot take the nominee's capacity (validates upstream accounting in this fork's baseline); then enable it and measure what additionally gets blocked.
- Production: the classification query from proposal 2.

---

<a name="f5"></a>
## F5. The alternatives analysis omits the most relevant alternative: out-of-tree plugins in a custom binary

**Category:** questionable strategic decision + doc gap. **Severity:** Medium, compounding — it is the fork's single largest standing cost, re-paid every upstream minor.

### Mechanism and reasoning

The design doc's alternatives section is unusually good — it compares Kueue, Volcano, and YuniKorn against R1–R6 and names the fork's honest cost: *"porting a ~4,300-line diff to every upstream minor."* But the comparison set is wrong by one entry. Both custom plugins are **ordinary scheduling-framework plugins**: they implement public extension-point interfaces, are registered via the standard registry, and touch no scheduler internals. Kubernetes has a first-class pattern for exactly this — an out-of-tree module that builds a custom `kube-scheduler` binary via `app.NewSchedulerCommand(app.WithPlugin(Name, New))`, as `sigs.k8s.io/scheduler-plugins` has done for years. Under that pattern:

- The plugins, args types, and validation live in your own repo; upstream adoption becomes a `go.mod` bump (`k8s.io/kubernetes v1.33.x`) plus recompile, instead of rebasing a fork of k/k.
- The deployment story is identical: one binary, one image, multi-profile config, same RBAC. R1–R6 are met exactly as today — the plugins *are* the same code.
- The genuinely fork-requiring part is only the decision logging (~300 lines in `schedule_one.go`, F6/F11) — `prioritizeNodes`/`findNodesThatFitPod` are not extensible. That could remain as a small carry-patch on top of upstream, or be accepted as the one thing the fork buys.

The history explains how the omission happened: the foundation decision (April 2026) was framed as *"in-tree fork build (not a sidecar/extender)"* — a false dichotomy. Extenders (webhook callouts) were rightly rejected; out-of-tree *plugins* are neither a sidecar nor an extender and were never evaluated. Every subsequent doc inherited the frame. The alternatives section then compares against three systems that all fail R4/R5 structurally, making the fork look uniquely necessary — while the alternative that meets all six requirements at a fraction of the standing cost goes unmentioned. The doc's own "revisit when" discipline (Future Work table) has no row for this.

Concretely, of the ~18k-line diff: plugins + tests + args + validation ≈ 3,000 lines (portable as-is), release/docs/dashboard ≈ 3,000 (repo-agnostic), generated code ≈ 200 (regenerated anywhere), `schedule_one.go` + tests ≈ 520 (the true fork). The rebase surface could shrink by roughly an order of magnitude.

### Steelman / self-challenge

- *"The fork already exists and works; migration is churn without user-visible benefit."* — True today. The benefit lands at the **first rebase** (the open question the doc itself flags: "upstream rebase cadence... one [policy] will be needed, eventually"). The right time to evaluate is before that rebase, not after paying it.
- *"Out-of-tree still pins you to upstream minors and still means building/releasing a binary."* — Correct: the operational footprint is unchanged. The difference is conflict-bearing rebases vs. mostly-mechanical dependency bumps; only the small logging patch can conflict.
- *"Importing `k8s.io/kubernetes` as a library is officially discouraged and requires the `replace`-directive dance for staging repos."* — The strongest technical counterpoint. It is awkward but well-trodden (scheduler-plugins does exactly this, tracking each minor). The awkwardness is bounded and documented; rebase conflicts are not.
- *"The decision logging is a hard requirement and needs the fork anyway, so the fork is justified."* — It justifies *a* patch, not keeping 3,000 portable lines inside the patch. Hybrid (out-of-tree plugins + minimal logging carry-patch) captures most of the value.
- What would invalidate this finding: a decision record showing out-of-tree plugins were evaluated and rejected for a concrete reason (e.g., a needed unexported internal). No such record exists, and the code uses only public framework API plus `preemption.Evaluator` — which is importable.

### Mitigations and proposals

1. **Doc (small):** add the out-of-tree pattern to the alternatives section with an honest row in the comparison table, and a Future Work row: *"Extract plugins out-of-tree — revisit at the first upstream rebase."*
2. **At the first rebase (large but self-funding):** move `namespaceresourceguarantee/`, `nominatednodereservation/`, args types, and validation to a new module; build the binary via `app.WithPlugin`; keep (or drop, per F4's data) the logging patch as the only fork remnant. The release script changes trivially (build a different repo).

### How to verify

Mechanical check that the plugins compile against the framework's public API from an external module — a half-day spike, worth doing before the doc claims it.

---

<a name="f6"></a>
## F6. Decision logging silently removed upstream V(10) behavior — violating the project's own R5

**Category:** misalignment with stated goals. **Severity:** Medium (a debugging regression for the whole default profile), trivially fixable. **Likelihood:** certain — it is unconditionally true in the code.

### Mechanism and reasoning

R5 (design doc §Goals): *"Stock behavior preserved **bit-for-bit** for every workload outside the guaranteed tier."* In `prioritizeNodes`, the three upstream V(10) blocks (`Plugin scored node...`, `Extender scored node...`, `Calculated node's final score...`) were **replaced** by `if decisionLogsEnabled` — the `loggerVTen.Enabled()` paths are gone (visible in the diff vs `59526cd4867`; current code around `schedule_one.go:1045-1141`). Result: for every profile other than `better-scheduler`, per-node score logging no longer exists **at any verbosity**. An operator debugging default-profile placement with `--v=10` — the documented upstream method, referenced by this project's own configuration.md §6 ("`--v=10` — upstream per-node scoring for **all** profiles") — silently gets nothing. Configuration.md is thus also describing behavior the fork no longer has.

This is exactly the class of drift the project's invariant list (README §"Working on the Code": *"Decision logs stay gated to the `better-scheduler` profile"*) was written to prevent — the gate was implemented by subtraction instead of addition, and no doc noticed.

### Steelman / self-challenge

- *"Nobody runs `-v=10` in production, so nothing of value was lost."* — The value of `-v=10` is precisely for the rare desperate debugging session; that is when discovering it silently does nothing is most expensive.
- *"R5 means scheduling behavior, not log output."* — Defensible reading, but the project documents `-v=10` as an operator tool in the same doc tree; observable operator-facing behavior is behavior. At minimum the docs and code must agree.
- Nothing invalidates the core fact; only its severity is arguable.

### Mitigation

One-line per site: `if decisionLogsEnabled || loggerVTen.Enabled() { ... }` (choosing the structured better-scheduler format when the profile gate is on, the upstream format otherwise — or simply emitting both when both are on; duplication at V(10) is harmless). Add a regression test beside `TestPrioritizeNodesDecisionLogs` asserting V(10) output for a non-better-scheduler profile.

---

<a name="f7"></a>
## F7. "Heaviest borrower pays first" only applies between *configured* namespaces

**Category:** design flaw (policy scope). **Severity:** Medium — the fairness policy silently doesn't govern the population most likely to be the heaviest borrowers. **Likelihood:** high wherever unconfigured namespaces run opportunistic GPU workloads, which the two-tier model explicitly invites.

### Mechanism and reasoning

The victim-ordering rule 2 — evict pods from the namespace using the most preemptible capacity of the deficient resources — depends on `namespacePreemptibleUsage` (`namespaceresourceguarantee.go:828-863`), which computes usage **only for namespaces present in `namespaceGuarantees`** (`protectedNamespaces := sets.KeySet(...)`, `:830`; pods elsewhere are skipped at `:851`). `compareNamespaceForEviction` (`:748-776`) then **abstains** (`return 0, false`) when either namespace lacks a usage entry (`:753-756`), causing `utilMoreImportantByNamespacePolicy` to fall through to start time — and note the abstention also skips rule 3 (own-namespace-first), not just rule 2.

But the opportunistic tier is deliberately open to *all* namespaces — that's R4/R5's "zero enrollment" selling point. A namespace with no guarantees (a scratch/experiment namespace, a team that never onboarded) can be the single largest consumer of preemptible GPU capacity, and it is precisely the population the fairness rule never charges. The stated intent — *"the cost lands first on the namespaces borrowing the most opportunistic capacity, rather than to whoever happens to have the newest pods"* (architecture §5.3) — holds only in the special case where every borrower is configured. Between one configured and one unconfigured namespace, or two unconfigured ones, the policy is exactly upstream's (start time): "whoever happens to have the newest pods".

The scoping looks like an implementation convenience (the map was sized to the config), not a policy decision: no doc mentions it, and the design doc's victim-ordering flowchart shows rule 2 applying unconditionally on equal priority.

### Worked example

`node-2` (8 GPU): `a-opp-1` (`team-a`, configured, 2 GPU, started January) and `x-opp-1` (`team-x`, *not configured*, 6 GPU, started yesterday). Cluster-wide preemptible GPU usage: `team-a` = 2, `team-x` = 40. A `team-a` guaranteed pod needs 4 GPU on this node.

- Intended by the stated policy: `team-x` — borrowing 20× more — pays.
- Actual: `team-x` has no usage entry → comparison abstains → start time decides → newest evicted first → `x-opp-1` happens to be evicted here, but flip the ages and **`a-opp-1` is evicted while the 40-GPU borrower is reprieved**. The policy outcome is age-lottery, not borrowing-proportional.

### Steelman / self-challenge

- *"Configured namespaces are the tenants we make promises to; ordering among strangers doesn't matter."* — The promise at stake is to the *victims'* owners as much as the preemptor's: the design sells eviction fairness as part of the SLA story (§Security: "victim ordering charges the heaviest borrowers first"). A configured tenant's opportunistic pod being evicted while an unconfigured namespace's larger footprint survives is the concrete unfairness the rule was built to prevent.
- *"Computing usage for all namespaces costs more."* — The scan already iterates every pod on every node (`:845-861`); the only change is not skipping unconfigured namespaces. Map size grows from ~tens to ~hundreds of entries per preemption attempt. Negligible.
- *"Maybe unconfigured namespaces shouldn't be on GPU nodes at all."* — Perhaps operationally true, but nothing in this system enforces it, and the design explicitly embraces unbounded opportunistic use.
- What would invalidate this finding: a deliberate policy statement that fairness accounting is a benefit reserved for configured tenants. None exists.

### Mitigation

Drop the `protectedNamespaces` filter in `namespacePreemptibleUsage` (build entries lazily for any namespace encountered). One conditional removed, plus a test with an unconfigured heavy-borrower namespace. Document the policy either way — the current behavior, if kept, belongs in architecture §5.3 and the design doc's flowchart caption.

---

<a name="f8"></a>
## F8. Reservation lifecycle and event classification are keyed on string-matching upstream status messages

**Category:** fragility by construction. **Severity:** Medium — silent degradation, and the trigger (upstream message rewording) coincides exactly with the project's recurring activity (rebases).

### Mechanism and reasoning

Three code paths branch on the *text* of statuses produced by the shared upstream preemption evaluator:

- `preemptionWaitingOnTerminatingVictims = "not eligible due to a terminating pod on the nominated node."` — must equal, byte-for-byte, the message this plugin's own `PodEligibleToPreemptOthers` returns *and* semantically mirror upstream's (`namespaceresourceguarantee.go:62`, compared at `:329` and `:1036`).
- `preemptionNoCandidateMessage = "no candidate node for preemption"` and `strings.Contains(msg, "Preemption is not helpful for scheduling")` (`:63-64`, used at `:332` and `:1049-1057`) — both strings are owned by `framework/preemption/preemption.go` (e.g., `:271`), private message text upstream is free to reword in any minor.

Consequences of a silent mismatch after a rebase: `syncNominatedNodeReservation` stops releasing reservations on no-candidate/not-helpful outcomes (the F3-correction path stops working — reservations then persist until the holder binds elsewhere or is deleted, re-elevating the F3/F4 blackout concerns), and `classifyPreemptionEvent` mis-buckets events into the fallback reason, quietly corrupting the dashboard's event taxonomy. Nothing fails loudly; a unit test comparing against the *same constants* won't catch it either, because the constants and the comparison drift together only if someone re-copies them.

This is a self-inflicted contract problem: the fork *owns* the evaluator code and could expose the outcome structurally.

### Steelman / self-challenge

- *"Rebases already require re-verifying everything; the strings are just one more item."* — The difference between a rebase item that fails compilation/tests and one that fails silently in production is the whole point. String contracts are in the second class.
- *"Upstream statuses genuinely don't carry a typed reason; string matching was the only non-invasive option."* — True for an out-of-tree plugin (relevant to F5!). Not true for a fork: adding a typed field is a five-line upstream-side change the fork is entitled to make.
- What would invalidate this: nothing fully; severity drops if F4 ends in retiring the reservation plugin (two of three match sites disappear).

### Mitigation

In the fork's `preemption.Evaluator`, return a typed outcome (e.g., a small enum on the status or a wrapper result: `Nominated | WaitingForVictims | NoCandidates | NotHelpful | Error`) and branch on that. Keep messages for humans only. Add a rebase-checklist item ("re-verify evaluator outcome mapping") to operations.md until then. If F5's extraction happens, this typed outcome is the one upstream-facing patch the plugins would still want — worth designing once.

---

<a name="f9"></a>
## F9. ~400 lines copied from DefaultPreemption to change one comparator

**Category:** maintenance liability. **Severity:** Medium — every upstream fix to preemption victim-selection now needs manual porting into a fork-private copy, forever.

### Mechanism and reasoning

`SelectVictimsOnNode` (`namespaceresourceguarantee.go:425-538`), `PodEligibleToPreemptOthers` (`:374-395`), `filterPodsWithPDBViolation` (`:1154-1190`), and `calculateNumCandidates`/`GetOffsetAndNumCandidates` (`:362,:709`) are near-verbatim copies of `defaultpreemption`. The entire semantic delta is: (a) the victim sort uses `utilMoreImportantByNamespacePolicy` instead of `util.MoreImportantPod`, (b) `orderedDeficientResources` feeds it, (c) trace recording. Everything else — the remove/reprieve loop, PDB accounting, eligibility rules — is duplicated logic whose upstream original keeps evolving (PDB edge cases and eligibility fixes land upstream regularly).

Two concrete losses beyond drift risk:

- **Upstream fixes don't propagate.** A rebase updates `defaultpreemption` automatically via git; the copy needs a human to notice, diff, and port. The rebase checklist doesn't mention it.
- **Configurability regressed.** DefaultPreemption exposes `minCandidateNodesPercentage/Absolute` as `DefaultPreemptionArgs`; the copy hardcodes them (`:57-58`). An operator who needed to tune candidate search for the guaranteed tier now needs a code change and release.

The alternative was available in-tree: since this is a fork, `defaultpreemption.Pod­EligibleToPreemptOthers`/`SelectVictimsOnNode` could be parameterized with an ordering func (a two-line upstream-side seam), and this plugin could embed the default plugin, overriding only the comparator. One preemption implementation, one place for upstream fixes to land, config knobs intact.

The v0.9 commit (`3961e2be095`) removes the last argument against this: it *already* modifies the shared preemption machinery — changing the `preemption.Interface.OrderedScoreFuncs` signature and updating `defaultpreemption` to match. The project evidently accepts seams in shared preemption code when a feature needs them; the same license applies to the ordering seam that would delete the 400-line copy.

### Steelman / self-challenge

- *"Copying decouples us from upstream churn — the copy can't be broken by upstream refactors."* — It also can't be *fixed* by upstream corrections, and preemption correctness bugs are the kind you want inherited. Decoupling by copy is only a win when upstream quality is a net negative; for kube-scheduler preemption it is not.
- *"Injecting a comparator into defaultpreemption is itself a fork patch that conflicts on rebase."* — Yes, but a ~10-line seam conflicts far less often, and far more loudly, than 400 silent lines.
- *"The copy was probably faster to ship."* — Certainly, and v0.3 shipped in a day per the history. Reasonable then; the liability accrues now, per rebase.

### Mitigation

At the next substantive touch of preemption code (or the F5 extraction, which forces the question): add the ordering seam to `defaultpreemption` and delegate; restore the candidate-sizing knobs by accepting a `DefaultPreemptionArgs`-shaped sub-config in `NamespaceResourceGuaranteeArgs`. Until then, add a rebase-checklist line: "diff `SelectVictimsOnNode`/eligibility against upstream `defaultpreemption` and port fixes."

---

<a name="f10"></a>
## F10. Load-bearing configuration invariants are enforced only by documentation

**Category:** robustness gap. **Severity:** Medium — each invariant, when violated, fails silently, and two of them disable core protections.

### Mechanism and reasoning

The design depends on invariants that live solely in prose:

1. **Reservation Filter in *every* profile** (architecture §6.3, configuration.md: "REQUIRED in every profile"). Forgotten in one profile ⇒ that profile's pods ignore reservations; the mechanism is defeated exactly as the docs warn — with zero startup or runtime signal.
2. **Packing Score weight must dominate** ("must dominate default spreading scores"). Weight 1 instead of 100 ⇒ packing silently ineffective; fragmentation returns; discovered only when an 8-GPU pod hangs months later.
3. **Neither plugin is in any default set** ("forgetting to enable one is silent" — the docs' words, README §2 and configuration.md).
4. **`DefaultPreemption` disabled in the better-scheduler profile** — if someone re-enables it (or forgets `disabled:` on a config rewrite), two PostFilter plugins run with interleaving semantics nobody has tested.

The scheduler *has* the entire multi-profile configuration at startup and already crash-fails on args validation errors (operations.md §2 treats loud rollout failure as the desired behavior). Cross-profile validation is the same pattern one level up, and the fork owns the config-loading code. The project's known-issues list flags the silence as a trap — a flagged trap that code could remove is a decision worth revisiting.

### Steelman / self-challenge

- *"Config flexibility is the point of profiles; the scheduler shouldn't second-guess the operator."* — For generic plugins, agreed. These two plugins are not generic: the reservation plugin's own documentation states it is only correct when globally enabled. Encoding a plugin's *documented correctness precondition* is not second-guessing.
- *"There's exactly one config in one cluster; the team knows the rules."* — Today. The docs are explicitly written for "the next session" and future operators; invariants that live in memory decay.
- Invalidated if: F4 retires the reservation plugin (kills invariant 1) — another reason to resolve F4 first.

### Mitigation

In the fork's scheduler setup (post-profile-construction), validate: if `NamespaceResourceGuarantee` is enabled in any profile, then (a) `NominatedNodeReservation` is in `filter`+`postBind` of **all** profiles, (b) `DefaultPreemption` is disabled in every profile where the guarantee plugin runs PostFilter, (c) warn (not fail) if the guarantee Score weight is below the max default plugin weight. ~50 lines + tests. Fail startup for (a)/(b) — consistent with existing config-error behavior.

---

<a name="f11"></a>
## F11. Decision logs are gated on a hardcoded profile-name literal in core code

**Category:** code/config coupling. **Severity:** Low-Medium — one renamed string in a ConfigMap silently disables the entire observability feature.

### Mechanism and reasoning

`detailedScoreLoggingProfile = "better-scheduler"` (`schedule_one.go:63-65`, checked at `:713`). The profile name is deployment configuration; the gate is compiled code. v0.9 added a **second, independent** copy of the same literal: `decisionLogProfile = "better-scheduler"` inside the plugin (`namespaceresourceguarantee.go:60`), which `preemptionDecisionLogKeyvals` (`:965`) stamps as the `profile` field on the new PostFilter decision log lines *unconditionally* — the plugin does not know its profile, so the label would simply be wrong if the plugin ever ran elsewhere (including under F2's proposal). Two constants that must agree with each other and with the ConfigMap, in two packages, is the coupling compounding. Rename the profile (new cluster, naming-convention change, a second deployment of the same binary for a different pool) and every decision log vanishes — no error, no warning, and the Grafana dashboard goes blank with no indication why. The coupling also means the binary can't serve two explained profiles, and the constant is invisible from the config file where an operator would look.

The natural home for this switch is per-profile configuration. The fork already extends the config API surface (`NamespaceResourceGuaranteeArgs`); a per-profile knob is the same pattern. Options in ascending invasiveness: an env var/flag naming the profile(s); a fork-added field on the profile config (`decisionLogs: true`); or keying off the presence of the `NamespaceResourceGuarantee` plugin in the profile (zero new config, approximately the right semantics — "explain the profiles running the custom policy").

### Steelman / self-challenge

- *"It's a fork for one cluster; the constant matches the one profile name we use, and a config knob is ceremony."* — Accepted as the reason it shipped this way (v0.6 history shows the gate was the pragmatic alternative to a metric or global verbosity). The cost is real only at rename/second-deployment time — but the failure at that time is maximally silent, and observability features that disappear silently are the worst kind.
- The "key off plugin presence" option has a wrinkle: F2's proposal enables the guarantee PreFilter in the default profile, which would then also enable decision logs there (large volume). So prefer an explicit knob if F2 is adopted.

### Mitigation

Add an explicit per-profile gate (config field or flag). Small change; do it whenever `schedule_one.go` is next touched (e.g., the F6 fix) to avoid a dedicated release.

---

<a name="minor"></a>
## F12–F18. Minor findings

These use a compact format deliberately — each is real but small, the mechanism is one sentence, and a full steelman/example section would add length, not insight. (Per-finding severity: all Low unless noted.)

**F12 — known-issues #5's "harmless" is wrong.** A non-protected pod routed to the `better-scheduler` profile loses preemption *entirely*: `DefaultPreemption` is disabled there and the plugin's PostFilter rejects non-protected pods (`namespaceresourceguarantee.go:236-237`). Any team that sets `schedulerName: better-scheduler` on a workload with a mid-tier PriorityClass silently loses that class's preemption rights. Not harmful today only if nobody does this — which is exactly what "harmless, but know it exists" fails to convey. *Fix:* reword known-issues #5 and configuration.md §4 to state the preemption loss explicitly. (Medium-low severity: it's a documented-wrong claim about a user-reachable behavior.)

**F13 — `PreemptionAttempts` metric semantics skewed (resolved).** The increment originally sat at the top of PostFilter, so every non-protected pod that merely reached PostFilter counted as a preemption attempt. It now runs only after the protected-pod and evaluator gates, so dashboards count eligible attempts.

**F14 — Packing weight flattens all user soft preferences for guaranteed GPU pods.** Weight 100 × score 0–100 = up to 10,000 points, dominating not only spreading (intended) but preferred node affinity, soft topology spread, and every other soft signal a user attaches to a guaranteed GPU pod. Users are not told their `preferredDuringScheduling...` terms are effectively dead in this tier. *Fix:* one paragraph in configuration.md §4 and architecture §5.6. (If real use-cases need soft preferences to survive, revisit the weight ratio — but that is a policy choice; the doc gap is the finding.)

**F15 — Config-rollout × in-memory-reservation compounding.** Every guarantee edit requires a Deployment rollout (documented), and every rollout drops all reservations (documented) — so each team onboarding reopens the nominated-node window for any in-flight preemption (mitigated by upstream nominee accounting per F4, which survives restarts via API-persisted nominations — worth stating, since it is the actual safety net during rollouts). *Fix:* connect the two facts in operations.md §2.

**F16 — `decisionID` is the pod UID; events can't be attempt-pinned.** The name oversells (it identifies a pod across *all* its attempts, not a decision); logs compensate with `attempt` — and v0.9 extended `attempt` to the plugin's PostFilter decision logs too (`framework.SchedulingDecisionAttemptFromState`) — but preemption/reservation **events** still carry only `decisionID`, so an event cannot be correlated to a specific attempt's log trace when a pod preempts twice. *Fix:* include `attempt` in event notes (cheap, bounded), or accept and document the asymmetry.

**F17 — No integration test covers the riskiest composition.** Unit coverage is genuinely good, but nothing exercises two profiles + preemption + reservation store + requeue hints in one process — the exact mechanism whose correctness depends on process-global state and cross-profile config (F4, F10). The design doc calls the missing e2e "accepted while the diff stays small"; the diff is 18k lines and both profiles of the entire cluster run through it. *Fix:* one `pkg/scheduler`-level integration test with two profiles reproducing the worked example (also the vehicle for F4's verification), plus the F2/F6 regression tests.

**F18 — `SharedStore()` package singleton couples the plugins.** The guarantee plugin reaches into `nominatednodereservation.SharedStore()` directly (`:290`); tests need `ResetSharedStoreForTest`. Works, but a handle-scoped or injected store would remove hidden global state and make the cross-plugin contract explicit. *Fix:* opportunistic refactor only; not worth a dedicated change (and moot if F4 retires the plugin).

---

## Release-focused findings: v0.8 and v0.9 (F19–F23)

The two releases that landed while this review was in progress were given their own analysis pass. v0.8 (`0eb020b3da3` per the ledger; branch commit `9c7c68970fe`, 2026-07-10/13) is the attempt-aware decision observability whose code was already covered by the main review — its new findings are about the release artifacts around it. v0.9 (`3961e2be095`, 2026-07-13) is preemption candidate scoring, reviewed here in full.

First, what v0.9 gets **right**, because it matters for weighing the findings: it closes a real gap the main review had missed. Before v0.9, the packing policy (F14, architecture §5.6) influenced only the *non-preemption* placement path — when a guaranteed GPU pod preempted, the candidate node was chosen purely by upstream tie-breakers (fewest victims, victim priorities, start times), so preemption placement could scatter the guaranteed tier and quietly undo the v0.5 packing objective. Ranking candidates by post-preemption protected-GPU packing (`OrderedScoreFuncs` → `preemptionProtectedGPUPackingScore`, `namespaceresourceguarantee.go:397-423,:642-682`) aligns both placement paths on one policy, and the accompanying decision logs and dashboard panels follow the project's established observability pattern. The design instinct is correct; the findings below are about how it was landed.

<a name="f19"></a>
### F19. v0.8 release hygiene: one mega-commit, an off-branch ledger sha, and stale release docs — the conventions born from the "test"-commit incident all slipped at once

**Category:** process. **Severity:** Medium — nothing is functionally broken, but the project's own traceability guarantees (ledger ↔ branch ↔ docs) are the things that broke, and they are load-bearing during incidents and rebases.

**Mechanism and evidence.** Three independent slips, same release:

1. **The mega-commit.** Branch commit `9c7c68970fe` is titled "add attempt-aware decision observability" but contains: the ~173-line `schedule_one.go` change *plus* the entire `docs/better-scheduler/` tree (~2,000 lines across seven files), the 1,228-line `design-doc.typ`, a **10,937-line compiled PDF binary**, the 1,072-line Grafana dashboard JSON, and the root-doc stub rewrite. The commit message describes perhaps 5% of the diff. This is precisely the anti-pattern the project documented for itself after commit `afe076663b4` ("test") — known-issues #7 and README §Conventions: *"Keep unrelated changes in separate commits; write real commit messages."*
2. **Ledger ↔ branch divergence.** The ledger's v0.8 row points at `0eb020b3da3`, a commit that exists only on the release worktree branch `release-better-scheduler-v0.8-20260710` — it is not an ancestor of `CLOUD-0-better-scheduler`. The v0.7 worktree release merged its state back (history.md documents that as the procedure); v0.8 did not. Anyone answering "what exactly is running as v0.8?" from the main branch gets the *wrong commit* — they must know to look for a worktree branch. The ledger's whole purpose (operations.md §1) is making that question trivially answerable.
3. **Stale release docs.** `history.md` has no v0.8 or v0.9 entries, and `README.md` still declares "Current release: **v1.32.4-bs-v0.7**" while the ledger records v0.9 — violating the "update docs in the same change set" convention (README, operations.md §6) in the exact change set that *added* those docs to the repo.
4. A durability footnote: the compiled PDF (~580 KB binary blob) is now tracked in git and will re-blob on every rebuild; the `.typ` source is the reviewable artifact. Consider generating the PDF at release time or accepting the growth knowingly.

**Steelman / self-challenge.** The mega-commit plausibly *is* the docs-import commit — importing a whole documentation tree is one logical change, and splitting docs-import from the code change may have felt pedantic in the moment. Fair for the docs files; not for bundling the `schedule_one.go` behavior change under the same message, which is what makes `git log pkg/scheduler/` misleading. The off-branch sha may simply be a not-yet-done merge-back — in which case this finding is a reminder, not a diagnosis. What would invalidate it: a merge-back and doc update landing promptly (the convention allows "same change set", and the window is still open — barely).

**Mitigations.** All small: write the v0.8/v0.9 `history.md` entries and fix the README current-release line; either merge/cherry-pick `0eb020b3da3` into the branch or add a ledger column/note stating the release branch for worktree releases; adopt "release commit must be an ancestor of the main branch before the tag is pushed" as a release-script check (`hack/release-better-scheduler.sh` already checks tags — one more `git merge-base --is-ancestor` guard); split code from docs in future commits.

**How to verify.** `git branch --contains 0eb020b3da3` (today: only the release branch); `grep "Current release" docs/better-scheduler/README.md`.

<a name="f20"></a>
### F20. v0.8's attempt-scoping is a default-empty manual textbox — and history.md documents a "hidden `latest_attempt` variable" that was never shipped

**Category:** doc-vs-artifact misalignment + usability gap in the shipped fix. **Severity:** Medium — the stated problem ("panels silently blend attempts") is replaced by a quieter one ("panels are silently empty"), and the internal documentation describes a mechanism that does not exist.

**Mechanism and evidence.** v0.8's stated driver (history.md, unreleased-2026-07-10 entry): derived panels using `last_over_time`/`max_over_time` *"could silently blend multiple scheduling attempts"*, and — per the same entry — *"The dashboard gained a hidden `latest_attempt` variable and all derived Loki panels now filter on it."* The shipped dashboard contains no such variable. Its template list is `loki_ds, prom_ds, workload_ns, workload_pod, final_node, scheduler_pod, attempt` — where `attempt` is a **visible textbox defaulting to the empty string**. Every derived panel filters `| attempt = `${attempt}``, so with the default value the label filter is `attempt=""`, which matches no log line (attempts are `1,2,3…`): **all derived panels render empty until the operator manually types an attempt number**, having first read the raw Decision Trace to learn which numbers exist. observability.md §4 describes the textbox correctly but never states the blank-by-default behavior; history.md describes a different design altogether — presumably an earlier iteration that was replaced without updating the entry.

Two distinct defects, then: (a) *docs*: history.md documents non-existent behavior — poisonous precisely because this doc tree is the designated source of truth and the entry reads as authoritative; (b) *design of the fix*: the blend-prevention is manual. The failure mode moved from "wrong data, looks right" to "no data, looks broken" — a safer default, genuinely, but the v0.8 goal ("isolate the *latest* attempt") is achieved by operator effort on every single investigation, which is the kind of friction that causes operators to stop using the dashboard.

**Steelman / self-challenge.** Empty-by-default may be a deliberate "force the operator to choose consciously" design — defensible, and strictly better than silent blending. And Grafana's Loki integration makes a computed "max attempt for this pod" variable genuinely awkward (query variables over Loki-extracted label values with template dependencies are fragile). If the team tried the hidden-variable design and abandoned it for that reason, the *code* decision is sound and only history.md is wrong. What would invalidate the usability half: evidence that investigations are infrequent enough that one manual step is noise. Nothing invalidates the docs half — the entry is factually wrong today.

**Mitigations.** (a) Fix the history.md v0.8 entry to describe the shipped textbox (one paragraph). (b) Cheap usability: default the textbox to `1` (correct for the common single-attempt case, wrong-but-visible for retries) or add a "Latest Attempt Seen" stat panel (`max_over_time(... | unwrap attempt)`) next to the Decision Trace so the operator learns the number without reading raw logs; note the blank-until-set behavior in observability.md §4 regardless.

**How to verify.** `jq '.templating.list[] | select(.name=="latest_attempt")' grafana-decision-dashboard.json` → empty today; the `attempt` variable's `current.value` is `""`.

<a name="f21"></a>
### F21. v0.9 silently re-defines the shared `OrderedScoreFuncs` contract, and the preemption-placement policy shift it enables is undocumented

**Category:** design decision + fork-drift in shared code. **Severity:** Medium — the feature's behavior is arguably *better* than upstream's contract, which is exactly why the divergence will be invisible and confusing later.

**Mechanism and evidence.** Upstream v1.32's contract for `preemption.Interface.OrderedScoreFuncs` is: *if a plugin returns score functions, they **replace** the default candidate-selection criteria entirely* — including the fewest-PDB-violations rule (`if len(scoreFuncs) == 0 { scoreFuncs = defaults }` in the pre-v0.9 `pickOneNodeForPreemption`). v0.9 rewrites this into a sandwich: PDB-minimization always first, then plugin functions, then the default tie-breakers always appended (`preemption.go:616-626`), and changes the interface signature to pass the preemptor (`:108-124`). Both are changes to *shared* scheduler code with three consequences:

1. **The contract now differs from upstream, and nothing marks it as a divergence.** To its credit, v0.9 rewrote the interface comment to state the new semantics (`preemption.go:123`: "PDB safety first, then these plugin-specific scores, then default victim tie-breakers"). But nothing marks this as a *fork-owned departure* from upstream's replace-all contract: a plugin author (or an out-of-tree plugin compiled into this fork) coming from upstream documentation gets sandwich semantics with no compile error (the signature change is loud; the semantic change is not), and every rebase must re-merge `pickOneNodeForPreemption` by hand and re-verify which contract survived — with no tripwire saying the local version is intentional. This is F8's lexical-contract problem in structural clothing: the fork and upstream now disagree about what a shared extension point *means*.
2. **A real policy shift ships undocumented.** With the sandwich, the packing score now outranks "spare the highest-priority victims", "fewest victims", and "prefer evicting younger pods" when choosing *which node* to preempt on. Concretely: given candidate node A (one victim, low disruption, no protected GPU pods) and node B (three victims including a higher-priority one, but existing protected GPU usage), v0.9 picks **B** — more disruption, better packing. That is presumably the intent — packing over victim-kindness, consistent with the project's philosophy — but it is a change in *whose pods get evicted*, i.e. tenant-facing behavior, and neither architecture.md §5.3/§5.6 nor the design doc mention it (the only docs touched were observability tables). The main review's F7 argument applies again: eviction-fairness properties are part of the SLA story and deserve explicit statements.
3. **The victim-*ordering* docs are now subtly incomplete**: architecture.md §5.3 describes victim selection fairness in detail but candidate-*node* selection not at all — before v0.9 that was upstream-default and arguably needed no doc; now it is custom policy.

**Steelman / self-challenge.** The sandwich is defensible engineering — arguably what upstream's contract *should* be (replace-all forcing every plugin to reimplement PDB safety is a footgun), and keeping PDB first while packing beats the remaining tie-breakers is a coherent priority order for this system. If the team believes that, the right move is to *propose the sandwich upstream* (it is a small, well-motivated patch) — upstream adoption would convert the divergence into anticipation. Also fair: only `defaultpreemption` (returns nil) and this plugin implement the interface in-tree today, so the blast radius is currently zero. The finding is about the un-flagged divergence and the undocumented policy shift, not the behavior itself — verified behavior matches the intent as far as static analysis can tell, and the new tests cover it.

**Mitigations.** (a) One paragraph in architecture.md §5.3 (or a new §5.3.1 "Candidate node selection") stating the ordering: PDB → packing → upstream tie-breakers, with the node-A/node-B example. (b) A comment block at `pickOneNodeForPreemption` stating "fork divergence from upstream contract: plugin funcs are sandwiched, not replacing" — the rebase-time tripwire. (c) Consider upstreaming the sandwich. (d) Fold into F9's seam work when it happens — this is now the second fork-owned modification to shared preemption code, strengthening the case for consolidating the fork's preemption surface deliberately.

**How to verify.** Unit: two candidates, one PDB-violating with better packing → PDB candidate must lose (sandwich holds). Rebase-time: diff `pickOneNodeForPreemption` against upstream before merging.

<a name="f22"></a>
### F22. The victim subtraction in v0.9's candidate packing score is a tautology — and it emits log fields that can never differ

**Category:** dead-in-practice logic / misleading observability. **Severity:** Low-Medium — no wrong behavior, but the code models a scenario the design forbids, and the dashboard now carries a distinction without a difference.

**Mechanism and evidence.** `preemptionProtectedGPUPackingScore` (`namespaceresourceguarantee.go:642-682`) computes the node's protected GPU usage, then subtracts each victim's *protected* GPU request (`:659-661`) to get `protectedGPUAfterVictims`, and scores packing on that. But victims are by construction **strictly lower priority** than the preemptor (`SelectVictimsOnNode`, `:455-463`), and the protected tier is defined as **one PriorityClass** whose members therefore share the preemptor's priority value — *a protected pod can never be a victim*, so `protectedResourceRequest(victim, …)` is always 0 and `protectedGPUAfterVictims == protectedGPUBefore`, always. The struct carries both fields, the new log lines and dashboard expose both (`protected_gpu_before`, `protected_gpu_after_victims`), and they are equal on every line ever emitted in a consistent cluster.

The one scenario where the subtraction is live: a pod admitted while the guaranteed PriorityClass had a *lower value* than it has now (pods snapshot `spec.priority` at admission; the class's value can be changed by recreating it). Such a legacy pod carries the protected class *name* with a lower priority *value* — it would count as protected usage everywhere in the plugin, yet be preemptable by newer guaranteed pods. If the subtraction was written for that case, it is (a) undocumented, and (b) evidence of a broader unexamined assumption: **the entire plugin equates "protected" with "one fixed priority value", and a PriorityClass value change would violate that equation in several places at once** (guarantee accounting counts the legacy pod; eligibility lets it be evicted; the "guaranteed pods do not preempt each other" doc claim breaks). The subtraction quietly handles one symptom of a scenario the rest of the system doesn't handle.

**Steelman / self-challenge.** Defensive redundancy in code that feeds a placement decision is cheap insurance, and deleting it saves nothing measurable — the objection is not cost but *communication*: the next reader must either derive the tautology themselves or wrongly conclude protected victims are possible. Also, if MIG-style multiple protected classes ever arrive (multiple names, multiple values), the subtraction becomes live and correct — a forward-compatibility reading. What would invalidate the finding: a comment saying exactly that.

**Mitigations.** Cheapest: a two-line comment at `:659` stating the invariant ("victims are lower-priority; with a single protected class this subtraction is zero — kept for the PriorityClass-value-change edge") — and drop one of the two always-equal log/dashboard fields, or keep both with the same note in observability.md. Better: decide explicitly whether the system supports PriorityClass value changes; if not, document that as an operational invariant in operations.md ("never change the guaranteed class's value in place — recreate pods"), which closes the broader assumption gap, not just this symptom.

**How to verify.** Assert in a unit test that `protected_gpu_before == protected_gpu_after_victims` for victims produced by `SelectVictimsOnNode` (documents the invariant executable-ly); grep production logs — any line where they differ means the legacy-priority scenario is live in the cluster and F22's "broader assumption" paragraph becomes urgent.

<a name="f23"></a>
### F23. The decision-log field contract now lives in two packages that must not drift

**Category:** maintenance liability. **Severity:** Low — until the first drift, which the dashboard will report as mysteriously empty panels.

**Mechanism and evidence.** The main review noted the logfmt contract (`profile`, `decisionID`, `attempt`, `pod` on every line) implemented by `schedulingDecisionLogContext.keyvals()` in `schedule_one.go`. v0.9 adds a second, independent implementation: `preemptionDecisionLogKeyvals` in the plugin (`namespaceresourceguarantee.go:965-972`), duplicating the field names, ordering, and the profile literal (F11). The Grafana dashboard's LogQL parses these fields by exact name across *both* sources (the Decision Trace panel mixes them). A rename or type change in one place breaks half the dashboard's lines while tests on each package individually stay green — nothing asserts the two implementations agree.

**Mitigation.** v0.9 itself created the natural home: `framework/cycle_state.go` now owns the attempt state; a `framework.DecisionLogKeyvals(profile string, pod *v1.Pod, state *CycleState)` helper next to it would give both packages one implementation (and one place for the F11 gate when it becomes config-driven). Alternatively, a single test that emits one line from each path and asserts identical field sets. Small either way; fold into the F6/F11 logging cleanup.

---

## Cross-cutting themes

1. **Enforcement by convention.** F1, F2, F10, F11 share a root cause: correctness properties that live in documentation or operator discipline when the process has the information to enforce them. The project's stated philosophy ("scheduler stability over feature breadth") actually *argues for* startup-time enforcement — it is the cheapest stability there is.
2. **The webhook is a single point of policy failure.** F2 and F12's surrounding docs delegate priority policing, MIG policy, and profile routing to an external webhook, while the scheduler-side backstops are partial (profile-scoped cap) or absent. The scheduler cannot authenticate intent, but it can make its *caps* unconditional.
3. **The fork's cost concentrates where its necessity is lowest.** The portable plugin code (F5) is the bulk of the diff; the fork-requiring logging (F6, F11) is small but sits in the hottest upstream file; the copied preemption code (F9) is fork-cost with an in-tree alternative. Each rebase pays for all three; only one of them needs to exist as fork material.
4. **Redundant mechanisms need their premises audited.** F4 is the cautionary tale: a mechanism built against an observed incident whose root cause (two processes) was *also* fixed in the same release, leaving the mechanism's marginal value unmeasured and its documented premise wrong. When two fixes ship together, attribute the win before enshrining both.
5. **Docs honesty is high — which makes the gaps findable.** Most findings here were locatable *because* the docs state intent precisely enough to diff against the code. That is a strength to protect: several mitigations above are doc fixes, and they matter as much as the code ones. F19–F21 show the convention under strain, though: the same week the doc tree was committed as the source of truth, two releases shipped without history entries, the README's current-release line went stale, and one history entry describes a dashboard mechanism that was never shipped (F20). Source-of-truth status is earned per change set, not once.

## What is genuinely good (for balance)

- The two-tier model instead of DRF/queues at this scale, and the explicit non-goals with revisit conditions, are model scoping discipline.
- Scheduler-plugin over ResourceQuota is the right layer for Pending-not-rejected semantics, and `UnschedulableAndUnresolvable` + targeted queueing hints (no polling, no watchers) is the correct idiomatic implementation.
- The event-note bounding after the 1024-char incident, with a regression test, is exemplary incident response.
- The decision logs' *honest accounting* (prefilter-pruned vs rejected vs unevaluated, `synthetic=true`) resists the temptation to over-claim what the scheduler examined — rarer than it should be.
- The failure-modes table, the worked example, and the alternatives comparison (within its comparison set — see F5) make the design doc genuinely evaluable. Most design docs cannot be reviewed this concretely; this one invited the scrutiny it is getting here.

## Prioritized action plan

**Tier 1 — docs only, no release needed:**
fix the example config + resource-union warning (F1), correct the reservation premise (F4.1), correct known-issues #5 (F12), document packing's soft-preference flattening (F14), connect rollout×reservation (F15), add out-of-tree pattern to alternatives + Future Work (F5.1), amend the failure-modes headroom-race row (F3.3), write the v0.8/v0.9 history entries + fix the README current-release line (F19), fix the history.md `latest_attempt` claim (F20), document the candidate-selection ordering in architecture.md (F21.a).

**Tier 2 — small code, one release:**
guarantee PreFilter in the default profile (F2), restore V(10) for other profiles + regression test (F6), rectangular-config validation (F1), cross-profile startup validation (F10), move the `PreemptionAttempts` increment (F13), all-namespace preemptible usage (F7), ledger-ancestry guard in the release script + merge back the v0.8 sha (F19), attempt-textbox default / "Latest Attempt Seen" panel (F20), fork-divergence comment at `pickOneNodeForPreemption` (F21.b), invariant comment + drop the always-equal log field (F22), shared decision-log keyvals helper (F23).

**Tier 3 — needs data or a design decision:**
measure the reservation plugin's marginal saves, then narrow/retire (F4.2-4), count nominated preemptors in guarantee accounting (F3.1), typed evaluator outcome (F8), decision-log config gate (F11), integration test (F17), decide whether PriorityClass value changes are supported and document the invariant (F22), consider proposing the `OrderedScoreFuncs` sandwich upstream (F21.c).

**Tier 4 — at the next rebase:**
evaluate out-of-tree extraction (F5.2), defaultpreemption ordering seam + restored knobs (F9), consolidate the fork's preemption surface — copy + interface change — deliberately (F9+F21).

## Open questions that need production data

1. **How often does the reservation Filter block a pod that upstream nominee accounting would *not* have blocked?** (Decides F4.) Loki sketch: count `Blocking pod on reserved nominated node` (currently V(3) — raise to V(2) or add a counter first), then for a sample, replay whether the blocked pod fit in `allocatable − usage − holder request`.
2. **Do same-namespace guaranteed pods overlap within preemption windows?** (Decides F3's urgency.) Query: guarantee-exceeded rejections (`Rejected node ... NamespaceResourceGuarantee` / the PreFilter message) for pods that hold a `PreemptionStarted` event with the same `decisionID` earlier in the same hour.
3. **Is the live config rectangular?** (Decides whether F1 is latent or active.) One PromQL over `scheduler_namespace_resource_guarantee_quota` label combinations.
4. **Does any workload currently use `schedulerName: better-scheduler` without the guaranteed PriorityClass, or the PriorityClass without the schedulerName?** (Sizes F12 and F2 exposure.) Two `kubectl`/kube-state-metrics queries.

---

<a name="addendum"></a>
## Addendum: how v0.8/v0.9 move the main findings

v0.8 and v0.9 landed while this review was being written. Their dedicated analysis lives in [F19–F23](#f19); this addendum only records how the two releases move the **main** findings:

- **F9 strengthened**: v0.9 changes the shared `preemption.Interface` (`OrderedScoreFuncs` gains a `pod` parameter) and updates `defaultpreemption` accordingly — direct precedent that shared-preemption seams are acceptable in this fork, which is exactly what F9 proposes to eliminate the 400-line copy (noted inline in F9; the divergence itself is F21).
- **F11 worsened**: the new v0.9 log lines hardcode `profile="better-scheduler"` a second time, inside the plugin, stamped without knowing the actual profile (noted inline in F11; the duplicated field contract is F23).
- **F16 partially improved**: the plugin's PostFilter logs now carry `attempt` (via the new `framework.SchedulingDecisionAttemptFromState`); events still don't.
- **F5 direction, mildly**: moving the attempt state into `framework/cycle_state.go` shrinks the `schedule_one.go` footprint — the right direction if plugins are ever extracted, though the F21 interface change adds fork surface elsewhere.
- **F14 partially answered**: v0.9 extends the packing policy to preemption placement, closing the placement-path inconsistency the main review had not called out (credited in the F19–F23 preamble); the soft-preference flattening in F14 itself is unchanged.

---

*Review conducted by static analysis of the repository at `9c7c68970fe` (v0.8 feature set), with file/line references and the addendum updated to HEAD `c67c49f0348` (v0.9); no cluster access was used. Where this document disagrees with `architecture.md` or the design PDF, the disagreement is the finding — reconcile by fixing whichever artifact is wrong.*
