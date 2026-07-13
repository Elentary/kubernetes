# NamespaceResourceGuarantee Plugin Design

**This document is superseded.** It described the initial (v0.1, PreFilter-only) design and no longer matches the implementation, which has since gained namespace-aware preemption, nominated-node reservation, GPU packing, and decision logging.

The current source of truth is the documentation tree at [`docs/better-scheduler/`](docs/better-scheduler/README.md):

- [Overview & quick start](docs/better-scheduler/README.md)
- [Architecture & scheduling semantics](docs/better-scheduler/architecture.md)
- [Configuration & deployment](docs/better-scheduler/configuration.md)
- [Observability (decision logs, metrics, dashboard)](docs/better-scheduler/observability.md)
- [Operations (releases, incidents)](docs/better-scheduler/operations.md)
- [History v0.1 → v0.7](docs/better-scheduler/history.md)
- [Known issues](docs/better-scheduler/known-issues.md)

Keep those docs in sync with every behavior change (the convention this file used to serve).
