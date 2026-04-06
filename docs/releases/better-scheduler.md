# Better Scheduler Manual Release Guide

This guide defines the manual release flow for the custom scheduler image.

## Image Target

- ECR repository: `767397673936.dkr.ecr.us-east-2.amazonaws.com/better-scheduler`
- Platform: `linux/amd64`

## Release Versioning

Use one canonical release string for both git tags and image tags:

- Format: `v<k8s-version>-bs-v<subversion>`
- Example: `v1.32.4-bs-v0.2`

Subversions are independent across upstream Kubernetes versions.

## `make quick-release-images` Environment Variables

These are the effective environment controls used by the release helper script:

- `KUBE_BUILD_PLATFORMS=linux/amd64`
  - Forces release output to amd64 regardless of host architecture.
- `KUBE_BUILD_CONFORMANCE=n`
  - Skips conformance image build for faster scheduler release builds.
- `KUBE_DOCKER_IMAGE_TAG=<release-tag>`
  - Sets the release image tag (for example `v1.32.4-bs-v0.2`).
- `KUBE_DOCKER_REGISTRY=registry.k8s.io`
  - Leaves local build tags in upstream format before retagging to ECR.
- `KUBE_FASTBUILD=true`
  - Automatically set by `make quick-release-images` in this repo.
- `DBG=1` (optional)
  - Unstripped debug build. Omit for default optimized release build.

Build command shape:

```bash
KUBE_BUILD_PLATFORMS=linux/amd64 \
KUBE_BUILD_CONFORMANCE=n \
KUBE_DOCKER_IMAGE_TAG="${RELEASE_TAG}" \
KUBE_DOCKER_REGISTRY=registry.k8s.io \
make quick-release-images
```

## Release Steps

1. Ensure working tree is clean and on a named branch.
2. Pick `k8s-version` and `subversion`, then compute release tag.
3. Verify tag does not already exist locally or on `origin`.
4. Build local image with `make quick-release-images`.
5. Retag built image to ECR target repo.
6. Push to ECR.
7. Capture pushed immutable digest.
8. Create and push annotated git tag with image digest in tag message.
9. Append release metadata row to `releases/better-scheduler/releases.csv`.

## Helper Script

Use:

```bash
hack/release-better-scheduler.sh --upstream-version 1.32.4 --subversion 0.2 --push
```

Without `--push`, the script only prints the computed release plan and exits.

## Prerequisites

- Docker daemon available locally.
- `git`, `make`, `docker`.
- ECR credential helper configured in Docker (script does not run explicit auth commands).
- Git remote `origin` reachable for remote tag uniqueness check and tag push.

