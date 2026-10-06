#!/usr/bin/env bash

# Copyright 2026 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

KUBE_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)

ECR_REPO_DEFAULT="767397673936.dkr.ecr.us-east-2.amazonaws.com/better-scheduler"
REGION_DEFAULT="us-east-2"
LEDGER_PATH_DEFAULT="releases/better-scheduler/releases.csv"

UPSTREAM_VERSION=""
SUBVERSION=""
ECR_REPO="${ECR_REPO_DEFAULT}"
REGION="${REGION_DEFAULT}"
PUSH="false"
SKIP_TESTS="false"

function usage() {
  cat <<EOF
Usage:
  $(basename "$0") --upstream-version <k8s-version> --subversion <subversion> [flags]

Required:
  --upstream-version   Kubernetes upstream version without 'v' (example: 1.32.4)
  --subversion         Better-scheduler subversion without 'v' (example: 0.2)

Optional:
  --ecr-repo           Target ECR repository (default: ${ECR_REPO_DEFAULT})
  --region             AWS region label for metadata (default: ${REGION_DEFAULT})
  --push               Execute build/tag/push/tagging workflow. Without this flag, script runs dry-run only.
  --skip-tests         Skip local preflight test commands.
  -h, --help           Show help.
EOF
}

function log() {
  printf ">>> %s\n" "$*"
}

function die() {
  printf "ERROR: %s\n" "$*" >&2
  exit 1
}

function require_cmd() {
  local cmd="$1"
  command -v "${cmd}" >/dev/null 2>&1 || die "missing required command: ${cmd}"
}

function parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --upstream-version)
        [[ $# -ge 2 ]] || die "--upstream-version requires a value"
        UPSTREAM_VERSION="$2"
        shift 2
        ;;
      --subversion)
        [[ $# -ge 2 ]] || die "--subversion requires a value"
        SUBVERSION="$2"
        shift 2
        ;;
      --ecr-repo)
        [[ $# -ge 2 ]] || die "--ecr-repo requires a value"
        ECR_REPO="$2"
        shift 2
        ;;
      --region)
        [[ $# -ge 2 ]] || die "--region requires a value"
        REGION="$2"
        shift 2
        ;;
      --push)
        PUSH="true"
        shift
        ;;
      --skip-tests)
        SKIP_TESTS="true"
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "unknown argument: $1"
        ;;
    esac
  done
}

function validate_inputs() {
  [[ -n "${UPSTREAM_VERSION}" ]] || die "--upstream-version is required"
  [[ -n "${SUBVERSION}" ]] || die "--subversion is required"

  [[ "${UPSTREAM_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-.][0-9A-Za-z.]+)?$ ]] || \
    die "invalid --upstream-version format: ${UPSTREAM_VERSION}"
  [[ "${SUBVERSION}" =~ ^[0-9]+(\.[0-9]+)*$ ]] || \
    die "invalid --subversion format: ${SUBVERSION}"
}

function ensure_clean_tree() {
  git diff --quiet || die "working tree has unstaged changes"
  git diff --cached --quiet || die "working tree has staged changes"
  [[ -z "$(git ls-files --others --exclude-standard)" ]] || die "working tree has untracked files"
}

function ensure_named_branch() {
  local branch
  branch=$(git symbolic-ref --short -q HEAD || true)
  [[ -n "${branch}" ]] || die "detached HEAD is not allowed for release"
}

function check_tag_uniqueness() {
  local tag="$1"

  if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null 2>&1; then
    die "release tag already exists locally: ${tag}"
  fi

  local remote_ref
  if ! remote_ref=$(git ls-remote --tags --refs origin "refs/tags/${tag}"); then
    die "failed to query origin for tag uniqueness"
  fi
  [[ -z "${remote_ref}" ]] || die "release tag already exists on origin: ${tag}"
}

function run_preflight_tests() {
  if [[ "${SKIP_TESTS}" == "true" ]]; then
    log "Skipping preflight tests by request"
    return
  fi

  log "Running preflight tests"
  go test ./pkg/scheduler/framework/plugins/namespaceresourceguarantee
  go test ./pkg/scheduler/apis/config/validation -run TestValidateNamespaceResourceGuaranteeArgs
}

function release_tag_from_inputs() {
  printf "v%s-bs-v%s" "${UPSTREAM_VERSION}" "${SUBVERSION}"
}

function build_release_image() {
  local release_tag="$1"
  log "Building release image via quick-release-images for tag ${release_tag}"
  (
    cd "${KUBE_ROOT}"
    KUBE_GIT_VERSION="${release_tag}" \
    KUBE_BUILD_PLATFORMS=linux/amd64 \
    KUBE_BUILD_CONFORMANCE=n \
    KUBE_DOCKER_IMAGE_TAG="${release_tag}" \
    KUBE_DOCKER_REGISTRY=registry.k8s.io \
    make quick-release-images
  )
}

function assert_local_image_exists() {
  local image="$1"
  docker image inspect "${image}" >/dev/null 2>&1 || die "expected local image not found: ${image}"
}

function get_pushed_digest_ref() {
  local ecr_repo="$1"
  local target_image="$2"
  local digest_ref
  digest_ref=$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "${target_image}" | grep "^${ecr_repo}@sha256:" | head -n1 || true)
  [[ -n "${digest_ref}" ]] || die "failed to resolve pushed digest for ${target_image}"
  printf "%s" "${digest_ref}"
}

function create_and_push_git_tag() {
  local tag="$1"
  local digest_ref="$2"
  local commit_sha
  commit_sha=$(git rev-parse --short HEAD)

  log "Creating annotated git tag ${tag}"
  git tag -a "${tag}" -m "better-scheduler release ${tag}

image: ${digest_ref}
commit: ${commit_sha}
"

  log "Pushing git tag ${tag} to origin"
  git push origin "refs/tags/${tag}"
}

function append_release_ledger() {
  local tag="$1"
  local image_ref="$2"
  local digest_ref="$3"
  local released_at
  local released_by
  local commit_sha

  released_at=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  released_by="${USER:-unknown}"
  commit_sha=$(git rev-parse --short HEAD)

  mkdir -p "${KUBE_ROOT}/releases/better-scheduler"
  if [[ ! -f "${KUBE_ROOT}/${LEDGER_PATH_DEFAULT}" ]]; then
    printf "release_tag,git_sha,image,image_digest,released_at_utc,released_by\n" > "${KUBE_ROOT}/${LEDGER_PATH_DEFAULT}"
  fi

  printf "%s,%s,%s,%s,%s,%s\n" \
    "${tag}" \
    "${commit_sha}" \
    "${image_ref}" \
    "${digest_ref}" \
    "${released_at}" \
    "${released_by}" >> "${KUBE_ROOT}/${LEDGER_PATH_DEFAULT}"
}

function main() {
  parse_args "$@"
  validate_inputs

  require_cmd git
  require_cmd make
  require_cmd docker
  require_cmd go

  cd "${KUBE_ROOT}"
  ensure_clean_tree
  ensure_named_branch

  local release_tag
  local source_image
  local target_image

  release_tag=$(release_tag_from_inputs)
  source_image="registry.k8s.io/kube-scheduler-amd64:${release_tag}"
  target_image="${ECR_REPO}:${release_tag}"

  check_tag_uniqueness "${release_tag}"

  log "Release plan"
  log "  release tag: ${release_tag}"
  log "  region: ${REGION}"
  log "  source image: ${source_image}"
  log "  target image: ${target_image}"
  log "  push: ${PUSH}"

  if [[ "${PUSH}" != "true" ]]; then
    log "Dry-run mode only (use --push to execute build and push)"
    exit 0
  fi

  run_preflight_tests
  build_release_image "${release_tag}"
  assert_local_image_exists "${source_image}"
  local actual_version
  actual_version=$(docker run --rm --entrypoint /usr/local/bin/kube-scheduler "${source_image}" --version)
  [[ "${actual_version}" == "Kubernetes ${release_tag}" ]] || \
    die "built scheduler version mismatch: ${actual_version} (expected Kubernetes ${release_tag})"
  [[ "$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "${source_image}")" == "linux/amd64" ]] || \
    die "built scheduler image must target linux/amd64"

  log "Tagging ${source_image} -> ${target_image}"
  docker tag "${source_image}" "${target_image}"

  log "Pushing ${target_image}"
  docker push "${target_image}"

  local digest_ref
  digest_ref=$(get_pushed_digest_ref "${ECR_REPO}" "${target_image}")
  log "Published digest: ${digest_ref}"

  create_and_push_git_tag "${release_tag}" "${digest_ref}"
  append_release_ledger "${release_tag}" "${target_image}" "${digest_ref}"
  log "Release complete"
}

main "$@"

