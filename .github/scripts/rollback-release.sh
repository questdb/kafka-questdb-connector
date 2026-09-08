#!/usr/bin/env bash

set -euo pipefail

release_tag="${1:-}"
expected_tag_oid="${2:-}"

if [[ ! "${release_tag}" =~ ^v[0-9]+\.[0-9]+(\.[0-9]+)?$ ]]; then
    echo "::warning::Refusing rollback for invalid release tag '${release_tag}'."
    exit 0
fi
if [[ ! "${expected_tag_oid}" =~ ^[0-9a-f]{40,64}$ ]]; then
    echo "::warning::Refusing rollback because the expected tag object ID is invalid."
    exit 0
fi
if [[ -z "${GITHUB_REPOSITORY:-}" || -z "${GH_TOKEN:-}" ]]; then
    echo "::warning::Cannot roll back ${release_tag}: GitHub repository or App token is unavailable."
    exit 0
fi

if ! remote_tag_refs="$(git ls-remote --refs origin "refs/tags/${release_tag}")"; then
    echo "::warning::Could not inspect remote tag ${release_tag}; preserving it for manual inspection."
    exit 0
fi
remote_tag_oid="$(printf '%s\n' "${remote_tag_refs}" | awk 'NR == 1 { print $1 }')"
if [[ -z "${remote_tag_oid}" ]]; then
    echo "No remote tag ${release_tag} exists; no rollback is needed."
    exit 0
fi
if [[ "${remote_tag_oid}" != "${expected_tag_oid}" ]]; then
    echo "::warning::Remote tag ${release_tag} does not match this run's tag object; preserving it."
    exit 0
fi

export RELEASE_TAG="${release_tag}"
if ! release_rows="$(gh api --paginate "repos/${GITHUB_REPOSITORY}/releases?per_page=100" \
    --jq '.[] | select(.tag_name == env.RELEASE_TAG) | [.id, .draft] | @tsv')"; then
    echo "::warning::Could not inspect releases for ${release_tag}; preserving the tag for manual inspection."
    exit 0
fi

if [[ -n "${release_rows}" ]]; then
    echo "::warning::A GitHub release exists for ${release_tag}; preserving the release and tag for manual inspection."
    exit 0
fi

auth_header="$(printf 'x-access-token:%s' "${GH_TOKEN}" | base64 | tr -d '\n')"
if git -c "http.https://github.com/.extraheader=AUTHORIZATION: basic ${auth_header}" \
    push --force-with-lease="refs/tags/${release_tag}:${expected_tag_oid}" \
    origin ":refs/tags/${release_tag}"; then
    echo "Deleted tag ${release_tag}; the run can be retried."
else
    echo "::warning::Could not safely delete tag ${release_tag}; inspect it manually before retrying."
fi
