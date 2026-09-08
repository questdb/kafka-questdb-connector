#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
RESOLVE_SCRIPT="${ROOT_DIR}/.github/scripts/resolve-release-versions.sh"
ROLLBACK_SCRIPT="${ROOT_DIR}/.github/scripts/rollback-release.sh"

test_count=0
TEST_TEMP_DIR=""

cleanup() {
    if [[ -n "${TEST_TEMP_DIR}" ]]; then
        rm -rf "${TEST_TEMP_DIR}"
    fi
}

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_eq() {
    local expected="$1"
    local actual="$2"
    local message="$3"
    if [[ "${actual}" != "${expected}" ]]; then
        fail "${message}: expected '${expected}', got '${actual}'"
    fi
}

assert_tag_exists() {
    local remote="$1"
    local tag="$2"
    git ls-remote --exit-code --tags "${remote}" "refs/tags/${tag}" >/dev/null 2>&1 \
        || fail "expected ${tag} to exist in ${remote}"
}

assert_tag_absent() {
    local remote="$1"
    local tag="$2"
    if git ls-remote --exit-code --tags "${remote}" "refs/tags/${tag}" >/dev/null 2>&1; then
        fail "expected ${tag} to be absent from ${remote}"
    fi
}

pass() {
    test_count=$((test_count + 1))
    echo "ok ${test_count} - $1"
}

test_resolves_snapshot_version() {
    local output
    output="$("${RESOLVE_SCRIPT}" "0.24-SNAPSHOT" "" "")"
    assert_eq $'release_version=0.24\nnext_development_version=0.25-SNAPSHOT' "${output}" \
        "default release versions"
    pass "snapshot POM resolves to release and next development versions"
}

test_rejects_final_pom_without_override() {
    local output
    if output="$("${RESOLVE_SCRIPT}" "0.24" "" "" 2>&1)"; then
        fail "final POM without an override was accepted"
    fi
    [[ "${output}" == *"must end in -SNAPSHOT"* ]] \
        || fail "unexpected final-POM error: ${output}"
    pass "final POM is rejected without a release override"
}

test_allows_explicit_release_override() {
    local output
    output="$("${RESOLVE_SCRIPT}" "0.24" "0.23.1" "0.24-SNAPSHOT")"
    assert_eq $'release_version=0.23.1\nnext_development_version=0.24-SNAPSHOT' "${output}" \
        "explicit release versions"
    pass "explicit release override permits an unusual release"
}

make_fake_gh() {
    local bin_dir="$1"
    mkdir -p "${bin_dir}"
    cat > "${bin_dir}/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >> "${FAKE_GH_LOG}"

if [[ "$*" == *"releases?per_page=100"* ]]; then
    case "${FAKE_RELEASE_STATE:-none}" in
        none)
            exit 0
            ;;
        draft)
            printf '101\ttrue\n'
            exit 0
            ;;
        published)
            printf '102\tfalse\n'
            exit 0
            ;;
        query-failure)
            exit 1
            ;;
        *)
            echo "unknown FAKE_RELEASE_STATE" >&2
            exit 2
            ;;
    esac
fi

echo "unexpected gh invocation: $*" >&2
exit 2
EOF
    chmod +x "${bin_dir}/gh"
}

make_tagged_repo() {
    local test_dir="$1"
    local remote="${test_dir}/remote.git"
    local work="${test_dir}/work"

    git init --quiet --bare "${remote}"
    git init --quiet "${work}"
    git -C "${work}" config user.name "Release Test"
    git -C "${work}" config user.email "release-test@example.com"
    echo "release fixture" > "${work}/fixture.txt"
    git -C "${work}" add fixture.txt
    git -C "${work}" commit --quiet -m "fixture"
    git -C "${work}" remote add origin "${remote}"
    git -C "${work}" tag -a v0.24 -m "v0.24"
    git -C "${work}" push --quiet origin refs/tags/v0.24

    printf '%s\n%s\n%s\n' "${remote}" "${work}" "$(git -C "${work}" rev-parse refs/tags/v0.24)"
}

run_rollback() {
    local work="$1"
    local fake_bin="$2"
    local log="$3"
    local state="$4"
    local expected_oid="$5"

    (
        cd "${work}"
        PATH="${fake_bin}:${PATH}" \
        FAKE_GH_LOG="${log}" \
        FAKE_RELEASE_STATE="${state}" \
        GITHUB_REPOSITORY="questdb/kafka-questdb-connector" \
        GH_TOKEN="test-token" \
            "${ROLLBACK_SCRIPT}" v0.24 "${expected_oid}"
    )
}

run_rollback_without_token() {
    local work="$1"
    local expected_oid="$2"

    (
        cd "${work}"
        GITHUB_REPOSITORY="questdb/kafka-questdb-connector" \
        GH_TOKEN="" \
            "${ROLLBACK_SCRIPT}" v0.24 "${expected_oid}"
    )
}

test_deletes_matching_orphan_tag() {
    local test_dir="$1/orphan-tag"
    mkdir -p "${test_dir}"
    local repo_data remote work expected_oid
    repo_data="$(make_tagged_repo "${test_dir}")"
    remote="$(printf '%s\n' "${repo_data}" | sed -n '1p')"
    work="$(printf '%s\n' "${repo_data}" | sed -n '2p')"
    expected_oid="$(printf '%s\n' "${repo_data}" | sed -n '3p')"
    local fake_bin="${test_dir}/bin"
    make_fake_gh "${fake_bin}"

    run_rollback "${work}" "${fake_bin}" "${test_dir}/gh.log" none "${expected_oid}"

    assert_tag_absent "${remote}" v0.24
    pass "matching orphan tag is deleted"
}

test_skips_missing_remote_tag_without_token_warning() {
    local test_dir="$1/no-remote-tag"
    mkdir -p "${test_dir}"
    local repo_data remote work expected_oid output
    repo_data="$(make_tagged_repo "${test_dir}")"
    remote="$(printf '%s\n' "${repo_data}" | sed -n '1p')"
    work="$(printf '%s\n' "${repo_data}" | sed -n '2p')"
    expected_oid="$(printf '%s\n' "${repo_data}" | sed -n '3p')"
    git -C "${work}" push --quiet origin ":refs/tags/v0.24"

    output="$(run_rollback_without_token "${work}" "${expected_oid}" 2>&1)"

    assert_eq "No remote tag v0.24 exists; no rollback is needed." "${output}" \
        "rollback output for an absent remote tag"
    assert_tag_absent "${remote}" v0.24
    pass "absent remote tag needs no token and emits no warning"
}

test_warns_when_matching_remote_tag_has_no_token() {
    local test_dir="$1/missing-token"
    mkdir -p "${test_dir}"
    local repo_data remote work expected_oid output
    repo_data="$(make_tagged_repo "${test_dir}")"
    remote="$(printf '%s\n' "${repo_data}" | sed -n '1p')"
    work="$(printf '%s\n' "${repo_data}" | sed -n '2p')"
    expected_oid="$(printf '%s\n' "${repo_data}" | sed -n '3p')"

    output="$(run_rollback_without_token "${work}" "${expected_oid}" 2>&1)"

    assert_eq "::warning::Cannot roll back v0.24: GitHub repository or App token is unavailable." "${output}" \
        "rollback output for a matching tag without credentials"
    assert_tag_exists "${remote}" v0.24
    pass "matching remote tag without a token emits a warning"
}

test_preserves_tag_when_draft_exists() {
    local test_dir="$1/draft-exists"
    mkdir -p "${test_dir}"
    local repo_data remote work expected_oid
    repo_data="$(make_tagged_repo "${test_dir}")"
    remote="$(printf '%s\n' "${repo_data}" | sed -n '1p')"
    work="$(printf '%s\n' "${repo_data}" | sed -n '2p')"
    expected_oid="$(printf '%s\n' "${repo_data}" | sed -n '3p')"
    local fake_bin="${test_dir}/bin"
    local log="${test_dir}/gh.log"
    make_fake_gh "${fake_bin}"

    run_rollback "${work}" "${fake_bin}" "${log}" draft "${expected_oid}"

    assert_tag_exists "${remote}" v0.24
    if grep -q -- '--method DELETE' "${log}"; then
        fail "rollback attempted to delete an existing release"
    fi
    pass "draft release and its tag are preserved for manual cleanup"
}

test_preserves_tag_for_published_release() {
    local test_dir="$1/published"
    mkdir -p "${test_dir}"
    local repo_data remote work expected_oid
    repo_data="$(make_tagged_repo "${test_dir}")"
    remote="$(printf '%s\n' "${repo_data}" | sed -n '1p')"
    work="$(printf '%s\n' "${repo_data}" | sed -n '2p')"
    expected_oid="$(printf '%s\n' "${repo_data}" | sed -n '3p')"
    local fake_bin="${test_dir}/bin"
    make_fake_gh "${fake_bin}"

    run_rollback "${work}" "${fake_bin}" "${test_dir}/gh.log" published "${expected_oid}"

    assert_tag_exists "${remote}" v0.24
    pass "published release keeps its tag"
}

test_preserves_tag_when_release_inspection_fails() {
    local test_dir="$1/query-failure"
    mkdir -p "${test_dir}"
    local repo_data remote work expected_oid
    repo_data="$(make_tagged_repo "${test_dir}")"
    remote="$(printf '%s\n' "${repo_data}" | sed -n '1p')"
    work="$(printf '%s\n' "${repo_data}" | sed -n '2p')"
    expected_oid="$(printf '%s\n' "${repo_data}" | sed -n '3p')"
    local fake_bin="${test_dir}/bin"
    make_fake_gh "${fake_bin}"

    run_rollback "${work}" "${fake_bin}" "${test_dir}/gh.log" query-failure "${expected_oid}"

    assert_tag_exists "${remote}" v0.24
    pass "release inspection failure preserves the tag"
}

test_preserves_tag_when_remote_oid_differs() {
    local test_dir="$1/oid-mismatch"
    mkdir -p "${test_dir}"
    local repo_data remote work expected_oid
    repo_data="$(make_tagged_repo "${test_dir}")"
    remote="$(printf '%s\n' "${repo_data}" | sed -n '1p')"
    work="$(printf '%s\n' "${repo_data}" | sed -n '2p')"
    expected_oid="$(git -C "${work}" rev-parse HEAD)"
    local fake_bin="${test_dir}/bin"
    make_fake_gh "${fake_bin}"

    run_rollback "${work}" "${fake_bin}" "${test_dir}/gh.log" none "${expected_oid}"

    assert_tag_exists "${remote}" v0.24
    pass "unexpected remote tag object is preserved"
}

main() {
    TEST_TEMP_DIR="$(mktemp -d)"
    trap cleanup EXIT

    test_resolves_snapshot_version
    test_rejects_final_pom_without_override
    test_allows_explicit_release_override
    test_deletes_matching_orphan_tag "${TEST_TEMP_DIR}"
    test_skips_missing_remote_tag_without_token_warning "${TEST_TEMP_DIR}"
    test_warns_when_matching_remote_tag_has_no_token "${TEST_TEMP_DIR}"
    test_preserves_tag_when_draft_exists "${TEST_TEMP_DIR}"
    test_preserves_tag_for_published_release "${TEST_TEMP_DIR}"
    test_preserves_tag_when_release_inspection_fails "${TEST_TEMP_DIR}"
    test_preserves_tag_when_remote_oid_differs "${TEST_TEMP_DIR}"

    echo "PASS: ${test_count} release script tests"
}

main "$@"
