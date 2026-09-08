#!/usr/bin/env bash

set -euo pipefail

pom_version="${1:-}"
release_version_override="${2:-}"
next_development_version_override="${3:-}"

if [[ -z "${pom_version}" ]]; then
    echo "::error::Could not read the project version." >&2
    exit 1
fi

if [[ -n "${release_version_override}" ]]; then
    release_version="${release_version_override}"
else
    if [[ "${pom_version}" != *-SNAPSHOT ]]; then
        echo "::error::Project version '${pom_version}' must end in -SNAPSHOT when no release override is provided." >&2
        exit 1
    fi
    release_version="${pom_version%-SNAPSHOT}"
fi

if [[ "${release_version}" == *-SNAPSHOT ]]; then
    echo "::error::Refusing to release a SNAPSHOT version (${release_version})." >&2
    exit 1
fi
if [[ ! "${release_version}" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]]; then
    echo "::error::Release version '${release_version}' is not in X.Y or X.Y.Z form." >&2
    exit 1
fi

if [[ -n "${next_development_version_override}" ]]; then
    next_development_version="${next_development_version_override}"
else
    prefix="${release_version%.*}"
    last="${release_version##*.}"
    next_development_version="${prefix}.$((last + 1))-SNAPSHOT"
fi
if [[ "${next_development_version}" != *-SNAPSHOT ]]; then
    echo "::error::Next development version '${next_development_version}' must end in -SNAPSHOT." >&2
    exit 1
fi

printf 'release_version=%s\n' "${release_version}"
printf 'next_development_version=%s\n' "${next_development_version}"
