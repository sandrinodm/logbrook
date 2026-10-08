#!/usr/bin/env bash
# Validate data from the read-only build job without executing its artifacts.
set -euo pipefail

fail() {
  echo "::error::$*" >&2
  exit 1
}

patch=${1:?Expected a release patch path}
: "${PREVIOUS_VERSION:?}" "${RELEASE_VERSION:?}" "${RELEASE_TAG:?}"
version_pattern='^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z-][0-9A-Za-z.-]*)?$'
[[ "$PREVIOUS_VERSION" =~ $version_pattern && "$RELEASE_VERSION" =~ $version_pattern ]] || fail 'Invalid version format.'
[[ "$RELEASE_TAG" == "v$RELEASE_VERSION" && ${#RELEASE_TAG} -le 128 ]] || fail 'Invalid release tag.'
git diff --quiet
git diff --cached --quiet

manifest_version() {
  git show "$1:Cargo.toml" | awk '
    /^\[package\]$/ { package = 1; next }
    /^\[/ { package = 0 }
    package && /^version[[:space:]]*=/ { split($0, value, "\""); print value[2] }
  '
}

[[ "$(manifest_version HEAD)" == "$PREVIOUS_VERSION" ]] || fail 'Base version differs from the prepared version.'
if [[ "$PREVIOUS_VERSION" == "$RELEASE_VERSION" ]]; then
  test ! -s "$patch"
  exit 0
fi

# Exactly one changed line in each existing regular file. Reject extra files,
# binaries, renames, file modes, and symlinks before applying anything.
expected=$(printf '1\t1\t%s\n' Cargo.lock Cargo.toml Dockerfile THIRD_PARTY_NOTICES.txt src/openapi.json)
[[ "$(git apply --numstat "$patch" | LC_ALL=C sort)" == "$expected" ]] || fail 'Patch must change one line in each of the five version files.'
[[ -z "$(git apply --summary "$patch")" ]] || fail 'Patch changes file paths or modes.'
git apply --check --index "$patch"
git apply --index "$patch"

checksum() {
  if command -v sha256sum > /dev/null; then
    sha256sum | cut -d ' ' -f 1
  else
    shasum -a 256 | cut -d ' ' -f 1
  fi
}

for file in Cargo.toml Cargo.lock Dockerfile src/openapi.json THIRD_PARTY_NOTICES.txt; do
  diff=$(git diff --cached --no-ext-diff --unified=0 -- "$file")
  removed=$(printf '%s\n' "$diff" | sed '/^---/d; /^-/!d; s/^-//')
  added=$(printf '%s\n' "$diff" | sed '/^+++/d; /^+/!d; s/^+//')

  case "$file" in
    Cargo.toml|Cargo.lock)
      [[ "$removed" =~ ^version[[:space:]]*=[[:space:]]*\" ]] || fail "Unexpected version line in $file."
      [[ "$removed" == *\""$PREVIOUS_VERSION"\"* ]] || fail "Unexpected old version in $file."
      expected=${removed/\"$PREVIOUS_VERSION\"/\"$RELEASE_VERSION\"}
      ;;
    Dockerfile)
      [[ "$removed" == "ARG VERSION=$PREVIOUS_VERSION" ]] || fail 'Unexpected Dockerfile change.'
      expected="ARG VERSION=$RELEASE_VERSION"
      ;;
    src/openapi.json)
      [[ "$removed" =~ ^[[:space:]]*\"version\": ]] || fail 'Unexpected OpenAPI change.'
      [[ "$removed" == *\""$PREVIOUS_VERSION"\"* ]] || fail 'Unexpected old API version.'
      expected=${removed/\"$PREVIOUS_VERSION\"/\"$RELEASE_VERSION\"}
      ;;
    THIRD_PARTY_NOTICES.txt)
      [[ "$removed" == "Cargo.lock SHA-256: $(git show HEAD:Cargo.lock | checksum)" ]] || fail 'Base notice fingerprint is incorrect.'
      expected="Cargo.lock SHA-256: $(git show :Cargo.lock | checksum)"
      ;;
  esac

  [[ "$added" == "$expected" ]] || fail "Unexpected replacement in $file."
done

# The changed version lines must belong to the application, not a dependency
# with the same old version or an example object elsewhere in the API schema.
[[ "$(manifest_version '')" == "$RELEASE_VERSION" ]] || fail 'Application version was not updated.'
locked=$(git show :Cargo.lock | awk '
  /^\[\[package\]\]$/ { package = 0 }
  /^name = "logbrook"$/ { package = 1 }
  package && /^version = / { split($0, value, "\""); print value[2] }
')
[[ "$locked" == "$RELEASE_VERSION" ]] || fail 'Application lockfile version was not updated.'
git show :src/openapi.json | jq -e --arg version "$RELEASE_VERSION" '.info.version == $version' > /dev/null
