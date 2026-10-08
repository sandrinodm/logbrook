#!/usr/bin/env bash
# Runs on a fresh runner after the prepared patch has passed independent checks.
set -euo pipefail
: "${RELEASE_BASE:?}" "${RELEASE_VERSION:?}" "${RELEASE_TAG:?}" "${GITHUB_OUTPUT:?}"
if [[ "$RELEASE_TAG" != "v$RELEASE_VERSION" || "$(git rev-parse HEAD)" != "$RELEASE_BASE" ]]; then
  echo '::error::Release tag or base commit differs from preparation.'
  exit 1
fi

git_options=(-c core.hooksPath=/dev/null)
if [[ -n "${RELEASE_PUSH_TOKEN:-}" ]]; then
  authorization=$(printf 'x-access-token:%s' "$RELEASE_PUSH_TOKEN" | base64 | tr -d '\n')
  echo "::add-mask::$authorization"
  git_options+=(-c "http.https://github.com/.extraheader=AUTHORIZATION: basic $authorization")
  unset RELEASE_PUSH_TOKEN
fi

git "${git_options[@]}" fetch origin main --tags
if [[ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]]; then
  echo '::error::main advanced after preparation. Start a new release run.'
  exit 1
fi
if git show-ref --verify --quiet "refs/tags/$RELEASE_TAG"; then
  echo "::error::Tag $RELEASE_TAG already exists. Retry its downstream failed jobs or select a new version."
  exit 1
fi

git config user.name 'github-actions[bot]'
git config user.email '41898282+github-actions[bot]@users.noreply.github.com'
if ! git diff --cached --quiet; then
  git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -m "chore(release): $RELEASE_TAG"
fi
git -c core.hooksPath=/dev/null -c tag.gpgsign=false tag --annotate "$RELEASE_TAG" --message "Logbrook $RELEASE_VERSION"

# Reject both refs if main advanced or someone created the tag after the fetch.
git "${git_options[@]}" push --atomic origin HEAD:refs/heads/main "refs/tags/$RELEASE_TAG"
echo "commit=$(git rev-parse HEAD)" >> "$GITHUB_OUTPUT"
