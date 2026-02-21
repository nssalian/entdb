#!/usr/bin/env bash
set -euo pipefail

if ! command -v gh >/dev/null 2>&1; then
  echo "gh CLI is required"
  exit 1
fi

OWNER="${1:-}"
REPO="${2:-}"
BRANCH="${3:-main}"

if [[ -z "$OWNER" || -z "$REPO" ]]; then
  echo "usage: $0 <owner> <repo> [branch]"
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RULE_FILE="$ROOT_DIR/.github/branch_protection.json"

if [[ ! -f "$RULE_FILE" ]]; then
  echo "missing branch protection template: $RULE_FILE"
  exit 1
fi

echo "Applying branch protection to ${OWNER}/${REPO}@${BRANCH}"
gh api \
  -X PUT \
  -H "Accept: application/vnd.github+json" \
  "repos/${OWNER}/${REPO}/branches/${BRANCH}/protection" \
  --input "$RULE_FILE" >/dev/null

echo "Branch protection applied."
