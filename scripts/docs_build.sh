#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
pushd docs-site >/dev/null
mdbook build
popd >/dev/null

echo "Docs built at docs-site/book/index.html"
