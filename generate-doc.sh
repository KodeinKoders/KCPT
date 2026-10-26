#!/usr/bin/env bash
# Builds the documentation site into build/site from the working tree, uncommitted files included.
# CI builds the vX.Y branches instead (see antora-playbook.yml); this is for previewing.
set -euo pipefail

cd "$(dirname "$0")"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
playbook="$tmp/playbook.yml"

# The copy lives outside the repository, so its relative paths are made absolute.
sed -e "s|url: ./|url: $PWD|" \
    -e "s|branches: 'v\*'|branches: HEAD|" \
    -e "s|dir: build/site|dir: $PWD/build/site|" \
    antora-playbook.yml > "$playbook"

npm install --no-audit --no-fund
npx antora "$playbook"

echo "Site generated in build/site (open build/site/kcpt/latest/index.html)"
