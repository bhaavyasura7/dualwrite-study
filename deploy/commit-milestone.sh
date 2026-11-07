#!/bin/bash
# Commits a milestone only if every acceptance check passes. Usage:
#   deploy/commit-milestone.sh <file holding the commit message>
set -euo pipefail

message=${1:?usage: commit-milestone.sh <message file>}
[ -f "$message" ] || { echo "no message file: $message" >&2; exit 2; }

make accept || { echo "acceptance failed: not committing" >&2; exit 1; }
git add -A
git commit -q -F "$message"
git log --oneline -1
