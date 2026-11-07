#!/bin/bash
# Copies results/ to the private results repo and pushes it. Results live apart from
# the code so that a backup never changes the commit recorded in running cycles.
set -euo pipefail

dest=${RESULTS_REPO_DIR:-../dualwrite-results}
[ -d "$dest/.git" ] || { echo "no results repo at $dest" >&2; exit 1; }
[ -d results ] || { echo "no results/ to back up" >&2; exit 1; }

too_big=$(find results -type f -size +95M)
[ -z "$too_big" ] || { echo "too large for GitHub: $too_big" >&2; exit 1; }

# Checksums for every run's files, so a restored copy can be verified.
for run in results/runs/*/ results/trials/*/; do
  [ -d "$run" ] || continue
  (cd "$run" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 shasum -a 256 > SHA256SUMS)
done

source_commit=$(git rev-parse --short=12 HEAD)
host=$(hostname -s)
mkdir -p "$dest/$host"
rsync -a results/ "$dest/$host/results/"

git -C "$dest" add -A
if git -C "$dest" diff --cached --quiet; then
  echo "nothing new to back up"
  exit 0
fi
git -C "$dest" commit -q -m "Results from $host at code commit $source_commit"
git -C "$dest" push -q origin HEAD
echo "backed up: $(git -C "$dest" log --oneline -1)"
