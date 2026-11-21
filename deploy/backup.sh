#!/bin/bash
# Copies results/ to the private results repo and pushes it. Results live apart from
# the code so that a backup never changes the commit recorded in running cycles.
set -euo pipefail

dest=${RESULTS_REPO_DIR:-../dualwrite-results}
[ -d "$dest/.git" ] || { echo "no results repo at $dest" >&2; exit 1; }
[ -d results ] || { echo "no results/ to back up" >&2; exit 1; }

too_big=$(find results -type f -size +95M -not -path "*/dumps/*")
[ -z "$too_big" ] || { echo "too large for GitHub: $too_big" >&2; exit 1; }

# Checksums for every run's files, so a restored copy can be verified.
for run in results/runs/*/ results/trials/*/; do
  [ -d "$run" ] || continue
  (cd "$run" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 shasum -a 256 > SHA256SUMS)
done

source_commit=$(git rev-parse --short=12 HEAD)
host=$(hostname -s)
mkdir -p "$dest/$host"
# Topic dumps (SPEC 14.6) are too large for the results repo; they go to the
# Drive folder in DRIVE_BACKUP_DIR only. SHA256SUMS above still covers them.
rsync -a --exclude dumps/ results/ "$dest/$host/results/"
if [ -n "${DRIVE_BACKUP_DIR:-}" ]; then
  for dumps in results/runs/*/dumps/ results/trials/*/dumps/; do
    [ -d "$dumps" ] || continue
    rel=${dumps#results/}
    mkdir -p "$DRIVE_BACKUP_DIR/$host/$rel"
    rsync -a --ignore-existing "$dumps" "$DRIVE_BACKUP_DIR/$host/$rel"
    cp "${dumps%dumps/}SHA256SUMS" "$DRIVE_BACKUP_DIR/$host/${rel%dumps/}SHA256SUMS"
  done
  echo "dumps copied to Drive"
elif ls -d results/runs/*/dumps results/trials/*/dumps >/dev/null 2>&1; then
  echo "warning: DRIVE_BACKUP_DIR is not set; topic dumps were not backed up" >&2
fi

git -C "$dest" add -A
if git -C "$dest" diff --cached --quiet; then
  echo "nothing new to back up"
  exit 0
fi
git -C "$dest" commit -q -m "Results from $host at code commit $source_commit"
git -C "$dest" push -q origin HEAD
echo "backed up: $(git -C "$dest" log --oneline -1)"
