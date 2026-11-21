#!/bin/bash
# Copies results/ to the private results repo and pushes it. Results live apart from
# the code so that a backup never changes the commit recorded in running cycles.
set -euo pipefail

dest=${RESULTS_REPO_DIR:-../dualwrite-results}
[ -d "$dest/.git" ] || { echo "no results repo at $dest" >&2; exit 1; }
[ -d results ] || { echo "no results/ to back up" >&2; exit 1; }

too_big=$(find results -type f -size +95M -not -path "*/dumps/*" -not -path "*/raw/*")
[ -z "$too_big" ] || { echo "too large for GitHub: $too_big" >&2; exit 1; }

# sha256sum on Linux, shasum on macOS; both write and check the same format.
# An array, not a function: xargs runs programs, and would find the unrelated
# system command "sum" instead of a shell function of that name.
if command -v sha256sum >/dev/null; then SUM=(sha256sum); else SUM=(shasum -a 256); fi

# Checksums for every session and trial directory (each has a manifest), so a
# restored copy can be verified with make verify. The list is written outside
# the directory first, so it never lists itself.
find results/runs results/trials -name manifest.json 2>/dev/null | while read -r manifest; do
  run=$(dirname "$manifest")
  tmp=$(mktemp)
  (cd "$run" && find . -type f ! -name SHA256SUMS ! -name '*.tmp' -print0 | sort -z | xargs -0 -r "${SUM[@]}" > "$tmp")
  mv "$tmp" "$run/SHA256SUMS"
done

source_commit=$(git rev-parse --short=12 HEAD)
host=$(hostname -s)
mkdir -p "$dest/$host"
# Topic dumps and raw files (SPEC 11.5) are too large for the results repo. On
# the Mac they go to the Drive folder in DRIVE_BACKUP_DIR; on the results
# machine the Mac pulls them with rsync (docs/SETUP.md, 10.9). SHA256SUMS above
# covers them either way.
rsync -a --exclude dumps/ --exclude raw/ --exclude '*.tmp' results/ "$dest/$host/results/"
if [ -n "${DRIVE_BACKUP_DIR:-}" ]; then
  mkdir -p "$DRIVE_BACKUP_DIR/$host"
  rsync -a --exclude '*.tmp' results/ "$DRIVE_BACKUP_DIR/$host/results/"
  echo "results, dumps and raw files copied to Drive"
fi

git -C "$dest" add -A
if git -C "$dest" diff --cached --quiet; then
  echo "nothing new to back up"
  exit 0
fi
git -C "$dest" commit -q -m "Results from $host at code commit $source_commit"
git -C "$dest" push -q origin HEAD
echo "backed up: $(git -C "$dest" log --oneline -1)"
