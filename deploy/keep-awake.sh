#!/bin/bash
# Runs a command while keeping the host awake (SPEC 14.4): caffeinate on macOS,
# systemd-inhibit on Linux. WSL2 has no sleep of its own; Windows is kept awake
# by its power settings (docs/SETUP.md, step 8), and systemd-inhibit is used
# when present so a Linux desktop cannot suspend either.
set -euo pipefail
case "$(uname -s)" in
  Darwin) exec caffeinate -i "$@" ;;
  Linux)
    if command -v systemd-inhibit >/dev/null && systemd-inhibit --list >/dev/null 2>&1; then
      exec systemd-inhibit --what=sleep:idle --why="dualwrite study session" "$@"
    fi
    exec "$@" ;;
  *) exec "$@" ;;
esac
