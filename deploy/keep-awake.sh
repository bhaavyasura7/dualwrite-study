#!/bin/bash
# Runs a command while keeping the host awake (SPEC 14.4): caffeinate on macOS,
# systemd-inhibit on a Linux desktop. WSL2 has no sleep of its own and its shell
# has no login session, so systemd-inhibit is refused there; Windows is kept
# awake by its power settings (docs/SETUP.md, step 7), and the command runs as is.
set -euo pipefail
case "$(uname -s)" in
  Darwin) exec caffeinate -i "$@" ;;
  Linux)
    if [ -n "${WSL_DISTRO_NAME:-}" ] || grep -qi microsoft /proc/version 2>/dev/null; then
      exec "$@"
    fi
    # Probe the lock itself, not just --list: taking it can need authorisation.
    if command -v systemd-inhibit >/dev/null &&
      systemd-inhibit --what=sleep:idle --why="probe" true >/dev/null 2>&1; then
      exec systemd-inhibit --what=sleep:idle --why="dualwrite study session" "$@"
    fi
    exec "$@" ;;
  *) exec "$@" ;;
esac
