#!/bin/bash
# Prints the machine the results come from (SPEC 2.1). The runner writes it to
# host-info.txt in each session or trial directory before the first cycle.
set -uo pipefail
echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "hostname: $(hostname)"
echo "kernel: $(uname -srm)"
case "$(uname -s)" in
  Darwin)
    echo "cpu: $(sysctl -n machdep.cpu.brand_string)"
    echo "cores: $(sysctl -n hw.ncpu)"
    echo "memory_bytes: $(sysctl -n hw.memsize)"
    echo "os: $(sw_vers -productName) $(sw_vers -productVersion)"
    ;;
  Linux)
    echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
    echo "cores: $(nproc)"
    echo "memory: $(grep MemTotal /proc/meminfo | tr -s ' ')"
    [ -r /etc/os-release ] && echo "os: $(. /etc/os-release && echo "$PRETTY_NAME")"
    if grep -qi microsoft /proc/version 2>/dev/null; then
      echo "wsl: yes"
      if command -v cmd.exe >/dev/null; then
        echo "windows: $(cmd.exe /c ver 2>/dev/null | tr -d '\r' | grep -v '^$')"
      fi
      if command -v wsl.exe >/dev/null; then
        echo "wsl_version: $(wsl.exe --version 2>/dev/null | tr -d '\0\r' | head -1)"
      fi
    fi
    ;;
esac
echo "docker: $(docker version --format '{{.Server.Version}}' 2>/dev/null)"
echo "docker_memory_bytes: $(docker info --format '{{.MemTotal}}' 2>/dev/null)"
echo "docker_cpus: $(docker info --format '{{.NCPU}}' 2>/dev/null)"
echo "go: $(go version 2>/dev/null)"
exit 0
