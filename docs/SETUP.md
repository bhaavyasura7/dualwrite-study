# Setting up and using the results machine

How to set up the college PC as the study's results machine (SPEC 2.1) and run the study on it. You reach the PC with **Chrome Remote Desktop**. Claude and all code changes stay on the Mac; the PC only runs the study.

```
Mac (you, Claude, the code)                 College PC (Windows → WSL2 Ubuntu)
  edit, commit, push    ── GitHub ──►         git pull in ~/capstone (inside Ubuntu)
  Chrome Remote Desktop ───────────►          Ubuntu window, tmux session "study"
  git pull results      ◄── GitHub ──         the runner's hourly backup pushes
                                              dumps + raw files stay on the PC
```

Steps 0–8 are done once. Section 9 is how you use the PC every day.

Everything here is free: WSL2 and Ubuntu, Docker Engine, Go, git, tmux, Chrome Remote Desktop.

**Why WSL2 is needed.** Docker, the Makefile and the scripts are Linux-only. WSL2 is a real Linux inside Windows. Everything below runs in the **Ubuntu window**, except the few steps marked *PowerShell* or *Windows*.

---

## 0. Before you start

- [ ] Chrome Remote Desktop works from the Mac to the PC (already set up).
- [ ] **Admin rights** on the PC (needed once, for steps 1, 2 and 7).
- [ ] **GitHub access** to `bhaavyasura7/dualwrite-study` (read) and `bhaavyasura7/dualwrite-results` (write).
- [ ] Nobody else will use the PC while a session runs.

A clone made with Git for **Windows** (in `C:\...`) is fine for reading the docs, but the study must run from the clone inside Ubuntu (step 5). Windows git changes line endings, which breaks the scripts, and Windows folders are slow from Linux, which would distort the measurements.

Allow about 1 hour for steps 1 to 8.

---

## 1. Install WSL2 and Ubuntu (PowerShell as Administrator)

Start menu → type **PowerShell** → right-click → **Run as administrator**:

```powershell
wsl -l -v
```

If `Ubuntu-24.04` with VERSION `2` is listed, go to step 2. Otherwise:

```powershell
wsl --install -d Ubuntu-24.04
```

Restart the PC when it asks. After the restart an Ubuntu window opens and asks for a **username and password**. Pick a short lowercase username. Then check again with `wsl -l -v`: VERSION must be 2.

---

## 2. Memory and systemd

**Memory (PowerShell).** By default WSL2 gets only half the RAM (16 GB).

```powershell
notepad $env:USERPROFILE\.wslconfig
```

Paste this, save and close:

```ini
[wsl2]
memory=24GB
swap=8GB
vmIdleTimeout=-1
```

`vmIdleTimeout=-1` stops WSL2 from shutting itself down when no window is open.

**systemd (Ubuntu window)** (Start menu → **Ubuntu 24.04**):

```bash
sudo tee /etc/wsl.conf <<'EOF'
[boot]
systemd=true
EOF
```

Then in **PowerShell**: `wsl --shutdown`. Open Ubuntu again and check:

```bash
systemctl is-system-running    # "running" or "degraded" are both fine
free -g                        # total should be about 23–24
```

---

## 3. Install the tools (Ubuntu window)

Copy one block at a time. To paste into the Ubuntu window, right-click.

**Basic tools:**

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git make tmux build-essential curl ca-certificates rsync perl
```

**Docker Engine** (free; do not install Docker Desktop):

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
sudo systemctl enable --now docker
```

**Go 1.27.3**, the same version as every earlier run:

```bash
cd /tmp
curl -LO https://go.dev/dl/go1.27.3.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.27.3.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin' >> ~/.bashrc
cd ~
```

**golangci-lint**, the same version as the Mac (`make accept` runs it):

```bash
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sudo sh -s -- -b /usr/local/go/bin v2.15.0
```

Now **close the Ubuntu window and open it again** (this applies the Docker group and the new PATH), then check:

```bash
docker run --rm hello-world                 # prints "Hello from Docker!"
docker info | grep "Total Memory"           # about 23–24 GiB
go version                                  # go1.27.3 linux/amd64
golangci-lint --version                     # 2.15.0
```

---

## 4. (Optional, skipped by default) Google Drive for the large files

Topic dumps and raw data (SPEC 11.5) are too big for GitHub, so by default they stay **only on the PC**, in `~/capstone/results/`. The backup then prints `WARNING: DRIVE_BACKUP_DIR not set`, which is expected. Never delete `results/` on the PC. Only if you later want a second copy, set up Drive as below.

1. Install **Google Drive for desktop** from <https://www.google.com/drive/download/> and sign in.
2. Open its settings (gear icon → **Preferences** → **My Drive**) and choose **Mirror files**. This keeps a real folder at `C:\Users\<windows-user>\My Drive`, which Ubuntu can reach.
3. In the **Ubuntu window**, point the backup at it. Replace `<windows-user>` with the folder name under `C:\Users`:

```bash
ls "/mnt/c/Users/<windows-user>/My Drive"   # must list your Drive files
echo 'export DRIVE_BACKUP_DIR="/mnt/c/Users/<windows-user>/My Drive/dualwrite-backup"' >> ~/.bashrc
source ~/.bashrc && mkdir -p "$DRIVE_BACKUP_DIR"
```

The runner's backup runs between cycles (never during one) and copies everything, including dumps and raw files, into that folder; Google Drive uploads it. Do this step before you first start tmux, so the `study` session has the setting.

---

## 5. Connect to GitHub and clone (Ubuntu window)

```bash
ssh-keygen -t ed25519 -C college-pc         # press Enter at every question
cat ~/.ssh/id_ed25519.pub
```

Copy the line it prints. On GitHub: **Settings → SSH and GPG keys → New SSH key**, paste it, save. Use an account that has write access to `dualwrite-results`.

Test it, then clone both repos **side by side in your Ubuntu home folder** (the backup script expects this layout; never under `/mnt/c`):

```bash
ssh -T git@github.com                       # "Hi <name>! You've successfully authenticated"
cd ~
git clone git@github.com:bhaavyasura7/dualwrite-study.git capstone
git clone git@github.com:bhaavyasura7/dualwrite-results.git
for r in capstone dualwrite-results; do
  git -C $r config user.name "DevLikhith5"
  git -C $r config user.email "<your GitHub email>"
done
cd ~/capstone && git checkout spec-2.9 && git log --oneline -1
```

---

## 6. Test the backup (Ubuntu window)

Do **not** add a cron job. The runner itself runs `make backup` between cycles, about once an hour, so a backup never overlaps a measurement (SPEC 14.5). Test once:

```bash
cd ~/capstone && mkdir -p results && make backup
```

It should end with a push to `dualwrite-results`, or say there is nothing new. If it fails, it's almost always GitHub access: check `ssh -T git@github.com`.

---

## 7. Keep Windows awake and out of the way (PowerShell as Administrator)

**No sleep or hibernate on mains power** (Chrome Remote Desktop needs this too):

```powershell
powercfg /change standby-timeout-ac 0
powercfg /change hibernate-timeout-ac 0
```

(The screen may still turn off; that's fine.)

**Pause Windows Update:** Settings → Windows Update → **Pause updates** → pick the longest option. On a college-managed PC this may be locked; if so, ask IT not to push restarts that week.

**Bring WSL2 back after a reboot.** This creates a task that starts Ubuntu (and with it Docker) at boot. It asks for your Windows password:

```powershell
schtasks /create /tn "WSL keepalive" /sc onstart /ru $env:USERNAME /rp * /rl highest `
  /tr "wsl.exe -d Ubuntu-24.04 --exec sleep infinity"
```

**Stop Defender scanning WSL2's disk** (scans slow the database and skew latency). Find the disk file, then exclude it:

```powershell
Get-ChildItem $env:LOCALAPPDATA -Recurse -Filter ext4.vhdx -ErrorAction SilentlyContinue | Select FullName
Add-MpPreference -ExclusionPath "<the FullName printed above>"
```

If the college manages Defender, this may be refused; note it and tell IT.

---

## 8. Final check

Restart the PC once. Connect with Chrome Remote Desktop, open Ubuntu and run:

```bash
docker info | grep "Total Memory"; go version; ls ~; echo $DRIVE_BACKUP_DIR
```

If it shows about 23–24 GiB, go1.27.3 and `capstone  dualwrite-results`, the setup is complete (the Drive path is empty unless you did step 4).

---

## 9. Using the PC

### 9.1 Rules for Chrome Remote Desktop

| Do | Don't |
|---|---|
| Run the study inside **tmux** in the Ubuntu window | Run it in a plain Ubuntu window: closing it could stop the run |
| End a remote session by closing the Chrome tab or **Disconnect** | **Sign out** of Windows or **Restart**: that stops WSL2 and the run |
| Leave the Ubuntu window open (minimised is fine) | Type in the `study` tmux session except to detach |
| Keep the PC on mains power | Use the PC for other heavy work during a session, and never during `perf` |

### 9.2 tmux

```bash
tmux new -As study        # join the session "study" (created the first time)
```

| You want to | Press or type |
|---|---|
| Leave, and keep everything running | `Ctrl-b`, release, then `d` (detach) |
| Come back later | `tmux new -As study` again |
| Scroll up through the output | `Ctrl-b`, then `[`; arrow keys or Page Up; `q` to stop |
| See whether the session exists | `tmux ls` |

**Never type `exit` or press `Ctrl-c` inside the `study` session while the study runs.** `exit` closes the session and `Ctrl-c` stops the run. Always detach with `Ctrl-b d`. For other commands, open a second Ubuntu tab (outside tmux).

### 9.3 Useful commands (in a second Ubuntu tab)

| Purpose | Command |
|---|---|
| What the run is printing right now | `tmux capture-pane -pt study \| tail -20` |
| Memory and disk | `free -g; df -h ~` |
| Containers up | `docker ps --format "{{.Names}} {{.Status}}"` |
| Cycles finished so far in a session | `wc -l ~/capstone/results/runs/*/must/results.csv` |
| Last finished cycles | `tail -3 ~/capstone/results/runs/*/must/results.csv \| cut -d, -f2,10` |
| Stop after the current cycle | `cd ~/capstone && make pause` |

Replace `must` with `perf` or `should` for the other sessions.

### 9.4 Getting code onto the PC

Code always goes Mac → GitHub → PC, never edited on the PC. **On the PC**, in Ubuntu:

```bash
cd ~/capstone
git fetch --all --tags
git checkout spec-2.9 && git pull    # a candidate branch, for make accept and the check run
git log --oneline -1                 # confirm the commit you expect
```

**For a real session**, check out the frozen tag instead (SPEC 14.7). The runner refuses to start unless `HEAD` is at the study tag and the tree is clean:

```bash
git checkout study-v4                # "detached HEAD" is expected here
git status --short                   # must print nothing
```

**Never fetch, pull or check out anything in `~/capstone` while a session runs.**

### 9.5 Running the study

The order is fixed by SPEC 14.7: `make accept` → check run → freeze → **perf → must → should**. Each starts inside tmux:

```bash
tmux new -As study
cd ~/capstone
make up                       # start the stack; wait for "healthy"
make accept                   # first time only: every check must pass
make check-run                # then make verify / make recompute on it (SPEC 14.7)
# after the freeze: make run-perf, then make run-must, then make run-should
# Ctrl-b d to detach
```

A session ends by itself. Check that it finished (9.6), then start the next one the same way. The runner writes `host-info.txt` into each session folder by itself.

### 9.6 Checking progress from the Mac

Without Remote Desktop, from the backup (at most about an hour old):

```bash
cd ~/Projects/dualwrite-results && git pull
ls                                           # one folder per machine; the PC's is its Windows computer name
ls <pc-name>/results/runs/<commit>/must/     # results.csv, results.order.txt, damage/, logs/
```

Dumps and raw files stay on the PC in `~/capstone/results/runs/<commit>/<session>/` (`dumps/`, `raw/`). To check them against the checksums, on the PC: `cd ~/capstone/results/runs/<commit>/must && sha256sum -c --quiet SHA256SUMS`. (`SHA256SUMS` is rewritten at each backup, so files from cycles finished since then are not in it yet.)

### 9.7 Pausing and resuming

In a second Ubuntu tab: `cd ~/capstone && make pause`. It finishes the current cycle, then stops.

To resume: `tmux new -As study` and run the **same** `make run-...` command again. It skips finished cycles. After a power cut or a Windows restart, do the same; the cycle that was interrupted is thrown away and run again.

### 9.8 Using Claude

Claude runs only on the Mac and has no connection to the PC. It sees the run through the backups in `dualwrite-results` (9.6). Ask it, for example, "How's the run going? Pull the results and check them." When something fails on the PC, copy the last lines of the tmux output (or a screenshot) into Claude on the Mac. Fixes are made on the Mac and reach the PC through GitHub (9.4).

### 9.9 Daily routine while the study runs

1. Remote Desktop → Ubuntu tab → `tmux capture-pane -pt study | tail -20`: still running, with no repeated errors?
2. On the Mac: `cd ~/Projects/dualwrite-results && git pull`: has a backup arrived in the last couple of hours?
3. On the PC: `du -sh ~/capstone/results`: is it growing?
4. `df -h ~` on the PC: more than 20 GB free?
5. Look for the signs of a harness fault listed in SPEC 14.7. If you see one, stop and decide with SPEC 14.7's rule whether it needs a fix-and-recompute or a new run.

Or ask Claude to do 2, 3 and 5 from the backup.

---

## 10. Troubleshooting

| Problem | Check | Fix |
|---|---|---|
| `docker: permission denied` | `groups` doesn't list `docker` | `sudo usermod -aG docker $USER`, then close and reopen Ubuntu |
| Runner says Docker has too little memory | `free -g` | Fix `.wslconfig` (step 2), then `wsl --shutdown` in PowerShell |
| Docker not running after a reboot | `systemctl status docker` | `sudo systemctl start docker`; check step 2 (systemd) |
| Backup not arriving on GitHub | The runner's log in tmux: search for `backup` | Usually GitHub auth: `ssh -T git@github.com` |
| Remote Desktop can't connect | The PC is off or asleep | Someone switches it on at college; then resume (9.7) |
| Disk filling up | `df -h ~` | The runner refuses to start under 20 GB free; delete old trial folders only after they are backed up |
| tmux session gone | `tmux ls` | The PC rebooted: `tmux new -As study` and rerun the same `make run-...` |
