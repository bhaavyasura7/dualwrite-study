# Setting up and using the results machine

How to set up the college PC as the study's results machine (SPEC 2.1), connect to it from the Mac, and run and watch the study from there. Claude and all development stay on the Mac; the PC only runs the study.

```
Mac (you, Claude, the code)                 College PC (Windows → WSL2 Ubuntu)
  edit, commit, push   ── GitHub ──►          git fetch / checkout in ~/capstone
  ssh college-pc       ── Tailscale ──►       tmux session "study" runs the study
  git pull results     ◄── GitHub ──          the runner's hourly backup pushes
  rsync dumps + raw    ◄── Tailscale ──       ~/capstone/results/runs/<commit>/<session>/
```

Steps 0–9 are done once. Section 10 is how you use the PC from the Mac every day.

Everything here is free: WSL2 and Ubuntu, Docker Engine, Tailscale (personal plan), Go, git, tmux.

---

## 0. Before you start

- [ ] **Permission from college IT** to install WSL2 and Tailscale, and to leave the PC on for about a week. Tailscale is needed because college networks block incoming connections; it opens no ports.
- [ ] **Admin rights** on the PC (needed once, for steps 2 and 8).
- [ ] **A Tailscale account.** Any free account (Google, GitHub or Microsoft login). The Mac and the PC must use the same one.
- [ ] **GitHub access** to `bhaavyasura7/dualwrite-study` (read) and `bhaavyasura7/dualwrite-results` (write).
- [ ] Nobody else will use the PC while a session runs.

Allow about 1.5 hours at the PC for steps 2 to 9.

---

## 1. On the Mac (do this first, at home)

1. Install Tailscale from <https://tailscale.com/download/mac> and sign in.
2. In the Tailscale admin page (<https://login.tailscale.com/admin/dns>), check that **MagicDNS** is on. It lets you type `college-pc` instead of an IP address.
3. Add this to `~/.ssh/config` on the Mac (create the file if it doesn't exist). Replace `<ubuntu-user>` with the Ubuntu username you'll create in step 2:

   ```
   Host college-pc
     User <ubuntu-user>
     ServerAliveInterval 30
     ServerAliveCountMax 6
   ```

   `ServerAlive` keeps the connection from dropping silently on bad wifi.

---

## 2. Install WSL2 and Ubuntu (PC, PowerShell as Administrator)

Open the Start menu, type **PowerShell**, right-click it and choose **Run as administrator**:

```powershell
wsl --install -d Ubuntu-24.04
```

Restart the PC when it asks. After the restart an Ubuntu window opens and asks you for a **username and password**. Pick a short lowercase username: this is `<ubuntu-user>` everywhere below.

Check that it is WSL **2**:

```powershell
wsl -l -v          # VERSION column must say 2
```

---

## 3. Give WSL2 enough memory (PC, PowerShell)

By default WSL2 gets only half the RAM (8 GB). The study needs at least 11 GiB for Docker.

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

---

## 4. Turn on systemd (PC, Ubuntu window)

```bash
sudo tee /etc/wsl.conf <<'EOF'
[boot]
systemd=true
EOF
```

Then in PowerShell:

```powershell
wsl --shutdown
```

Open Ubuntu again (Start menu → **Ubuntu 24.04**). Check:

```bash
systemctl is-system-running    # "running" or "degraded" are both fine
free -g                        # total should be about 12–13
```

---

## 5. Install the tools (PC, Ubuntu window)

Copy one block at a time.

**Basic tools:**
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git make tmux build-essential curl ca-certificates rsync perl
```
(`perl` provides `shasum`, which `make backup` and `make verify` use.)

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

**Tailscale**, with its built-in SSH server:
```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up --ssh --hostname college-pc
```
It prints a link. Open it in a browser and log in with **the same Tailscale account as the Mac**.

Now **close the Ubuntu window and open it again** (this applies the Docker group and the new PATH), then check:

```bash
docker run --rm hello-world                 # prints "Hello from Docker!"
docker info | grep "Total Memory"           # about 12–13 GiB
go version                                  # go1.27.3 linux/amd64
golangci-lint --version                     # 2.15.0
tailscale status                            # lists college-pc and your Mac
```

---

## 6. Connect the PC to GitHub (PC, Ubuntu window)

```bash
ssh-keygen -t ed25519 -C college-pc         # press Enter at every question
cat ~/.ssh/id_ed25519.pub
```

Copy the line it prints. On GitHub: **Settings → SSH and GPG keys → New SSH key**, paste it, save. Use an account that has write access to `dualwrite-results`.

Test it, then clone both repos **side by side in your home folder** (the backup script expects this layout; never put them under `/mnt/c`):

```bash
ssh -T git@github.com                       # "Hi <name>! You've successfully authenticated"
cd ~
git clone git@github.com:bhaavyasura7/dualwrite-study.git capstone
git clone git@github.com:bhaavyasura7/dualwrite-results.git
for r in capstone dualwrite-results; do
  git -C $r config user.name "DevLikhith5"
  git -C $r config user.email "<your GitHub email>"
done
```

---

## 7. Backup (PC, Ubuntu window)

Do **not** add a cron job. From spec 2.9 the runner itself runs `make backup` between cycles, about once an hour, so a backup never overlaps a measurement (SPEC 14.5). Just test once that a backup can push:

```bash
cd ~/capstone && mkdir -p results && make backup
```

It should end with a push to `dualwrite-results`, or say there is nothing new. If it fails, it's almost always GitHub access: check `ssh -T git@github.com`.

---

## 8. Keep Windows awake and out of the way (PC, PowerShell as Administrator)

**No sleep or hibernate on mains power:**
```powershell
powercfg /change standby-timeout-ac 0
powercfg /change hibernate-timeout-ac 0
```
(The screen may still turn off; that's fine.)

**Pause Windows Update:** Settings → Windows Update → **Pause updates** → pick the longest option. On a college-managed PC this may be locked; if so, ask IT not to push restarts that week.

**Bring WSL2 back after a reboot.** This creates a task that starts Ubuntu (and with it Docker and Tailscale) at boot. It asks for your Windows password:
```powershell
schtasks /create /tn "WSL keepalive" /sc onstart /ru $env:USERNAME /rp * /rl highest `
  /tr "wsl.exe -d Ubuntu-24.04 --exec sleep infinity"
```
If a reboot still leaves `ssh college-pc` failing, someone has to log in on the PC once; WSL2 then starts again.

**Stop Defender scanning WSL2's disk** (scans slow the database and skew latency). Find the disk file, then exclude it:
```powershell
Get-ChildItem $env:LOCALAPPDATA -Recurse -Filter ext4.vhdx -ErrorAction SilentlyContinue | Select FullName
Add-MpPreference -ExclusionPath "<the FullName printed above>"
```
If the college manages Defender, this may be refused; note it and tell IT.

---

## 9. Test the connection before leaving the PC

On the **Mac**:

```bash
ssh college-pc
```

The first time, Tailscale may open a browser page asking you to approve the login: approve it. You should get the Ubuntu prompt. Then, still from the Mac:

```bash
ssh college-pc 'docker info | grep "Total Memory"; go version; ls ~'
```

Restart the PC once and repeat this test. If it still works after a reboot, the setup is complete.

---

## 10. Working from the Mac

Everything in this section is typed **on the Mac**, in the Terminal app (or VS Code's terminal), unless it says otherwise. It all works only after step 9 passed.

### 10.1 How the connection works

Three tools, each with one job:

| Tool | What it does for you |
|---|---|
| **Tailscale** | Puts the Mac and the PC on one private network, wherever each of them is. The PC is reachable by the name `college-pc` from home, from college, from a phone hotspot. Nothing is opened to the internet |
| **SSH** | Gives you a terminal on the PC's Ubuntu from the Mac's terminal. `ssh college-pc` logs you in; `ssh college-pc '<command>'` runs one command there and prints its output here |
| **tmux** | Keeps a terminal session alive **on the PC**. The study runs inside it, so closing the laptop, losing wifi or ending SSH never stops the run |

Before each session, check that Tailscale is connected on the Mac (the Tailscale icon in the menu bar shows *Connected*, or `tailscale status` lists `college-pc`).

### 10.2 Opening a terminal on the PC

```bash
ssh college-pc            # 1. log in: the prompt changes to <ubuntu-user>@<pc-name>
tmux new -As study        # 2. join the session "study" (created the first time)
```

You are now in the same terminal as the running study. To get out:

| You want to | Press or type |
|---|---|
| Leave, and keep everything running | `Ctrl-b`, release, then `d` (detach). Then `exit` to close SSH |
| Come back later | `ssh college-pc`, then `tmux new -As study` again |
| Scroll up through the output | `Ctrl-b`, then `[`; arrow keys or Page Up; `q` to stop |
| See whether the session exists | `tmux ls` (on the PC) |

**Never type `exit` or press `Ctrl-c` inside the `study` session while the study runs.** `exit` closes the session and `Ctrl-c` stops the run. Always detach with `Ctrl-b d`.

### 10.3 Running one command without logging in

Put the command in quotes after `ssh college-pc`. It runs on the PC and the output appears on the Mac. Useful ones:

| Purpose | Command |
|---|---|
| What the run is printing right now | `ssh college-pc 'tmux capture-pane -pt study \| tail -20'` |
| Memory and disk | `ssh college-pc 'free -g; df -h ~'` |
| Containers up | `ssh college-pc 'docker ps --format "{{.Names}} {{.Status}}"'` |
| Cycles finished so far in a session | `ssh college-pc 'wc -l ~/capstone/results/runs/*/must/results.csv'` |
| Last finished cycles | `ssh college-pc 'tail -3 ~/capstone/results/runs/*/must/results.csv \| cut -d, -f2,10'` |
| Stop after the current cycle | `ssh college-pc 'cd ~/capstone && make pause'` |

Replace `must` with `perf` or `should` for the other sessions.

### 10.4 Looking at the PC's files in VS Code

1. In VS Code on the Mac, open Extensions (`⌘⇧X`), search **Remote - SSH** (by Microsoft) and install it.
2. Press `⌘⇧P`, type **Remote-SSH: Connect to Host**, choose **college-pc**. A new window opens; the first time it installs a small server on the PC (about a minute).
3. **File → Open Folder** → `/home/<ubuntu-user>/capstone`.

You can now browse results, open logs and use VS Code's terminal, all on the PC. Rules while a session runs: **read only**. Don't edit or save files in `~/capstone`, and don't run `git` there; any change to the tree breaks the frozen commit. Close the window when done.

### 10.5 Getting code onto the PC

Code always goes Mac → GitHub → PC, never edited on the PC.

**On the Mac**, push the branch you want on the PC:

```bash
cd ~/Projects/capstone
git push -u origin spec-2.9          # or whichever branch holds the candidate
```

**On the PC** (or through `ssh college-pc '...'`):

```bash
cd ~/capstone
git fetch --all --tags
git checkout spec-2.9 && git pull    # a candidate branch, for make accept and the check run
git log --oneline -1                 # confirm the commit you expect
```

**For a real session**, check out the frozen tag instead (SPEC 14.7, step 6). The runner refuses to start unless `HEAD` is at the study tag and the tree is clean:

```bash
git checkout study-v4                # "detached HEAD" is expected here
git status --short                   # must print nothing
```

**Never fetch, pull or check out anything in `~/capstone` while a session runs.**

### 10.6 Running the study

The order is fixed by SPEC 14.7: `make accept` → check run → freeze → **perf → must → should**. Each session starts inside tmux on the PC:

```bash
ssh college-pc
tmux new -As study
cd ~/capstone
make up                       # start the stack; wait for "healthy"
make run-perf                 # then, when it ends: make run-must, then make run-should
# Ctrl-b d to detach
```

A session ends by itself. Check that it finished (10.7), then start the next one the same way.

The runner writes `host-info.txt` (the machine's details) into each session folder by itself. Before the real sessions, run the check run on the same commit: `make check-run`, then `make verify RUN=results/trials/<commit>-check` and `make recompute RUN=results/trials/<commit>-check` (SPEC 14.7).

### 10.7 Checking progress and results

**From the backup** (no SSH needed, at most about an hour old). The runner pushes results to `dualwrite-results` between cycles, in a folder named after the PC's hostname:

```bash
cd ~/Projects/dualwrite-results && git pull
ls                                           # one folder per machine; the PC's is its Windows computer name
ls <pc-name>/results/runs/<commit>/must/     # results.csv, results.order.txt, damage/, logs/
```

**Live**, with the commands in 10.3.

**Through Claude**, see 10.10.

### 10.8 Pausing and resuming

```bash
ssh college-pc 'cd ~/capstone && make pause'    # finishes the current cycle, then stops
```

To resume: open the session (10.2) and run the **same** `make run-...` command again. It skips finished cycles. After a power cut or a Windows restart, do the same; the cycle that was interrupted is thrown away and run again. If `ssh college-pc` fails after a restart, someone has to log in on the PC once (step 8).

### 10.9 Copying topic dumps and raw data to the Mac

Topic dumps and each cycle's raw data (SPEC 11.5) are too big for GitHub, so the Mac pulls them over Tailscale **at least once a day**. They are what lets a metric be recomputed later without running the study again, so don't skip this.

Once, on the Mac, tell your shell where the Drive folder is. Use the path of your own Google Drive folder:

```bash
echo 'export DRIVE_BACKUP_DIR="$HOME/Library/CloudStorage/GoogleDrive-<your account>/My Drive/dualwrite-backup"' >> ~/.zshrc
source ~/.zshrc && mkdir -p "$DRIVE_BACKUP_DIR"
```

Then, each day:

```bash
rsync -av college-pc:capstone/results/runs/ "$DRIVE_BACKUP_DIR/college-pc/runs/"
```

`rsync` copies only what is new, so later runs are quick. It copies everything under `runs/`, including `dumps/` and `raw/`. To check a copy against the PC's checksums:

```bash
cd "$DRIVE_BACKUP_DIR/college-pc/runs/<commit>/must" && shasum -a 256 -c --quiet SHA256SUMS
```

(`SHA256SUMS` is rewritten at each backup, so files from cycles finished since then are not in it yet.)

### 10.10 Using Claude from the Mac

Claude runs only on the Mac. It reaches the PC the same way you do, with `ssh college-pc '...'`, so nothing of Claude's is installed on the PC.

1. Open Claude Code in `~/Projects/capstone` on the Mac.
2. Ask in plain words, for example:
   - "How's the run on the college PC going?"
   - "Why did cycle C-1_P4_c2_r03 fail? Check its logs on the PC."
   - "Copy today's dumps and raw data to the Mac and verify them."
3. Claude runs `ssh college-pc` commands, reads the output, and reports back. It asks before anything that changes the PC (pause, start a session, check out code).

So Claude doesn't ask permission for every read-only SSH command, allow them once: in Claude Code type `/permissions`, add an **allow** rule `Bash(ssh college-pc:*)`, and save it to the project's local settings. You can remove it the same way.

Claude follows the same rules as you: no `git` in `~/capstone` and no edits on the PC while a session runs. Code fixes are made on the Mac and only reach the PC through GitHub (10.5).

### 10.11 Daily routine while the study runs

1. `ssh college-pc 'tmux capture-pane -pt study | tail -20'`: is it still running, and with no repeated errors?
2. `cd ~/Projects/dualwrite-results && git pull`: has the backup arrived in the last couple of hours?
3. `rsync` the dumps and raw data (10.9).
4. `ssh college-pc 'df -h ~'`: more than 20 GB free?
5. Look for the signs of a harness fault listed in SPEC 14.7. If you see one, stop and decide with SPEC 14.7's rule whether it needs a fix-and-recompute or a new run.

Or ask Claude to do all five ("do the daily check on the college PC").

---

## 11. Troubleshooting

| Problem | Check | Fix |
|---|---|---|
| `ssh college-pc` hangs or "could not resolve" | `tailscale status` on the Mac: is `college-pc` listed and online? | If offline, the PC is off, asleep or WSL2 isn't running: someone logs in on the PC. If not listed, run `sudo tailscale up --ssh --hostname college-pc` on the PC again |
| "Permission denied" over SSH | You used the Windows username | Use the Ubuntu username in `~/.ssh/config` |
| `docker: permission denied` | `groups` doesn't list `docker` | `sudo usermod -aG docker $USER`, then close and reopen Ubuntu |
| Runner says Docker has too little memory | `free -g` on the PC | Fix `.wslconfig` (step 3), then `wsl --shutdown` in PowerShell |
| Docker not running after a reboot | `systemctl status docker` | `sudo systemctl start docker`; check step 4 (systemd) |
| Backup not arriving | The runner's log in tmux: search for `backup` | Usually GitHub auth: `ssh -T git@github.com` |
| Disk filling up | `df -h ~` | The runner refuses to start under 20 GB free; delete old trial folders only after they are backed up |
| tmux session gone | `tmux ls` | The PC rebooted: `tmux new -As study` and rerun the same `make run-...` |
