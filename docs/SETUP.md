# Setting up and using the results machine

How to set up the college PC as the study's results machine (SPEC 2.1), connect to it from the Mac, and run and watch the study from there. Claude and all development stay on the Mac; the PC only runs the study.

```
Mac (you, Claude, the code)                 College PC (Windows → WSL2 Ubuntu)
  edit, commit, push   ── GitHub ──►          git pull ~/capstone
  ssh college-pc       ── Tailscale ──►       tmux session "study" runs the study
  git pull results     ◄── GitHub ──          the runner's hourly backup pushes
  rsync dumps          ◄── Tailscale ──       ~/capstone/results/runs/.../dumps
```

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
memory=13GB
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

### Opening the session

```bash
ssh college-pc
tmux new -As study        # creates "study" the first time, reattaches after that
```

Inside tmux, anything you start keeps running when you close the laptop or lose wifi.

| tmux key | What it does |
|---|---|
| `Ctrl-b` then `d` | Detach: leave everything running and go back to the plain shell |
| `tmux new -As study` | Reattach later |
| `Ctrl-b` then `[` | Scroll back (arrow keys, `q` to stop) |

Don't type `exit` inside tmux while a session runs; that closes it. Detach instead.

### Getting new code onto the PC

Commit and push on the Mac, then:

```bash
ssh college-pc 'cd ~/capstone && git pull && git log --oneline -1'
```

During a study session the PC's `~/capstone` must stay at the study tag. **Never pull while a session is running.**

### Running the study

Follow SPEC 14.7 in order: `make accept`, then the check run, then the freeze, then the sessions. Each session starts inside tmux:

```bash
ssh college-pc
tmux new -As study
cd ~/capstone && make up && make run-must     # later: make run-should, make run-perf
# Ctrl-b d to detach
```

`make run-should` and the per-session folders come with the v2.8 code (SPEC_NOTES, "Spec 2.8: code not yet matching the spec"). Until that code lands, only `make run-must` and `make run-perf` exist.

### Checking progress without touching the PC

On the Mac, from the backup (at most an hour old):

```bash
cd ~/Projects/dualwrite-results && git pull
```

Or live, without attaching:

```bash
ssh college-pc 'tmux capture-pane -pt study | tail -20'
ssh college-pc 'free -g; df -h ~; docker ps --format "{{.Names}} {{.Status}}"'
```

### Pausing and resuming

```bash
ssh college-pc 'cd ~/capstone && make pause'    # stops after the current cycle
```

To resume, attach to tmux and run the same `make run-...` command again; it skips finished cycles. After a power cut or reboot, do the same: the interrupted cycle is thrown away and rerun.

### Copying topic dumps to the Mac (once a day)

The dumps are too big for GitHub, so the Mac pulls them (SPEC 14.5):

```bash
rsync -av college-pc:capstone/results/runs/ "$DRIVE_BACKUP_DIR/college-pc/runs/"
```

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
