#!/usr/bin/env python3
"""
GitPulse // Agent-Native LAN Client
Connect any autonomous agent, script, or peer on your LAN to the GitPulse AST Mesh.
Zero external dependencies (uses standard library urllib).
"""

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# A proxy answering for a host it can't reach (530 is Cloudflare's "tunnel is gone"), not the host itself.
GATEWAY_ERRORS = {502, 503, 504, 520, 521, 522, 523, 524, 530}


def discover_lan_host(timeout: float = 1.2) -> str:
    """Broadcasts UDP packet on LAN port 9999 to auto-discover the GitPulse Host Server."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(timeout)
        sock.sendto(b"GITPULSE_DISCOVER", ("255.255.255.255", 9999))
        data, _ = sock.recvfrom(1024)
        sock.close()
        info = json.loads(data.decode("utf-8"))
        if info.get("url"):
            return info["url"]
    except Exception:
        pass
    return "http://localhost:8000"


class GitPulseAgent:
    def __init__(
        self,
        server_url: str = "http://localhost:8000",
        agent_id: str = "agent-worker",
        role: str = "Autonomous Worker",
        target_module: str = "services/worker.py",
        deps: Optional[List[str]] = None,
        session: str = "",
    ):
        self.session = session
        self.server_url = server_url.rstrip("/")
        self.agent_id = agent_id
        self.role = role
        self.target_module = target_module
        self.deps = deps or []

    def _post(self, path: str, payload: dict) -> dict:
        url = f"{self.server_url}{path}"
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:  # the server answered, with an error
            if e.code in GATEWAY_ERRORS:
                return {"status": "error", "error": str(e), "unreachable": True}
            try:
                return {"status": "error", **json.loads(e.read().decode("utf-8"))}
            except Exception:
                return {"status": "error", "error": str(e)}
        except Exception as e:  # nobody answered: host down, network gone, timeout
            return {"status": "error", "error": str(e), "unreachable": True}

    def _get(self, path: str) -> dict:
        url = f"{self.server_url}{path}"
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.URLError as e:
            return {"status": "error", "error": str(e)}

    def register(
        self,
        initial_code: Optional[str] = None,
        engine: str = "Custom",
        machine: str = "",
        branch: str = "main",
    ) -> dict:
        code = initial_code or (
            f"# {self.role} ({self.agent_id})\n"
            f"def process_task(payload: dict) -> dict:\n"
            f"    return {{'status': 'ok', 'agent': '{self.agent_id}'}}\n"
        )
        payload = {
            "agent_id": self.agent_id,
            "role": self.role,
            "target_module": self.target_module,
            "initial_code": code,
            "deps": self.deps,
            "engine": engine,
            "machine": machine or socket.gethostname(),
            "branch": branch,
            "session": self.session,
        }
        res = self._post("/api/agents/register", payload)
        return res

    def send_idea(self, intent: str, gossip_message: Optional[str] = None, code: Optional[str] = None, target_agent: Optional[str] = None) -> dict:
        """Broadcast an idea or gossip circle across the mesh to peer agents."""
        payload = {
            "agent_id": self.agent_id,
            "intent": intent,
            "gossip_message": gossip_message or intent,
            "code": code,
            "target_agent": target_agent,
            "session": self.session,
        }
        return self._post("/api/agents/gossip", payload)

    def mutate_code(self, code: str, intent: str) -> dict:
        """Mutate code and broadcast contract change (or suppress private AST edits)."""
        payload = {
            "agent_id": self.agent_id,
            "code": code,
            "intent": intent,
        }
        return self._post("/api/mutate", payload)

    def get_mesh_state(self) -> dict:
        return self._get("/api/state")


def interactive_repl(agent: GitPulseAgent):
    print(f"\n==========================================")
    print(f"  Connected to GitPulse Mesh")
    print(f"  Host: {agent.server_url}")
    print(f"  Agent ID: {agent.agent_id} ({agent.role})")
    print(f"  Target: {agent.target_module}")
    print(f"==========================================")
    print("Commands:")
    print("  idea <text>              Broadcast an idea/gossip bubble to all peers")
    print("  tell <agent_id> <text>   Send a direct gossip bubble to a specific agent")
    print("  mutate <code>            Mutate agent code (triggers AST diff & locality filter)")
    print("  state                    View current mesh agents and vector clocks")
    print("  quit / exit              Exit client\n")

    reg = agent.register()
    print(f"[*] Registered on mesh: {reg.get('status')}\n")

    while True:
        try:
            line = input(f"[{agent.agent_id}] > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break

        if not line:
            continue
        if line in ("quit", "exit"):
            break

        parts = line.split(maxsplit=2)
        cmd = parts[0].lower()

        if cmd == "idea":
            text = line[4:].strip()
            if not text:
                print("Usage: idea <your thought/proposal>")
                continue
            res = agent.send_idea(intent=text, gossip_message=text)
            print(f"[+] Broadcast bubble sent: {res.get('status')}")

        elif cmd == "tell":
            if len(parts) < 3:
                print("Usage: tell <target_agent> <message>")
                continue
            target = parts[1]
            text = parts[2]
            res = agent.send_idea(intent=f"Direct note to {target}", gossip_message=text, target_agent=target)
            print(f"[+] Direct bubble sent to {target}: {res.get('status')}")

        elif cmd == "mutate":
            code = line[6:].strip()
            res = agent.mutate_code(code, "Live AST mutation from CLI")
            print(f"[+] Mutated code on mesh: {res.get('status')}")

        elif cmd == "state":
            st = agent.get_mesh_state()
            nodes = st.get("nodes", {})
            print(f"\nLive Mesh Nodes ({len(nodes)}):")
            for nid, node in nodes.items():
                status = "ONLINE" if node.get("online") else "OFFLINE"
                print(f"  - {nid} [{status}]: {node.get('role')} | Clock: {node.get('vector_clock')}")
            print()
        else:
            print("Unknown command. Type 'idea <text>', 'tell <agent> <msg>', 'mutate <code>', 'state', or 'exit'.")


def _fallback_agent() -> dict:
    """No agent_detect / nothing running: describe this shell as one agent so you still join."""
    host = os.environ.get("GITPULSE_MACHINE") or socket.gethostname()
    cwd = os.getcwd()
    try:
        import agent_detect
        ws = agent_detect.workspace_info(cwd)
    except Exception:
        ws = {"path": cwd, "name": os.path.basename(cwd), "is_repo": False, "repos": []}
    return {
        "agent_id": f"{host.lower()}-shell-{os.getpid()}",
        "pid": os.getpid(), "engine": "Shell", "color": "#a78bfa", "machine": host,
        "cwd": cwd, "workspace": ws, "status": "running", "started": time.time(),
        "title": "", "last_prompt": "", "files": [], "last_active": None,
    }


def detect_here() -> List[dict]:
    try:
        import agent_detect
        found = agent_detect.detect_agents()
    except Exception as e:
        print(f"[!] agent detection unavailable ({e}); joining as a single shell agent")
        found = []
    return found or [_fallback_agent()]


def _register_live(agent: GitPulseAgent, a: dict, session: str) -> dict:
    repos = a["workspace"].get("repos") or []
    return agent._post("/api/agents/register", {
        "agent_id": a["agent_id"],
        "role": a.get("title") or a["engine"],
        "target_module": f"live/{a['agent_id']}.py",
        "initial_code": "# live agent\n",
        "engine": a["engine"],
        "machine": a["machine"],
        "branch": ", ".join(f"{r['name']}:{r['branch']}" for r in repos) or "no git",
        "session": session,
        "meta": a,
    })


HOOK_ENGINES = {"Claude Code": ".claude/settings.local.json", "Antigravity": ".agents/hooks.json"}


def _under(path: str, root: str) -> bool:
    if not root:
        return False
    p, r = os.path.normcase(os.path.abspath(path)), os.path.normcase(os.path.abspath(root)).rstrip("\\/")
    return p == r or p.startswith(r + os.sep)


def hook_skip_reason(a: dict) -> str:
    """Folders where a project hook makes no sense (system dirs, app data, scratch dirs, no git)."""
    cwd = a.get("cwd") or ""
    home = os.path.expanduser("~")
    if os.path.normcase(os.path.abspath(cwd)) == os.path.normcase(os.path.abspath(home)):
        return "home folder"
    system = [os.environ.get(k, "") for k in ("WINDIR", "SystemRoot", "ProgramFiles", "ProgramFiles(x86)", "ProgramData",
                                              "APPDATA", "LOCALAPPDATA")]
    system += [tempfile.gettempdir(), os.path.join(home, "AppData"), os.path.join(home, "Library"),
               os.path.join(home, ".gitpulse"), "/usr", "/opt", "/etc", "/var", "/tmp", "/System", "/Applications"]
    if any(_under(cwd, r) for r in system if r):
        return "system or app-data folder"
    if not (a.get("workspace") or {}).get("repos"):
        return "not a git project"
    return ""


def ensure_hook(a: dict, server: str, session: str, mode: str, decided: Dict[str, bool]) -> None:
    """Claude Code / Antigravity can only *receive* updates through a hook. Ask once per project folder."""
    a["hook_installed"] = False
    engine = a.get("engine")
    if engine not in HOOK_ENGINES or not a.get("cwd"):
        return
    try:
        import gitpulse_hook
    except Exception:
        return
    cwd = a["cwd"]
    key = f"{engine}|{os.path.normcase(cwd)}"
    if key not in decided:
        have = gitpulse_hook.installed_for(engine, cwd)
        why = "" if have else hook_skip_reason(a)
        if why:
            print(f"[-] {engine} in {cwd}: {why}, no hook needed (it still shows on the dashboard)")
            ok = False
        elif mode == "no":
            ok = have
        elif mode == "yes" or have:
            ok = True
        else:
            try:
                ans = input(f"[?] Let {engine} in {cwd} receive other agents' changes? "
                            f"Adds a hook to {HOOK_ENGINES[engine]} [Y/n] ").strip().lower()
            except (EOFError, OSError):
                ans = "n"
                print("    (no terminal to ask, skipped. Re-run with --install-hooks)")
            ok = ans in ("", "y", "yes")
        if ok:
            try:
                path = gitpulse_hook.install_for(engine, cwd, server, session)
                print(f"[+] hook ready: {path}")
                if not have:
                    print("    Antigravity loads it on the next prompt (the folder must be a trusted workspace)."
                          if engine == "Antigravity" else
                          "    Restart that Claude Code session (or type /hooks in it) so it loads the hook.")
            except Exception as e:  # PermissionError, bad JSON, read-only disk...
                print(f"[!] could not add hook in {cwd}: {e}")
                ok = False
        decided[key] = ok
    a["hook_installed"] = decided[key]


# ---------------------------------------------------------------------------------------------
# Host transfer. Every register reply lists the joined machines in join order ("succession").
# If the host stops answering, the first machine in that list starts its own GitPulse server with
# the same session code; everyone else looks for it at that machine's addresses and switches over.
# Hooks follow automatically because they read ~/.gitpulse/server_<session>.txt.
# ---------------------------------------------------------------------------------------------
GP_DIR = os.path.join(os.path.expanduser("~"), ".gitpulse")
HOST_DIR = os.path.join(GP_DIR, "host")
DOWN_AFTER = float(os.environ.get("GITPULSE_DOWN_AFTER", "15.0"))  # seconds without the host before the first successor takes over


def write_server_file(session: str, url: str) -> None:
    if not session:
        return
    try:
        os.makedirs(GP_DIR, exist_ok=True)
        with open(os.path.join(GP_DIR, f"server_{session}.txt"), "w", encoding="utf-8") as f:
            f.write(url)
    except OSError:
        pass


def own_addrs() -> List[str]:
    out = []
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.2)
        s.connect(("8.8.8.8", 80))
        out.append(s.getsockname()[0])
        s.close()
    except Exception:
        pass
    try:
        import psutil
        for addrs in psutil.net_if_addrs().values():
            for x in addrs:
                if x.family == socket.AF_INET and x.address.startswith("100.") and 64 <= int(x.address.split(".")[1]) <= 127:
                    out.append(x.address)
    except Exception:
        pass
    return [a for i, a in enumerate(out) if a and not a.startswith("127.") and a not in out[:i]]


def prefetch_bundle(server: str, session: str) -> bool:
    """Cache the server files now, while the host is still up, so this machine can take over later."""
    try:
        os.makedirs(HOST_DIR, exist_ok=True)
        for name in ("mesh_engine.py", "index.html", "agent_detect.py", "gitpulse_hook.py", "agent_client.py"):
            with urllib.request.urlopen(f"{server}/bundle/{name}?s={session}", timeout=15) as r:
                data = r.read()
            with open(os.path.join(HOST_DIR, name), "wb") as f:
                f.write(data)
        return True
    except Exception:
        return False


def is_alive(url: str, session: str, timeout: float = 2.0) -> bool:
    try:
        with urllib.request.urlopen(f"{url}/api/session?s={session}", timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8")).get("code") == session
    except Exception:
        return False


def _topic(session: str) -> str:
    import hashlib
    return "gitpulse-" + hashlib.sha256(f"gitpulse-rendezvous:{session}".encode()).hexdigest()[:24]


def rendezvous_publish(session: str, url: str) -> None:
    """Post the new host's public link to ntfy.sh so machines on other networks can find it."""
    try:
        req = urllib.request.Request(f"https://ntfy.sh/{_topic(session)}", data=url.encode("utf-8"), method="POST")
        urllib.request.urlopen(req, timeout=5).close()
    except Exception:
        pass


def rendezvous_lookup(session: str) -> List[str]:
    try:
        with urllib.request.urlopen(f"https://ntfy.sh/{_topic(session)}/json?poll=1&since=2h", timeout=5) as r:
            msgs = [json.loads(l) for l in r.read().decode("utf-8").splitlines() if l.strip()]
        return [m["message"].strip() for m in reversed(msgs) if m.get("event") == "message" and m.get("message", "").startswith("http")]
    except Exception:
        return []


def _deps_ok(py: str) -> bool:
    return subprocess.run([py, "-c", "import fastapi, uvicorn, pydantic, psutil"], capture_output=True).returncode == 0


def promote_to_host(session: str, port: int, old_host: str, public: bool) -> Optional[tuple]:
    """Start a GitPulse server here with the same session code. Returns (process, url) or None."""
    if not os.path.isfile(os.path.join(HOST_DIR, "mesh_engine.py")):
        print("[!] can't take over: server files were never cached")
        return None
    py = sys.executable
    if not _deps_ok(py):
        print("[*] installing the server parts (fastapi, uvicorn) into ~/.gitpulse ...")
        uv = os.path.join(GP_DIR, "uv.exe" if sys.platform == "win32" else "uv")
        cmd = ([uv, "pip", "install", "-q", "--python", py] if os.path.isfile(uv) else [py, "-m", "pip", "install", "-q"])
        subprocess.run(cmd + ["fastapi", "uvicorn", "pydantic", "psutil"])
        if not _deps_ok(py):
            print("[!] can't take over: could not install fastapi/uvicorn")
            return None
    env = {**os.environ, "GITPULSE_SESSION": session, "GITPULSE_PORT": str(port), "GITPULSE_TAKEOVER": old_host or "previous host"}
    log = open(os.path.join(HOST_DIR, "server.log"), "a", encoding="utf-8")
    proc = subprocess.Popen([py, "mesh_engine.py"], cwd=HOST_DIR, env=env, stdout=log, stderr=subprocess.STDOUT,
                            creationflags=0x08000000 if sys.platform == "win32" else 0)
    local = f"http://127.0.0.1:{port}"
    for _ in range(40):
        if proc.poll() is not None:
            print(f"[!] takeover server exited, see {os.path.join(HOST_DIR, 'server.log')}")
            return None
        if is_alive(local, session, 1.0):
            break
        time.sleep(0.5)
    else:
        proc.terminate()
        print("[!] takeover server did not start in 20 s")
        return None
    if public:
        try:
            urllib.request.urlopen(urllib.request.Request(f"{local}/api/tunnel", data=b"", method="POST"), timeout=5).close()
            for _ in range(60):
                with urllib.request.urlopen(f"{local}/api/tunnel", timeout=3) as r:
                    t = json.loads(r.read().decode("utf-8"))
                if t.get("url"):
                    rendezvous_publish(session, t["url"])
                    print(f"[+] public link for the others: {t['url']}")
                    break
                if t.get("status") == "error":
                    break
                time.sleep(1)
        except Exception:
            pass
    return proc, local


class HostTracker:
    """Remembers the succession list and decides when / where to fail over."""

    def __init__(self, server: str, session: str, rendezvous: bool, takeover: bool = True):
        self.server = server
        self.session = session
        self.rendezvous = rendezvous
        self.takeover = takeover
        self.public = server.startswith("https://")
        self.host = ""
        self.port = 8000
        self.succession: List[dict] = []
        self.down_since = 0.0
        self.last_try = 0.0
        self.cached = False
        self.me = os.environ.get("GITPULSE_MACHINE") or socket.gethostname()

    def seen(self, res: dict) -> None:
        self.down_since = 0.0
        self.host = res.get("host") or self.host
        self.port = int(res.get("port") or self.port)
        self.public = self.public or bool(res.get("tunnel"))
        if "succession" in res:
            self.succession = res["succession"]
        if not self.cached and self.session:
            self.cached = prefetch_bundle(self.server, self.session)

    def candidates(self) -> List[str]:
        urls = []
        for m in self.succession:
            if m["machine"] != self.me:
                urls += [f"http://{ip}:{self.port}" for ip in m.get("addrs") or []]
        if self.public and self.rendezvous:
            urls += rendezvous_lookup(self.session)
        return [u for i, u in enumerate(urls) if u not in urls[:i] and u.rstrip("/") != self.server]

    def next_in_line(self) -> List[str]:
        return [m["machine"] for m in self.succession]

    def failover(self) -> Optional[tuple]:
        """Called while the host is unreachable. Returns ("join", url) / ("host", (proc, url)) / None."""
        now = time.time()
        if not self.down_since:
            self.down_since = now
            line = self.next_in_line()
            print(f"[!] lost the host {self.host or self.server}. Next in line: {', '.join(line) or 'nobody'}")
            return None
        down = now - self.down_since
        if down < DOWN_AFTER or now - self.last_try < 6:
            return None
        self.last_try = now
        for url in self.candidates():
            if is_alive(url, self.session):
                return "join", url
        line = self.next_in_line()
        if self.takeover and self.me in line and down >= DOWN_AFTER * (line.index(self.me) + 1):
            print(f"[*] {self.host or 'the host'} is gone and this machine is next in line: taking over session {self.session}")
            got = promote_to_host(self.session, self.port, self.host, self.public and self.rendezvous)
            if got:
                return "host", got
        return None


def auto_watch_daemon(agent: GitPulseAgent, session: str, hooks: str = "ask", rendezvous: bool = True, takeover: bool = True):
    """Find coding agents on this machine, keep them registered, and stream their .py edits."""
    try:
        import agent_detect
        watcher = agent_detect.RepoWatcher()
    except Exception:
        agent_detect = None
        watcher = None
    host = os.environ.get("GITPULSE_MACHINE") or socket.gethostname()
    print(f"[*] GitPulse watcher on {host} -> {agent.server_url} (session {session or 'local'})")
    write_server_file(session, agent.server_url)
    tracker = HostTracker(agent.server_url, session, rendezvous, takeover)
    known: Dict[str, str] = {}
    decided: Dict[str, bool] = {}
    addrs = own_addrs()
    while True:
        try:
            agents = detect_here()
            reachable = False
            for a in agents:
                ensure_hook(a, agent.server_url, session, hooks, decided)
                res = _register_live(agent, {**a, "addrs": addrs}, session)
                if res.get("status") != "ok":
                    if res.get("unreachable"):
                        break
                    reachable = True
                    print(f"[!] register failed: {res.get('detail') or res.get('error')}")
                    if "session" in str(res.get("detail", "")):
                        return
                    continue
                reachable = True
                tracker.seen(res)
                if a["agent_id"] not in known:
                    known[a["agent_id"]] = res.get("label", a["engine"])
                    repos = ", ".join(r["name"] for r in a["workspace"].get("repos") or []) or "no git repo"
                    print(f"[+] {known[a['agent_id']]} (pid {a['pid']}) in {a['cwd']} [{repos}]")
            if not reachable and session:
                move = tracker.failover()
                if move and move[0] == "join":
                    agent.server_url = tracker.server = move[1]
                    tracker.down_since = 0.0
                    write_server_file(session, move[1])
                    print(f"[+] session {session} continues on {move[1]}")
                elif move and move[0] == "host":
                    proc, url = move[1]
                    write_server_file(session, url)
                    print(f"[+] this machine now hosts session {session}. Dashboard: {url}")
                    print("    It scans the agents here itself. Keep this window open; Ctrl+C ends the session.")
                    try:
                        proc.wait()
                    except KeyboardInterrupt:
                        proc.terminate()
                        print("\nStopped.")
                    return
                time.sleep(3.0)
                continue
            if watcher is not None:
                roots = sorted({r["root"] for a in agents for r in a["workspace"].get("repos") or [] if r.get("root")})
                for ch in watcher.poll(roots):
                    who = agent_detect.attribute_change(ch, agents)
                    if not who:
                        continue
                    res = agent._post("/api/agents/file_change", {
                        "agent_id": who["agent_id"], "session": session,
                        "repo": os.path.basename(ch["repo_root"]), "path": ch["rel"],
                        "old_code": ch["old"], "new_code": ch["new"],
                    })
                    kind = res.get("kind", res.get("status"))
                    if kind == "collision":
                        print(f"[!] COLLISION PRE-EMPTED on {ch['rel']}!")
                    else:
                        print(f"[^] {ch['rel']}: {kind} ({res.get('bytes_sent', 0)} B sent)")

                # Pull queued AST disk patches for this machine (leaves LLM hook inbox untouched for gitpulse_hook.py)
                try:
                    patch_res = agent._post("/api/agents/patches/pull", {
                        "session": session,
                        "machine": host,
                    })
                    items = patch_res.get("items") or []
                    if items:
                        import gitpulse_hook
                        cwd_roots = sorted({a["cwd"] for a in agents if a.get("cwd")} | set(roots))
                        for it in items:
                            if it.get("is_collision"):
                                print(f"[!] {it.get('warning', 'COLLISION WARNING')}")
                            for p in it.get("patches") or []:
                                rel = p.get("path") or p.get("file")
                                repo = p.get("repo") or ""
                                action = p.get("action", "upsert")
                                imports = p.get("imports")
                                created = p.get("created", False)
                                if not rel:
                                    continue
                                patched_paths = set()
                                for base in cwd_roots:
                                    candidates = [
                                        Path(base) / rel,
                                        Path(base) / repo / rel if repo else None,
                                        Path(base) / rel.split("/", 1)[-1] if "/" in rel else None,
                                    ]
                                    for target in candidates:
                                        if not target or target.suffix != ".py":
                                            continue
                                        resolved = os.path.normcase(str(target.resolve()))
                                        if resolved in patched_paths:
                                            continue
                                        if not target.is_file():
                                            if not created or action == "delete":
                                                continue
                                            repo_base = Path(base) / repo if (repo and (Path(base) / repo).is_dir()) else (Path(base) if (Path(base) / ".git").exists() else None)
                                            if not repo_base or not str(target.resolve()).startswith(str(repo_base.resolve())):
                                                continue
                                        patched_paths.add(resolved)
                                        old_txt = target.read_text(encoding="utf-8") if target.is_file() else ""
                                        new_txt = gitpulse_hook.patch_source_simple(
                                            old_txt,
                                            p["symbol"],
                                            p.get("code", ""),
                                            action=action,
                                            imports=imports,
                                        )
                                        if new_txt != old_txt:
                                            target.parent.mkdir(parents=True, exist_ok=True)
                                            target.write_text(new_txt, encoding="utf-8")
                                            watcher.mark_synced(str(target), new_txt)
                                            print(f"[+] Synced AST {action} '{p['symbol']}' into {target.name}")
                except Exception:
                    pass
            time.sleep(3.0)
        except KeyboardInterrupt:
            print("\nStopped.")
            break


def main():
    parser = argparse.ArgumentParser(description="GitPulse LAN Agent Client & Auto-Watcher")
    parser.add_argument("--server", default="auto", help="GitPulse mesh server URL (default: auto-discover via LAN UDP :9999)")
    parser.add_argument("--id", default="", help="Unique Agent ID")
    parser.add_argument("--role", default="Autonomous Agent", help="Role / Title of this agent")
    parser.add_argument("--module", default="services/worker.py", help="Target module path")
    parser.add_argument("--engine", default="", help="Agent Engine (Antigravity, Claude Code, Codex, Cursor)")
    parser.add_argument("--branch", default="", help="Git branch name")
    parser.add_argument("--session", default="", help="Session code from the host join link")
    parser.add_argument("--gossip", help="One-shot broadcast an idea/gossip and exit")
    parser.add_argument("--watch", action="store_true", help="Auto-detect local agents + git branch and continuously stream updates")
    parser.add_argument("--install-hooks", action="store_true", help="Add the receive hook to every Claude Code project without asking")
    parser.add_argument("--no-hooks", action="store_true", help="Never add the receive hook")
    parser.add_argument("--no-rendezvous", action="store_true",
                        help="On a host transfer over the internet, don't use ntfy.sh to share the new host's link")
    parser.add_argument("--no-takeover", action="store_true",
                        help="If the host goes away, follow its new link but never host the session from this machine")
    parser.add_argument("--machine", default=os.environ.get("GITPULSE_MACHINE", ""), help="Override machine name (for simulation)")
    parser.add_argument("--interactive", "-i", action="store_true", help="Launch interactive CLI REPL")

    args = parser.parse_args()
    if args.machine:
        os.environ["GITPULSE_MACHINE"] = args.machine
    mach = os.environ.get("GITPULSE_MACHINE") or socket.gethostname()
    server_url = discover_lan_host() if args.server == "auto" else args.server
    engine = args.engine or "Custom"
    branch = args.branch or "main"
    agent_id = args.id or f"agent-{mach.lower()[:8]}-{branch.replace('/', '-')[:8]}"

    agent = GitPulseAgent(
        server_url=server_url,
        agent_id=agent_id,
        role=args.role,
        target_module=args.module,
        session=args.session,
    )

    if args.watch:
        auto_watch_daemon(agent, args.session, "yes" if args.install_hooks else "no" if args.no_hooks else "ask",
                          rendezvous=not args.no_rendezvous, takeover=not args.no_takeover)
    elif args.gossip:
        agent.register(engine=engine, machine=mach, branch=branch)
        res = agent.send_idea(intent=args.gossip, gossip_message=args.gossip)
        print(f"Gossip sent: {res}")
    else:
        agent.register(engine=engine, machine=mach, branch=branch)
        interactive_repl(agent)


if __name__ == "__main__":
    main()
