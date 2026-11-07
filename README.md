# GitPulse // Agent-Native AST Version Control & Gossip Mesh

**GitPulse** is a peer-to-peer Semantic Staging, Speculative Execution, and AST Gossip Layer that sits between autonomous coding agents and GitHub.

## Core Capabilities

1. **Tree-Level AST Diffing & Locality Filtering**: Parses Python AST functions and classes into separate `signature_hash` and `body_hash` digests. Private body-only edits are suppressed locally (`0 bytes` broadcast to peers), while public contract changes broadcast lightweight Semantic Intent Packets.
2. **Causal Vector Clocks & Offline-First Speculation**: When network links between agent teams drop, partitioned nodes continue mutating local AST state and queue outbound contract deltas in a causal CRDT outbox, automatically reconciling and adapting call sites on reconnect.
3. **Speculative Branching & Automated Unit-Test Arbiter**: Forks 3 parallel candidate AST branches, runs real security and latency assertions in isolated sandboxes, merges the fastest passing AST node, and prunes failing branches from the DAG.
4. **Content-Addressed AST Commit DAG & Semantic Rollback**: Every mutation is recorded in an append-only AST DAG with one-click rollback across all services.
5. **Epoch-Based Atomic Git Flush**: Compacts noisy multi-agent micro-mutations into a single verified atomic Git commit.

## Quickstart

```bash
pip install -r requirements.txt
python mesh_engine.py --check   # Run built-in verification suite
python mesh_engine.py           # Launch live server + dashboard on http://localhost:8000
```

## Live mode (default)

`python mesh_engine.py` auto-detects coding agents running on this machine (Claude Code, Antigravity, Codex, Aider...)
by process working directory, finds the git repos + GitHub remotes they work in, and watches their `.py` edits:

- body-only edit: stays private, 0 bytes sent
- signature / class / constant added, changed or removed: that "contract" is pushed to every other agent's inbox

Dashboard shows agents, repos, the network and a feed of exactly what got passed.

## Inviting people

Click **Invite people** in the dashboard. It gives a join link (`http://<lan-ip>:8000/join/<code>`) with
one-line installers:

```powershell
irm http://<host>:8000/join/<code>/ps1 | iex      # Windows
```
```bash
curl -fsSL http://<host>:8000/join/<code>/sh | sh  # macOS / Linux
```

Off the same network: use Tailscale, or `cloudflared tunnel --url http://localhost:8000` and share that URL + `/join/<code>`.
Note: anyone with the link can see agent session titles and last prompts.

| Env / flag | Effect |
|---|---|
| `--demo` / `GITPULSE_DEMO=1` | scripted 3-agent demo instead of live detection |
| `GITPULSE_PORT` | server port (default 8000) |
| `GITPULSE_SESSION` | fixed session code (default: random, reused for 12 h across host restarts) |
| `GITPULSE_NEW_SESSION=1` | ignore the saved session and make a new code |
| `GITPULSE_NO_RENDEZVOUS=1` | host doesn't post new tunnel links to ntfy.sh, so internet peers can't follow them automatically |
| `agent_client.py --no-takeover` | follow the host's new link, but never host the session from this machine |
