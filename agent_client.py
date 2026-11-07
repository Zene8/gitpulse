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
import sys
import time
import urllib.error
import urllib.request
from typing import Dict, List, Optional


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
    ):
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
        except urllib.error.URLError as e:
            return {"status": "error", "error": str(e)}

    def _get(self, path: str) -> dict:
        url = f"{self.server_url}{path}"
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.URLError as e:
            return {"status": "error", "error": str(e)}

    def register(self, initial_code: Optional[str] = None) -> dict:
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


def main():
    parser = argparse.ArgumentParser(description="GitPulse LAN Agent Client")
    parser.add_argument("--server", default="auto", help="GitPulse mesh server URL (default: auto-discover via LAN UDP :9999)")
    parser.add_argument("--id", default=f"agent-{os.getpid() % 1000}", help="Unique Agent ID")
    parser.add_argument("--role", default="Worker Node", help="Role / Title of this agent")
    parser.add_argument("--module", default="services/worker.py", help="Target module path")
    parser.add_argument("--gossip", help="One-shot broadcast an idea/gossip and exit")
    parser.add_argument("--interactive", "-i", action="store_true", help="Launch interactive CLI REPL")

    args = parser.parse_args()
    server_url = discover_lan_host() if args.server == "auto" else args.server
    agent = GitPulseAgent(
        server_url=server_url,
        agent_id=args.id,
        role=args.role,
        target_module=args.module,
    )

    if args.gossip:
        agent.register()
        res = agent.send_idea(intent=args.gossip, gossip_message=args.gossip)
        print(f"Gossip sent: {res}")
    else:
        interactive_repl(agent)


if __name__ == "__main__":
    main()
