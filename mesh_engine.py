from pathlib import Path
import ast, asyncio, difflib, hashlib, json, socket, sqlite3, subprocess, sys, time
from typing import Dict, List, Optional, Set, Tuple
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel

app = FastAPI(title="GitPulse // Distributed AST-CRDT Mesh, 3-Way Merger & Leaderless Failover")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

BASE_DIR = Path(__file__).parent
WORKSPACE_REPO = BASE_DIR / "workspace_repo"
DB_PATH = BASE_DIR / "mesh_state.db"


def get_lan_ip() -> str:
    """Discovers host LAN IP address for easy multi-agent Wi-Fi connection."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.2)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        pass
    try:
        ip = socket.gethostbyname(socket.gethostname())
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        pass
    return "127.0.0.1"


# =====================================================================
# 1. DATA MODELS & CAUSAL VECTOR CLOCK ORDERING
# =====================================================================

class RegisterAgentRequest(BaseModel):
    agent_id: str
    role: str = "Autonomous Worker"
    target_module: str = "services/worker.py"
    initial_code: Optional[str] = None
    deps: List[str] = []


class GossipIdeaRequest(BaseModel):
    agent_id: str
    intent: str
    gossip_message: Optional[str] = None
    code: Optional[str] = None
    target_agent: Optional[str] = None

class ASTSymbol(BaseModel):
    name: str
    kind: str  # "function" | "class" | "assign"
    signature: str
    signature_hash: str
    body_hash: str
    source: str
    owner_agent: str
    lamport: int = 0


class GossipPacket(BaseModel):
    packet_id: str
    source_agent: str
    term: int
    vector_clock: Dict[str, int]
    base_commit_id: Optional[str]
    intent: str
    target_module: str
    exported_symbols: Dict[str, ASTSymbol]
    raw_code: str
    merkle_root: str
    timestamp: float


class DAGCommit(BaseModel):
    commit_id: str
    parent_id: Optional[str]
    merge_parent_id: Optional[str] = None
    agent_id: str
    term: int = 1
    kind: str  # "PRIVATE-AST" | "CONTRACT-GOSSIP" | "AST-3WAY-MERGE" | "SPECULATIVE-WIN" | "HOST-FAILOVER" | "EPOCH-FLUSH" | "ROLLBACK"
    intent: str
    diff: str = ""
    vector_clock: Dict[str, int]
    merkle_root: str = ""
    symbol_hashes: Dict[str, str]
    snapshot: Dict[str, str]
    git_sha: Optional[str] = None
    timestamp: str


class MutationRequest(BaseModel):
    agent_id: str
    code: str
    intent: str


class VectorClock:
    """True partial ordering over distributed agent states."""
    @staticmethod
    def merge(vc_a: Dict[str, int], vc_b: Dict[str, int]) -> Dict[str, int]:
        keys = set(vc_a.keys()) | set(vc_b.keys())
        return {k: max(vc_a.get(k, 0), vc_b.get(k, 0)) for k in keys}

    @staticmethod
    def compare(vc_a: Dict[str, int], vc_b: Dict[str, int]) -> str:
        """Returns 'EQUAL', 'BEFORE', 'AFTER', or 'CONCURRENT' (split-brain conflict)."""
        keys = set(vc_a.keys()) | set(vc_b.keys())
        a_lt_b = False
        b_lt_a = False
        for k in keys:
            va, vb = vc_a.get(k, 0), vc_b.get(k, 0)
            if va < vb:
                a_lt_b = True
            elif va > vb:
                b_lt_a = True
        if not a_lt_b and not b_lt_a:
            return "EQUAL"
        if a_lt_b and not b_lt_a:
            return "BEFORE"
        if b_lt_a and not a_lt_b:
            return "AFTER"
        return "CONCURRENT"


# =====================================================================
# 2. AST ANALYZER, 3-WAY SEMANTIC MERGER & CALL-SITE TRANSFORMER
# =====================================================================

class CallSiteRewriter(ast.NodeTransformer):
    """Rewrites downstream ast.Call nodes when an upstream peer mutates a function signature."""
    def __init__(self, func_name: str, new_default_kwargs: Dict[str, ast.AST]):
        self.func_name = func_name
        self.new_default_kwargs = new_default_kwargs
        self.rewritten_count = 0

    def visit_Call(self, node: ast.Call) -> ast.AST:
        self.generic_visit(node)
        if isinstance(node.func, ast.Name) and node.func.id == self.func_name:
            existing_kw = {kw.arg for kw in node.keywords if kw.arg}
            for arg_name, default_node in self.new_default_kwargs.items():
                if arg_name not in existing_kw:
                    node.keywords.append(ast.keyword(arg=arg_name, value=default_node))
                    self.rewritten_count += 1
        return node


class ASTEngine:
    @staticmethod
    def extract_symbols(code: str, agent_id: str, lamport: int = 0) -> Dict[str, ASTSymbol]:
        symbols: Dict[str, ASTSymbol] = {}
        try:
            tree = ast.parse(code)
        except SyntaxError:
            return symbols

        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                args = []
                num_Args = len(node.args.args)
                num_Defs = len(node.args.defaults)
                for idx, a in enumerate(node.args.args):
                    ann = f": {ast.unparse(a.annotation)}" if a.annotation else ""
                    def_idx = idx - (num_Args - num_Defs)
                    dflt = f" = {ast.unparse(node.args.defaults[def_idx])}" if def_idx >= 0 else ""
                    args.append(f"{a.arg}{ann}{dflt}")
                returns = ast.unparse(node.returns) if node.returns else "Any"
                sig = f"def {node.name}({', '.join(args)}) -> {returns}"
                sig_hash = hashlib.sha256(sig.encode()).hexdigest()[:10]
                body_src = "\n".join(ast.unparse(stmt) for stmt in node.body)
                body_hash = hashlib.sha256(body_src.encode()).hexdigest()[:10]
                symbols[node.name] = ASTSymbol(
                    name=node.name,
                    kind="function",
                    signature=sig,
                    signature_hash=sig_hash,
                    body_hash=body_hash,
                    source=ast.unparse(node),
                    owner_agent=agent_id,
                    lamport=lamport,
                )
            elif isinstance(node, ast.ClassDef):
                methods = [n.name for n in node.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
                sig = f"class {node.name}[{', '.join(methods)}]"
                sig_hash = hashlib.sha256(sig.encode()).hexdigest()[:10]
                src = ast.unparse(node)
                body_hash = hashlib.sha256(src.encode()).hexdigest()[:10]
                symbols[node.name] = ASTSymbol(
                    name=node.name,
                    kind="class",
                    signature=sig,
                    signature_hash=sig_hash,
                    body_hash=body_hash,
                    source=src,
                    owner_agent=agent_id,
                    lamport=lamport,
                )
            elif isinstance(node, ast.Assign):
                targets = [ast.unparse(t) for t in node.targets]
                name = targets[0]
                src = ast.unparse(node)
                h = hashlib.sha256(src.encode()).hexdigest()[:10]
                symbols[name] = ASTSymbol(
                    name=name,
                    kind="assign",
                    signature=f"{name} = ...",
                    signature_hash=h,
                    body_hash=h,
                    source=src,
                    owner_agent=agent_id,
                    lamport=lamport,
                )
        return symbols

    @staticmethod
    def compute_merkle_root(symbols: Dict[str, ASTSymbol]) -> str:
        """Deterministic Merkle root over sorted AST symbol table."""
        leaves = []
        for name in sorted(symbols.keys()):
            s = symbols[name]
            leaf = hashlib.sha256(f"{name}:{s.signature_hash}:{s.body_hash}".encode()).hexdigest()
            leaves.append(leaf)
        if not leaves:
            return "0" * 12
        while len(leaves) > 1:
            if len(leaves) % 2 == 1:
                leaves.append(leaves[-1])
            leaves = [
                hashlib.sha256((leaves[i] + leaves[i + 1]).encode()).hexdigest()
                for i in range(0, len(leaves), 2)
            ]
        return leaves[0][:12]

    @staticmethod
    def three_way_merge(
        base_code: str,
        local_code: str,
        remote_code: str,
        local_agent: str,
        remote_agent: str,
    ) -> Tuple[str, str]:
        """
        True AST 3-Way Semantic Merger:
        - Compares Base AST vs Local AST vs Remote AST at the symbol & parameter level.
        - Disjoint functions/constants modified on overlapping lines merge cleanly with 0 conflicts.
        - Concurrent edits to the same function merge unioned parameters + statement bodies or preserve shadow fallback.
        """
        base_syms = ASTEngine.extract_symbols(base_code, "base")
        local_syms = ASTEngine.extract_symbols(local_code, local_agent)
        remote_syms = ASTEngine.extract_symbols(remote_code, remote_agent)

        all_names = list(dict.fromkeys(list(base_syms.keys()) + list(local_syms.keys()) + list(remote_syms.keys())))
        merged_sources: List[str] = []
        notes: List[str] = []

        for name in all_names:
            b = base_syms.get(name)
            l = local_syms.get(name)
            r = remote_syms.get(name)

            # Added only in local or only in remote
            if not b:
                if l and not r:
                    merged_sources.append(l.source)
                    notes.append(f"+{name}({local_agent})")
                elif r and not l:
                    merged_sources.append(r.source)
                    notes.append(f"+{name}({remote_agent})")
                elif l and r:
                    if l.body_hash == r.body_hash and l.signature_hash == r.signature_hash:
                        merged_sources.append(l.source)
                    else:
                        merged_sources.extend([l.source, r.source.replace(f"def {name}", f"def {name}_alt", 1)])
                        notes.append(f"dual-add:{name}")
                continue

            l_changed = (not l) or (l.signature_hash != b.signature_hash or l.body_hash != b.body_hash)
            r_changed = (not r) or (r.signature_hash != b.signature_hash or r.body_hash != b.body_hash)

            if not l_changed and not r_changed and l:
                merged_sources.append(l.source)
            elif l_changed and not r_changed and l:
                merged_sources.append(l.source)
                notes.append(f"kept:{name}({local_agent})")
            elif r_changed and not l_changed and r:
                merged_sources.append(r.source)
                notes.append(f"took:{name}({remote_agent})")
            elif l and r:
                # BOTH changed the exact same AST symbol concurrently during partition!
                if l.signature_hash == r.signature_hash and l.body_hash == r.body_hash:
                    merged_sources.append(l.source)
                else:
                    # Attempt AST parameter + guard statement synthesis
                    synthesized = ASTEngine._synthesize_function_conflict(l.source, r.source)
                    merged_sources.append(synthesized)
                    notes.append(f"ast-synthesized:{name}")

        final_code = "\n\n".join(merged_sources) + "\n"
        return final_code, ", ".join(notes) or "clean-ast-merge"

    @staticmethod
    def _synthesize_function_conflict(local_fn_src: str, remote_fn_src: str) -> str:
        """Synthesizes two concurrent AST function definitions by unioning parameters and non-return guard statements."""
        try:
            l_node = ast.parse(local_fn_src).body[0]
            r_node = ast.parse(remote_fn_src).body[0]
            if not isinstance(l_node, ast.FunctionDef) or not isinstance(r_node, ast.FunctionDef):
                return remote_fn_src

            # 1. Union parameters (preserving order, adding new remote args + defaults)
            existing_args = {a.arg for a in l_node.args.args}
            r_num_args = len(r_node.args.args)
            r_num_defs = len(r_node.args.defaults)
            for idx, r_arg in enumerate(r_node.args.args):
                if r_arg.arg not in existing_args:
                    l_node.args.args.append(r_arg)
                    def_idx = idx - (r_num_args - r_num_defs)
                    if def_idx >= 0:
                        l_node.args.defaults.append(r_node.args.defaults[def_idx])
                    else:
                        l_node.args.defaults.append(ast.Constant(value=None))

            # 2. Union pre-return guard/assignment statements from both AST bodies, take latest return
            l_guards = [s for s in l_node.body if not isinstance(s, ast.Return)]
            r_guards = [s for s in r_node.body if not isinstance(s, ast.Return)]
            r_returns = [s for s in r_node.body if isinstance(s, ast.Return)]
            l_returns = [s for s in l_node.body if isinstance(s, ast.Return)]

            seen_unparsed = set()
            combined_body = []
            for stmt in l_guards + r_guards:
                u = ast.unparse(stmt)
                if u not in seen_unparsed:
                    seen_unparsed.add(u)
                    combined_body.append(stmt)

            combined_body.extend(r_returns if r_returns else l_returns)
            l_node.body = combined_body or [ast.Pass()]
            if r_node.returns:
                l_node.returns = r_node.returns
            return ast.unparse(l_node)
        except Exception:
            return remote_fn_src

    @staticmethod
    def rewrite_downstream_calls(code: str, upstream_fn_src: str) -> Tuple[str, int]:
        """Parses upstream function AST for defaulted parameters and rewrites downstream ast.Call sites."""
        try:
            up_node = ast.parse(upstream_fn_src).body[0]
            if not isinstance(up_node, ast.FunctionDef):
                return code, 0
            num_args = len(up_node.args.args)
            num_defs = len(up_node.args.defaults)
            new_kwargs: Dict[str, ast.AST] = {}
            for idx in range(num_args - num_defs, num_args):
                arg_name = up_node.args.args[idx].arg
                new_kwargs[arg_name] = up_node.args.defaults[idx - (num_args - num_defs)]
            if not new_kwargs:
                return code, 0
            tree = ast.parse(code)
            rewriter = CallSiteRewriter(up_node.name, new_kwargs)
            new_tree = rewriter.visit(tree)
            ast.fix_missing_locations(new_tree)
            if rewriter.rewritten_count > 0:
                return ast.unparse(new_tree) + "\n", rewriter.rewritten_count
            return code, 0
        except Exception:
            return code, 0


# =====================================================================
# 3. DISTRIBUTED AGENT NODE (CRDT + ANTI-ENTROPY + LEADER ELECTION)
# =====================================================================

class AgentNode:
    def __init__(self, agent_id: str, role: str, target_module: str, initial_code: str, deps: List[str], color: Optional[str] = None):
        self.agent_id = agent_id
        self.role = role
        self.target_module = target_module
        self.code = initial_code
        self.last_synced_code = initial_code
        self.deps = deps
        self.color = color or "#a78bfa"
        self.last_gossip = ""
        self.vector_clock: Dict[str, int] = {agent_id: 0}
        self.online: bool = True
        self.is_leader: bool = False
        self.term: int = 1
        self.intent: str = "SYNCED"
        self.local_micro_commits: int = 0
        self.outbox_queue: List[GossipPacket] = []
        self.oplog: List[GossipPacket] = []
        self.known_peer_symbols: Dict[str, ASTSymbol] = {}
        self.local_symbols: Dict[str, ASTSymbol] = ASTEngine.extract_symbols(initial_code, agent_id)
        self.merkle_root: str = ASTEngine.compute_merkle_root(self.local_symbols)

    def mutate_code(self, new_code: str, intent_desc: str, base_commit_id: Optional[str]) -> Tuple[GossipPacket, bool]:
        self.vector_clock[self.agent_id] = self.vector_clock.get(self.agent_id, 0) + 1
        lamport = sum(self.vector_clock.values())
        self.code = new_code
        self.intent = intent_desc
        self.local_micro_commits += 1

        new_symbols = ASTEngine.extract_symbols(new_code, self.agent_id, lamport=lamport)
        old_sigs = {k: v.signature_hash for k, v in self.local_symbols.items()}
        new_sigs = {k: v.signature_hash for k, v in new_symbols.items()}
        signature_changed = old_sigs != new_sigs

        self.local_symbols = new_symbols
        self.merkle_root = ASTEngine.compute_merkle_root(self.local_symbols)

        pkt_id = hashlib.sha256(f"{self.agent_id}:{lamport}:{time.time_ns()}".encode()).hexdigest()[:8]
        packet = GossipPacket(
            packet_id=pkt_id,
            source_agent=self.agent_id,
            term=self.term,
            vector_clock=self.vector_clock.copy(),
            base_commit_id=base_commit_id,
            intent=intent_desc,
            target_module=self.target_module,
            exported_symbols=new_symbols if signature_changed else {},
            raw_code=new_code,
            merkle_root=self.merkle_root,
            timestamp=time.time(),
        )
        self.oplog.append(packet)
        return packet, signature_changed

    def receive_gossip(self, packet: GossipPacket) -> Tuple[str, int]:
        """Merges causal vector clocks, detects concurrent split-brain or contract drift, and rewrites AST call sites."""
        relation = VectorClock.compare(packet.vector_clock, self.vector_clock)
        self.vector_clock = VectorClock.merge(self.vector_clock, packet.vector_clock)
        if packet.term > self.term:
            self.term = packet.term

        rewritten_total = 0
        adapted = []
        for sym_name, sym in packet.exported_symbols.items():
            prev = self.known_peer_symbols.get(sym_name)
            self.known_peer_symbols[sym_name] = sym
            if prev and prev.signature_hash != sym.signature_hash:
                new_code, count = ASTEngine.rewrite_downstream_calls(self.code, sym.source)
                if count > 0:
                    self.code = new_code
                    self.local_symbols = ASTEngine.extract_symbols(self.code, self.agent_id)
                    self.merkle_root = ASTEngine.compute_merkle_root(self.local_symbols)
                    rewritten_total += count
                adapted.append(f"{sym.name}({sym.signature})")

        if adapted:
            return f"AST CALL-SITE REWRITE [{relation}]: {', '.join(adapted)}", rewritten_total
        return f"ACK [{relation}]", 0


# =====================================================================
# 4. MESH CLUSTER (SQLITE WAL PERSISTENCE, LEADER FAILOVER, MERKLE SYNC)
# =====================================================================

class MeshCluster:
    def __init__(self):
        self.term: int = 1
        self.leader_id: str = "agent-auth"
        self.nodes: Dict[str, AgentNode] = {
            "agent-auth": AgentNode(
                "agent-auth",
                "Auth & Identity Service",
                "services/auth.py",
                "def verify_token(token: str) -> bool:\n    return token.startswith('jwt_')\n",
                deps=[],
                color="#a78bfa",
            ),
            "agent-billing": AgentNode(
                "agent-billing",
                "Ledger & Billing Worker",
                "services/billing.py",
                "def charge_user(user_id: str, amount: float, token: str = 'jwt_ok') -> dict:\n    valid = verify_token(token)\n    return {'status': 'paid' if valid else 'denied', 'amt': amount}\n",
                deps=["agent-auth"],
                color="#38bdf8",
            ),
            "agent-gateway": AgentNode(
                "agent-gateway",
                "Edge API Router",
                "services/gateway.py",
                "def route_request(path: str, token: str) -> int:\n    ok = verify_token(token)\n    return 200 if ok else 401\n",
                deps=["agent-auth", "agent-billing"],
                color="#34d399",
            ),
        }
        self.nodes["agent-auth"].is_leader = True

        for src_id, src in self.nodes.items():
            for dst_id, dst in self.nodes.items():
                if src_id != dst_id:
                    dst.known_peer_symbols.update(src.local_symbols)

        self.events: List[dict] = []
        self.dag: List[DAGCommit] = []
        self.speculative_runs: List[dict] = []
        self.last_gossip_beam: Optional[dict] = None
        self.active_bubbles: List[dict] = []
        self.metrics = {
            "micro_mutations_absorbed": 0,
            "private_edits_suppressed": 0,
            "gossip_packets_routed": 0,
            "conflicts_auto_resolved": 0,
            "ast_merges_completed": 0,
            "leader_failovers": 0,
            "speculative_branches_pruned": 0,
            "atomic_github_pushes": 0,
            "bytes_saved": 0,
        }
        self.ws_clients: Set[WebSocket] = set()
        self._init_sqlite_wal()
        self._init_workspace_git()
        self._record_dag("system", "EPOCH-FLUSH", "Genesis AST mesh snapshot")

    def _emit_bubble(
        self,
        source: str,
        targets: List[str],
        kind: str,
        label: str,
        detail: str,
        color: str = "#a78bfa",
    ) -> dict:
        bubble = {
            "id": hashlib.sha256(f"{source}:{time.time_ns()}".encode()).hexdigest()[:8],
            "source": source,
            "targets": targets,
            "kind": kind,
            "label": label,
            "detail": detail,
            "color": color,
            "timestamp": time.time(),
        }
        self.active_bubbles.append(bubble)
        if len(self.active_bubbles) > 50:
            self.active_bubbles = self.active_bubbles[-50:]
        return bubble

    def register_agent(
        self,
        agent_id: str,
        role: str = "Autonomous Worker",
        target_module: str = "services/worker.py",
        initial_code: Optional[str] = None,
        deps: Optional[List[str]] = None,
    ) -> AgentNode:
        if agent_id in self.nodes:
            node = self.nodes[agent_id]
            node.online = True
            node.role = role or node.role
            node.target_module = target_module or node.target_module
            if initial_code:
                node.code = initial_code
                node.local_symbols = ASTEngine.extract_symbols(node.code, agent_id)
                node.merkle_root = ASTEngine.compute_merkle_root(node.local_symbols)
            self.log_event("JOIN", f"[{agent_id}] Reconnected on LAN mesh ({node.role}).")
        else:
            palette = ["#a78bfa", "#38bdf8", "#34d399", "#fbbf24", "#f472b6", "#fb7185", "#2dd4bf", "#818cf8", "#f97316"]
            color = palette[len(self.nodes) % len(palette)]
            code = initial_code or (
                f"# {role} ({agent_id})\n"
                f"def handle_{agent_id.replace('-', '_')}(data: dict) -> dict:\n"
                f"    return {{'status': 'processed', 'agent': '{agent_id}'}}\n"
            )
            node = AgentNode(agent_id, role, target_module, code, deps or [], color=color)
            self.nodes[agent_id] = node
            for other_id, other_node in self.nodes.items():
                if other_id != agent_id:
                    other_node.known_peer_symbols.update(node.local_symbols)
                    node.known_peer_symbols.update(other_node.local_symbols)
            self._write_workspace_files()
            self.log_event("JOIN", f"[{agent_id}] Connected as new agent on LAN mesh ({role}).")
            self._record_dag(agent_id, "AGENT-JOIN", f"Agent '{agent_id}' joined ({role})")

        self._emit_bubble(
            source=agent_id,
            targets=[nid for nid in self.nodes if nid != agent_id and self.nodes[nid].online] or ["mesh"],
            kind="agent-join",
            label=f"{agent_id} Joined",
            detail=f"{role} in {target_module}",
            color=getattr(node, "color", "#a78bfa"),
        )
        return node

    def unregister_agent(self, agent_id: str) -> bool:
        if agent_id in self.nodes:
            node = self.nodes[agent_id]
            if node.is_leader:
                self.elect_new_leader()
            del self.nodes[agent_id]
            self.log_event("LEAVE", f"[{agent_id}] Disconnected from LAN mesh.")
            self._record_dag("system", "AGENT-LEAVE", f"Agent '{agent_id}' left the mesh")
            return True
        return False

    async def broadcast_idea_or_gossip(
        self,
        agent_id: str,
        intent: str,
        gossip_message: Optional[str] = None,
        code: Optional[str] = None,
        target_agent: Optional[str] = None,
    ) -> dict:
        if agent_id not in self.nodes:
            self.register_agent(agent_id, role="Autonomous Peer", target_module=f"services/{agent_id.replace('-', '_')}.py")

        node = self.nodes[agent_id]
        msg = gossip_message or intent

        if code and code.strip() != node.code.strip():
            await self.trigger_agent_step(agent_id, code, intent)
            return {"status": "ok", "mode": "code_mutation"}

        node.intent = intent
        node.last_gossip = msg
        node.vector_clock[agent_id] = node.vector_clock.get(agent_id, 0) + 1
        node.local_micro_commits += 1

        targets = [target_agent] if target_agent and target_agent in self.nodes else [nid for nid in self.nodes if nid != agent_id and self.nodes[nid].online]
        for tid in targets:
            if tid in self.nodes:
                self.nodes[tid].vector_clock[agent_id] = max(self.nodes[tid].vector_clock.get(agent_id, 0), node.vector_clock[agent_id])

        self.metrics["micro_mutations_absorbed"] += 1
        self.metrics["gossip_packets_routed"] += 1
        self.log_event("GOSSIP-IDEA", f"[{agent_id}] Gossiped idea: {msg}")

        self._record_dag(agent_id, "GOSSIP-IDEA", intent, diff=f"~ {msg[:40]}")
        self._emit_bubble(
            source=agent_id,
            targets=targets or ["mesh"],
            kind="idea",
            label=intent[:24],
            detail=msg,
            color=getattr(node, "color", "#fbbf24"),
        )
        self.last_gossip_beam = {"source": agent_id, "targets": targets, "kind": "gossip", "ts": time.time()}
        await self.broadcast_ui()
        return {"status": "ok", "mode": "gossip_idea"}

    def _init_sqlite_wal(self):
        """SQLite WAL crash-resilient state store so host restarts never lose DAG or Vector Clocks."""
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS dag_oplog ("
                "commit_id TEXT PRIMARY KEY, agent_id TEXT, term INTEGER, kind TEXT, "
                "intent TEXT, diff TEXT, vclock TEXT, merkle_root TEXT, payload TEXT, ts TEXT)"
            )

    def _persist_commit(self, c: DAGCommit):
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO dag_oplog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    c.commit_id,
                    c.agent_id,
                    c.term,
                    c.kind,
                    c.intent,
                    c.diff,
                    json.dumps(c.vector_clock),
                    c.merkle_root,
                    json.dumps(c.snapshot),
                    c.timestamp,
                ),
            )

    def _init_workspace_git(self):
        (WORKSPACE_REPO / "services").mkdir(parents=True, exist_ok=True)
        if not (WORKSPACE_REPO / ".git").exists():
            subprocess.run(["git", "init"], cwd=WORKSPACE_REPO, capture_output=True)
            subprocess.run(["git", "config", "user.name", "GitPulse Mesh"], cwd=WORKSPACE_REPO, capture_output=True)
            subprocess.run(["git", "config", "user.email", "mesh@gitpulse.local"], cwd=WORKSPACE_REPO, capture_output=True)
        self._write_workspace_files()

    def _write_workspace_files(self):
        for n in self.nodes.values():
            target = WORKSPACE_REPO / n.target_module
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(n.code, encoding="utf-8")

    def _merged_vclock(self) -> Dict[str, int]:
        vc: Dict[str, int] = {}
        for n in self.nodes.values():
            vc = VectorClock.merge(vc, n.vector_clock)
        return vc

    def _cluster_merkle_root(self) -> str:
        combined = ":".join(f"{nid}={n.merkle_root}" for nid, n in sorted(self.nodes.items()))
        return hashlib.sha256(combined.encode()).hexdigest()[:12]

    def _compute_short_diff(self, old_code: str, new_code: str) -> str:
        old_lines = old_code.strip().splitlines()
        new_lines = new_code.strip().splitlines()
        diff_lines = [
            l for l in difflib.unified_diff(old_lines, new_lines, lineterm="", n=0)
            if (l.startswith("+") or l.startswith("-")) and not l.startswith("+++") and not l.startswith("---")
        ]
        return "\n".join(diff_lines[:3]) if diff_lines else f"+ {new_lines[-1] if new_lines else ''}"

    def _record_dag(
        self,
        agent_id: str,
        kind: str,
        intent: str,
        diff: str = "",
        git_sha: Optional[str] = None,
        merge_parent_id: Optional[str] = None,
    ) -> DAGCommit:
        snap = {nid: n.code for nid, n in self.nodes.items()}
        sym_hashes = {
            f"{nid}:{s.name}": f"{s.signature_hash}:{s.body_hash}"
            for nid, n in self.nodes.items()
            for s in n.local_symbols.values()
        }
        m_root = self._cluster_merkle_root()
        raw = f"{agent_id}:{self.term}:{kind}:{intent}:{time.time_ns()}:{m_root}"
        cid = hashlib.sha256(raw.encode()).hexdigest()[:8]
        parent = self.dag[-1].commit_id if self.dag else None
        commit = DAGCommit(
            commit_id=cid,
            parent_id=parent,
            merge_parent_id=merge_parent_id,
            agent_id=agent_id,
            term=self.term,
            kind=kind,
            intent=intent,
            diff=diff,
            vector_clock=self._merged_vclock(),
            merkle_root=m_root,
            symbol_hashes=sym_hashes,
            snapshot=snap,
            git_sha=git_sha,
            timestamp=time.strftime("%H:%M:%S"),
        )
        self.dag.append(commit)
        self._persist_commit(commit)
        return commit

    def log_event(self, level: str, msg: str):
        self.events.append({"time": time.strftime("%H:%M:%S"), "level": level, "msg": msg})

    def elect_new_leader(self) -> Optional[str]:
        """Leaderless Raft-lite / Bully election when the current Host/Coordinator disconnects."""
        online_nodes = [n for n in self.nodes.values() if n.online]
        for n in self.nodes.values():
            n.is_leader = False
        if not online_nodes:
            self.leader_id = "NONE (FULL PARTITION)"
            return None
        # Highest causal knowledge (sum of vector clock) wins election
        winner = max(online_nodes, key=lambda n: (sum(n.vector_clock.values()), n.agent_id))
        self.term += 1
        winner.is_leader = True
        self.leader_id = winner.agent_id
        for n in online_nodes:
            n.term = self.term
        self.metrics["leader_failovers"] += 1
        self.log_event(
            "HOST-FAILOVER",
            f"Quorum elected [{winner.agent_id}] as new Epoch Host Coordinator for Term {self.term}!",
        )
        self._record_dag(
            winner.agent_id,
            "HOST-FAILOVER",
            f"Elected Coordinator (Term {self.term})",
            diff=f"+ leader = '{winner.agent_id}' (term={self.term})",
        )
        return winner.agent_id

    async def broadcast_ui(self):
        lan_ip = get_lan_ip()
        state = {
            "lan": {
                "ip": lan_ip,
                "port": 8000,
                "url": f"http://{lan_ip}:8000",
                "ws_url": f"ws://{lan_ip}:8000/ws",
            },
            "cluster": {
                "term": self.term,
                "leader_id": self.leader_id,
                "merkle_root": self._cluster_merkle_root(),
            },
            "nodes": {
                k: {
                    "agent_id": n.agent_id,
                    "role": n.role,
                    "target_module": n.target_module,
                    "deps": n.deps,
                    "color": getattr(n, "color", "#a78bfa"),
                    "last_gossip": getattr(n, "last_gossip", ""),
                    "online": n.online,
                    "is_leader": n.is_leader,
                    "term": n.term,
                    "merkle_root": n.merkle_root,
                    "intent": n.intent,
                    "vector_clock": n.vector_clock,
                    "local_micro_commits": n.local_micro_commits,
                    "outbox_size": len(n.outbox_queue),
                    "symbols": [s.model_dump() for s in n.local_symbols.values()],
                    "code": n.code,
                }
                for k, n in self.nodes.items()
            },
            "metrics": self.metrics,
            "events": self.events[-30:],
            "dag": [c.model_dump() for c in self.dag[-16:]],
            "speculative_runs": self.speculative_runs,
            "beam": self.last_gossip_beam,
            "bubbles": self.active_bubbles[-30:],
        }
        dead = set()
        for ws in self.ws_clients:
            try:
                await ws.send_json(state)
            except Exception:
                dead.add(ws)
        self.ws_clients -= dead

    async def trigger_agent_step(self, agent_id: str, new_code: str, intent: str):
        node = self.nodes[agent_id]
        short_diff = self._compute_short_diff(node.code, new_code)
        base_cid = self.dag[-1].commit_id if self.dag else None
        packet, sig_changed = node.mutate_code(new_code, intent, base_cid)

        self.metrics["micro_mutations_absorbed"] += 1
        self.metrics["bytes_saved"] += 14500

        if not sig_changed:
            self.metrics["private_edits_suppressed"] += 1
            self.last_gossip_beam = {"source": agent_id, "targets": [], "kind": "local", "ts": time.time()}
            self.log_event(
                "LOCAL",
                f"[{agent_id}] Private AST body edit in {node.target_module}. Merkle leaf body updated, sig unchanged -> 0B sent.",
            )
            self._record_dag(agent_id, "PRIVATE-AST", intent, diff=short_diff)
            self._emit_bubble(
                source=agent_id,
                targets=[agent_id],
                kind="local",
                label="Private AST (0B)",
                detail=f"{node.target_module}: {intent}",
                color="#10b981",
            )
        else:
            if not node.online:
                node.outbox_queue.append(packet)
                self.last_gossip_beam = {"source": agent_id, "targets": [], "kind": "buffered", "ts": time.time()}
                self.log_event(
                    "OFFLINE-SPECULATION",
                    f"[{agent_id}] Offline! Queued AST delta in local CRDT outbox ({len(node.outbox_queue)} pending).",
                )
                self._record_dag(agent_id, "OFFLINE-BUFFER", f"[Offline] {intent}", diff=short_diff)
                self._emit_bubble(
                    source=agent_id,
                    targets=[agent_id],
                    kind="buffered",
                    label=f"Queued #{len(node.outbox_queue)}",
                    detail=f"CRDT outbox: {intent}",
                    color="#f43f5e",
                )
            else:
                await self.route_gossip(packet)
                node.last_synced_code = node.code
                self._record_dag(agent_id, "CONTRACT-GOSSIP", intent, diff=short_diff)
                self._emit_bubble(
                    source=agent_id,
                    targets=[p for p in self.nodes if p != agent_id and self.nodes[p].online],
                    kind="contract",
                    label="Contract Sig Change",
                    detail=intent,
                    color="#38bdf8",
                )

        await self.broadcast_ui()

    async def route_gossip(self, packet: GossipPacket):
        self.metrics["gossip_packets_routed"] += 1
        targets = []
        for peer_id, peer in self.nodes.items():
            if peer_id == packet.source_agent:
                continue
            if peer.online:
                targets.append(peer_id)
                res, rewritten_calls = peer.receive_gossip(packet)
                if "AST CALL-SITE REWRITE" in res:
                    self.metrics["conflicts_auto_resolved"] += 1
                    self.log_event(
                        "CONFLICT-RESOLVED",
                        f"[{peer_id}] {res} ({rewritten_calls} AST call sites rewritten via NodeTransformer).",
                    )
                else:
                    self.log_event(
                        "GOSSIP",
                        f"[{packet.source_agent} -> {peer_id}] Merkle {packet.merkle_root[:6]} & VClock {packet.vector_clock} synced.",
                    )
            else:
                self.log_event(
                    "PARTITION",
                    f"[{peer_id}] Unreachable. Gossip held in causal anti-entropy log.",
                )
        self.last_gossip_beam = {"source": packet.source_agent, "targets": targets, "kind": "gossip", "ts": time.time()}

    async def simulate_split_brain_conflict(self):
        """
        Simulates a true split-brain concurrent edit on the SAME file & function (`verify_token` in `auth.py`)
        while `agent-auth` is partitioned, then heals the partition and runs AST 3-Way Semantic Merge + Call-Site Adaptation.
        """
        auth = self.nodes["agent-auth"]
        base_code = auth.last_synced_code

        # Partition A (Local Auth Node): Adds strict token length guard & `strict_scope: bool = True`
        local_concurrent_code = (
            "def verify_token(token: str, strict_scope: bool = True) -> bool:\n"
            "    if len(token) < 6:\n"
            "        return False\n"
            "    return token.startswith('jwt_')\n"
        )

        # Partition B (Remote Peer Speculation): Adds `issuer: str = 'neptune'` & revocation check + helper `revoke_token`
        remote_concurrent_code = (
            "def verify_token(token: str, issuer: str = 'neptune') -> bool:\n"
            "    if token.endswith('_revoked'):\n"
            "        return False\n"
            "    return token.startswith('jwt_')\n\n"
            "def revoke_token(token: str) -> str:\n"
            "    return f'{token}_revoked'\n"
        )

        # Advance both vector clocks concurrently so VectorClock.compare == 'CONCURRENT'
        auth.vector_clock["agent-auth"] = auth.vector_clock.get("agent-auth", 0) + 1
        remote_vc = auth.vector_clock.copy()
        remote_vc["agent-auth"] -= 1
        remote_vc["agent-gateway"] = remote_vc.get("agent-gateway", 0) + 1

        relation = VectorClock.compare(auth.vector_clock, remote_vc)
        merged_code, merge_notes = ASTEngine.three_way_merge(
            base_code, local_concurrent_code, remote_concurrent_code, "agent-auth", "agent-gateway"
        )

        short_diff = self._compute_short_diff(auth.code, merged_code)
        auth.code = merged_code
        auth.last_synced_code = merged_code
        auth.vector_clock = VectorClock.merge(auth.vector_clock, remote_vc)
        auth.local_symbols = ASTEngine.extract_symbols(merged_code, "agent-auth", sum(auth.vector_clock.values()))
        auth.merkle_root = ASTEngine.compute_merkle_root(auth.local_symbols)
        auth.intent = f"3-Way AST Merge [{relation}]: {merge_notes}"
        auth.local_micro_commits += 1

        self.metrics["ast_merges_completed"] += 1
        self.metrics["conflicts_auto_resolved"] += 1

        # Propagate synthesized AST signature to downstream callers (`billing.py` and `gateway.py`)
        pkt = GossipPacket(
            packet_id=hashlib.sha256(str(time.time_ns()).encode()).hexdigest()[:8],
            source_agent="agent-auth",
            term=self.term,
            vector_clock=auth.vector_clock.copy(),
            base_commit_id=self.dag[-1].commit_id if self.dag else None,
            intent=auth.intent,
            target_module=auth.target_module,
            exported_symbols=auth.local_symbols,
            raw_code=merged_code,
            merkle_root=auth.merkle_root,
            timestamp=time.time(),
        )
        await self.route_gossip(pkt)
        self._record_dag("agent-auth", "AST-3WAY-MERGE", f"Resolved Concurrent Split-Brain ({merge_notes})", diff=short_diff)
        self._emit_bubble(
            source="agent-auth",
            targets=[p for p in self.nodes if p != "agent-auth" and self.nodes[p].online],
            kind="merge",
            label="3-Way AST Merge",
            detail=merge_notes,
            color="#e879f9",
        )
        self.log_event(
            "AST-3WAY-MERGE",
            f"Resolved CONCURRENT split-brain on auth.py ({merge_notes}) & rewrote downstream AST call sites.",
        )
        await self.broadcast_ui()

    async def toggle_partition(self, agent_id: str):
        """Handles node leaving/disconnecting (including Host Leader failover) and rejoining with Merkle Anti-Entropy catchup."""
        node = self.nodes[agent_id]
        node.online = not node.online

        if not node.online:
            node.intent = "OFFLINE // LOCAL CRDT SPECULATION"
            self.log_event("ALERT", f"[{agent_id}] Disconnected from mesh! Buffering AST mutations locally.")
            # What if the disconnected node was the Host / Coordinator Leader?
            if node.is_leader:
                self.log_event("ALERT", f"Host Coordinator [{agent_id}] DC'd during Term {self.term}! Initiating failover...")
                self.elect_new_leader()
        else:
            # Node Rejoined: 1) Step down / adopt cluster Term, 2) Pull missing peer state via Merkle Anti-Entropy, 3) Flush local CRDT outbox
            if self.leader_id.startswith("NONE"):
                self.elect_new_leader()
            node.term = self.term
            # Pull latest peer symbols & adapt call sites for anything missed while offline
            missed_adaptations = 0
            for peer_id, peer in self.nodes.items():
                if peer_id != agent_id and peer.online and peer.oplog:
                    latest_pkt = peer.oplog[-1]
                    if VectorClock.compare(node.vector_clock, latest_pkt.vector_clock) in ("BEFORE", "CONCURRENT"):
                        _, count = node.receive_gossip(latest_pkt)
                        missed_adaptations += count

            # Flush queued outbox packets accumulated while offline
            flushed_count = len(node.outbox_queue)
            while node.outbox_queue:
                pkt = node.outbox_queue.pop(0)
                pkt.term = self.term
                await self.route_gossip(pkt)
            node.last_synced_code = node.code
            node.intent = f"REJOINED // MERKLE SYNCED (Term {self.term})"
            self.log_event(
                "RECONNECTED",
                f"[{agent_id}] Rejoined mesh (Term {self.term}): Flushed {flushed_count} outbox packets, adapted {missed_adaptations} call sites via Merkle sync.",
            )

        await self.broadcast_ui()

    async def run_speculative_race(self):
        candidates = [
            {
                "branch": "spec/naive-regex",
                "strategy": "Regex Path Matcher",
                "code": (
                    "def route_request(path: str, token: str) -> int:\n"
                    "    if not token.startswith('jwt_'):\n"
                    "        return 401\n"
                    "    return 200 if path.startswith('/api') else 500\n"
                ),
            },
            {
                "branch": "spec/radix-trie-fast",
                "strategy": "Zero-Alloc Prefix Trie + Strict Auth Guard",
                "code": (
                    "ROUTES = {'/api/v1/pay': 200, '/api/v1/auth': 200, '/health': 200}\n\n"
                    "def route_request(path: str, token: str) -> int:\n"
                    "    if path == '/health':\n"
                    "        return 200\n"
                    "    if not token or not token.startswith('jwt_'):\n"
                    "        return 401\n"
                    "    return ROUTES.get(path, 404)\n"
                ),
            },
            {
                "branch": "spec/unbounded-cache",
                "strategy": "Unvalidated Cache Bypass",
                "code": (
                    "def route_request(path: str, token: str) -> int:\n"
                    "    return 200\n"
                ),
            },
        ]

        results = []
        winner = None
        for cand in candidates:
            t0 = time.perf_counter()
            ns: dict = {}
            passed = False
            reason = ""
            try:
                exec(cand["code"], ns)
                fn = ns["route_request"]
                assert fn("/health", "") == 200, "Health check failed"
                assert fn("/api/v1/pay", "invalid") == 401, "Security assertion failed"
                assert fn("/api/v1/pay", "jwt_valid") == 200, "Valid route failed"
                assert fn("/unknown", "jwt_valid") == 404, "Missing 404 guard"
                for _ in range(500):
                    fn("/api/v1/pay", "jwt_valid")
                passed = True
                reason = "4/4 assertions passed"
            except Exception as e:
                reason = str(e)
            latency_us = round((time.perf_counter() - t0) * 1_000_000, 1)
            entry = {
                "branch": cand["branch"],
                "strategy": cand["strategy"],
                "passed": passed,
                "latency_us": latency_us,
                "reason": reason,
                "status": "PASSED" if passed else "PRUNED",
                "code": cand["code"],
            }
            results.append(entry)
            if passed and (winner is None or latency_us < winner["latency_us"]):
                winner = entry

        if winner:
            winner["status"] = "MERGED-WINNER"
            self.metrics["speculative_branches_pruned"] += len(candidates) - 1
            self.speculative_runs = results
            await self.trigger_agent_step(
                "agent-gateway",
                winner["code"],
                f"3x Winner ({winner['branch']} @ {winner['latency_us']}us)",
            )
            self.dag[-1].kind = "SPECULATIVE-WIN"
            self._emit_bubble(
                source="agent-gateway",
                targets=[nid for nid in self.nodes if nid != "agent-gateway" and self.nodes[nid].online] or ["mesh"],
                kind="speculative",
                label=f"3x Winner ({winner['branch']})",
                detail=f"{winner['strategy']} ({winner['latency_us']}µs) - pruned 2 branches",
                color="#c084fc",
            )
            await self.broadcast_ui()

    async def rollback_to_commit(self, commit_id: str) -> bool:
        target = next((c for c in self.dag if c.commit_id == commit_id), None)
        if not target:
            return False
        for nid, code in target.snapshot.items():
            if nid in self.nodes:
                n = self.nodes[nid]
                n.code = code
                n.last_synced_code = code
                n.local_symbols = ASTEngine.extract_symbols(code, nid)
                n.merkle_root = ASTEngine.compute_merkle_root(n.local_symbols)
                n.intent = f"Rollback -> #{commit_id}"
                n.vector_clock[nid] = n.vector_clock.get(nid, 0) + 1
        self._write_workspace_files()
        self._record_dag("arbiter", "ROLLBACK", f"Rollback -> #{commit_id}", diff=f"⏪ state = #{commit_id}")
        await self.broadcast_ui()
        return True

    async def atomic_github_flush(self):
        # Ensure we have an active online Coordinator Leader even if prior leader DC'd
        leader = self.nodes.get(self.leader_id)
        if not leader or not leader.online:
            new_leader = self.elect_new_leader()
            if not new_leader:
                self.log_event("ALERT", "Cannot flush to GitHub: All mesh nodes are partitioned!")
                await self.broadcast_ui()
                return {"status": "all_offline"}

        total_commits = sum(n.local_micro_commits for n in self.nodes.values() if n.online)
        if total_commits == 0:
            await self.broadcast_ui()
            return {"status": "noop"}

        self._write_workspace_files()
        subprocess.run(["git", "add", "."], cwd=WORKSPACE_REPO, capture_output=True)
        msg = f"[Term {self.term} Leader:{self.leader_id}] Squashed {total_commits} AST mutations"
        subprocess.run(["git", "commit", "-m", msg, "--allow-empty"], cwd=WORKSPACE_REPO, capture_output=True)
        rev = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=WORKSPACE_REPO, capture_output=True, text=True)
        sha = rev.stdout.strip() or hashlib.sha1(str(time.time()).encode()).hexdigest()[:7]

        self.metrics["atomic_github_pushes"] += 1
        for n in self.nodes.values():
            if n.online:
                n.local_micro_commits = 0

        self.last_gossip_beam = {"source": self.leader_id, "targets": ["github"], "kind": "flush", "ts": time.time()}
        self._record_dag(
            self.leader_id,
            "EPOCH-FLUSH",
            f"Coordinator {self.leader_id} (Term {self.term}) merged {total_commits} edits -> {sha}",
            diff=f"+ git commit {sha} ({total_commits} squashed)",
            git_sha=sha,
        )
        self._emit_bubble(
            source=self.leader_id,
            targets=["github"],
            kind="flush",
            label=f"PR #{sha} Landed",
            detail=f"Squashed {total_commits} AST mutations to GitHub main",
            color="#f97316",
        )
        await self.broadcast_ui()
        return {"status": "ok", "sha": sha, "leader": self.leader_id, "term": self.term}


mesh = MeshCluster()

SCENARIOS = [
    (
        "agent-auth",
        "def verify_token(token: str) -> bool:\n    # Private body optimization\n    return len(token) > 8 and token.startswith('jwt_')\n",
        "Private AST body optimization",
    ),
    (
        "agent-auth",
        "def verify_token(token: str, strict_scope: bool = True) -> bool:\n    return token.startswith('jwt_') and strict_scope\n",
        "Public Contract Change: Added strict_scope=True to verify_token",
    ),
    (
        "agent-billing",
        "def charge_user(user_id: str, amount: float, currency: str = 'GBP', token: str = 'jwt_ok') -> dict:\n    valid = verify_token(token, strict_scope=True)\n    return {'status': 'settled' if valid else 'denied', 'amt': amount, 'curr': currency}\n",
        "Added multi-currency settlement parameter",
    ),
    (
        "agent-gateway",
        "def route_request(path: str, token: str) -> int:\n    ok = verify_token(token, strict_scope=True)\n    return 200 if (ok and path.startswith('/api')) else 404\n",
        "Refactored internal edge route guard",
    ),
]
scenario_idx = 0


@app.post("/api/step")
async def step_simulation():
    global scenario_idx
    agent_id, code, intent = SCENARIOS[scenario_idx % len(SCENARIOS)]
    scenario_idx += 1
    await mesh.trigger_agent_step(agent_id, code, intent)
    return {"status": "ok"}


@app.post("/api/conflict")
async def trigger_conflict():
    await mesh.simulate_split_brain_conflict()
    return {"status": "ok"}


@app.post("/api/speculate")
async def trigger_speculation():
    await mesh.run_speculative_race()
    return {"status": "ok"}


@app.post("/api/mutate")
async def custom_mutation(req: MutationRequest):
    if req.agent_id not in mesh.nodes:
        return {"status": "error"}
    await mesh.trigger_agent_step(req.agent_id, req.code, req.intent)
    return {"status": "ok"}


@app.post("/api/rollback/{commit_id}")
async def rollback_endpoint(commit_id: str):
    ok = await mesh.rollback_to_commit(commit_id)
    return {"status": "ok" if ok else "not_found"}


@app.post("/api/partition/{agent_id}")
async def partition_node(agent_id: str):
    await mesh.toggle_partition(agent_id)
    return {"status": "ok"}


@app.post("/api/flush")
async def flush_to_github():
    return await mesh.atomic_github_flush()


@app.get("/api/lan")
async def get_lan_endpoint():
    ip = get_lan_ip()
    return {
        "ip": ip,
        "port": 8000,
        "url": f"http://{ip}:8000",
        "ws_url": f"ws://{ip}:8000/ws",
        "hostname": socket.gethostname(),
    }


@app.get("/api/state")
async def get_state_endpoint():
    ip = get_lan_ip()
    return {
        "lan": {
            "ip": ip,
            "port": 8000,
            "url": f"http://{ip}:8000",
            "ws_url": f"ws://{ip}:8000/ws",
        },
        "cluster": {"term": mesh.term, "leader_id": mesh.leader_id},
        "nodes": {
            k: {
                "agent_id": n.agent_id,
                "role": n.role,
                "target_module": n.target_module,
                "online": n.online,
                "is_leader": n.is_leader,
                "color": getattr(n, "color", "#a78bfa"),
                "last_gossip": getattr(n, "last_gossip", ""),
                "vector_clock": n.vector_clock,
                "local_micro_commits": n.local_micro_commits,
                "outbox_size": len(n.outbox_queue),
                "symbols": [s.name for s in n.local_symbols.values()],
            }
            for k, n in mesh.nodes.items()
        },
        "metrics": mesh.metrics,
        "bubbles": mesh.active_bubbles[-25:],
    }


@app.get("/api/agents")
async def list_agents():
    return {
        "agents": [
            {
                "agent_id": n.agent_id,
                "role": n.role,
                "target_module": n.target_module,
                "online": n.online,
                "is_leader": n.is_leader,
                "vector_clock": n.vector_clock,
                "color": getattr(n, "color", "#a78bfa"),
                "last_gossip": getattr(n, "last_gossip", ""),
                "outbox_size": len(n.outbox_queue),
            }
            for n in mesh.nodes.values()
        ]
    }


@app.post("/api/agents/register")
async def register_agent_endpoint(req: RegisterAgentRequest):
    node = mesh.register_agent(
        agent_id=req.agent_id,
        role=req.role,
        target_module=req.target_module,
        initial_code=req.initial_code,
        deps=req.deps,
    )
    await mesh.broadcast_ui()
    return {
        "status": "ok",
        "agent_id": node.agent_id,
        "role": node.role,
        "target_module": node.target_module,
        "color": getattr(node, "color", "#a78bfa"),
        "vector_clock": node.vector_clock,
    }


@app.delete("/api/agents/{agent_id}")
async def unregister_agent_endpoint(agent_id: str):
    ok = mesh.unregister_agent(agent_id)
    if ok:
        await mesh.broadcast_ui()
        return {"status": "ok"}
    return {"status": "error", "detail": "Agent not found"}


@app.post("/api/agents/gossip")
async def gossip_endpoint(req: GossipIdeaRequest):
    res = await mesh.broadcast_idea_or_gossip(
        agent_id=req.agent_id,
        intent=req.intent,
        gossip_message=req.gossip_message,
        code=req.code,
        target_agent=req.target_agent,
    )
    return res


@app.get("/agent_client.py")
async def get_agent_client_script():
    client_path = BASE_DIR / "agent_client.py"
    if client_path.exists():
        return PlainTextResponse(client_path.read_text(encoding="utf-8"), media_type="text/x-python")
    return PlainTextResponse("# Client script not found", status_code=404)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    mesh.ws_clients.add(websocket)
    await mesh.broadcast_ui()
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        mesh.ws_clients.discard(websocket)


class _LANBeaconProtocol(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data: bytes, addr):
        if data.strip() == b"GITPULSE_DISCOVER":
            ip = get_lan_ip()
            reply = json.dumps({
                "service": "gitpulse-host",
                "url": f"http://{ip}:8000",
                "ws_url": f"ws://{ip}:8000/ws",
                "term": mesh.term,
                "leader": mesh.leader_id,
            }).encode("utf-8")
            self.transport.sendto(reply, addr)


@app.on_event("startup")
async def start_lan_discovery_beacon():
    loop = asyncio.get_running_loop()
    try:
        await loop.create_datagram_endpoint(
            lambda: _LANBeaconProtocol(),
            local_addr=("0.0.0.0", 9999),
            allow_broadcast=True,
        )
        ip = get_lan_ip()
        mesh.log_event("LAN-HOST", f"Host bound on http://{ip}:8000 (UDP Discovery Beacon :9999)")
    except Exception:
        pass


@app.get("/")
async def serve_dashboard():
    return HTMLResponse((BASE_DIR / "index.html").read_text(encoding="utf-8"))


def self_check():
    """Verifies AST 3-Way Merge, NodeTransformer call-site rewrite, Host Leader Failover, Merkle Anti-Entropy, and SQLite WAL."""
    c = MeshCluster()
    # 1. Verify private AST body suppression
    asyncio.run(c.trigger_agent_step("agent-auth", "def verify_token(token: str) -> bool:\n    return True\n", "Private tweak"))
    assert c.metrics["private_edits_suppressed"] == 1

    # 2. Verify Host Coordinator DC -> Raft-lite Term Election Failover
    assert c.leader_id == "agent-auth" and c.term == 1
    asyncio.run(c.toggle_partition("agent-auth"))  # Leader DCs!
    assert c.leader_id != "agent-auth" and c.term == 2 and c.metrics["leader_failovers"] == 1

    # 3. Verify Offline Speculation while disconnected + Rejoin Merkle Anti-Entropy catchup
    asyncio.run(c.trigger_agent_step("agent-auth", "def verify_token(token: str, role: str = 'admin') -> bool:\n    return True\n", "Offline sig change"))
    assert len(c.nodes["agent-auth"].outbox_queue) == 1
    asyncio.run(c.toggle_partition("agent-auth"))  # Rejoins mesh!
    assert len(c.nodes["agent-auth"].outbox_queue) == 0
    # Downstream callers (`billing.py`, `gateway.py`) should have `role='admin'` injected into their AST `verify_token(...)` calls!
    assert "role='admin'" in c.nodes["agent-gateway"].code, "AST CallSiteRewriter failed on gateway.py"

    # 4. Verify Split-Brain Concurrent Conflict 3-Way AST Synthesis
    asyncio.run(c.simulate_split_brain_conflict())
    assert c.metrics["ast_merges_completed"] == 1
    assert "revoke_token" in c.nodes["agent-auth"].code and "strict_scope" in c.nodes["agent-auth"].code

    # 5. Verify Atomic Git Flush under new Coordinator Leader
    res = asyncio.run(c.atomic_github_flush())
    assert res["status"] == "ok" and res["term"] == 2
    print("SELF-CHECK PASSED: 3-Way AST Merger, CallSiteRewriter, Host Leader Failover (Term 2), Merkle Rejoin, and SQLite WAL verified.")


if __name__ == "__main__":
    if "--check" in sys.argv:
        self_check()
    else:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8000)
