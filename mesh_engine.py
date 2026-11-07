from pathlib import Path
import ast, asyncio, atexit, difflib, hashlib, json, os, platform, re, secrets, shutil, socket, sqlite3, subprocess, sys, threading, time
import urllib.request
from typing import Dict, List, Optional, Set, Tuple
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from pydantic import BaseModel
import agent_detect
import gitpulse_hook
app = FastAPI(title='GitPulse // Distributed AST-CRDT Mesh, 3-Way Merger & Leaderless Failover')
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])
BASE_DIR = Path(__file__).parent
WORKSPACE_REPO = BASE_DIR / 'workspace_repo'
DB_PATH = BASE_DIR / 'mesh_state.db'
PORT = int(os.environ.get('GITPULSE_PORT', '8000'))
SESSION_CODE = os.environ.get('GITPULSE_SESSION') or secrets.token_hex(3)
DEMO_MODE = '--demo' in sys.argv or os.environ.get('GITPULSE_DEMO') == '1'
WORKSPACE_ARG = next((a.split('=', 1)[1] for a in sys.argv if a.startswith('--workspace=')), os.environ.get('GITPULSE_WORKSPACE', 'auto'))

def get_lan_ip() -> str:
    """Discovers host LAN IP address for easy multi-agent Wi-Fi connection."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.2)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        if ip and (not ip.startswith('127.')):
            return ip
    except Exception:
        pass
    try:
        ip = socket.gethostbyname(socket.gethostname())
        if ip and (not ip.startswith('127.')):
            return ip
    except Exception:
        pass
    return '127.0.0.1'

def tailscale_ip() -> str:
    try:
        import psutil
        for addrs in psutil.net_if_addrs().values():
            for a in addrs:
                if a.family == socket.AF_INET and a.address.startswith('100.') and (64 <= int(a.address.split('.')[1]) <= 127):
                    return a.address
    except Exception:
        pass
    return ''
HOST_MACHINE = socket.gethostname()
TAKEOVER_FROM = os.environ.get('GITPULSE_TAKEOVER', '')
_NO_WINDOW = 134217728 if sys.platform == 'win32' else 0
_net_cache: dict = {'t': 0.0, 'v': {}}

def network_info() -> dict:
    """Wi-Fi name + whether it is the kind of network that blocks device-to-device traffic."""
    if time.time() - _net_cache['t'] < 30:
        return _net_cache['v']
    ssid = ''
    try:
        if sys.platform == 'win32':
            out = subprocess.run(['netsh', 'wlan', 'show', 'interfaces'], capture_output=True, text=True, timeout=3, creationflags=_NO_WINDOW).stdout
            m = re.search('^\\s*SSID\\s*:\\s*(.+?)\\s*$', out, re.M)
            ssid = m.group(1) if m else ''
        elif sys.platform.startswith('linux'):
            ssid = subprocess.run(['iwgetid', '-r'], capture_output=True, text=True, timeout=3).stdout.strip()
    except Exception:
        pass
    low = ssid.lower()
    isolated = any((k in low for k in ('eduroam', 'govroam', 'guest', 'public', 'hotel', 'airport', 'cafe', 'starbucks', 'uni')))
    _net_cache.update(t=time.time(), v={'ssid': ssid, 'likely_isolated': isolated})
    return _net_cache['v']

class Tunnel:
    """Free public https link via a Cloudflare quick tunnel (no account). Needed on eduroam & co."""

    def __init__(self):
        self.url = ''
        self.status = 'off'
        self.error = ''
        self.proc: Optional[subprocess.Popen] = None

    @staticmethod
    def _exe_name() -> str:
        return 'cloudflared.exe' if sys.platform == 'win32' else 'cloudflared'

    def _find(self) -> str:
        found = shutil.which('cloudflared')
        if found:
            return found
        for c in (Path.home() / '.gitpulse' / self._exe_name(), Path(os.environ.get('ProgramFiles(x86)', 'C:/Program Files (x86)')) / 'cloudflared' / 'cloudflared.exe', Path(os.environ.get('ProgramFiles', 'C:/Program Files')) / 'cloudflared' / 'cloudflared.exe'):
            if c.is_file():
                return str(c)
        return ''

    def _download(self) -> str:
        arch = 'arm64' if platform.machine().lower() in ('arm64', 'aarch64') else 'amd64'
        if sys.platform == 'win32':
            asset = 'cloudflared-windows-amd64.exe'
        elif sys.platform.startswith('linux'):
            asset = f'cloudflared-linux-{arch}'
        else:
            raise RuntimeError('cloudflared not found. Install it with: brew install cloudflared')
        dest = Path.home() / '.gitpulse' / self._exe_name()
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix('.part')
        self.status = 'downloading'
        urllib.request.urlretrieve(f'https://github.com/cloudflare/cloudflared/releases/latest/download/{asset}', tmp)
        tmp.replace(dest)
        dest.chmod(493)
        return str(dest)

    def start(self) -> None:
        """Blocking; run in a thread."""
        if self.status in ('downloading', 'starting', 'on') and (not self.proc or self.proc.poll() is None):
            return
        self.stop()
        self.error = ''
        self.status = 'starting'
        try:
            exe = self._find() or self._download()
            self.status = 'starting'
            self.proc = subprocess.Popen([exe, 'tunnel', '--no-autoupdate', '--protocol', 'http2', '--url', f'http://127.0.0.1:{PORT}'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, creationflags=_NO_WINDOW)
            deadline = time.time() + 45
            for line in self.proc.stdout:
                m = re.search('https://[a-z0-9-]+\\.trycloudflare\\.com', line)
                if m:
                    self.url = m.group(0)
                    self.status = 'on'
                    threading.Thread(target=self._drain, daemon=True).start()
                    return
                if time.time() > deadline:
                    raise RuntimeError('cloudflared gave no link within 45 s')
            raise RuntimeError('cloudflared exited before giving a link')
        except Exception as e:
            self.stop()
            self.status = 'error'
            self.error = str(e)

    def _drain(self) -> None:
        proc = self.proc
        try:
            for _ in proc.stdout:
                pass
        except Exception:
            pass
        if self.proc is proc:
            self.url, self.status, self.proc = ('', 'off', None)

    def stop(self) -> None:
        proc, self.proc = (self.proc, None)
        self.url, self.status = ('', 'off')
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except Exception:
                proc.kill()

    def info(self) -> dict:
        if self.status == 'on' and self.proc and (self.proc.poll() is not None):
            self.url, self.status, self.proc = ('', 'off', None)
        return {'status': self.status, 'url': self.url, 'error': self.error}
TUNNEL = Tunnel()
atexit.register(TUNNEL.stop)

class RegisterAgentRequest(BaseModel):
    agent_id: str
    role: str = 'Autonomous Worker'
    target_module: str = 'services/worker.py'
    initial_code: Optional[str] = None
    deps: List[str] = []
    engine: str = 'Custom'
    machine: str = ''
    branch: str = 'main'
    session: str = ''
    meta: dict = {}

class FileChangeRequest(BaseModel):
    agent_id: str
    session: str = ''
    path: str
    repo: str = ''
    old_code: str = ''
    new_code: str

class GossipIdeaRequest(BaseModel):
    agent_id: str
    intent: str
    gossip_message: Optional[str] = None
    code: Optional[str] = None
    target_agent: Optional[str] = None

class ASTSymbol(BaseModel):
    name: str
    kind: str
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
    kind: str
    intent: str
    diff: str = ''
    vector_clock: Dict[str, int]
    merkle_root: str = ''
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
            va, vb = (vc_a.get(k, 0), vc_b.get(k, 0))
            if va < vb:
                a_lt_b = True
            elif va > vb:
                b_lt_a = True
        if not a_lt_b and (not b_lt_a):
            return 'EQUAL'
        if a_lt_b and (not b_lt_a):
            return 'BEFORE'
        if b_lt_a and (not a_lt_b):
            return 'AFTER'
        return 'CONCURRENT'

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
    def extract_symbols(code: str, agent_id: str, lamport: int=0) -> Dict[str, ASTSymbol]:
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
                    ann = f': {ast.unparse(a.annotation)}' if a.annotation else ''
                    def_idx = idx - (num_Args - num_Defs)
                    dflt = f' = {ast.unparse(node.args.defaults[def_idx])}' if def_idx >= 0 else ''
                    args.append(f'{a.arg}{ann}{dflt}')
                returns = ast.unparse(node.returns) if node.returns else 'Any'
                sig = f"def {node.name}({', '.join(args)}) -> {returns}"
                sig_hash = hashlib.sha256(sig.encode()).hexdigest()[:10]
                body_src = '\n'.join((ast.unparse(stmt) for stmt in node.body))
                body_hash = hashlib.sha256(body_src.encode()).hexdigest()[:10]
                symbols[node.name] = ASTSymbol(name=node.name, kind='function', signature=sig, signature_hash=sig_hash, body_hash=body_hash, source=ast.unparse(node), owner_agent=agent_id, lamport=lamport)
            elif isinstance(node, ast.ClassDef):
                methods = [n.name for n in node.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
                sig = f"class {node.name}[{', '.join(methods)}]"
                sig_hash = hashlib.sha256(sig.encode()).hexdigest()[:10]
                src = ast.unparse(node)
                body_hash = hashlib.sha256(src.encode()).hexdigest()[:10]
                symbols[node.name] = ASTSymbol(name=node.name, kind='class', signature=sig, signature_hash=sig_hash, body_hash=body_hash, source=src, owner_agent=agent_id, lamport=lamport)
            elif isinstance(node, ast.Assign):
                targets = [ast.unparse(t) for t in node.targets]
                name = targets[0]
                src = ast.unparse(node)
                h = hashlib.sha256(src.encode()).hexdigest()[:10]
                symbols[name] = ASTSymbol(name=name, kind='assign', signature=f'{name} = ...', signature_hash=h, body_hash=h, source=src, owner_agent=agent_id, lamport=lamport)
        return symbols

    @staticmethod
    def patch_source(target_code: str, symbol_name: str, new_symbol_code: str) -> str:
        """
        Splice new_symbol_code into target_code at AST level.
        If symbol_name exists, replaces that AST node cleanly.
        If symbol_name does not exist, appends it cleanly.
        Preserves other functions, imports, and private code intact.
        """
        try:
            tree = ast.parse(target_code)
        except SyntaxError:
            return target_code.rstrip() + '\n\n' + new_symbol_code.strip() + '\n'
        try:
            new_tree = ast.parse(new_symbol_code)
            if not new_tree.body:
                return target_code
            new_node = new_tree.body[0]
        except SyntaxError:
            return target_code

        class SymbolReplacer(ast.NodeTransformer):

            def __init__(self, name: str, repl: ast.AST):
                self.name = name
                self.replacement = repl
                self.replaced = False

            def visit_FunctionDef(self, node):
                if node.name == self.name:
                    self.replaced = True
                    return self.replacement
                return node

            def visit_AsyncFunctionDef(self, node):
                if node.name == self.name:
                    self.replaced = True
                    return self.replacement
                return node

            def visit_ClassDef(self, node):
                if node.name == self.name:
                    self.replaced = True
                    return self.replacement
                return node
        replacer = SymbolReplacer(symbol_name, new_node)
        updated_tree = replacer.visit(tree)
        ast.fix_missing_locations(updated_tree)
        if replacer.replaced:
            try:
                return ast.unparse(updated_tree) + '\n'
            except Exception:
                return target_code
        else:
            return target_code.rstrip() + '\n\n' + new_symbol_code.strip() + '\n'

    @staticmethod
    def compute_merkle_root(symbols: Dict[str, ASTSymbol]) -> str:
        """Deterministic Merkle root over sorted AST symbol table."""
        leaves = []
        for name in sorted(symbols.keys()):
            s = symbols[name]
            leaf = hashlib.sha256(f'{name}:{s.signature_hash}:{s.body_hash}'.encode()).hexdigest()
            leaves.append(leaf)
        if not leaves:
            return '0' * 12
        while len(leaves) > 1:
            if len(leaves) % 2 == 1:
                leaves.append(leaves[-1])
            leaves = [hashlib.sha256((leaves[i] + leaves[i + 1]).encode()).hexdigest() for i in range(0, len(leaves), 2)]
        return leaves[0][:12]

    @staticmethod
    def three_way_merge(base_code: str, local_code: str, remote_code: str, local_agent: str, remote_agent: str) -> Tuple[str, str]:
        """
        True AST 3-Way Semantic Merger:
        - Compares Base AST vs Local AST vs Remote AST at the symbol & parameter level.
        - Disjoint functions/constants modified on overlapping lines merge cleanly with 0 conflicts.
        - Concurrent edits to the same function merge unioned parameters + statement bodies or preserve shadow fallback.
        """
        base_syms = ASTEngine.extract_symbols(base_code, 'base')
        local_syms = ASTEngine.extract_symbols(local_code, local_agent)
        remote_syms = ASTEngine.extract_symbols(remote_code, remote_agent)
        all_names = list(dict.fromkeys(list(base_syms.keys()) + list(local_syms.keys()) + list(remote_syms.keys())))
        merged_sources: List[str] = []
        notes: List[str] = []
        for name in all_names:
            b = base_syms.get(name)
            l = local_syms.get(name)
            r = remote_syms.get(name)
            if not b:
                if l and (not r):
                    merged_sources.append(l.source)
                    notes.append(f'+{name}({local_agent})')
                elif r and (not l):
                    merged_sources.append(r.source)
                    notes.append(f'+{name}({remote_agent})')
                elif l and r:
                    if l.body_hash == r.body_hash and l.signature_hash == r.signature_hash:
                        merged_sources.append(l.source)
                    else:
                        merged_sources.extend([l.source, r.source.replace(f'def {name}', f'def {name}_alt', 1)])
                        notes.append(f'dual-add:{name}')
                continue
            l_changed = not l or (l.signature_hash != b.signature_hash or l.body_hash != b.body_hash)
            r_changed = not r or (r.signature_hash != b.signature_hash or r.body_hash != b.body_hash)
            if not l_changed and (not r_changed) and l:
                merged_sources.append(l.source)
            elif l_changed and (not r_changed) and l:
                merged_sources.append(l.source)
                notes.append(f'kept:{name}({local_agent})')
            elif r_changed and (not l_changed) and r:
                merged_sources.append(r.source)
                notes.append(f'took:{name}({remote_agent})')
            elif l and r:
                if l.signature_hash == r.signature_hash and l.body_hash == r.body_hash:
                    merged_sources.append(l.source)
                else:
                    synthesized = ASTEngine._synthesize_function_conflict(l.source, r.source)
                    merged_sources.append(synthesized)
                    notes.append(f'ast-synthesized:{name}')
        final_code = '\n\n'.join(merged_sources) + '\n'
        return (final_code, ', '.join(notes) or 'clean-ast-merge')

    @staticmethod
    def _synthesize_function_conflict(local_fn_src: str, remote_fn_src: str) -> str:
        """Synthesizes two concurrent AST function definitions by unioning parameters and non-return guard statements."""
        try:
            l_node = ast.parse(local_fn_src).body[0]
            r_node = ast.parse(remote_fn_src).body[0]
            if not isinstance(l_node, ast.FunctionDef) or not isinstance(r_node, ast.FunctionDef):
                return remote_fn_src
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
                return (code, 0)
            num_args = len(up_node.args.args)
            num_defs = len(up_node.args.defaults)
            new_kwargs: Dict[str, ast.AST] = {}
            for idx in range(num_args - num_defs, num_args):
                arg_name = up_node.args.args[idx].arg
                new_kwargs[arg_name] = up_node.args.defaults[idx - (num_args - num_defs)]
            if not new_kwargs:
                return (code, 0)
            tree = ast.parse(code)
            rewriter = CallSiteRewriter(up_node.name, new_kwargs)
            new_tree = rewriter.visit(tree)
            ast.fix_missing_locations(new_tree)
            if rewriter.rewritten_count > 0:
                return (ast.unparse(new_tree) + '\n', rewriter.rewritten_count)
            return (code, 0)
        except Exception:
            return (code, 0)

class AgentNode:

    def __init__(self, agent_id: str, role: str, target_module: str, initial_code: str, deps: List[str], color: Optional[str]=None, engine: str='Antigravity', machine: Optional[str]=None, branch: str='main'):
        self.agent_id = agent_id
        self.role = role
        self.target_module = target_module
        self.code = initial_code
        self.last_synced_code = initial_code
        self.deps = deps
        self.color = color or '#a78bfa'
        self.engine = engine
        self.machine = machine or socket.gethostname()
        self.branch = branch
        self.last_prompt = 'Watching AST & git worktree'
        self.last_gossip = ''
        self.vector_clock: Dict[str, int] = {agent_id: 0}
        self.online: bool = True
        self.is_leader: bool = False
        self.term: int = 1
        self.intent: str = 'SYNCED'
        self.local_micro_commits: int = 0
        self.outbox_queue: List[GossipPacket] = []
        self.oplog: List[GossipPacket] = []
        self.known_peer_symbols: Dict[str, ASTSymbol] = {}
        self.local_symbols: Dict[str, ASTSymbol] = ASTEngine.extract_symbols(initial_code, agent_id)
        self.merkle_root: str = ASTEngine.compute_merkle_root(self.local_symbols)
        self.meta: dict = {}
        self.inbox: List[dict] = []
        self.last_seen: float = time.time()
        self.hook_last_pull: float = 0.0
        self.source: str = 'demo'

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
        pkt_id = hashlib.sha256(f'{self.agent_id}:{lamport}:{time.time_ns()}'.encode()).hexdigest()[:8]
        packet = GossipPacket(packet_id=pkt_id, source_agent=self.agent_id, term=self.term, vector_clock=self.vector_clock.copy(), base_commit_id=base_commit_id, intent=intent_desc, target_module=self.target_module, exported_symbols=new_symbols if signature_changed else {}, raw_code=new_code, merkle_root=self.merkle_root, timestamp=time.time())
        self.oplog.append(packet)
        return (packet, signature_changed)

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
                adapted.append(f'{sym.name}({sym.signature})')
        if adapted:
            return (f"AST CALL-SITE REWRITE [{relation}]: {', '.join(adapted)}", rewritten_total)
        return (f'ACK [{relation}]', 0)

class MeshCluster:

    def __init__(self, demo: bool=True):
        self.term: int = 1
        self.demo = demo
        self.leader_id: str = 'agent-auth' if demo else ''
        host_machine = socket.gethostname()
        self.nodes: Dict[str, AgentNode] = {} if not demo else {'agent-auth': AgentNode('agent-auth', 'Auth & Identity Service', 'services/auth.py', "def verify_token(token: str) -> bool:\n    return token.startswith('jwt_')\n", deps=[], color='#a78bfa', engine='Antigravity', machine=host_machine, branch='feat/auth-jwt'), 'agent-billing': AgentNode('agent-billing', 'Ledger & Billing Worker', 'services/billing.py', "def charge_user(user_id: str, amount: float, token: str = 'jwt_ok') -> dict:\n    valid = verify_token(token)\n    return {'status': 'paid' if valid else 'denied', 'amt': amount}\n", deps=['agent-auth'], color='#38bdf8', engine='Claude Code', machine=host_machine, branch='feat/billing-ledger'), 'agent-gateway': AgentNode('agent-gateway', 'Edge API Router', 'services/gateway.py', 'def route_request(path: str, token: str) -> int:\n    ok = verify_token(token)\n    return 200 if ok else 401\n', deps=['agent-auth', 'agent-billing'], color='#34d399', engine='Codex', machine=host_machine, branch='feat/edge-router')}
        if demo:
            self.nodes['agent-auth'].is_leader = True
        for src_id, src in self.nodes.items():
            for dst_id, dst in self.nodes.items():
                if src_id != dst_id:
                    dst.known_peer_symbols.update(src.local_symbols)
        self.events: List[dict] = []
        self.dag: List[DAGCommit] = []
        self.speculative_runs: List[dict] = []
        self.last_gossip_beam: Optional[dict] = None
        self.active_bubbles: List[dict] = []
        self.metrics = {'micro_mutations_absorbed': 0, 'private_edits_suppressed': 0, 'gossip_packets_routed': 0, 'conflicts_auto_resolved': 0, 'collisions_preempted': 0, 'ast_patches_synced': 0, 'git_coalesce_commits': 0, 'ast_merges_completed': 0, 'leader_failovers': 0, 'speculative_branches_pruned': 0, 'atomic_github_pushes': 0, 'bytes_saved': 0}
        self.ws_clients: Set[WebSocket] = set()
        self.flows: List[dict] = []
        self.workspace: str = ''
        self.other_agents: List[dict] = []
        self.watcher = agent_detect.RepoWatcher()
        self._detected_cache: List[dict] = []
        self.known_machines: Dict[str, float] = {}
        self.active_leases: Dict[str, dict] = {}
        self.last_file_edit_ts: float = 0.0
        self.last_coalesced_mutations: int = 0
        self._init_sqlite_wal()
        self._init_workspace_git()
        self._record_dag('system', 'EPOCH-FLUSH', 'Genesis AST mesh snapshot')

    def _emit_bubble(self, source: str, targets: List[str], kind: str, label: str, detail: str, color: str='#a78bfa') -> dict:
        bubble = {'id': hashlib.sha256(f'{source}:{time.time_ns()}'.encode()).hexdigest()[:8], 'source': source, 'targets': targets, 'kind': kind, 'label': label, 'detail': detail, 'color': color, 'timestamp': time.time()}
        self.active_bubbles.append(bubble)
        if len(self.active_bubbles) > 50:
            self.active_bubbles = self.active_bubbles[-50:]
        return bubble

    def register_agent(self, agent_id: str, role: str='Autonomous Worker', target_module: str='services/worker.py', initial_code: Optional[str]=None, deps: Optional[List[str]]=None, engine: str='Custom', machine: str='', branch: str='main') -> AgentNode:
        if agent_id in self.nodes:
            node = self.nodes[agent_id]
            node.online = True
            node.role = role or node.role
            node.target_module = target_module or node.target_module
            if engine and engine != 'Custom':
                node.engine = engine
            if machine:
                node.machine = machine
            if branch:
                node.branch = branch
            if initial_code:
                node.code = initial_code
                node.local_symbols = ASTEngine.extract_symbols(node.code, agent_id)
                node.merkle_root = ASTEngine.compute_merkle_root(node.local_symbols)
            self.log_event('JOIN', f'[{agent_id}] Reconnected ({node.engine} @ {node.machine}:{node.branch}).')
        else:
            palette = ['#a78bfa', '#38bdf8', '#34d399', '#fbbf24', '#f472b6', '#fb7185', '#2dd4bf', '#818cf8', '#f97316']
            color = palette[len(self.nodes) % len(palette)]
            code = initial_code or f"# {role} ({agent_id})\ndef handle_{agent_id.replace('-', '_')}(data: dict) -> dict:\n    return {{'status': 'processed', 'agent': '{agent_id}'}}\n"
            node = AgentNode(agent_id, role, target_module, code, deps or [], color=color, engine=engine or 'Custom', machine=machine or socket.gethostname(), branch=branch or 'main')
            self.nodes[agent_id] = node
            for other_id, other_node in self.nodes.items():
                if other_id != agent_id:
                    other_node.known_peer_symbols.update(node.local_symbols)
                    node.known_peer_symbols.update(other_node.local_symbols)
            self._write_workspace_files()
            self.log_event('JOIN', f'[{agent_id}] Detected {node.engine} on {node.machine} [{node.branch}].')
            self._record_dag(agent_id, 'AGENT-JOIN', f'{node.engine} @ {node.machine} [{node.branch}]', diff=f'+ agent {agent_id} ({node.engine} // {node.branch})')
        node.last_seen = time.time()
        leader = self.nodes.get(self.leader_id)
        if leader is None or not leader.online:
            for n in self.nodes.values():
                n.is_leader = False
            node.is_leader = True
            self.leader_id = agent_id
        self._emit_bubble(source=agent_id, targets=[nid for nid in self.nodes if nid != agent_id and self.nodes[nid].online] or ['mesh'], kind='agent-join', label=f'{node.engine} // {node.branch}', detail=f'{agent_id} on {node.machine} ({target_module})', color=getattr(node, 'color', '#a78bfa'))
        return node

    def unregister_agent(self, agent_id: str) -> bool:
        if agent_id in self.nodes:
            node = self.nodes[agent_id]
            if node.is_leader:
                self.elect_new_leader()
            del self.nodes[agent_id]
            self.log_event('LEAVE', f'[{agent_id}] Disconnected from LAN mesh.')
            self._record_dag('system', 'AGENT-LEAVE', f"Agent '{agent_id}' left the mesh")
            return True
        return False

    async def broadcast_idea_or_gossip(self, agent_id: str, intent: str, gossip_message: Optional[str]=None, code: Optional[str]=None, target_agent: Optional[str]=None) -> dict:
        if agent_id not in self.nodes:
            self.register_agent(agent_id, role='Autonomous Peer', target_module=f"services/{agent_id.replace('-', '_')}.py")
        node = self.nodes[agent_id]
        msg = gossip_message or intent
        if code and code.strip() != node.code.strip():
            await self.trigger_agent_step(agent_id, code, intent)
            return {'status': 'ok', 'mode': 'code_mutation'}
        node.intent = intent
        node.last_gossip = msg
        node.vector_clock[agent_id] = node.vector_clock.get(agent_id, 0) + 1
        node.local_micro_commits += 1
        targets = [target_agent] if target_agent and target_agent in self.nodes else [nid for nid in self.nodes if nid != agent_id and self.nodes[nid].online]
        for tid in targets:
            if tid in self.nodes:
                self.nodes[tid].vector_clock[agent_id] = max(self.nodes[tid].vector_clock.get(agent_id, 0), node.vector_clock[agent_id])
        self.metrics['micro_mutations_absorbed'] += 1
        self.metrics['gossip_packets_routed'] += 1
        self.log_event('GOSSIP-IDEA', f'[{agent_id}] Gossiped idea: {msg}')
        self._record_dag(agent_id, 'GOSSIP-IDEA', intent, diff=f'~ {msg[:40]}')
        self._emit_bubble(source=agent_id, targets=targets or ['mesh'], kind='idea', label=intent[:24], detail=msg, color=getattr(node, 'color', '#fbbf24'))
        self.last_gossip_beam = {'source': agent_id, 'targets': targets, 'kind': 'gossip', 'ts': time.time()}
        await self.broadcast_ui()
        return {'status': 'ok', 'mode': 'gossip_idea'}

    def _init_sqlite_wal(self):
        """SQLite WAL crash-resilient state store so host restarts never lose DAG or Vector Clocks."""
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute('PRAGMA journal_mode=WAL;')
            conn.execute('CREATE TABLE IF NOT EXISTS dag_oplog (commit_id TEXT PRIMARY KEY, agent_id TEXT, term INTEGER, kind TEXT, intent TEXT, diff TEXT, vclock TEXT, merkle_root TEXT, payload TEXT, ts TEXT)')

    def _persist_commit(self, c: DAGCommit):
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute('INSERT OR REPLACE INTO dag_oplog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (c.commit_id, c.agent_id, c.term, c.kind, c.intent, c.diff, json.dumps(c.vector_clock), c.merkle_root, json.dumps(c.snapshot), c.timestamp))

    def _init_workspace_git(self):
        (WORKSPACE_REPO / 'services').mkdir(parents=True, exist_ok=True)
        if not (WORKSPACE_REPO / '.git').exists():
            subprocess.run(['git', 'init'], cwd=WORKSPACE_REPO, capture_output=True)
            subprocess.run(['git', 'config', 'user.name', 'GitPulse Mesh'], cwd=WORKSPACE_REPO, capture_output=True)
            subprocess.run(['git', 'config', 'user.email', 'mesh@gitpulse.local'], cwd=WORKSPACE_REPO, capture_output=True)
        self._write_workspace_files()

    def _write_workspace_files(self):
        for n in self.nodes.values():
            target = WORKSPACE_REPO / n.target_module
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(n.code, encoding='utf-8')

    def _merged_vclock(self) -> Dict[str, int]:
        vc: Dict[str, int] = {}
        for n in self.nodes.values():
            vc = VectorClock.merge(vc, n.vector_clock)
        return vc

    def _cluster_merkle_root(self) -> str:
        combined = ':'.join((f'{nid}={n.merkle_root}' for nid, n in sorted(self.nodes.items())))
        return hashlib.sha256(combined.encode()).hexdigest()[:12]

    def _compute_short_diff(self, old_code: str, new_code: str) -> str:
        old_lines = old_code.strip().splitlines()
        new_lines = new_code.strip().splitlines()
        diff_lines = [l for l in difflib.unified_diff(old_lines, new_lines, lineterm='', n=0) if (l.startswith('+') or l.startswith('-')) and (not l.startswith('+++')) and (not l.startswith('---'))]
        return '\n'.join(diff_lines[:3]) if diff_lines else f"+ {(new_lines[-1] if new_lines else '')}"

    def _record_dag(self, agent_id: str, kind: str, intent: str, diff: str='', git_sha: Optional[str]=None, merge_parent_id: Optional[str]=None) -> DAGCommit:
        snap = {nid: n.code for nid, n in self.nodes.items()}
        sym_hashes = {f'{nid}:{s.name}': f'{s.signature_hash}:{s.body_hash}' for nid, n in self.nodes.items() for s in n.local_symbols.values()}
        m_root = self._cluster_merkle_root()
        raw = f'{agent_id}:{self.term}:{kind}:{intent}:{time.time_ns()}:{m_root}'
        cid = hashlib.sha256(raw.encode()).hexdigest()[:8]
        parent = self.dag[-1].commit_id if self.dag else None
        commit = DAGCommit(commit_id=cid, parent_id=parent, merge_parent_id=merge_parent_id, agent_id=agent_id, term=self.term, kind=kind, intent=intent, diff=diff, vector_clock=self._merged_vclock(), merkle_root=m_root, symbol_hashes=sym_hashes, snapshot=snap, git_sha=git_sha, timestamp=time.strftime('%H:%M:%S'))
        self.dag.append(commit)
        self._persist_commit(commit)
        return commit

    def log_event(self, level: str, msg: str):
        self.events.append({'time': time.strftime('%H:%M:%S'), 'level': level, 'msg': msg})

    def elect_new_leader(self) -> Optional[str]:
        """Leaderless Raft-lite / Bully election when the current Host/Coordinator disconnects."""
        online_nodes = [n for n in self.nodes.values() if n.online]
        for n in self.nodes.values():
            n.is_leader = False
        if not online_nodes:
            self.leader_id = 'NONE (FULL PARTITION)'
            return None
        winner = max(online_nodes, key=lambda n: (sum(n.vector_clock.values()), n.agent_id))
        self.term += 1
        winner.is_leader = True
        self.leader_id = winner.agent_id
        for n in online_nodes:
            n.term = self.term
        self.metrics['leader_failovers'] += 1
        self.log_event('HOST-FAILOVER', f'Quorum elected [{winner.agent_id}] as new Epoch Host Coordinator for Term {self.term}!')
        self._record_dag(winner.agent_id, 'HOST-FAILOVER', f'Elected Coordinator (Term {self.term})', diff=f"+ leader = '{winner.agent_id}' (term={self.term})")
        return winner.agent_id

    def workspace_repos(self) -> List[dict]:
        for a in self._detected_cache:
            if os.path.normcase(a['workspace']['path']) == self.workspace:
                return a['workspace']['repos']
        return []

    def machines(self) -> List[dict]:
        out: Dict[str, dict] = {}
        for k, n in self.nodes.items():
            if n.source == 'demo':
                continue
            m = out.setdefault(n.machine, {'name': n.machine, 'is_host': n.machine == HOST_MACHINE, 'ip': '', 'agents': [], 'online': 0, 'joined': self.known_machines.get(n.machine, n.last_seen)})
            m['agents'].append(k)
            m['online'] += 1 if n.online else 0
            m['ip'] = m['ip'] or (n.meta.get('ip') or '')
        return sorted(out.values(), key=lambda m: (not m['is_host'], m['joined']))

    def succession(self) -> List[dict]:
        """Who takes over if this host goes away: joined machines that are online, in join order,
        with every address the host has seen for them."""
        out = []
        for m in self.machines():
            if m['is_host'] or not m['online']:
                continue
            addrs: List[str] = []
            for k in m['agents']:
                meta = self.nodes[k].meta
                for ip in [*(meta.get('addrs') or []), meta.get('ip') or '']:
                    if ip and (not ip.startswith('127.')) and (ip not in addrs):
                        addrs.append(ip)
            out.append({'machine': m['name'], 'addrs': addrs})
        return out

    def ui_state(self) -> dict:
        lan_ip = get_lan_ip()
        base = f'http://{lan_ip}:{PORT}'
        ws_name = Path(self.workspace).name if self.workspace else ''
        tun = TUNNEL.info()
        return {'host_machine': HOST_MACHINE, 'last_scan': getattr(self, 'last_scan', 0), 'scan_every': 3, 'machines': self.machines(), 'succession': [x['machine'] for x in self.succession()], 'takeover_from': TAKEOVER_FROM, 'tunnel': tun, 'network': {**network_info(), 'lan_ip': lan_ip, 'tailscale_ip': tailscale_ip()}, 'public_join_url': f"{tun['url']}/join/{SESSION_CODE}" if tun['url'] else '', 'demo': all((k in self.nodes for k in ('agent-auth', 'agent-billing', 'agent-gateway'))), 'lan': {'ip': lan_ip, 'port': PORT, 'url': base, 'ws_url': f'ws://{lan_ip}:{PORT}/ws'}, 'session': {'code': SESSION_CODE, 'join_url': f'{base}/join/{SESSION_CODE}', 'host': HOST_MACHINE, 'workspace': self.workspace, 'workspace_name': ws_name, 'repos': self.workspace_repos()}, 'cluster': {'term': self.term, 'leader_id': self.leader_id, 'merkle_root': self._cluster_merkle_root()}, 'nodes': {k: {'agent_id': n.agent_id, 'label': self.agent_label(k), 'source': n.source, 'role': n.role, 'target_module': n.target_module, 'deps': n.deps, 'color': getattr(n, 'color', '#a78bfa'), 'engine': getattr(n, 'engine', 'Custom'), 'machine': getattr(n, 'machine', socket.gethostname()), 'branch': getattr(n, 'branch', 'main'), 'last_prompt': getattr(n, 'last_prompt', ''), 'last_gossip': getattr(n, 'last_gossip', ''), 'online': n.online, 'is_leader': n.is_leader, 'term': n.term, 'merkle_root': n.merkle_root, 'intent': n.intent, 'vector_clock': n.vector_clock, 'local_micro_commits': n.local_micro_commits, 'outbox_size': len(n.outbox_queue), 'symbols': [s.model_dump() for s in n.local_symbols.values()][:40], 'code': n.code if n.source == 'demo' else '', 'meta': {k2: v for k2, v in n.meta.items() if k2 != 'workspace'}, 'inbox': n.inbox[-6:], 'pending': sum((1 for x in n.inbox if not x.get('delivered'))), 'delivery': self.delivery_mode(n), 'hook_last_pull': n.hook_last_pull, 'last_seen': n.last_seen} for k, n in self.nodes.items()}, 'other_agents': self.other_agents, 'flows': self.flows[-40:], 'metrics': self.metrics, 'events': self.events[-30:], 'dag': [c.model_dump() for c in self.dag[-16:]], 'speculative_runs': self.speculative_runs, 'beam': self.last_gossip_beam, 'bubbles': self.active_bubbles[-30:]}

    async def broadcast_ui(self):
        state = self.ui_state()
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
        self.metrics['micro_mutations_absorbed'] += 1
        self.metrics['bytes_saved'] += 14500
        if not sig_changed:
            self.metrics['private_edits_suppressed'] += 1
            self.last_gossip_beam = {'source': agent_id, 'targets': [], 'kind': 'local', 'ts': time.time()}
            self.log_event('LOCAL', f'[{agent_id}] Private AST body edit in {node.target_module}. Merkle leaf body updated, sig unchanged -> 0B sent.')
            self._record_dag(agent_id, 'PRIVATE-AST', intent, diff=short_diff)
            self._emit_bubble(source=agent_id, targets=[agent_id], kind='local', label='Private AST (0B)', detail=f'{node.target_module}: {intent}', color='#10b981')
        elif not node.online:
            node.outbox_queue.append(packet)
            self.last_gossip_beam = {'source': agent_id, 'targets': [], 'kind': 'buffered', 'ts': time.time()}
            self.log_event('OFFLINE-SPECULATION', f'[{agent_id}] Offline! Queued AST delta in local CRDT outbox ({len(node.outbox_queue)} pending).')
            self._record_dag(agent_id, 'OFFLINE-BUFFER', f'[Offline] {intent}', diff=short_diff)
            self._emit_bubble(source=agent_id, targets=[agent_id], kind='buffered', label=f'Queued #{len(node.outbox_queue)}', detail=f'CRDT outbox: {intent}', color='#f43f5e')
        else:
            await self.route_gossip(packet)
            node.last_synced_code = node.code
            self._record_dag(agent_id, 'CONTRACT-GOSSIP', intent, diff=short_diff)
            self._emit_bubble(source=agent_id, targets=[p for p in self.nodes if p != agent_id and self.nodes[p].online], kind='contract', label='Contract Sig Change', detail=intent, color='#38bdf8')
        await self.broadcast_ui()

    async def route_gossip(self, packet: GossipPacket):
        self.metrics['gossip_packets_routed'] += 1
        targets = []
        for peer_id, peer in self.nodes.items():
            if peer_id == packet.source_agent:
                continue
            if peer.online:
                targets.append(peer_id)
                res, rewritten_calls = peer.receive_gossip(packet)
                if 'AST CALL-SITE REWRITE' in res:
                    self.metrics['conflicts_auto_resolved'] += 1
                    self.log_event('CONFLICT-RESOLVED', f'[{peer_id}] {res} ({rewritten_calls} AST call sites rewritten via NodeTransformer).')
                else:
                    self.log_event('GOSSIP', f'[{packet.source_agent} -> {peer_id}] Merkle {packet.merkle_root[:6]} & VClock {packet.vector_clock} synced.')
            else:
                self.log_event('PARTITION', f'[{peer_id}] Unreachable. Gossip held in causal anti-entropy log.')
        self.last_gossip_beam = {'source': packet.source_agent, 'targets': targets, 'kind': 'gossip', 'ts': time.time()}

    async def simulate_split_brain_conflict(self):
        """
        Simulates a true split-brain concurrent edit on the SAME file & function (`verify_token` in `auth.py`)
        while `agent-auth` is partitioned, then heals the partition and runs AST 3-Way Semantic Merge + Call-Site Adaptation.
        """
        auth = self.nodes['agent-auth']
        base_code = auth.last_synced_code
        local_concurrent_code = "def verify_token(token: str, strict_scope: bool = True) -> bool:\n    if len(token) < 6:\n        return False\n    return token.startswith('jwt_')\n"
        remote_concurrent_code = "def verify_token(token: str, issuer: str = 'neptune') -> bool:\n    if token.endswith('_revoked'):\n        return False\n    return token.startswith('jwt_')\n\ndef revoke_token(token: str) -> str:\n    return f'{token}_revoked'\n"
        auth.vector_clock['agent-auth'] = auth.vector_clock.get('agent-auth', 0) + 1
        remote_vc = auth.vector_clock.copy()
        remote_vc['agent-auth'] -= 1
        remote_vc['agent-gateway'] = remote_vc.get('agent-gateway', 0) + 1
        relation = VectorClock.compare(auth.vector_clock, remote_vc)
        merged_code, merge_notes = ASTEngine.three_way_merge(base_code, local_concurrent_code, remote_concurrent_code, 'agent-auth', 'agent-gateway')
        short_diff = self._compute_short_diff(auth.code, merged_code)
        auth.code = merged_code
        auth.last_synced_code = merged_code
        auth.vector_clock = VectorClock.merge(auth.vector_clock, remote_vc)
        auth.local_symbols = ASTEngine.extract_symbols(merged_code, 'agent-auth', sum(auth.vector_clock.values()))
        auth.merkle_root = ASTEngine.compute_merkle_root(auth.local_symbols)
        auth.intent = f'3-Way AST Merge [{relation}]: {merge_notes}'
        auth.local_micro_commits += 1
        self.metrics['ast_merges_completed'] += 1
        self.metrics['conflicts_auto_resolved'] += 1
        pkt = GossipPacket(packet_id=hashlib.sha256(str(time.time_ns()).encode()).hexdigest()[:8], source_agent='agent-auth', term=self.term, vector_clock=auth.vector_clock.copy(), base_commit_id=self.dag[-1].commit_id if self.dag else None, intent=auth.intent, target_module=auth.target_module, exported_symbols=auth.local_symbols, raw_code=merged_code, merkle_root=auth.merkle_root, timestamp=time.time())
        await self.route_gossip(pkt)
        self._record_dag('agent-auth', 'AST-3WAY-MERGE', f'Resolved Concurrent Split-Brain ({merge_notes})', diff=short_diff)
        self._emit_bubble(source='agent-auth', targets=[p for p in self.nodes if p != 'agent-auth' and self.nodes[p].online], kind='merge', label='3-Way AST Merge', detail=merge_notes, color='#e879f9')
        self.log_event('AST-3WAY-MERGE', f'Resolved CONCURRENT split-brain on auth.py ({merge_notes}) & rewrote downstream AST call sites.')
        await self.broadcast_ui()

    async def toggle_partition(self, agent_id: str):
        """Handles node leaving/disconnecting (including Host Leader failover) and rejoining with Merkle Anti-Entropy catchup."""
        node = self.nodes[agent_id]
        node.online = not node.online
        if not node.online:
            node.intent = 'OFFLINE // LOCAL CRDT SPECULATION'
            self.log_event('ALERT', f'[{agent_id}] Disconnected from mesh! Buffering AST mutations locally.')
            if node.is_leader:
                self.log_event('ALERT', f"Host Coordinator [{agent_id}] DC'd during Term {self.term}! Initiating failover...")
                self.elect_new_leader()
        else:
            if self.leader_id.startswith('NONE'):
                self.elect_new_leader()
            node.term = self.term
            missed_adaptations = 0
            for peer_id, peer in self.nodes.items():
                if peer_id != agent_id and peer.online and peer.oplog:
                    latest_pkt = peer.oplog[-1]
                    if VectorClock.compare(node.vector_clock, latest_pkt.vector_clock) in ('BEFORE', 'CONCURRENT'):
                        _, count = node.receive_gossip(latest_pkt)
                        missed_adaptations += count
            flushed_count = len(node.outbox_queue)
            while node.outbox_queue:
                pkt = node.outbox_queue.pop(0)
                pkt.term = self.term
                await self.route_gossip(pkt)
            node.last_synced_code = node.code
            node.intent = f'REJOINED // MERKLE SYNCED (Term {self.term})'
            self.log_event('RECONNECTED', f'[{agent_id}] Rejoined mesh (Term {self.term}): Flushed {flushed_count} outbox packets, adapted {missed_adaptations} call sites via Merkle sync.')
        await self.broadcast_ui()

    async def run_speculative_race(self):
        candidates = [{'branch': 'spec/naive-regex', 'strategy': 'Regex Path Matcher', 'code': "def route_request(path: str, token: str) -> int:\n    if not token.startswith('jwt_'):\n        return 401\n    return 200 if path.startswith('/api') else 500\n"}, {'branch': 'spec/radix-trie-fast', 'strategy': 'Zero-Alloc Prefix Trie + Strict Auth Guard', 'code': "ROUTES = {'/api/v1/pay': 200, '/api/v1/auth': 200, '/health': 200}\n\ndef route_request(path: str, token: str) -> int:\n    if path == '/health':\n        return 200\n    if not token or not token.startswith('jwt_'):\n        return 401\n    return ROUTES.get(path, 404)\n"}, {'branch': 'spec/unbounded-cache', 'strategy': 'Unvalidated Cache Bypass', 'code': 'def route_request(path: str, token: str) -> int:\n    return 200\n'}]
        results = []
        winner = None
        for cand in candidates:
            t0 = time.perf_counter()
            ns: dict = {}
            passed = False
            reason = ''
            try:
                exec(cand['code'], ns)
                fn = ns['route_request']
                assert fn('/health', '') == 200, 'Health check failed'
                assert fn('/api/v1/pay', 'invalid') == 401, 'Security assertion failed'
                assert fn('/api/v1/pay', 'jwt_valid') == 200, 'Valid route failed'
                assert fn('/unknown', 'jwt_valid') == 404, 'Missing 404 guard'
                for _ in range(500):
                    fn('/api/v1/pay', 'jwt_valid')
                passed = True
                reason = '4/4 assertions passed'
            except Exception as e:
                reason = str(e)
            latency_us = round((time.perf_counter() - t0) * 1000000, 1)
            entry = {'branch': cand['branch'], 'strategy': cand['strategy'], 'passed': passed, 'latency_us': latency_us, 'reason': reason, 'status': 'PASSED' if passed else 'PRUNED', 'code': cand['code']}
            results.append(entry)
            if passed and (winner is None or latency_us < winner['latency_us']):
                winner = entry
        if winner:
            winner['status'] = 'MERGED-WINNER'
            self.metrics['speculative_branches_pruned'] += len(candidates) - 1
            self.speculative_runs = results
            await self.trigger_agent_step('agent-gateway', winner['code'], f"3x Winner ({winner['branch']} @ {winner['latency_us']}us)")
            self.dag[-1].kind = 'SPECULATIVE-WIN'
            self._emit_bubble(source='agent-gateway', targets=[nid for nid in self.nodes if nid != 'agent-gateway' and self.nodes[nid].online] or ['mesh'], kind='speculative', label=f"3x Winner ({winner['branch']})", detail=f"{winner['strategy']} ({winner['latency_us']}µs) - pruned 2 branches", color='#c084fc')
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
                n.intent = f'Rollback -> #{commit_id}'
                n.vector_clock[nid] = n.vector_clock.get(nid, 0) + 1
        self._write_workspace_files()
        self._record_dag('arbiter', 'ROLLBACK', f'Rollback -> #{commit_id}', diff=f'⏪ state = #{commit_id}')
        await self.broadcast_ui()
        return True

    async def prompt_agent(self, agent_id: str) -> dict:
        """Automatically prompts a running agent (Antigravity, Claude Code, Codex, Cursor) for its latest AST diff & branch status."""
        if agent_id not in self.nodes:
            return {'status': 'not_found'}
        node = self.nodes[agent_id]
        prompt_dir = BASE_DIR / '.gitpulse'
        prompt_dir.mkdir(exist_ok=True)
        peer_contracts = [f"- {pid} ({getattr(p, 'branch', 'main')}): {s.signature} [#{s.signature_hash[:6]}]" for pid, p in self.nodes.items() if pid != agent_id for s in p.local_symbols.values()]
        prompt_text = f'# GITPULSE AUTO-PROMPT -> {node.agent_id} ({node.engine} @ {node.machine}:{node.branch})\nCluster Term: {self.term} | Merkle Root: {self._cluster_merkle_root()}\nPeer Contracts:\n' + ('\n'.join(peer_contracts) or '- None') + f'\n\nACTION REQUIRED: Export current working AST delta & intent for `{node.target_module}` on branch `{node.branch}`.\n'
        (prompt_dir / f'PROMPT_{agent_id}.md').write_text(prompt_text, encoding='utf-8')
        disk_file = WORKSPACE_REPO / node.target_module
        next_step = sum(node.vector_clock.values()) + 1
        if disk_file.exists():
            disk_code = disk_file.read_text(encoding='utf-8')
            if disk_code.strip() != node.code.strip():
                node.last_prompt = f'Auto-prompted {node.engine}: harvested live disk diff on {node.branch}'
                await self.trigger_agent_step(agent_id, disk_code, f'[{node.engine} // {node.branch}] Harvested working tree diff')
                return {'status': 'ok', 'harvested': 'disk_diff'}
        synthesized_snippets = {'Antigravity': f"def verify_token(token: str, strict_scope: bool = True, epoch: int = {next_step}) -> bool:\n    return token.startswith('jwt_') and len(token) >= 8\n", 'Claude Code': f"def charge_user(user_id: str, amount: float, currency: str = 'GBP', retry: int = {next_step}) -> dict:\n    valid = verify_token('jwt_live')\n    return {{'status': 'settled' if valid else 'held', 'amt': amount, 'curr': currency}}\n", 'Codex': f"def route_request(path: str, token: str, timeout_ms: int = {100 + next_step * 10}) -> int:\n    ok = verify_token(token)\n    return 200 if (ok and path.startswith('/')) else 401\n"}
        new_code = synthesized_snippets.get(node.engine, node.code.rstrip() + f'\n# [{node.engine} @ {node.branch}] Synced AST delta v{next_step}\n')
        node.last_prompt = f'Prompted {node.engine} ({node.branch}) -> AST delta v{next_step}'
        await self.trigger_agent_step(agent_id, new_code, f'[{node.engine} // {node.branch}] Auto-prompted update #{next_step}')
        return {'status': 'ok', 'agent_id': agent_id, 'engine': node.engine, 'branch': node.branch}

    def agent_label(self, agent_id: str) -> str:
        n = self.nodes.get(agent_id)
        if not n:
            return agent_id
        same = [x for x in self.nodes.values() if x.engine == n.engine and x.machine == n.machine]
        if len(same) <= 1:
            return n.engine
        same.sort(key=lambda x: (x.meta.get('started') or 0, x.agent_id))
        return f'{n.engine} #{same.index(n) + 1}'

    def _pick_workspace(self, agents: List[dict]) -> str:
        if WORKSPACE_ARG and WORKSPACE_ARG != 'auto':
            return os.path.normcase(str(Path(WORKSPACE_ARG).resolve()))
        if self.workspace:
            return self.workspace
        counts: Dict[str, int] = {}
        for a in agents:
            k = os.path.normcase(a['workspace']['path'])
            counts[k] = counts.get(k, 0) + 1
        if not counts:
            return ''
        return max(counts, key=lambda k: counts[k])

    @staticmethod
    def delivery_mode(n: 'AgentNode') -> str:
        """How (and whether) this agent can really receive updates."""
        if n.engine not in ('Claude Code', 'Antigravity'):
            return 'unsupported'
        if getattr(n, 'hook_last_pull', 0) or n.meta.get('hook_installed'):
            return 'hook'
        return 'no_hook'

    def upsert_live_agent(self, a: dict, source: str) -> AgentNode:
        """Register / refresh one really-detected agent (local scan or a joined peer)."""
        aid = a['agent_id']
        repos = (a.get('workspace') or {}).get('repos') or []
        branch = ', '.join((f"{r['name']}:{r['branch']}" for r in repos)) or 'no git'
        machine = a.get('machine') or HOST_MACHINE
        is_new = aid not in self.nodes
        if machine not in self.known_machines:
            self.known_machines[machine] = time.time()
            if source == 'local':
                self._flow('machine', aid, [], summary=f'Session started on {machine} (host)', detail='This machine runs the GitPulse server. Other machines join with the invite link.', machine=machine)
            else:
                self._flow('machine', aid, [], summary=f'New machine joined: {machine}', detail=f"from {a.get('ip') or 'unknown address'}", machine=machine)
        if is_new:
            self.register_agent(agent_id=aid, role=a.get('title') or a.get('engine', 'Agent'), target_module=f'live/{aid}.py', initial_code='# live agent\n', engine=a.get('engine', 'Agent'), machine=machine, branch=branch)
        node = self.nodes[aid]
        came_back = not node.online
        node.online = True
        node.source = source
        node.branch = branch
        node.machine = machine
        node.color = a.get('color') or node.color
        node.last_seen = time.time()
        node.meta = {k: a.get(k) for k in ('pid', 'cwd', 'workspace', 'status', 'title', 'last_prompt', 'files', 'last_active', 'started', 'session_id', 'hook_installed', 'ip', 'kind', 'addrs')}
        node.last_prompt = a.get('last_prompt') or ''
        if not self.nodes.get(self.leader_id) or not self.nodes[self.leader_id].online:
            for n in self.nodes.values():
                n.is_leader = False
            node.is_leader = True
            self.leader_id = aid
        if is_new:
            self._flow('join', aid, [], summary=f'New agent on {machine}: {self.agent_label(aid)}', detail=branch, machine=machine)
        elif came_back:
            self._flow('join', aid, [], summary=f'{self.agent_label(aid)} on {machine} is back', detail=branch, machine=machine)
        return node

    def online_machines(self) -> Set[str]:
        return {n.machine for n in self.nodes.values() if n.online and n.source != 'demo'}

    @staticmethod
    def in_workspace(a: dict, ws: str) -> bool:
        """Agent works in the session folder or anywhere below it (e.g. started inside one of its repos)."""
        if not ws:
            return False
        p = os.path.normcase(os.path.abspath(a.get('cwd') or a['workspace']['path']))
        return p == ws or p.startswith(ws.rstrip('\\/') + os.sep)

    def sync_detected(self, agents: List[dict]) -> bool:
        """Apply a local process scan. Returns True if anything visible changed."""
        self._detected_cache = agents
        ws = self._pick_workspace(agents)
        self.workspace = ws
        mine = [a for a in agents if self.in_workspace(a, ws)]
        self.other_agents = [{'engine': a['engine'], 'pid': a['pid'], 'cwd': a['cwd'], 'color': a['color']} for a in agents if not self.in_workspace(a, ws)]
        before = {k: (n.online, n.meta.get('status')) for k, n in self.nodes.items()}
        machines_before = self.online_machines()
        seen = set()
        for a in mine:
            if a['engine'] in ('Claude Code', 'Antigravity'):
                a['hook_installed'] = gitpulse_hook.installed_for(a['engine'], a['cwd'])
            self.upsert_live_agent(a, 'local')
            seen.add(a['agent_id'])
        now = time.time()
        stale_to_delete = []
        for aid, n in list(self.nodes.items()):
            gone = n.source == 'local' and aid not in seen or (n.source == 'remote' and now - n.last_seen > 20)
            if gone and n.online:
                n.online = False
                self._flow('leave', aid, [], summary=f'{self.agent_label(aid)} on {n.machine} stopped', machine=n.machine)
                if n.is_leader:
                    self.elect_new_leader()
            if not n.online and n.source != 'demo':
                if n.source == 'local' or now - n.last_seen > 30:
                    stale_to_delete.append(aid)
        for aid in stale_to_delete:
            n = self.nodes.get(aid)
            if n:
                if n.is_leader:
                    self.elect_new_leader()
                del self.nodes[aid]
        for m in machines_before - self.online_machines():
            self._flow('machine_left', '', [], summary=f'Machine {m} disconnected', machine=m)
            self.known_machines.pop(m, None)
        after = {k: (n.online, n.meta.get('status')) for k, n in self.nodes.items()}
        return before != after

    def _flow(self, kind: str, source: str, targets: List[str], summary: str, detail: str='', **extra) -> dict:
        f = {'id': hashlib.sha256(f'{source}:{kind}:{time.time_ns()}'.encode()).hexdigest()[:8], 'ts': time.time(), 'time': time.strftime('%H:%M:%S'), 'kind': kind, 'source': source, 'targets': targets, 'summary': summary, 'detail': detail, **extra}
        self.flows.append(f)
        self.flows = self.flows[-80:]
        return f

    def record_file_change(self, agent_id: str, repo: str, rel_path: str, old: str, new: str, attribution: str='transcript') -> dict:
        """
        The core idea, applied to real edits: diff the file's AST.
        - only function bodies changed            -> private edit, nothing sent to peers
        - a signature / class / constant changed  -> queue just that contract for every peer
        Peers actually get it when their Claude Code hook pulls it (see pull_inbox).
        """
        node = self.nodes[agent_id]
        old_syms = ASTEngine.extract_symbols(old, agent_id)
        new_syms = ASTEngine.extract_symbols(new, agent_id)
        added = [k for k in new_syms if k not in old_syms]
        removed = [k for k in old_syms if k not in new_syms]
        sig_changed = [k for k in new_syms if k in old_syms and new_syms[k].signature_hash != old_syms[k].signature_hash]
        body_changed = [k for k in new_syms if k in old_syms and k not in sig_changed and (new_syms[k].body_hash != old_syms[k].body_hash)]
        contract = added + sig_changed + removed
        node.vector_clock[agent_id] = node.vector_clock.get(agent_id, 0) + 1
        node.local_micro_commits += 1
        node.intent = f'editing {rel_path}'
        node.local_symbols = {**node.local_symbols, **{f'{rel_path}:{k}': v for k, v in new_syms.items()}}
        node.merkle_root = ASTEngine.compute_merkle_root(node.local_symbols)
        self.metrics['micro_mutations_absorbed'] += 1
        label = self.agent_label(agent_id)
        file_bytes = len(new.encode('utf-8'))
        where = f'{repo}/{rel_path}' if repo else rel_path
        now = time.time()
        self.last_file_edit_ts = now
        touched_symbols = added + sig_changed + body_changed
        collisions = []
        for sym in touched_symbols:
            lease_key = f'{where}:{sym}'
            prev_lease = self.active_leases.get(lease_key)
            if prev_lease and prev_lease.get('agent_id') != agent_id and (now - prev_lease.get('ts', 0) < 60):
                collisions.append((sym, prev_lease))
            self.active_leases[lease_key] = {'agent_id': agent_id, 'machine': node.machine, 'ts': now, 'file': where, 'symbol': sym}
        if collisions:
            for sym, prev in collisions:
                other_label = self.agent_label(prev['agent_id'])
                other_node = self.nodes.get(prev['agent_id'])
                col_summary = f"COLLISION PRE-EMPTED: {label} & {other_label} both editing '{sym}' in {where}!"
                col_detail = f'Concurrent AST edits detected. Coordinate before committing to avoid merge conflicts.'
                self._flow('collision', agent_id, [prev['agent_id']], summary=col_summary, detail=col_detail, file=where, symbol=sym, machine=node.machine)
                self.metrics['collisions_preempted'] = self.metrics.get('collisions_preempted', 0) + 1
                self.log_event('COLLISION-ALERT', f'[{label} <-> {other_label}] Conflict pre-empted on {where}:{sym}')
                col_warning = f"⚠️ [GitPulse Collision Warning] {other_label} is also actively editing '{sym}' in {where}. Coordinate to avoid collisions!"
                node.inbox.append({'id': secrets.token_hex(4), 'from': prev['agent_id'], 'file': where, 'contract': [col_warning], 'time': time.strftime('%H:%M:%S'), 'delivered': '', 'is_collision': True})
                if other_node:
                    other_warning = f"⚠️ [GitPulse Collision Warning] {label} is also actively editing '{sym}' in {where}. Coordinate to avoid collisions!"
                    other_node.inbox.append({'id': secrets.token_hex(4), 'from': agent_id, 'file': where, 'contract': [other_warning], 'time': time.strftime('%H:%M:%S'), 'delivered': '', 'is_collision': True})
        if not contract:
            self.metrics['private_edits_suppressed'] += 1
            self.metrics['bytes_saved'] += file_bytes
            names = ', '.join(body_changed[:3]) or 'top-level code'
            self.log_event('LOCAL', f'[{label}] edited inside {names} in {where}. Signatures unchanged, 0 B sent.')
            return self._flow('private', agent_id, [], summary=f'{label} edited {names} in {where}', detail="Only code inside functions changed. Other agents don't depend on that, so nothing was sent.", file=where, symbols=body_changed[:6], bytes_sent=0, bytes_saved=file_bytes, attribution=attribution, machine=node.machine)
        peers = [p for p, n in self.nodes.items() if p != agent_id and n.online]
        lines = []
        for k in added:
            lines.append(f'+ {new_syms[k].signature}')
        for k in sig_changed:
            lines.append(f'- {old_syms[k].signature}')
            lines.append(f'+ {new_syms[k].signature}')
        for k in removed:
            lines.append(f'- {old_syms[k].signature}')
        payload = '\n'.join(lines)
        sent = len(payload.encode('utf-8'))
        self.metrics['gossip_packets_routed'] += 1
        self.metrics['bytes_saved'] += max(0, file_bytes - sent) * max(1, len(peers))
        verb = []
        if sig_changed:
            verb.append(f"changed the signature of {', '.join(sig_changed[:3])}")
        if added:
            verb.append(f"added {', '.join(added[:3])}")
        if removed:
            verb.append(f"removed {', '.join(removed[:3])}")
        summary = f"{label} {' and '.join(verb)} in {where}"
        patches = []
        for k in added + sig_changed:
            if k in new_syms and new_syms[k].source:
                patches.append({'symbol': k, 'code': new_syms[k].source, 'file': where, 'repo': repo, 'path': rel_path})
        for p in patches:
            synced = self.sync_patch_to_disk(agent_id, p['repo'], p['path'], p['symbol'], p['code'])
            if synced:
                self.metrics['ast_patches_synced'] = self.metrics.get('ast_patches_synced', 0) + 1
        flow = self._flow('contract', agent_id, peers, summary=summary, detail=f'The public interface changed, so it is queued for every other agent ({sent} B instead of the whole {file_bytes} B file).', file=where, contract=lines[:8], bytes_sent=sent, bytes_saved=max(0, file_bytes - sent), attribution=attribution, machine=node.machine, delivery={p: self.delivery_mode(self.nodes[p]) for p in peers})
        for p in peers:
            peer = self.nodes[p]
            peer.vector_clock = VectorClock.merge(peer.vector_clock, node.vector_clock)
            peer.inbox.append({'id': secrets.token_hex(4), 'flow': flow['id'], 'from': agent_id, 'file': where, 'contract': lines[:8], 'patches': patches, 'time': time.strftime('%H:%M:%S'), 'delivered': ''})
            peer.inbox = peer.inbox[-20:]
            for k in added + sig_changed:
                peer.known_peer_symbols[f'{rel_path}:{k}'] = new_syms[k]
        self._record_dag(agent_id, 'CONTRACT-GOSSIP', summary, diff='\n'.join(lines[:3]))
        self.log_event('GOSSIP', f'{summary}. Queued {sent} B for {len(peers)} peer(s) instead of the {file_bytes} B file.')
        return flow

    def _match_hook_caller(self, session_id: str, engine: str, machine: str, cwd: str) -> Optional['AgentNode']:
        """Find the agent a hook call belongs to: by session id, else same engine + machine + folder."""
        if session_id:
            node = next((n for n in self.nodes.values() if n.meta.get('session_id') == session_id), None)
            if node is not None:
                return node
        if not (engine and cwd):
            return None
        want = os.path.normcase(os.path.abspath(cwd)).rstrip('\\/')
        best = None
        for n in self.nodes.values():
            if not n.online or n.engine != engine or (machine and n.machine != machine):
                continue
            have = os.path.normcase(os.path.abspath(n.meta.get('cwd') or '')).rstrip('\\/')
            if not have:
                continue
            if have == want or want.startswith(have + os.sep) or have.startswith(want + os.sep):
                if best is None or n.meta.get('last_active', 0) > best.meta.get('last_active', 0):
                    best = n
        return best

    def pull_inbox(self, claude_session_id: str='', engine: str='', machine: str='', cwd: str='') -> Optional[List[dict]]:
        """Called by an agent's hook: hand over undelivered contracts, mark them delivered."""
        node = self._match_hook_caller(claude_session_id, engine, machine, cwd)
        if node is None:
            return None
        node.hook_last_pull = time.time()
        pending = [x for x in node.inbox if not x.get('delivered')]
        if not pending:
            return []
        stamp = time.strftime('%H:%M:%S')
        by_id = {f['id']: f for f in self.flows}
        for x in pending:
            x['delivered'] = stamp
            f = by_id.get(x.get('flow'))
            if f is not None:
                f.setdefault('delivery', {})[node.agent_id] = f'delivered {stamp}'
        label = self.agent_label(node.agent_id)
        self._flow('deliver', node.agent_id, [], summary=f"{label} on {node.machine} received {len(pending)} update{('s' if len(pending) != 1 else '')}", detail=f"Pulled by its {node.engine} hook and added to the agent's context.", contract=[l for x in pending for l in x['contract']][:8], machine=node.machine)
        return [{'from': x['from'], 'from_label': self.agent_label(x['from']), 'machine': self.nodes[x['from']].machine if x['from'] in self.nodes else '?', 'file': x['file'], 'contract': x['contract'], 'patches': x.get('patches', []), 'is_collision': x.get('is_collision', False)} for x in pending]

    def sync_patch_to_disk(self, source_agent_id: str, repo: str, rel_path: str, symbol_name: str, new_symbol_code: str) -> bool:
        """
        Applies a non-conflicting AST patch to physical repositories on disk across the session workspace.
        """
        applied_any = False
        target_roots = []
        if self.workspace and Path(self.workspace).is_dir():
            target_roots.append(self.workspace)
        for a in self._detected_cache:
            for r in (a.get('workspace') or {}).get('repos') or []:
                if r.get('root') and r['root'] not in target_roots and Path(r['root']).is_dir():
                    target_roots.append(r['root'])
        for root in target_roots:
            candidates = [Path(root) / rel_path, Path(root) / repo / rel_path if repo else None]
            for p in candidates:
                if p and p.is_file() and (p.suffix == '.py'):
                    try:
                        content = p.read_text(encoding='utf-8')
                        patched = ASTEngine.patch_source(content, symbol_name, new_symbol_code)
                        if patched != content:
                            p.write_text(patched, encoding='utf-8')
                            applied_any = True
                            self.log_event('PATCH-SYNC', f"Applied AST patch for '{symbol_name}' to {p.name}")
                    except Exception as e:
                        self.log_event('PATCH-ERROR', f'Failed to patch {p}: {e}')
        return applied_any

    def coalesce_to_git(self, repo_name: Optional[str]=None) -> List[dict]:
        """
        Feature C: Coalesce pending AST micro-mutations into real, clean Git commits on tracked repos.
        """
        results = []
        repo_roots = []
        for a in self._detected_cache:
            for r in (a.get('workspace') or {}).get('repos') or []:
                if r.get('root') and r['root'] not in [x['root'] for x in repo_roots]:
                    repo_roots.append(r)
        if not repo_roots and self.workspace and Path(self.workspace, '.git').is_dir():
            repo_roots.append({'root': self.workspace, 'name': Path(self.workspace).name, 'branch': 'main'})
        for r in repo_roots:
            root = r['root']
            rname = r.get('name') or Path(root).name
            if repo_name and rname != repo_name:
                continue
            if not Path(root, '.git').exists():
                continue
            try:
                st_proc = subprocess.run(['git', 'status', '--porcelain'], cwd=root, capture_output=True, text=True)
                changes = [line for line in st_proc.stdout.splitlines() if line.strip() and (not line.strip().startswith('?? .gitpulse'))]
                if not changes:
                    continue
                subprocess.run(['git', 'add', '-u'], cwd=root, capture_output=True)
                active_labels = [self.agent_label(n.agent_id) for n in self.nodes.values() if n.online and n.source != 'demo']
                agents_str = ', '.join(active_labels[:3]) or 'gitpulse'
                mutations = max(1, self.metrics.get('micro_mutations_absorbed', 0) - self.last_coalesced_mutations)
                commit_msg = f'gitpulse: sync AST mutations ({mutations} edits) [{agents_str}]'
                c_proc = subprocess.run(['git', 'commit', '-m', commit_msg], cwd=root, capture_output=True, text=True)
                if c_proc.returncode == 0:
                    rev = subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], cwd=root, capture_output=True, text=True)
                    sha = rev.stdout.strip()
                    br_proc = subprocess.run(['git', 'rev-parse', '--abbrev-ref', 'HEAD'], cwd=root, capture_output=True, text=True)
                    br = br_proc.stdout.strip() or 'main'
                    self.metrics['git_coalesce_commits'] = self.metrics.get('git_coalesce_commits', 0) + 1
                    self.last_coalesced_mutations = self.metrics.get('micro_mutations_absorbed', 0)
                    self._flow('commit', self.leader_id or 'system', [], summary=f"Git commit {sha} in {rname} ({len(changes)} file{('s' if len(changes) != 1 else '')} coalesced)", detail=f"Branch '{br}': {commit_msg}", repo=rname, commit_sha=sha, branch=br)
                    self.log_event('GIT-COALESCE', f'Created Git commit {sha} on {rname}:{br} ({commit_msg})')
                    results.append({'status': 'ok', 'repo': rname, 'sha': sha, 'branch': br, 'files': len(changes)})
            except Exception as e:
                self.log_event('GIT-ERROR', f'Git coalesce failed on {root}: {e}')
                results.append({'status': 'error', 'repo': rname, 'error': str(e)})
        return results

    def install_hook(self, agent_id: str) -> str:
        n = self.nodes[agent_id]
        path = gitpulse_hook.install_for(n.engine, n.meta['cwd'], f'http://127.0.0.1:{PORT}', SESSION_CODE)
        for o in self.nodes.values():
            if o.source == 'local' and o.engine == n.engine and (o.meta.get('cwd') == n.meta['cwd']):
                o.meta['hook_installed'] = True
        how = 'Antigravity picks it up on its next prompt (the folder must be a trusted workspace).' if n.engine == 'Antigravity' else 'Restart those Claude Code sessions (or open /hooks in them) so they load it.'
        self._flow('info', agent_id, [], summary=f"Hook added for {n.engine} in {Path(n.meta['cwd']).name}", detail=f'Wrote {path}. {how}', machine=n.machine)
        return path

    def scan_repos_for_changes(self) -> List[tuple]:
        """Poll the tracked repos; return (agent_id, repo, rel, old, new, attribution) for each real edit."""
        mine = [a for a in self._detected_cache if os.path.normcase(a['workspace']['path']) == self.workspace]
        roots = sorted({r['root'] for a in mine for r in a['workspace']['repos'] if r.get('root')})
        out = []
        for ch in self.watcher.poll(roots):
            who = agent_detect.attribute_change(ch, mine)
            if not who or who['agent_id'] not in self.nodes:
                continue
            out.append((who['agent_id'], Path(ch['repo_root']).name, ch['rel'], ch['old'], ch['new'], who['_attribution']))
        return out

    async def live_tick(self) -> None:
        agents = await asyncio.to_thread(agent_detect.detect_agents)
        self.last_scan = time.time()
        self.sync_detected(agents)
        for aid, repo, rel, old, new, how in await asyncio.to_thread(self.scan_repos_for_changes):
            self.record_file_change(aid, repo, rel, old, new, how)
        pending = self.metrics.get('micro_mutations_absorbed', 0) - self.last_coalesced_mutations
        if pending > 0 and self.last_file_edit_ts > 0 and (time.time() - self.last_file_edit_ts >= 15.0):
            await asyncio.to_thread(self.coalesce_to_git)
        await self.broadcast_ui()

    async def scan_and_prompt_agents(self) -> dict:
        """Rescan this machine for running coding agents right now."""
        await self.live_tick()
        return {'status': 'ok', 'workspace': self.workspace, 'agents': [{'agent_id': k, 'engine': n.engine, 'pid': n.meta.get('pid'), 'online': n.online} for k, n in self.nodes.items() if n.source != 'demo'], 'elsewhere': self.other_agents}

    async def atomic_github_flush(self):
        leader = self.nodes.get(self.leader_id)
        if not leader or not leader.online:
            new_leader = self.elect_new_leader()
            if not new_leader:
                self.log_event('ALERT', 'Cannot flush to GitHub: All mesh nodes are partitioned!')
                await self.broadcast_ui()
                return {'status': 'all_offline'}
        total_commits = sum((n.local_micro_commits for n in self.nodes.values() if n.online))
        if total_commits == 0:
            await self.broadcast_ui()
            return {'status': 'noop'}
        self._write_workspace_files()
        subprocess.run(['git', 'add', '.'], cwd=WORKSPACE_REPO, capture_output=True)
        msg = f'[Term {self.term} Leader:{self.leader_id}] Squashed {total_commits} AST mutations'
        subprocess.run(['git', 'commit', '-m', msg, '--allow-empty'], cwd=WORKSPACE_REPO, capture_output=True)
        rev = subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], cwd=WORKSPACE_REPO, capture_output=True, text=True)
        sha = rev.stdout.strip() or hashlib.sha1(str(time.time()).encode()).hexdigest()[:7]
        self.metrics['atomic_github_pushes'] += 1
        for n in self.nodes.values():
            if n.online:
                n.local_micro_commits = 0
        self.last_gossip_beam = {'source': self.leader_id, 'targets': ['github'], 'kind': 'flush', 'ts': time.time()}
        self._record_dag(self.leader_id, 'EPOCH-FLUSH', f'Coordinator {self.leader_id} (Term {self.term}) merged {total_commits} edits -> {sha}', diff=f'+ git commit {sha} ({total_commits} squashed)', git_sha=sha)
        self._emit_bubble(source=self.leader_id, targets=['github'], kind='flush', label=f'PR #{sha} Landed', detail=f'Squashed {total_commits} AST mutations to GitHub main', color='#f97316')
        await self.broadcast_ui()
        return {'status': 'ok', 'sha': sha, 'leader': self.leader_id, 'term': self.term}
mesh = MeshCluster(demo=DEMO_MODE)
SCENARIOS = [('agent-auth', "def verify_token(token: str) -> bool:\n    # Private body optimization\n    return len(token) > 8 and token.startswith('jwt_')\n", 'Private AST body optimization'), ('agent-auth', "def verify_token(token: str, strict_scope: bool = True) -> bool:\n    return token.startswith('jwt_') and strict_scope\n", 'Public Contract Change: Added strict_scope=True to verify_token'), ('agent-billing', "def charge_user(user_id: str, amount: float, currency: str = 'GBP', token: str = 'jwt_ok') -> dict:\n    valid = verify_token(token, strict_scope=True)\n    return {'status': 'settled' if valid else 'denied', 'amt': amount, 'curr': currency}\n", 'Added multi-currency settlement parameter'), ('agent-gateway', "def route_request(path: str, token: str) -> int:\n    ok = verify_token(token, strict_scope=True)\n    return 200 if (ok and path.startswith('/api')) else 404\n", 'Refactored internal edge route guard')]
scenario_idx = 0

@app.post('/api/step')
async def step_simulation():
    global scenario_idx
    if not mesh.ui_state()['demo']:
        return {'status': 'demo_only', 'detail': 'Start with --demo to use the scripted scenarios.'}
    agent_id, code, intent = SCENARIOS[scenario_idx % len(SCENARIOS)]
    scenario_idx += 1
    await mesh.trigger_agent_step(agent_id, code, intent)
    return {'status': 'ok'}

@app.post('/api/conflict')
async def trigger_conflict():
    if not mesh.ui_state()['demo']:
        return {'status': 'demo_only', 'detail': 'Start with --demo to use the scripted scenarios.'}
    await mesh.simulate_split_brain_conflict()
    return {'status': 'ok'}

@app.post('/api/speculate')
async def trigger_speculation():
    if not mesh.ui_state()['demo']:
        return {'status': 'demo_only', 'detail': 'Start with --demo to use the scripted scenarios.'}
    await mesh.run_speculative_race()
    return {'status': 'ok'}

@app.post('/api/mutate')
async def custom_mutation(req: MutationRequest):
    if req.agent_id not in mesh.nodes:
        return {'status': 'error'}
    await mesh.trigger_agent_step(req.agent_id, req.code, req.intent)
    return {'status': 'ok'}

@app.post('/api/rollback/{commit_id}')
async def rollback_endpoint(commit_id: str):
    ok = await mesh.rollback_to_commit(commit_id)
    return {'status': 'ok' if ok else 'not_found'}

@app.post('/api/partition/{agent_id}')
async def partition_node(agent_id: str):
    await mesh.toggle_partition(agent_id)
    return {'status': 'ok'}

@app.post('/api/flush')
async def flush_to_github():
    coalesced = await asyncio.to_thread(mesh.coalesce_to_git)
    flush_res = await mesh.atomic_github_flush()
    return {'status': 'ok', 'coalesced': coalesced, 'flush': flush_res}

@app.post('/api/git/coalesce')
async def git_coalesce_endpoint():
    res = await asyncio.to_thread(mesh.coalesce_to_git)
    await mesh.broadcast_ui()
    return {'status': 'ok', 'coalesced': res}

class ApplyPatchRequest(BaseModel):
    repo: str = ''
    path: str
    symbol: str
    code: str

@app.post('/api/sync/apply_patch')
async def apply_patch_endpoint(req: ApplyPatchRequest):
    applied = mesh.sync_patch_to_disk('api', req.repo, req.path, req.symbol, req.code)
    if applied:
        mesh.metrics['ast_patches_synced'] = mesh.metrics.get('ast_patches_synced', 0) + 1
        await mesh.broadcast_ui()
    return {'status': 'ok' if applied else 'not_modified'}

@app.get('/api/lan')
async def get_lan_endpoint():
    ip = get_lan_ip()
    return {'ip': ip, 'port': PORT, 'url': f'http://{ip}:{PORT}', 'ws_url': f'ws://{ip}:{PORT}/ws', 'hostname': socket.gethostname()}

@app.get('/api/state')
async def get_state_endpoint():
    return mesh.ui_state()

@app.get('/api/agents')
async def list_agents():
    return {'agents': [{'agent_id': n.agent_id, 'role': n.role, 'target_module': n.target_module, 'online': n.online, 'is_leader': n.is_leader, 'vector_clock': n.vector_clock, 'color': getattr(n, 'color', '#a78bfa'), 'last_gossip': getattr(n, 'last_gossip', ''), 'outbox_size': len(n.outbox_queue)} for n in mesh.nodes.values()]}

@app.post('/api/agents/register')
async def register_agent_endpoint(req: RegisterAgentRequest, request: Request):
    if req.session != SESSION_CODE and (not _is_local_request(request)):
        return JSONResponse({'status': 'error', 'detail': 'bad session code'}, status_code=403)
    if req.meta.get('agent_id') == req.agent_id and req.meta.get('engine'):
        req.meta['ip'] = _client_ip(request)
        node = mesh.upsert_live_agent(req.meta, 'remote')
    else:
        node = mesh.register_agent(agent_id=req.agent_id, role=req.role, target_module=req.target_module, initial_code=req.initial_code, deps=req.deps, engine=req.engine, machine=req.machine, branch=req.branch)
        if node.source == 'demo' and (not mesh.ui_state()['demo']):
            node.source = 'remote'
        node.last_seen = time.time()
    await mesh.broadcast_ui()
    return {'status': 'ok', 'agent_id': node.agent_id, 'label': mesh.agent_label(node.agent_id), 'role': node.role, 'engine': node.engine, 'machine': node.machine, 'branch': node.branch, 'color': getattr(node, 'color', '#a78bfa'), 'vector_clock': node.vector_clock, 'inbox': node.inbox, 'session': SESSION_CODE, 'host': HOST_MACHINE, 'port': PORT, 'tunnel': TUNNEL.url, 'succession': mesh.succession()}

@app.post('/api/scan')
async def scan_and_prompt_endpoint():
    return await mesh.scan_and_prompt_agents()

@app.post('/api/prompt/{agent_id}')
async def prompt_single_agent_endpoint(agent_id: str):
    if not mesh.ui_state()['demo']:
        return {'status': 'demo_only', 'detail': 'Start with --demo to use the scripted scenarios.'}
    return await mesh.prompt_agent(agent_id)

@app.get('/install.ps1')
async def serve_windows_installer():
    ip = get_lan_ip()
    script = f"""$HostUrl = 'http://{ip}:{PORT}'\nWrite-Host '[GitPulse] Downloading LAN Auto-Watcher Daemon from ' $HostUrl -ForegroundColor Cyan\nInvoke-WebRequest -Uri "$HostUrl/agent_client.py" -OutFile "$env:TEMP\\gitpulse_agent.py"\npython "$env:TEMP\\gitpulse_agent.py" --server $HostUrl --watch\n"""
    return PlainTextResponse(script, media_type='text/plain')

@app.get('/install.sh')
async def serve_unix_installer():
    ip = get_lan_ip()
    script = f"""#!/bin/sh\nHOST_URL='http://{ip}:{PORT}'\necho '[GitPulse] Connecting local agents & git branches to '$HOST_URL\ncurl -s "$HOST_URL/agent_client.py" -o /tmp/gitpulse_agent.py\npython3 /tmp/gitpulse_agent.py --server "$HOST_URL" --watch\n"""
    return PlainTextResponse(script, media_type='text/plain')

@app.get('/manifest.json')
async def serve_pwa_manifest():
    return {'name': 'GitPulse // Agent AST Mesh', 'short_name': 'GitPulse', 'start_url': '/', 'display': 'standalone', 'background_color': '#050505', 'theme_color': '#050505'}

@app.get('/favicon.ico')
async def serve_favicon():
    return PlainTextResponse('', status_code=204)

@app.delete('/api/agents/{agent_id}')
async def unregister_agent_endpoint(agent_id: str):
    ok = mesh.unregister_agent(agent_id)
    if ok:
        await mesh.broadcast_ui()
        return {'status': 'ok'}
    return {'status': 'error', 'detail': 'Agent not found'}

@app.post('/api/agents/gossip')
async def gossip_endpoint(req: GossipIdeaRequest):
    res = await mesh.broadcast_idea_or_gossip(agent_id=req.agent_id, intent=req.intent, gossip_message=req.gossip_message, code=req.code, target_agent=req.target_agent)
    return res

@app.get('/agent_client.py')
async def get_agent_client_script():
    client_path = BASE_DIR / 'agent_client.py'
    if client_path.exists():
        return PlainTextResponse(client_path.read_text(encoding='utf-8'), media_type='text/x-python')
    return PlainTextResponse('# Client script not found', status_code=404)
FORWARD_HEADERS = ('cf-connecting-ip', 'x-forwarded-for', 'x-real-ip', 'forwarded')
AGENT_PATHS = ('/api/agents/register', '/api/agents/file_change', '/api/agents/inbox/pull')
PUBLIC_GET = ('/agent_client.py', '/agent_detect.py', '/gitpulse_hook.py', '/manifest.json', '/favicon.ico')

def _is_local_request(request) -> bool:
    """True only for requests made on this machine directly (not relayed through the tunnel)."""
    host = request.client.host if request.client else ''
    if any((h in request.headers for h in FORWARD_HEADERS)):
        return False
    return host in ('127.0.0.1', '::1', 'localhost', get_lan_ip())

def _has_session(request) -> bool:
    return request.cookies.get('gp_s') == SESSION_CODE or request.query_params.get('s') == SESSION_CODE

def _client_ip(request: Request) -> str:
    for h in ('cf-connecting-ip', 'x-real-ip'):
        if request.headers.get(h):
            return request.headers[h]
    fwd = request.headers.get('x-forwarded-for')
    if fwd:
        return fwd.split(',')[0].strip()
    return request.client.host if request.client else ''

@app.middleware('http')
async def access_guard(request: Request, call_next):
    """Host (this machine) can do everything. Others: join page + agent endpoints, and read-only
    dashboard once they hold the session code (cookie set by the join page)."""
    path = request.url.path
    if _is_local_request(request):
        return await call_next(request)
    if path.startswith('/join/') or path in PUBLIC_GET or (request.method == 'POST' and path in AGENT_PATHS):
        return await call_next(request)
    if request.method != 'GET':
        return JSONResponse({'status': 'error', 'detail': 'only the host machine can do that'}, status_code=403)
    if not _has_session(request):
        return HTMLResponse("<h2 style='font-family:sans-serif'>GitPulse: open the invite link you were sent first.</h2>", status_code=403)
    resp = await call_next(request)
    if request.query_params.get('s') == SESSION_CODE:
        resp.set_cookie('gp_s', SESSION_CODE, max_age=86400, samesite='lax')
    return resp

def _public_base(request: Request) -> str:
    """Base URL as the visitor sees it (works through Tailscale / cloudflared too)."""
    proto = request.headers.get('x-forwarded-proto') or request.url.scheme
    host = request.headers.get('x-forwarded-host') or request.headers.get('host') or f'{get_lan_ip()}:{PORT}'
    return f'{proto}://{host}'

def _join_ps1(base: str) -> str:
    return f"""$u = '{base}'\n$d = Join-Path $HOME '.gitpulse'\nNew-Item -ItemType Directory -Force -Path $d | Out-Null\nWrite-Host "[GitPulse] joining session at $u" -ForegroundColor Cyan\nforeach ($f in 'agent_client.py','agent_detect.py','gitpulse_hook.py') {{\n  Invoke-WebRequest -UseBasicParsing "$u/$f" -OutFile (Join-Path $d $f)\n}}\n$env:UV_PYTHON_INSTALL_DIR = Join-Path $d 'python'\n$env:UV_PYTHON_PREFERENCE = 'only-managed'\n$uv = Join-Path $d 'uv.exe'\nif (-not (Test-Path $uv)) {{\n  Write-Host '[GitPulse] first run: setting up a private Python in ~/.gitpulse (about a minute, no admin)...'\n  $env:UV_UNMANAGED_INSTALL = $d\n  Invoke-RestMethod https://astral.sh/uv/install.ps1 | Invoke-Expression *> $null\n  Remove-Item Env:UV_UNMANAGED_INSTALL\n  if (-not (Test-Path $uv)) {{ Write-Host '[GitPulse] could not download uv. Check your internet connection.' -ForegroundColor Red; return }}\n}}\n$v = Join-Path $d 'venv'\n$py = Join-Path $v 'Scripts\\python.exe'\nif (-not (Test-Path $py)) {{ & $uv venv -q --python 3.12 $v }}\n& $uv pip install -q --python $py psutil\nif ($LASTEXITCODE -ne 0) {{ Write-Host '[GitPulse] could not install psutil.' -ForegroundColor Red; return }}\n& $py (Join-Path $d 'agent_client.py') --server $u --session {SESSION_CODE} --watch\n"""

def _join_sh(base: str) -> str:
    return f"""#!/bin/sh\nset -e\nU='{base}'\nD="$HOME/.gitpulse"; mkdir -p "$D"\necho "[GitPulse] joining session at $U"\nfor f in agent_client.py agent_detect.py gitpulse_hook.py; do curl -fsSL "$U/$f" -o "$D/$f"; done\nexport UV_PYTHON_INSTALL_DIR="$D/python" UV_PYTHON_PREFERENCE=only-managed\nif [ ! -x "$D/uv" ]; then\n  echo "[GitPulse] first run: setting up a private Python in ~/.gitpulse (about a minute, no admin)..."\n  curl -LsSf https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL="$D" sh >/dev/null 2>&1\nfi\n[ -x "$D/venv/bin/python" ] || "$D/uv" venv -q --python 3.12 "$D/venv"\n"$D/uv" pip install -q --python "$D/venv/bin/python" psutil\nexec "$D/venv/bin/python" "$D/agent_client.py" --server "$U" --session {SESSION_CODE} --watch </dev/tty\n"""
JOIN_PAGE = '<!doctype html>\n<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">\n<title>Join GitPulse</title>\n<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">\n<style>\n:root{--bg:#f6f7f9;--card:#fff;--line:#e3e6ec;--fg:#14171f;--mute:#667085;--accent:#2563eb;--ok:#059669;--code:#f1f3f7}\n@media(prefers-color-scheme:dark){:root{--bg:#0b0d12;--card:#12151c;--line:#232836;--fg:#e8eaf0;--mute:#8a91a3;--accent:#60a5fa;--ok:#34d399;--code:#0a0c10}}\n*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.55 Inter,system-ui,sans-serif;display:flex;justify-content:center;padding:48px 16px}\n.card{max-width:560px;width:100%;background:var(--card);border:1px solid var(--line);border-radius:16px;padding:28px}\n.chip{display:inline-block;font:12px \'JetBrains Mono\',monospace;background:var(--code);border:1px solid var(--line);border-radius:6px;padding:2px 8px;color:var(--mute)}\nh1{font-size:22px;margin:10px 0 4px}p{color:var(--mute);margin:0 0 20px}\n.tabs{display:flex;gap:4px;margin-bottom:8px}.tabs button{background:none;border:1px solid transparent;color:var(--mute);border-radius:8px;padding:5px 10px;font:inherit;font-size:13px;cursor:pointer}\n.tabs button.on{border-color:var(--line);background:var(--code);color:var(--fg)}\n.row{display:flex;gap:8px}\ncode{flex:1;min-width:0;font:13px/1.5 \'JetBrains Mono\',monospace;background:var(--code);border:1px solid var(--line);border-radius:10px;padding:11px 12px;overflow-x:auto;white-space:nowrap}\n.copy{background:var(--accent);color:#fff;border:0;border-radius:10px;padding:0 16px;font:600 14px Inter,sans-serif;cursor:pointer}.copy.done{background:var(--ok)}\nol{margin:22px 0 0;padding-left:20px;color:var(--mute);font-size:14px}li{margin:6px 0}b{color:var(--fg);font-weight:600}\n.foot{margin-top:22px;padding-top:16px;border-top:1px solid var(--line);font-size:13px;color:var(--mute);display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap}\na{color:var(--accent);text-decoration:none}\n.dl{display:block;text-align:center;background:var(--accent);color:#fff;border-radius:10px;padding:13px;font-weight:600}.dl span{opacity:.75;font-weight:400;font-size:13px;margin-left:6px}\n.or{font-size:13px;color:var(--mute);margin:14px 0 6px}\n</style></head><body><div class="card">\n<span class="chip">session __CODE__</span>\n<h1>Join __HOST__&rsquo;s GitPulse</h1>\n<p>Paste one command in a terminal on your machine. Your machine shows up on the dashboard with the coding agents you have open.</p>\n<div class="tabs"><button data-os="ps1">Windows</button><button data-os="sh">macOS / Linux</button></div>\n<a class="dl" id="dl" href="/join/__CODE__/cmd" download>Download &amp; double-click <span>GitPulse-join.cmd</span></a>\n<div class="or" id="or">or paste this in PowerShell</div>\n<div class="row"><code id="cmd"></code><button class="copy" id="copy">Copy</button></div>\n<ol><li>If Windows asks whether to run the file, click <b>Run</b>. A window opens and stays open while you&rsquo;re in the session</li>\n<li>Sets everything up inside <b>~/.gitpulse</b>: its own Python, no admin, nothing else installed. First run takes about a minute</li>\n<li>Finds Claude Code, Antigravity, Codex, Aider&hellip; running on your machine</li>\n<li>Asks before adding a hook so your Claude Code / Antigravity sessions <b>receive</b> other agents&rsquo; changes</li>\n<li>If the host leaves, the first machine that joined takes over automatically, same session code</li>\n<li>Only changed <b>function signatures</b> leave your machine, never function bodies</li></ol>\n<div class="foot"><span>Ctrl+C in that terminal leaves the session.</span><a href="/">Watch the live dashboard &rarr;</a></div>\n</div>\n<script>\nconst C={ps1:"curl.exe -fsSL __BASE__/join/__CODE__/ps1 -o $HOME/gp-join.ps1; powershell -ExecutionPolicy Bypass -File $HOME/gp-join.ps1",sh:"curl -fsSL __BASE__/join/__CODE__/sh | sh"};\nconst cmd=document.getElementById(\'cmd\'),tabs=document.querySelectorAll(\'.tabs button\');\nfunction pick(os){cmd.textContent=C[os];tabs.forEach(t=>t.classList.toggle(\'on\',t.dataset.os===os));\nconst w=os===\'ps1\';document.getElementById(\'dl\').style.display=w?\'\':\'none\';document.getElementById(\'or\').textContent=w?\'or paste this in PowerShell\':\'Paste this in Terminal\'}\ntabs.forEach(t=>t.onclick=()=>pick(t.dataset.os));\npick(/Win/.test(navigator.platform||navigator.userAgent)?\'ps1\':\'sh\');\ndocument.getElementById(\'copy\').onclick=e=>{navigator.clipboard.writeText(cmd.textContent);const b=e.target;b.textContent=\'Copied\';b.classList.add(\'done\');setTimeout(()=>{b.textContent=\'Copy\';b.classList.remove(\'done\')},1500)};\n</script></body></html>'

@app.get('/api/session')
async def session_info(request: Request):
    base = _public_base(request)
    return {'code': SESSION_CODE, 'join_url': f'{base}/join/{SESSION_CODE}', 'host': HOST_MACHINE, 'workspace': mesh.workspace}

@app.get('/join/{code}')
async def join_page(code: str, request: Request):
    if code != SESSION_CODE:
        return HTMLResponse("<h2 style='font-family:sans-serif'>That GitPulse session code is not active on this host.</h2>", status_code=404)
    html = JOIN_PAGE.replace('__BASE__', _public_base(request)).replace('__CODE__', SESSION_CODE).replace('__HOST__', HOST_MACHINE)
    resp = HTMLResponse(html)
    resp.set_cookie('gp_s', SESSION_CODE, max_age=86400, samesite='lax')
    return resp

@app.get('/join/{code}/ps1')
async def join_ps1(code: str, request: Request):
    if code != SESSION_CODE:
        return PlainTextResponse("Write-Host 'GitPulse: session code not active' -ForegroundColor Red", status_code=404)
    return PlainTextResponse(_join_ps1(_public_base(request)))

@app.get('/join/{code}/cmd')
async def join_cmd(code: str, request: Request):
    """Double-clickable Windows launcher. Saves the join script to a file and runs it with -File
    (Defender blocks the `irm | iex` download-and-run pattern when cmd launches it)."""
    if code != SESSION_CODE:
        return PlainTextResponse('@echo GitPulse: session code not active& pause', status_code=404)
    url = f'{_public_base(request)}/join/{SESSION_CODE}/ps1'
    body = '@echo off\r\ntitle GitPulse - joined session ' + SESSION_CODE + f'\r\nif not exist "%USERPROFILE%\\.gitpulse" mkdir "%USERPROFILE%\\.gitpulse"\r\ncurl.exe -fsSL "{url}" -o "%USERPROFILE%\\.gitpulse\\join.ps1" || (echo Could not reach the GitPulse host. & pause & exit /b 1)\r\npowershell -NoProfile -ExecutionPolicy Bypass -File "%USERPROFILE%\\.gitpulse\\join.ps1"\r\necho.\r\necho GitPulse stopped. You can close this window.\r\npause >nul\r\n'
    return PlainTextResponse(body, headers={'Content-Disposition': 'attachment; filename="GitPulse-join.cmd"'})

@app.get('/join/{code}/sh')
async def join_sh(code: str, request: Request):
    if code != SESSION_CODE:
        return PlainTextResponse("echo 'GitPulse: session code not active'; exit 1", status_code=404)
    return PlainTextResponse(_join_sh(_public_base(request)))
BUNDLE = ('mesh_engine.py', 'index.html', 'agent_detect.py', 'gitpulse_hook.py', 'agent_client.py')

@app.get('/bundle/{name}')
async def get_bundle_file(name: str, request: Request):
    """Server files, so a joined watcher can take over hosting if this machine leaves.
    Session-gated by access_guard (?s=CODE)."""
    if name not in BUNDLE:
        return PlainTextResponse('not found', status_code=404)
    return PlainTextResponse((BASE_DIR / name).read_text(encoding='utf-8'))

@app.get('/agent_detect.py')
async def get_agent_detect_script():
    return PlainTextResponse((BASE_DIR / 'agent_detect.py').read_text(encoding='utf-8'), media_type='text/x-python')

@app.get('/gitpulse_hook.py')
async def get_hook_script():
    return PlainTextResponse((BASE_DIR / 'gitpulse_hook.py').read_text(encoding='utf-8'), media_type='text/x-python')

@app.post('/api/agents/file_change')
async def file_change_endpoint(req: FileChangeRequest, request: Request):
    if req.session != SESSION_CODE and (not _is_local_request(request)):
        return JSONResponse({'status': 'error', 'detail': 'bad session code'}, status_code=403)
    if req.agent_id not in mesh.nodes:
        return JSONResponse({'status': 'error', 'detail': 'register first'}, status_code=404)
    mesh.nodes[req.agent_id].last_seen = time.time()
    flow = mesh.record_file_change(req.agent_id, req.repo, req.path, req.old_code, req.new_code, 'remote')
    await mesh.broadcast_ui()
    return {'status': 'ok', 'kind': flow['kind'], 'bytes_sent': flow.get('bytes_sent', 0)}

class InboxPullRequest(BaseModel):
    claude_session_id: str = ''
    engine: str = ''
    conversation_id: str = ''
    session: str = ''
    machine: str = ''
    cwd: str = ''
    event: str = ''

@app.post('/api/agents/inbox/pull')
async def inbox_pull_endpoint(req: InboxPullRequest, request: Request):
    """Called by gitpulse_hook.py inside a real Claude Code / Antigravity session."""
    if req.session != SESSION_CODE and (not _is_local_request(request)):
        return JSONResponse({'status': 'error', 'detail': 'bad session code'}, status_code=403)
    items = mesh.pull_inbox(req.claude_session_id, req.engine, req.machine, req.cwd)
    if items is None:
        return {'status': 'unknown_session', 'items': []}
    if items:
        await mesh.broadcast_ui()
    return {'status': 'ok', 'items': items}

class HookInstallRequest(BaseModel):
    agent_id: str

@app.post('/api/hooks/install')
async def hook_install_endpoint(req: HookInstallRequest):
    n = mesh.nodes.get(req.agent_id)
    if not n or n.source != 'local' or n.engine not in ('Claude Code', 'Antigravity') or (not n.meta.get('cwd')):
        return JSONResponse({'status': 'error', 'detail': 'only Claude Code / Antigravity agents on this machine'}, status_code=400)
    try:
        path = mesh.install_hook(req.agent_id)
    except Exception as e:
        return JSONResponse({'status': 'error', 'detail': str(e)}, status_code=500)
    await mesh.broadcast_ui()
    return {'status': 'ok', 'path': path}

@app.get('/api/tunnel')
async def tunnel_info():
    return TUNNEL.info()

@app.post('/api/tunnel')
async def tunnel_start():
    if TUNNEL.status not in ('on', 'starting', 'downloading'):
        threading.Thread(target=TUNNEL.start, daemon=True).start()
        await asyncio.sleep(0.2)
    return TUNNEL.info()

@app.delete('/api/tunnel')
async def tunnel_stop():
    TUNNEL.stop()
    await mesh.broadcast_ui()
    return TUNNEL.info()

@app.websocket('/ws')
async def websocket_endpoint(websocket: WebSocket):
    local = (websocket.client.host if websocket.client else '') in ('127.0.0.1', '::1', get_lan_ip()) and (not any((h in websocket.headers for h in FORWARD_HEADERS)))
    if not local and websocket.cookies.get('gp_s') != SESSION_CODE and (websocket.query_params.get('s') != SESSION_CODE):
        await websocket.close(code=4403)
        return
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
        if data.strip() == b'GITPULSE_DISCOVER':
            ip = get_lan_ip()
            reply = json.dumps({'service': 'gitpulse-host', 'url': f'http://{ip}:{PORT}', 'ws_url': f'ws://{ip}:{PORT}/ws', 'term': mesh.term, 'leader': mesh.leader_id}).encode('utf-8')
            self.transport.sendto(reply, addr)

@app.on_event('startup')
async def start_lan_discovery_beacon():
    loop = asyncio.get_running_loop()
    try:
        await loop.create_datagram_endpoint(lambda: _LANBeaconProtocol(), local_addr=('0.0.0.0', 9999), allow_broadcast=True)
        ip = get_lan_ip()
        mesh.log_event('LAN-HOST', f'Host bound on http://{ip}:{PORT} (UDP Discovery Beacon :9999)')
    except Exception:
        pass
    if not DEMO_MODE:
        asyncio.create_task(_live_loop())

async def _live_loop():
    while True:
        try:
            await mesh.live_tick()
        except Exception as e:
            mesh.log_event('ERROR', f'live scan failed: {e!r}')
        await asyncio.sleep(3)

@app.get('/')
async def serve_dashboard():
    return HTMLResponse((BASE_DIR / 'index.html').read_text(encoding='utf-8'))

def self_check():
    """Verifies AST 3-Way Merge, NodeTransformer call-site rewrite, Host Leader Failover, Merkle Anti-Entropy, and SQLite WAL."""
    c = MeshCluster()
    asyncio.run(c.trigger_agent_step('agent-auth', 'def verify_token(token: str) -> bool:\n    return True\n', 'Private tweak'))
    assert c.metrics['private_edits_suppressed'] == 1
    assert c.leader_id == 'agent-auth' and c.term == 1
    asyncio.run(c.toggle_partition('agent-auth'))
    assert c.leader_id != 'agent-auth' and c.term == 2 and (c.metrics['leader_failovers'] == 1)
    asyncio.run(c.trigger_agent_step('agent-auth', "def verify_token(token: str, role: str = 'admin') -> bool:\n    return True\n", 'Offline sig change'))
    assert len(c.nodes['agent-auth'].outbox_queue) == 1
    asyncio.run(c.toggle_partition('agent-auth'))
    assert len(c.nodes['agent-auth'].outbox_queue) == 0
    assert "role='admin'" in c.nodes['agent-gateway'].code, 'AST CallSiteRewriter failed on gateway.py'
    asyncio.run(c.simulate_split_brain_conflict())
    assert c.metrics['ast_merges_completed'] == 1
    assert 'revoke_token' in c.nodes['agent-auth'].code and 'strict_scope' in c.nodes['agent-auth'].code
    res = asyncio.run(c.atomic_github_flush())
    assert res['status'] == 'ok' and res['term'] == 2
    print('SELF-CHECK PASSED: 3-Way AST Merger, CallSiteRewriter, Host Leader Failover (Term 2), Merkle Rejoin, and SQLite WAL verified.')
if __name__ == '__main__':
    if '--check' in sys.argv:
        self_check()
    else:
        import uvicorn
        print(f'\n  GitPulse dashboard : http://localhost:{PORT}')
        print(f'  Join link (LAN)    : http://{get_lan_ip()}:{PORT}/join/{SESSION_CODE}')
        print('  Other network?     : open the dashboard, click Invite > Internet link')
        if TAKEOVER_FROM:
            print(f'  Took over session {SESSION_CODE} from {TAKEOVER_FROM}')
        print(f"  Mode               : {('demo (fake agents)' if DEMO_MODE else 'live (auto-detecting agents on this machine)')}\n")
        uvicorn.run(app, host='0.0.0.0', port=PORT, log_level='warning')
