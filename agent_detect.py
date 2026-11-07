"""
GitPulse // real local agent detection.

Finds AI coding agents actually running on this machine (Claude Code, Antigravity,
Codex, Gemini CLI, Aider, Cursor/Windsurf agents), the folder each one is working in,
and the git repo / branch / GitHub remote behind that folder.

Standard library only; uses psutil when installed (needed for per-process cwd).
"""
from __future__ import annotations
import json
import os
import socket
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Optional
try:
    import psutil
except Exception:
    psutil = None
HOME = Path.home()
EXE_ENGINES = {'claude': 'Claude Code', 'agy': 'Antigravity', 'antigravity': 'Antigravity', 'codex': 'Codex', 'aider': 'Aider', 'gemini': 'Gemini CLI', 'cursor-agent': 'Cursor', 'windsurf': 'Windsurf', 'opencode': 'OpenCode', 'goose': 'Goose'}
SCRIPT_ENGINES = {'@anthropic-ai/claude-code': 'Claude Code', '@openai/codex': 'Codex', '@google/gemini-cli': 'Gemini CLI', 'aider': 'Aider'}
ENGINE_COLORS = {'Claude Code': '#d97757', 'Antigravity': '#4f8cff', 'Codex': '#10a37f', 'Gemini CLI': '#8e75ff', 'Aider': '#e5b93c', 'Cursor': '#9aa4b2', 'Windsurf': '#2dd4bf', 'OpenCode': '#f472b6', 'Goose': '#fb923c'}
HEADLESS_FLAGS = {'-p', '--print'}

def _git(args: List[str], cwd: str, timeout: float=2.0) -> str:
    try:
        out = subprocess.run(['git', *args], cwd=cwd, capture_output=True, text=True, timeout=timeout, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        return out.stdout.strip() if out.returncode == 0 else ''
    except Exception:
        return ''

def github_web_url(remote: str) -> str:
    """git@github.com:owner/repo.git -> https://github.com/owner/repo"""
    r = remote.strip()
    if not r:
        return ''
    if r.startswith('git@'):
        host, _, path = r[4:].partition(':')
        r = f'https://{host}/{path}'
    if r.endswith('.git'):
        r = r[:-4]
    return r
_repo_cache: Dict[str, tuple] = {}

def repo_info(path: str) -> dict:
    """Git info for a folder. Cached for 5 s."""
    hit = _repo_cache.get(path)
    if hit and time.time() - hit[0] < 5:
        return hit[1]
    root = _git(['rev-parse', '--show-toplevel'], path)
    if not root:
        info = {'root': '', 'name': '', 'branch': '', 'remote': '', 'web_url': '', 'dirty': 0}
    else:
        root = str(Path(root))
        remote = _git(['remote', 'get-url', 'origin'], root)
        status = _git(['status', '--porcelain'], root)
        info = {'root': root, 'name': Path(root).name, 'branch': _git(['rev-parse', '--abbrev-ref', 'HEAD'], root) or 'main', 'remote': remote, 'web_url': github_web_url(remote), 'dirty': len(status.splitlines()) if status else 0}
    _repo_cache[path] = (time.time(), info)
    return info

def workspace_info(cwd: str) -> dict:
    """The folder an agent works in. If it is not itself a repo, list the git repos directly inside it."""
    p = Path(cwd)
    own = repo_info(cwd)
    repos = []
    if own['root']:
        repos = [own]
    else:
        try:
            for child in sorted(p.iterdir()):
                if child.is_dir() and (child / '.git').exists():
                    repos.append(repo_info(str(child)))
        except Exception:
            pass
    return {'path': str(p), 'name': p.name, 'is_repo': bool(own['root']), 'repos': repos}

def _claude_project_dir(cwd: str) -> Path:
    slug = ''.join((c if c.isalnum() else '-' for c in cwd))
    return HOME / '.claude' / 'projects' / slug

def _claude_session_meta(pid: int) -> dict:
    f = HOME / '.claude' / 'sessions' / f'{pid}.json'
    try:
        return json.loads(f.read_text(encoding='utf-8'))
    except Exception:
        return {}

def _tail_jsonl(path: Path, max_bytes: int=250000) -> List[dict]:
    try:
        with open(path, 'rb') as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - max_bytes))
            raw = fh.read().decode('utf-8', errors='ignore')
    except Exception:
        return []
    out = []
    for line in raw.splitlines()[1:]:
        try:
            out.append(json.loads(line))
        except Exception:
            pass
    return out

def claude_activity(cwd: str, session_id: str) -> dict:
    """Last prompt topic + files this Claude session touched recently (from its own transcript)."""
    res = {'title': '', 'last_prompt': '', 'files': [], 'last_active': 0.0}
    if not session_id:
        return res
    log = _claude_project_dir(cwd) / f'{session_id}.jsonl'
    if not log.exists():
        return res
    res['last_active'] = log.stat().st_mtime
    files: List[str] = []
    for rec in _tail_jsonl(log):
        t = rec.get('type')
        if t == 'custom-title' and rec.get('customTitle'):
            res['title'] = str(rec['customTitle'])
        if t == 'last-prompt' and rec.get('lastPrompt'):
            res['last_prompt'] = str(rec['lastPrompt'])
        msg = rec.get('message') or {}
        content = msg.get('content')
        if t == 'user' and isinstance(content, str) and (not content.startswith('<')):
            res['last_prompt'] = content
        if isinstance(content, list):
            for b in content:
                if isinstance(b, dict) and b.get('type') == 'tool_use' and (b.get('name') in ('Edit', 'Write', 'MultiEdit', 'NotebookEdit')):
                    fp = (b.get('input') or {}).get('file_path')
                    if fp:
                        files.append(str(fp))
    seen, uniq = (set(), [])
    for f in reversed(files):
        if f not in seen:
            seen.add(f)
            uniq.append(f)
    res['files'] = uniq[:6]
    res['last_prompt'] = ' '.join(res['last_prompt'].split())[:140]
    return res

def antigravity_activity() -> dict:
    res = {'title': '', 'last_prompt': '', 'files': [], 'last_active': 0.0}
    conv = HOME / '.gemini' / 'antigravity-cli' / 'conversations'
    try:
        newest = max(conv.iterdir(), key=lambda p: p.stat().st_mtime)
        res['last_active'] = newest.stat().st_mtime
    except Exception:
        pass
    return res

def _engine_for(name: str, cmdline: List[str]) -> Optional[str]:
    base = name.lower()
    if base.endswith('.exe'):
        base = base[:-4]
    if base in EXE_ENGINES:
        return EXE_ENGINES[base]
    if base in ('node', 'python', 'python3', 'bun', 'deno'):
        joined = ' '.join(cmdline).lower().replace('\\', '/')
        for needle, engine in SCRIPT_ENGINES.items():
            if needle == 'aider':
                if base.startswith('python') and ('/aider' in joined or ' -m aider' in joined):
                    return engine
            elif needle in joined:
                return engine
    return None

def detect_agents() -> List[dict]:
    """Every AI coding agent process running on this machine, with its working folder and repo."""
    if psutil is None:
        return []
    found: Dict[int, dict] = {}
    for p in psutil.process_iter(['pid', 'ppid', 'name', 'create_time']):
        name = p.info.get('name') or ''
        try:
            cmdline = p.cmdline()
        except Exception:
            cmdline = []
        engine = _engine_for(name, cmdline)
        if not engine:
            continue
        headless = any((a in HEADLESS_FLAGS for a in cmdline[1:]))
        try:
            cwd = p.cwd()
        except Exception:
            continue
        found[p.info['pid']] = {'pid': p.info['pid'], 'ppid': p.info.get('ppid'), 'engine': engine, 'cwd': cwd, 'started': p.info.get('create_time') or 0, 'headless': headless}
    agents = [a for a in found.values() if not (a['ppid'] in found and found[a['ppid']]['engine'] == a['engine'] and (not (a['engine'] == 'Claude Code' and _claude_session_meta(a['pid']))))]
    host = os.environ.get('GITPULSE_MACHINE') or socket.gethostname()
    out = []
    for a in sorted(agents, key=lambda x: x['started']):
        detail = {'title': '', 'last_prompt': '', 'files': [], 'last_active': 0.0}
        status = 'running'
        session_id = ''
        if a['engine'] == 'Claude Code':
            meta = _claude_session_meta(a['pid'])
            session_id = meta.get('sessionId', '')
            detail = claude_activity(meta.get('cwd') or a['cwd'], meta.get('sessionId', ''))
            detail['title'] = detail['title'] or meta.get('name', '')
            status = meta.get('status') or status
        elif a['engine'] == 'Antigravity':
            detail = antigravity_activity()
        out.append({'agent_id': f"{host.lower()}-{a['engine'].split()[0].lower()}-{a['pid']}", 'pid': a['pid'], 'engine': a['engine'], 'color': ENGINE_COLORS.get(a['engine'], '#9aa4b2'), 'machine': host, 'cwd': a['cwd'], 'workspace': workspace_info(a['cwd']), 'status': status, 'kind': 'background' if a['headless'] else 'interactive', 'started': a['started'], 'session_id': session_id, **detail})
    return out
SKIP_DIRS = {'.git', '.venv', 'venv', 'env', 'node_modules', '__pycache__', 'workspace_repo', 'dist', 'build', '.mypy_cache', '.pytest_cache'}
MAX_FILE_BYTES = 300000
MAX_FILES = 3000

class RepoWatcher:
    """Polls *.py files under a set of repo roots. First poll is a silent baseline."""

    def __init__(self):
        self.state: Dict[str, tuple] = {}
        self.roots: set = set()

    def mark_synced(self, path: str, src: str) -> None:
        """Record a file write performed by GitPulse itself so the next poll does not echo it back."""
        try:
            full = str(Path(path).resolve())
            mtime = os.stat(full).st_mtime
        except OSError:
            return
        norm = os.path.normcase(full)
        self.state[full] = (mtime, src)
        for k in list(self.state.keys()):
            if os.path.normcase(os.path.abspath(k)) == norm:
                self.state[k] = (mtime, src)

    def poll(self, roots: List[str]) -> List[dict]:
        changes = []
        new_roots = set(roots) - self.roots
        self.roots |= set(roots)
        for root in sorted(self.roots):
            baseline = root in new_roots
            count = 0
            for dirpath, dirnames, filenames in os.walk(root):
                dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS and (not d.startswith('.'))]
                for fn in filenames:
                    if not fn.endswith('.py'):
                        continue
                    count += 1
                    if count > MAX_FILES:
                        break
                    full = os.path.join(dirpath, fn)
                    try:
                        st = os.stat(full)
                    except OSError:
                        continue
                    if st.st_size > MAX_FILE_BYTES:
                        continue
                    prev = self.state.get(full)
                    if prev and prev[0] == st.st_mtime:
                        continue
                    try:
                        src = Path(full).read_text(encoding='utf-8', errors='ignore')
                    except OSError:
                        continue
                    self.state[full] = (st.st_mtime, src)
                    if baseline:
                        continue
                    old = prev[1] if prev else ''
                    if old == src:
                        continue
                    changes.append({'path': full, 'repo_root': root, 'rel': os.path.relpath(full, root).replace('\\', '/'), 'old': old, 'new': src, 'created': prev is None})
        return changes

def attribute_change(change: dict, agents: List[dict]) -> Optional[dict]:
    """Which agent made this edit? Agents whose own transcript lists the file win; else the most recently active agent whose workspace holds the repo."""
    path = os.path.normcase(change['path'])
    for a in agents:
        if any((os.path.normcase(f) == path for f in a.get('files') or [])):
            return {**a, '_attribution': 'transcript'}
    root = os.path.normcase(change['repo_root'])
    candidates = [a for a in agents if any((os.path.normcase(r['root']) == root for r in a['workspace']['repos']))]
    if not candidates:
        return None
    best = max(candidates, key=lambda a: (a.get('status') == 'busy', a.get('last_active') or 0, a.get('started') or 0))
    return {**best, '_attribution': 'guess'}
if __name__ == '__main__':
    for ag in detect_agents():
        ws = ag['workspace']
        repos = ', '.join((f"{r['name']}@{r['branch']} -> {r['web_url'] or 'no remote'}" for r in ws['repos'])) or 'no git repo'
        print(f"{ag['engine']:<12} pid={ag['pid']:<6} {ag['status']:<8} {ws['path']}  [{repos}]")
