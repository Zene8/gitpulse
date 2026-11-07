#!/usr/bin/env python3
"""
GitPulse // Claude Code hook

This is how a Claude Code agent really *receives* updates from other agents.
Claude Code runs this script on every prompt (UserPromptSubmit) and after every
tool call (PostToolUse). It asks the GitPulse server "anything new for my session?"
and, if there is, hands the changed signatures to the agent as extra context.

Antigravity (agy) works the same way through its PreInvocation hook
(<workspace>/.agents/hooks.json); run with --agy it reads agy's stdin shape and
answers with {"injectSteps": [{"ephemeralMessage": ...}]}.

Stdlib only. If the server is down it exits silently in < 1.5 s, so it never blocks the agent.

  run as a hook :  python gitpulse_hook.py --server URL [--session CODE] [--agy]
  install       :  python gitpulse_hook.py install   --dir PROJECT --server URL [--session CODE] [--agy]
  uninstall     :  python gitpulse_hook.py uninstall --dir PROJECT [--agy]
"""

import ast
import json
import os
import socket
import sys
import urllib.request
from pathlib import Path

MARKER = "gitpulse_hook.py"
EVENTS = ("UserPromptSubmit", "PostToolUse")


def _arg(name: str, default: str = "") -> str:
    if name in sys.argv:
        i = sys.argv.index(name)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def _pull(server: str, body: dict) -> list:
    req = urllib.request.Request(f"{server}/api/agents/inbox/pull", data=json.dumps(body).encode("utf-8"),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=3.5) as r:
            return json.loads(r.read().decode("utf-8")).get("items") or []
    except Exception:
        return []


def _extract_imports_simple(code: str) -> list:
    imports = []
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return imports
    raw_lines = code.splitlines()
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            end = getattr(node, "end_lineno", node.lineno)
            stmt = "\n".join(raw_lines[node.lineno - 1:end]).strip()
            if stmt and stmt not in imports:
                imports.append(stmt)
    return imports


def _ensure_imports_simple(target_code: str, imports: list = None) -> str:
    if not imports:
        return target_code
    existing = set(_extract_imports_simple(target_code))
    missing = [imp.strip() for imp in imports if imp.strip() and imp.strip() not in existing]
    if not missing:
        return target_code
    try:
        tree = ast.parse(target_code)
        insert_line = 0
        for idx, node in enumerate(tree.body):
            if idx == 0 and isinstance(node, ast.Expr) and isinstance(getattr(node, "value", None), ast.Constant) and isinstance(node.value.value, str):
                insert_line = getattr(node, "end_lineno", node.lineno)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                insert_line = getattr(node, "end_lineno", node.lineno)
            else:
                break
        lines = target_code.splitlines(keepends=True)
        block = "".join(m + "\n" for m in missing)
        if insert_line == 0 and lines and lines[0].startswith("#!"):
            insert_line = 1
        return "".join(lines[:insert_line]) + block + "".join(lines[insert_line:])
    except SyntaxError:
        return "\n".join(missing) + "\n" + target_code


def patch_source_simple(
    target_code: str,
    symbol_name: str,
    new_symbol_code: str,
    action: str = "upsert",
    imports: list = None,
) -> str:
    """Line-exact splice of function, class, or assignment into target_code (preserves formatting & comments)."""
    try:
        target_code = _ensure_imports_simple(target_code, imports)
        tree = ast.parse(target_code)
        if action != "delete":
            new_tree = ast.parse(new_symbol_code)
            if not new_tree.body:
                return target_code
        lines = target_code.splitlines(keepends=True)
        for node in tree.body:
            matched = False
            start = node.lineno - 1
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.name == symbol_name:
                matched = True
                start = min([node.lineno] + [d.lineno for d in getattr(node, "decorator_list", [])]) - 1
            elif isinstance(node, ast.Assign):
                targets = [ast.unparse(t) for t in node.targets]
                if targets and targets[0] == symbol_name:
                    matched = True
                    start = node.lineno - 1
            if matched:
                end = node.end_lineno
                if action == "delete":
                    while end < len(lines) and not lines[end].strip():
                        end += 1
                    return "".join(lines[:start]) + "".join(lines[end:])
                repl = new_symbol_code.strip("\n") + "\n"
                return "".join(lines[:start]) + repl + "".join(lines[end:])
        if action == "delete":
            return target_code
        if not target_code.strip():
            return new_symbol_code.strip() + "\n"
        return target_code.rstrip() + "\n\n" + new_symbol_code.strip() + "\n"
    except Exception:
        return target_code


def _apply_patches_locally(items: list, cwd: str) -> int:
    applied = 0
    if not cwd:
        return 0
    cwd_path = Path(cwd)
    for it in items:
        for p in it.get("patches") or []:
            sym = p.get("symbol")
            code = p.get("code", "")
            action = p.get("action", "upsert")
            imports = p.get("imports")
            rel = p.get("path") or p.get("file")
            if not (sym and rel):
                continue
            candidates = [cwd_path / rel, cwd_path / rel.split("/", 1)[-1] if "/" in rel else None]
            for target in candidates:
                if target and target.is_file() and target.suffix == ".py":
                    try:
                        old_text = target.read_text(encoding="utf-8")
                        new_text = patch_source_simple(old_text, sym, code, action=action, imports=imports)
                        if new_text != old_text:
                            target.write_text(new_text, encoding="utf-8")
                            applied += 1
                    except Exception:
                        pass
    return applied


def _message(items: list) -> str:
    lines = []
    collisions = [it for it in items if it.get("is_collision")]
    gossips = [it for it in items if it.get("is_gossip") and not it.get("is_collision")]
    contracts = [it for it in items if not it.get("is_collision") and not it.get("is_gossip")]

    if collisions:
        lines.append("⚠️ [GITPULSE COLLISION WARNING] Concurrent edits detected on the same code symbol:")
        for it in collisions:
            for l in it.get("contract") or []:
                lines.append(f"  {l}")
        lines.append("Coordinate with your peer or edit another function to prevent merge conflicts.\n")

    if gossips:
        lines.append("💬 [GitPulse Peer Context] Message(s) from other agents in your mesh:")
        for it in gossips:
            for l in it.get("contract") or []:
                lines.append(f"  {l}")

    if contracts:
        lines.append(f"[GitPulse] {len(contracts)} interface change(s) from other agents working in this codebase. "
                     "Code that calls these must match the new signatures:")
        for it in contracts:
            lines.append(f"- {it.get('from_label', it.get('from'))} on {it.get('machine', '?')} changed {it.get('file')}:")
            lines.extend(f"    {l}" for l in it.get("contract") or [])
    return "\n".join(lines)


def _server() -> str:
    """--server, or the current host written by the watcher (survives a host transfer)."""
    sess = _arg("--session")
    if sess:
        try:
            cur = server_file(sess).read_text(encoding="utf-8").strip()
            if cur:
                return cur.rstrip("/")
        except Exception:
            pass
    return _arg("--server").rstrip("/")


def server_file(session: str) -> Path:
    """Where the watcher records the session's current host URL (it changes on a host transfer)."""
    return Path.home() / ".gitpulse" / f"server_{session}.txt"


def run_hook() -> None:
    server = _server()
    try:
        evt = json.loads(sys.stdin.read() or "{}")
    except Exception:
        evt = {}
    if "--agy" in sys.argv:
        # Antigravity: always answer with JSON, even when there is nothing to say.
        items = []
        ws = evt.get("workspacePaths") or []
        target_dir = _arg("--dir") or (ws[0] if ws else "")
        if server and (evt.get("conversationId") or ws):
            items = _pull(server, {
                "engine": "Antigravity", "conversation_id": evt.get("conversationId", ""),
                "session": _arg("--session"), "machine": socket.gethostname(),
                "cwd": target_dir, "event": "PreInvocation",
            })
        if items and target_dir and "--apply-disk" in sys.argv:
            _apply_patches_locally(items, target_dir)
        print(json.dumps({"injectSteps": [{"ephemeralMessage": _message(items)}]} if items else {}))
        return
    sid = evt.get("session_id") or ""
    if not server or not sid:
        return
    items = _pull(server, {
        "claude_session_id": sid, "engine": "Claude Code",
        "session": _arg("--session"), "machine": socket.gethostname(),
        "cwd": evt.get("cwd", ""), "event": evt.get("hook_event_name", ""),
    })
    if not items:
        return
    if evt.get("cwd") and "--apply-disk" in sys.argv:
        _apply_patches_locally(items, evt.get("cwd"))
    print(json.dumps({"hookSpecificOutput": {
        "hookEventName": evt.get("hook_event_name") or "UserPromptSubmit",
        "additionalContext": _message(items),
    }}))


def _cmd(server: str, session: str, extra: str = "") -> str:
    py = Path(sys.executable).as_posix()
    me = Path(__file__).resolve().as_posix()
    return f'"{py}" "{me}" --server {server.rstrip("/")}' + (f" --session {session}" if session else "") + extra


# ---- Antigravity: <workspace>/.agents/hooks.json, one top-level key per hook set ----
AGY_KEY = "gitpulse"


def _agy_path(project_dir: str) -> Path:
    return Path(project_dir) / ".agents" / "hooks.json"


def install_agy(project_dir: str, server: str, session: str = "") -> str:
    path = _agy_path(project_dir)
    try:
        cfg = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except Exception:
        raise RuntimeError(f"{path} is not valid JSON, not touching it")
    cmd = _cmd(server, session, f' --agy --dir "{Path(project_dir).as_posix()}"')
    cfg[AGY_KEY] = {"PreInvocation": [{"type": "command", "command": cmd, "timeout": 10}]}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cfg, indent=2) + "\n", encoding="utf-8")
    return str(path)


def uninstall_agy(project_dir: str) -> str:
    path = _agy_path(project_dir)
    if not path.exists():
        return ""
    cfg = json.loads(path.read_text(encoding="utf-8"))
    cfg.pop(AGY_KEY, None)
    if cfg:
        path.write_text(json.dumps(cfg, indent=2) + "\n", encoding="utf-8")
    else:
        path.unlink()
    return str(path)


def install_for(engine: str, project_dir: str, server: str, session: str = "") -> str:
    return (install_agy if engine == "Antigravity" else install)(project_dir, server, session)


def installed_for(engine: str, project_dir: str) -> bool:
    if engine == "Antigravity":
        try:
            return MARKER in _agy_path(project_dir).read_text(encoding="utf-8")
        except Exception:
            return False
    return is_installed(project_dir)


def _settings_path(project_dir: str) -> Path:
    return Path(project_dir) / ".claude" / "settings.local.json"


def _strip(settings: dict) -> dict:
    hooks = settings.get("hooks") or {}
    for ev in list(hooks):
        groups = []
        for g in hooks[ev] or []:
            g = dict(g)
            g["hooks"] = [h for h in g.get("hooks") or [] if MARKER not in str(h.get("command", ""))]
            if g["hooks"]:
                groups.append(g)
        if groups:
            hooks[ev] = groups
        else:
            hooks.pop(ev)
    if hooks:
        settings["hooks"] = hooks
    else:
        settings.pop("hooks", None)
    return settings


def install(project_dir: str, server: str, session: str = "") -> str:
    """Add (or refresh) the GitPulse hook in PROJECT/.claude/settings.local.json. Returns the file path."""
    path = _settings_path(project_dir)
    try:
        settings = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except Exception:
        raise RuntimeError(f"{path} is not valid JSON, not touching it")
    settings = _strip(settings)
    cmd = _cmd(server, session)
    hook = {"type": "command", "command": cmd, "timeout": 5}
    hooks = settings.setdefault("hooks", {})
    hooks.setdefault("UserPromptSubmit", []).append({"hooks": [hook]})
    hooks.setdefault("PostToolUse", []).append({"matcher": "*", "hooks": [hook]})
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(settings, indent=2) + "\n", encoding="utf-8")
    return str(path)


def uninstall(project_dir: str) -> str:
    path = _settings_path(project_dir)
    if not path.exists():
        return ""
    settings = _strip(json.loads(path.read_text(encoding="utf-8")))
    path.write_text(json.dumps(settings, indent=2) + "\n", encoding="utf-8")
    return str(path)


def is_installed(project_dir: str) -> bool:
    try:
        return MARKER in _settings_path(project_dir).read_text(encoding="utf-8")
    except Exception:
        return False


if __name__ == "__main__":
    agy = "--agy" in sys.argv
    if len(sys.argv) > 1 and sys.argv[1] == "install":
        print("wrote", install_for("Antigravity" if agy else "Claude Code", _arg("--dir", os.getcwd()),
                                   _arg("--server", "http://127.0.0.1:8000"), _arg("--session")))
    elif len(sys.argv) > 1 and sys.argv[1] == "uninstall":
        d = _arg("--dir", os.getcwd())
        print("cleaned", (uninstall_agy(d) if agy else uninstall(d)) or "(nothing to do)")
    else:
        run_hook()
