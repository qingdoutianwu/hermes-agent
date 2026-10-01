"""Standalone stable bootstrap; install independently OUTSIDE selected versions.

Read-only consumer of source_only_binding (4b406455) selector/manifest schema.
No selected-source imports occur until exec. No fallback, polling or writes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys

ENTRYPOINT = "gateway/startup_entry.py"
SELECTOR_KEYS = {"source_commit", "source_tree", "source_manifest_sha256"}


class ConsumerError(ValueError):
    pass


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ConsumerError("duplicate JSON key")
        result[key] = value
    return result


def _identity(value, length):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{%d}" % length, value):
        raise ConsumerError("invalid immutable identity")


def _regular(path, mode):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != mode:
        raise ConsumerError("unsealed/nonregular file: " + str(path))
    if info.st_uid != os.geteuid():
        raise ConsumerError("unapproved file owner")


def _directory(path, sealed=False):
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
        raise ConsumerError("unapproved directory")
    mode = stat.S_IMODE(info.st_mode)
    if (sealed and mode != 0o555) or mode & 0o022:
        raise ConsumerError("directory seal/authority permissions")


def _blob(data):
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()


def _tree(rows):
    # Reconstruct Git's recursive tree from the verified inventory, without Git
    # or the mutable checkout. A matching JSON tree label alone is not proof.
    root = {}
    for row in rows:
        node = root
        parts = PurePosixPath(row["path"]).parts
        for part in parts[:-1]:
            node = node.setdefault(part, {})
            if not isinstance(node, dict):
                raise ConsumerError("file/directory collision")
        if parts[-1] in node:
            raise ConsumerError("duplicate inventory path")
        node[parts[-1]] = (row["git_mode"], row["blob_sha1"])

    def encode(node):
        data = b""
        for name, child in sorted(node.items(), key=lambda item: (item[0] + ("/" if isinstance(item[1], dict) else "")).encode()):
            mode, oid = ("40000", encode(child)) if isinstance(child, dict) else child
            data += mode.encode() + b" " + name.encode() + b"\0" + bytes.fromhex(oid)
        return hashlib.sha1(b"tree " + str(len(data)).encode() + b"\0" + data).hexdigest()
    return encode(root)


def resolve_once(authority_root):
    raw = Path(authority_root)
    if not raw.is_absolute() or raw.resolve(strict=True) != raw:
        raise ConsumerError("authority must be canonical absolute path")
    _directory(raw)
    versions = raw / "versions"
    _directory(versions)
    selector = raw / "future-source.json"
    _regular(selector, 0o444)
    # Single selector open/read. Subsequent verification uses this pinned record.
    selector_bytes = selector.read_bytes()
    record = json.loads(selector_bytes, object_pairs_hook=_pairs)
    if not isinstance(record, dict) or set(record) != SELECTOR_KEYS:
        raise ConsumerError("unsupported selector schema")
    _identity(record["source_commit"], 40)
    _identity(record["source_tree"], 40)
    _identity(record["source_manifest_sha256"], 64)
    commit = record["source_commit"]
    manifest_path = versions / (commit + ".manifest.json")
    _regular(manifest_path, 0o444)
    manifest_bytes = manifest_path.read_bytes()
    if hashlib.sha256(manifest_bytes).hexdigest() != record["source_manifest_sha256"]:
        raise ConsumerError("manifest hash mismatch")
    manifest = json.loads(manifest_bytes, object_pairs_hook=_pairs)
    if not isinstance(manifest, dict) or set(manifest) != {"schema_version", "source_commit", "source_tree", "files"}:
        raise ConsumerError("unsupported manifest schema")
    if type(manifest["schema_version"]) is not int or manifest["schema_version"] != 1:
        raise ConsumerError("unsupported manifest version")
    if (manifest["source_commit"], manifest["source_tree"]) != (commit, record["source_tree"]):
        raise ConsumerError("manifest commit/tree mismatch")
    root = versions / commit
    _directory(root, sealed=True)
    if root.resolve(strict=True) != root or not root.is_relative_to(versions):
        raise ConsumerError("source root boundary")
    if Path(__file__).resolve().is_relative_to(root):
        raise ConsumerError("consumer must be outside selected source")
    rows = manifest["files"]
    if not isinstance(rows, list) or not rows:
        raise ConsumerError("empty inventory")
    expected = set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"path", "git_mode", "blob_sha1", "sha256", "bytes"}:
            raise ConsumerError("inventory schema")
        name = row["path"]
        if not isinstance(name, str) or "\0" in name or "\\" in name:
            raise ConsumerError("inventory path")
        relative = PurePosixPath(name)
        if relative.is_absolute() or not relative.parts or ".." in relative.parts or relative.as_posix() != name:
            raise ConsumerError("inventory path escape")
        if name in expected:
            raise ConsumerError("duplicate inventory")
        expected.add(name)
        _identity(row["blob_sha1"], 40)
        _identity(row["sha256"], 64)
        if type(row["bytes"]) is not int or row["bytes"] < 0:
            raise ConsumerError("inventory size")
        path = root.joinpath(*relative.parts)
        for parent in path.parents:
            if parent == root:
                break
            _directory(parent, sealed=True)
        mode = row["git_mode"]
        if mode == "120000":
            if not path.is_symlink():
                raise ConsumerError("symlink mode")
            target = os.readlink(path)
            if Path(target).is_absolute() or not path.resolve(strict=True).is_relative_to(root):
                raise ConsumerError("symlink escape")
            data = target.encode()
        elif mode in {"100644", "100755"}:
            _regular(path, 0o444 if mode == "100644" else 0o555)
            data = path.read_bytes()
        else:
            raise ConsumerError("unsupported Git mode")
        if len(data) != row["bytes"] or hashlib.sha256(data).hexdigest() != row["sha256"] or _blob(data) != row["blob_sha1"]:
            raise ConsumerError("source bytes/blob mismatch")
    actual = set()
    for path in root.rglob("*"):
        if path.is_symlink() or path.is_file():
            actual.add(path.relative_to(root).as_posix())
        else:
            _directory(path, sealed=True)
    if actual != expected or _tree(rows) != record["source_tree"]:
        raise ConsumerError("inventory/tree mismatch")
    if ENTRYPOINT not in expected or (root / ENTRYPOINT).is_symlink():
        raise ConsumerError("selected entrypoint missing/nonregular")
    return {**record, "source_root": str(root), "selector_sha256": hashlib.sha256(selector_bytes).hexdigest(),
            "selector_read_count": 1, "consumption_mode": "STARTUP_ONLY"}


def exec_selected(selection, arguments):
    # Preserve the approved interpreter/dependency sites, not an old mutable
    # source sys.path injected by a dependency launcher. -I excludes PYTHONPATH.
    dependency_sites = [p for p in sys.path if Path(p).name == "site-packages"]
    entry = str(Path(selection["source_root"]) / ENTRYPOINT)
    body = ("import sys,site,runpy,json;"
            "s=json.loads(sys.argv[1]);"
            "[site.addsitedir(p) for p in s['dependency_sites']];"
            # Managed Hermes editable metadata belongs to the old working tree,
            # not to approved third-party dependencies. Do not use it as fallback.
            "sys.meta_path=[f for f in sys.meta_path if not getattr(f,'__module__','').startswith('__editable___hermes_agent')];"
            "sys.path_hooks=[f for f in sys.path_hooks if not getattr(f,'__module__','').startswith('__editable___hermes_agent')];"
            "sys.path=[p for p in sys.path if not p.startswith('__editable__.hermes_agent')];"
            "[sys.path_importer_cache.pop(p,None) for p in list(sys.path_importer_cache) if p.startswith('__editable__.hermes_agent')];"
            "sys.path.insert(0,s['source_root']);"
            "sys.argv=[s['entrypoint'],*sys.argv[2:]];"
            "runpy.run_path(s['entrypoint'],run_name='__main__')")
    pinned = {"source_root": selection["source_root"], "entrypoint": entry, "dependency_sites": dependency_sites}
    os.execve(sys.executable, [sys.executable, "-I", "-B", "-c", body, json.dumps(pinned), *arguments], dict(os.environ))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--authority-root", required=True, type=Path)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    arguments = args.arguments[1:] if args.arguments[:1] == ["--"] else args.arguments
    if not arguments:
        parser.error("existing Gateway startup arguments required")
    try:
        selection = resolve_once(args.authority_root)
        exec_selected(selection, arguments)
    except (OSError, ValueError, KeyError, TypeError, RecursionError) as exc:
        print("FUTURE_SOURCE_STARTUP_FAIL_CLOSED: " + str(exc), file=sys.stderr)
        return 78


if __name__ == "__main__":
    raise SystemExit(main())
