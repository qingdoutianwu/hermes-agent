"""Real atomic-tool → stable consumer → exec fixtures; no real Gateway start."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import selectors
import subprocess
import sys
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP = ROOT / "bootstrap/future_source_startup.py"


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


consumer = load(BOOTSTRAP, "stable_consumer")
tool = load(Path(__file__).parent / "fixtures/source_only_binding_4b406455.py", "pinned_atomic_tool")


@pytest.fixture
def authority(tmp_path):
    tmp_path = tmp_path.resolve()
    repo = tmp_path / "source"; repo.mkdir()
    def git(*args):
        return subprocess.check_output(["/usr/bin/git", "-C", str(repo), *args], stderr=subprocess.DEVNULL)
    git("init"); git("config", "user.name", "Fixture"); git("config", "user.email", "fixture@example.invalid")
    (repo / "gateway").mkdir()
    program = '''import json,os,sys
import ruamel.yaml
from source_identity import SOURCE
try:
    import hermes_constants
    foreign_source_fallback = True
except ModuleNotFoundError:
    foreign_source_fallback = False
def evidence():
    return {'source':SOURCE,'pid':os.getpid(),'cwd':os.getcwd(),'arguments':sys.argv[1:],
            'identity':os.environ.get('HERMES_SCHEDULER_CORE_ACTIVATION_ID'),
            'home':os.environ.get('HERMES_HOME'),'sys_path':sys.path,
            'python':sys.executable,'version':list(sys.version_info[:3]),'required_import':'PASS',
            'foreign_source_fallback':foreign_source_fallback}
print(json.dumps(evidence()),flush=True)
for line in sys.stdin:
    print(json.dumps(evidence()),flush=True)
'''
    (repo / consumer.ENTRYPOINT).write_text(program)
    (repo / "source_identity.py").write_text("SOURCE='A'\n")
    git("add", "."); git("commit", "-m", "A"); a = git("rev-parse", "HEAD").decode().strip()
    (repo / "source_identity.py").write_text("SOURCE='B'\n")
    git("add", "."); git("commit", "-m", "B"); b = git("rev-parse", "HEAD").decode().strip()
    root = tmp_path / "authority"; root.mkdir(mode=0o700)
    version = root / "versions" / a; version.mkdir(parents=True)
    ma = tool.source_manifest(repo, a)
    for row in ma["files"]:
        path = version / row["path"]; path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(git("cat-file", "blob", row["blob_sha1"])); path.chmod(0o444)
    for path in sorted(version.rglob("*"), key=lambda p:len(p.parts), reverse=True):
        if path.is_dir(): path.chmod(0o555)
    version.chmod(0o555)
    mp = root / "versions" / (a + ".manifest.json"); mp.write_bytes(tool.encode(ma)); mp.chmod(0o444)
    previous = {"source_commit":a,"source_tree":ma["source_tree"],"source_manifest_sha256":tool.digest(mp.read_bytes())}
    selector = root / "future-source.json"; selector.write_bytes(tool.encode(previous)); selector.chmod(0o444)
    mb = tool.source_manifest(repo, b)
    manifest = tmp_path / "b.manifest.json"; manifest.write_bytes(tool.encode(mb)); manifest.chmod(0o444)
    args = dict(repo=repo, authority_root=root, base_commit=a, successor_commit=b, successor_tree=mb["source_tree"],
                source_manifest_path=manifest, source_manifest_sha256=tool.digest(manifest.read_bytes()),
                expected_current_authority=previous, transaction_id="consumer-e2e-fixture-0001")
    return root, args


def start(root, tmp_path, python=None):
    env = {**os.environ, "HERMES_HOME":str(tmp_path), "HERMES_SCHEDULER_CORE_ACTIVATION_ID":"isolated-start"}
    # Existing production argument shape, but a nonexistent inert fixture path.
    args = [str(tmp_path / "sealed/gateway_bootstrap.py"), "gateway", "run", "--external-supervisor"]
    p = subprocess.Popen([python or sys.executable,"-I","-B",str(BOOTSTRAP),"--authority-root",str(root),"--",*args],
                         cwd=tmp_path, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p, args


def read(p):
    poll = selectors.DefaultSelector(); poll.register(p.stdout, selectors.EVENT_READ)
    try:
        assert poll.select(15), "fixture startup timeout"
        line = p.stdout.readline()
        assert line, p.stderr.read()
        return json.loads(line)
    finally:
        poll.close()


def finish(p):
    p.stdin.close()
    assert p.wait(timeout=15) == 0, p.stderr.read()
    p.stdout.close(); p.stderr.close()


def test_real_tool_switch_is_next_start_only_and_preserves_context(authority, tmp_path):
    root, args = authority
    p1, forwarded = start(root, tmp_path)
    try:
        one = read(p1)
        assert one["source"] == "A" and one["pid"] == p1.pid
        assert one["foreign_source_fallback"] is False
        assert one["arguments"] == forwarded and one["cwd"] == str(tmp_path)
        assert one["identity"] == "isolated-start" and one["home"] == str(tmp_path)
        assert tool.bind(**args)["phase"] == "COMMITTED"
        p1.stdin.write("after switch\n"); p1.stdin.flush()
        assert read(p1)["source"] == "A"
        p2, _ = start(root, tmp_path)
        try:
            two = read(p2)
            assert two["source"] == "B" and two["pid"] == p2.pid
            assert two["foreign_source_fallback"] is False
            assert two["sys_path"][0] == str(root / "versions" / args["successor_commit"])
        finally:
            finish(p2)
        # Loss of the selector cannot affect an already exec'd instance.
        (root / "future-source.json").unlink()
        p1.stdin.write("no selector\n"); p1.stdin.flush()
        assert read(p1)["source"] == "A"
    finally:
        finish(p1)


def test_single_read_and_readonly_resolution(authority, monkeypatch):
    root, args = authority; reads = []; original = Path.read_bytes
    before = {p.relative_to(root).as_posix():p.read_bytes() for p in root.rglob("*") if p.is_file()}
    def record(p):
        if p == root / "future-source.json": reads.append(p)
        return original(p)
    monkeypatch.setattr(Path, "read_bytes", record)
    result = consumer.resolve_once(root)
    assert result["source_commit"] == args["base_commit"]
    assert result["selector_read_count"] == len(reads) == 1
    monkeypatch.setattr(Path, "read_bytes", original)
    assert {p.relative_to(root).as_posix():p.read_bytes() for p in root.rglob("*") if p.is_file()} == before


@pytest.mark.parametrize("failure", ["selector_missing","selector_malformed","schema","version_missing","manifest_missing",
                                    "commit","tree","unsealed","entrypoint_missing","traversal","version_symlink",
                                    "manifest_symlink","directory_symlink","bytes","duplicate_key","tree_forgery"])
def test_fail_closed_never_execs(authority, tmp_path, failure, monkeypatch):
    root, args = authority; selector = root / "future-source.json"
    record = tool.read_authority(root); version = root / "versions" / args["base_commit"]
    mp = root / "versions" / (args["base_commit"] + ".manifest.json")
    manifest = json.loads(mp.read_bytes())
    def write_manifest():
        tool._atomic(mp, tool.encode(manifest)); record["source_manifest_sha256"] = tool.digest(mp.read_bytes())
    if failure == "selector_missing": selector.unlink()
    elif failure == "selector_malformed": tool._atomic(selector, b"{")
    elif failure == "schema": record["schema_version"] = 2
    elif failure == "version_missing":
        version.chmod(0o755); version.rename(version.with_name("missing"))
    elif failure == "manifest_missing": mp.unlink()
    elif failure in {"commit","tree"}:
        manifest["source_" + failure] = "0" * 40; write_manifest()
    elif failure == "unsealed": version.chmod(0o755)
    elif failure == "entrypoint_missing":
        version.chmod(0o755); (version / "gateway").chmod(0o755); (version / consumer.ENTRYPOINT).unlink()
    elif failure == "traversal": manifest["files"][0]["path"] = "../escape"; write_manifest()
    elif failure == "version_symlink":
        version.chmod(0o755); saved = version.with_name("saved"); version.rename(saved); version.symlink_to(saved, target_is_directory=True)
    elif failure == "manifest_symlink":
        saved = mp.with_name("saved.json"); mp.rename(saved); mp.symlink_to(saved)
    elif failure == "directory_symlink":
        version.chmod(0o755); p = version / "gateway"; p.chmod(0o755)
        saved = tmp_path / "gateway"; p.rename(saved); p.symlink_to(saved); version.chmod(0o555)
    elif failure == "bytes":
        p = version / "source_identity.py"; p.chmod(0o644); p.write_text("tampered"); p.chmod(0o444)
    elif failure == "duplicate_key":
        tool._atomic(selector, ('{"source_commit":"' + args["base_commit"] + '","source_commit":"0"}').encode())
    elif failure == "tree_forgery":
        manifest["source_tree"] = record["source_tree"] = "0" * 40; write_manifest()
    if failure not in {"selector_missing","selector_malformed","duplicate_key"}: tool._atomic(selector, tool.encode(record))
    monkeypatch.setattr(consumer, "exec_selected", lambda *a: pytest.fail("unsafe exec"))
    assert consumer.main(["--authority-root", str(root), "--", "fixture.py", "gateway", "run"]) == 78


def test_exec_preserves_environment_and_uses_approved_python(authority, monkeypatch):
    root, _ = authority; calls = []; environment = dict(os.environ); cwd = os.getcwd()
    monkeypatch.setattr(os, "execve", lambda *a: calls.append(a))
    selection = consumer.resolve_once(root)
    consumer.exec_selected(selection, ["sealed.py", "gateway", "run"])
    exe, argv, env = calls[0]
    assert exe == sys.executable and argv[0] == exe
    assert env == environment and os.getcwd() == cwd
    assert argv[-3:] == ["sealed.py", "gateway", "run"]


@pytest.mark.parametrize("after_commit", [False, True])
def test_journal_recovery_integrates_without_starting(authority, after_commit):
    root, args = authority; atomic = tool._atomic
    def fail(path, data, mode=0o444):
        if path.name == "future-source.json":
            if after_commit: atomic(path, data, mode)
            raise OSError("isolated publication interruption")
        atomic(path, data, mode)
    with patch.object(tool, "_atomic", side_effect=fail), pytest.raises(OSError): tool.bind(**args)
    recovered = tool.reconcile_transaction(root, args["transaction_id"])
    assert recovered["phase"] == ("COMMITTED" if after_commit else "ABORTED")
    resolved = consumer.resolve_once(root)
    assert resolved["source_commit"] == args["successor_commit" if after_commit else "base_commit"]
    # A incomplete journal is not an authority; consumer rejects invalid selector.
    tool._atomic(root / "future-source.json", tool.encode({"source_commit":"incomplete"}))
    with pytest.raises(consumer.ConsumerError): consumer.resolve_once(root)
