"""Candidate proof contract: native fixture CAS + real harmless child processes.

These are unit execution fixtures, not natural scheduler evidence. No AUTH3,
business entry, provider, terminal adapter or Delivery is invoked by this file.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cron import executions
from cron.child_execution_proof import install_signer, prepare_child, ProofError
from cron.execution_identity import enter_cron_execution, exit_cron_execution
from cron.jobs import create_job

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def contract(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir(mode=0o700)
    monkeypatch.setenv("HERMES_HOME", str(home))
    key = Ed25519PrivateKey.generate()
    entry = home / "probe.py"
    trust = {"home": str(home), "profile": "default", "entry": str(entry),
             "public_key": key.public_key().public_bytes_raw().hex()}
    # Trusted test consumer pins the key itself; command arguments cannot override it.
    code = "\n".join([
        "import os, sys, json", "sys.path.insert(0, " + repr(str(ROOT)) + ")",
        "from cron.child_execution_proof import read_stdin_proof, verify_proof, ProofError",
        "trust = " + repr(trust),
        "import hashlib; trust['entry_sha256'] = hashlib.sha256(open(__file__,'rb').read()).hexdigest()",
        "raw = read_stdin_proof()", "os.read(int(sys.argv[1]), 1)",
        "case = sys.argv[2]", "claimed = {}",
        "if case == 'wrong_job': claimed['HERMES_CRON_JOB_ID'] = 'unit-wrong-job'",
        "if case.startswith('other_job:'): claimed['HERMES_CRON_JOB_ID'] = case.split(':',1)[1]",
        "if case == 'wrong_execution': claimed['HERMES_CRON_EXECUTION_ID'] = 'unit-wrong-execution'",
        "if case == 'legacy_conflict': claimed['FINANCE_TRIGGER_TYPE'] = 'MANUAL'",
        "if case == 'wrong_profile': trust['profile'] = 'unit-other-profile'",
        "if case == 'wrong_entry': trust['entry_sha256'] = '0' * 64",
        "try:",
        "    now = json.loads(raw)['body']['expires_at'] + 1 if case == 'expired' else None",
        "    if case == 'concurrent':",
        "        import concurrent.futures",
        "        def consume_once():",
        "            try: verify_proof(raw, trust); return 'PASS'",
        "            except ProofError as exc: return exc.code",
        "        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:",
        "            results = sorted(pool.map(lambda _: consume_once(), range(2)))",
        "        print(json.dumps({'code': 'ONE_PASS_ONE_REPLAY' if results == ['PASS','PROOF_REPLAY_DENIED'] else 'CONCURRENT_FAILURE'}))",
        "        sys.exit(0)",
        "    verify_proof(raw, trust, now=now, claimed=claimed)",
        "    if case == 'replay': verify_proof(raw, trust)",
        "    print(json.dumps({'code': 'ORIGIN_PASS'}))",
        "except ProofError as exc: print(json.dumps({'code': exc.code}))",
    ])
    entry.write_text(code)
    entry.chmod(0o444)
    trust["entry_sha256"] = hashlib.sha256(entry.read_bytes()).hexdigest()
    install_signer(home, key, entry)
    # Fixture through native registration and ownership APIs, NOT a timer claim.
    job = create_job(prompt=None, schedule="in 30m", script=str(entry), no_agent=True, deliver="local")
    record = executions.create_execution(job["id"], source="builtin", scheduled_instant=job["next_run_at"])
    row = executions.mark_execution_running(record["id"])
    assert row is not None
    token = enter_cron_execution(job, row["id"], row)
    yield home, key, entry, trust, row
    exit_cron_execution(token)


def fire_fixture(contract, case="valid", *, ended=False, corrupt_signer=False, native_mismatch=None):
    _, _key, entry, _, row = contract
    issue = prepare_child(entry)
    assert issue is not None
    if corrupt_signer:
        issue.key = Ed25519PrivateKey.generate()  # Negative issuer fixture; never installed runtime authority.
    read_fd, write_fd = os.pipe()
    env = {"PATH": "/usr/bin:/bin", "HOME": str(entry.parent), "HERMES_HOME": str(entry.parent),
           "PYTHONDONTWRITEBYTECODE": "1"}
    process = subprocess.Popen([sys.executable, str(entry), str(read_fd), case], stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                               env=env, pass_fds=(read_fd,))
    os.close(read_fd)
    try:
        issue.deliver(process)
        if ended:
            executions.finish_execution(row["id"], success=False, error="unit fixture terminal")
        if native_mismatch:
            columns = {"wrong_job": "job_id", "wrong_execution": "id", "wrong_owner": "process_id"}
            with executions._transaction() as conn:
                conn.execute("UPDATE executions SET " + columns[native_mismatch] + "=? WHERE id=?",
                             ("unit-invalid-native-binding", row["id"]))
        os.write(write_fd, b"1")
        output, error = process.communicate(timeout=10)
        assert process.returncode == 0, error
        return json.loads(output)["code"]
    finally:
        os.close(write_fd)
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


def test_real_native_owned_fixture_proof(contract):
    assert fire_fixture(contract) == "ORIGIN_PASS"


@pytest.mark.parametrize("case,code", [
    ("wrong_job", "PROOF_CALLER_FIELD_MISMATCH"),
    ("wrong_execution", "PROOF_CALLER_FIELD_MISMATCH"),
    ("wrong_profile", "PROOF_TARGET_BINDING_DENIED"),
    ("wrong_entry", "PROOF_TARGET_BINDING_DENIED"),
    ("expired", "PROOF_EXPIRED"),
    ("replay", "PROOF_REPLAY_DENIED"),
    ("legacy_conflict", "PROOF_CALLER_FIELD_MISMATCH"),
    ("concurrent", "ONE_PASS_ONE_REPLAY"),
])
def test_negative_proof_bindings(contract, case, code):
    assert fire_fixture(contract, case) == code


def test_ended_execution_denied(contract):
    assert fire_fixture(contract, ended=True) == "PROOF_NATIVE_EXECUTION_DENIED"


def test_forged_signed_fields_denied(contract):
    assert fire_fixture(contract, corrupt_signer=True) == "PROOF_SIGNATURE_INVALID"


def test_manual_context_cannot_issue(contract):
    from cron.execution_identity import _CURRENT
    token = _CURRENT.set(None)  # Negative unit fixture, not runtime provenance injection.
    try:
        with pytest.raises(ProofError, match="SCHEDULER_ORIGIN_REQUIRED"):
            prepare_child(contract[2])
    finally:
        _CURRENT.reset(token)


def test_ended_owner_cannot_issue(contract):
    executions.finish_execution(contract[4]["id"], success=False, error="unit fixture terminal")
    with pytest.raises(ProofError, match="PROOF_EXECUTION_OWNERSHIP_DENIED"):
        prepare_child(contract[2])


def test_unbound_script_keeps_existing_semantics(tmp_path):
    entry = tmp_path / "ordinary.py"
    entry.write_text("print('ordinary')")
    assert prepare_child(entry) is None


def test_different_registered_job_cannot_reuse_proof(contract):
    other = create_job(prompt=None, schedule="in 40m", script=str(contract[2]), no_agent=True, deliver="local")
    assert fire_fixture(contract, "other_job:" + other["id"]) == "PROOF_CALLER_FIELD_MISMATCH"


def test_direct_manual_execution_cannot_issue(contract):
    row = executions.create_execution(contract[4]["job_id"], source="direct")
    row = executions.mark_execution_running(row["id"])
    token = enter_cron_execution({"id": row["job_id"]}, row["id"], row)
    try:
        with pytest.raises(ProofError, match="PROOF_EXECUTION_OWNERSHIP_DENIED"):
            prepare_child(contract[2])
    finally:
        exit_cron_execution(token)


@pytest.mark.parametrize("mismatch", ["wrong_job", "wrong_execution", "wrong_owner"])
def test_actual_native_row_mismatch_denied(contract, mismatch):
    assert fire_fixture(contract, native_mismatch=mismatch) == "PROOF_NATIVE_EXECUTION_DENIED"


def test_delegated_context_cannot_issue(contract):
    from agent.delegation_context import delegated_child_context
    with delegated_child_context("unit-child"):
        with pytest.raises(ProofError, match="SCHEDULER_ORIGIN_REQUIRED"):
            prepare_child(contract[2])
