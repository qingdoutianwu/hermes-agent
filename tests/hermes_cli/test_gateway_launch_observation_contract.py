"""R3A component tests: real launcher-generated child, fixture handoff, no Gateway.

An isolated output pathname here is not an installed receipt rotation mechanism.
No source/PM/service binding is changed and no business or production state is used.
"""
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

from hermes_cli.gateway_dependency_launcher import launch_command


def _fixture(tmp_path):
    facts = tmp_path / "fixture-facts.json"
    facts.write_text('{"scope":"TEST_ONLY"}')
    prefix = Path(sys.prefix)
    site = prefix / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    assert site.is_dir()
    binding = {
        "python": sys.executable, "physical_python": str(Path(sys.executable).resolve()),
        "project": str(tmp_path), "generation": str(prefix), "site": str(site),
        "facts": str(facts), "facts_sha256": hashlib.sha256(facts.read_bytes()).hexdigest(),
    }
    marker = tmp_path / "handoff-count.txt"
    bootstrap = tmp_path / "fixture_handoff.py"
    bootstrap.write_text(
        "from pathlib import Path\n"
        f"p=Path({str(marker)!r})\n"
        "p.write_text(p.read_text()+'handoff\\n' if p.exists() else 'handoff\\n')\n"
    )
    receipt = tmp_path / "startup-observation.json"
    command = launch_command(binding, bootstrap, [], receipt)
    env = {"HOME": str(tmp_path), "HERMES_HOME": str(tmp_path / "profile"),
           "PATH": "/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1"}
    return facts, binding, marker, receipt, command, env


def test_first_observation_precedes_handoff_and_collision_preserves_original(tmp_path):
    facts, binding, marker, receipt, command, env = _fixture(tmp_path)
    first = subprocess.run(command, env=env, capture_output=True, text=True, timeout=15)
    assert first.returncode == 0, first.stderr
    payload = json.loads(receipt.read_text())
    assert payload["ruamel_import"] == "PASS"
    assert payload["dependency_generation"] == binding["generation"]
    assert payload["facts_sha256"] == binding["facts_sha256"]
    assert payload["pid"] > 0
    assert marker.read_text() == "handoff\n"
    before = (receipt.read_bytes(), receipt.stat().st_mtime_ns, receipt.stat().st_mode)
    second = subprocess.run(command, env=env, capture_output=True, text=True, timeout=15)
    assert second.returncode != 0 and "FileExistsError" in second.stderr
    assert (receipt.read_bytes(), receipt.stat().st_mtime_ns, receipt.stat().st_mode) == before
    assert marker.read_text() == "handoff\n"  # collision fails before fixture/Core handoff


@pytest.mark.parametrize("drift", ["facts", "interpreter"])
def test_binding_assertion_fails_before_observation_and_handoff(tmp_path, drift):
    facts, binding, marker, receipt, command, env = _fixture(tmp_path)
    if drift == "facts":
        facts.write_text('{"scope":"TEST_ONLY_CHANGED"}')
    else:
        binding["physical_python"] = str(tmp_path / "not-the-real-interpreter")
        command = launch_command(binding, tmp_path / "fixture_handoff.py", [], receipt)
    result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=15)
    assert result.returncode != 0 and "AssertionError" in result.stderr
    assert not receipt.exists() and not marker.exists()
