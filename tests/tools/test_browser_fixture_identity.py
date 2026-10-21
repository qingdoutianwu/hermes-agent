"""Offline diagnostic contracts; no browser, network or production data."""
import json
from types import SimpleNamespace

import pytest

from tests.tools.browser_fixture_identity import IdentityReceipt, argv_summary, compare_creation, file_identity


@pytest.mark.parametrize('change,field', [
    ('argv', 'ARGV_AFTER_EXECUTABLE'), ('exe', 'EXECUTABLE_NORMALIZED_PATH'), ('none', 'NONE')])
def test_paired_identity_first_mismatch(tmp_path, change, field):
    executable = str(tmp_path / 'browser')
    args = [executable, '--headless=new']
    actual = [executable, '--changed'] if change == 'argv' else args
    result = compare_creation(args, actual, executable + '-other' if change == 'exe' else executable)
    assert result['first_mismatch_field'] == field
    assert 'EXPECTED' in result and 'ACTUAL' in result


def test_sensitive_arguments_never_serialized():
    raw = ['browser', '--proxy-server=https://user:password@host', '--token=secret-value']
    serialized = json.dumps(argv_summary(raw))
    assert all(value not in serialized for value in ['password', 'secret-value', 'user', 'host'])
    assert len(argv_summary(raw)) == len(raw)


def test_native_guard_diagnostic_preserves_rejection(monkeypatch):
    class Rejected(ValueError):
        pass

    def reject(ok):
        if not ok:
            raise Rejected('SAFE_ERROR')

    module = SimpleNamespace(_Binding=lambda *a: None, _require=reject)
    receipt = object.__new__(IdentityReceipt)
    receipt.path = None
    receipt.data = {'first_rejection': None, 'stages': []}
    receipt.instrument_registration(monkeypatch, module)
    with pytest.raises(Rejected, match='SAFE_ERROR'):
        module._require(False)
    assert receipt.data['first_rejection']['function'] == 'test_native_guard_diagnostic_preserves_rejection'
    assert receipt.data['first_rejection']['safe_error_code'] == 'OWNED_BROWSER_IDENTITY_REJECTED'


def test_executable_comparison_normalizes_alias(tmp_path):
    executable = tmp_path / 'browser'
    executable.touch()
    alias = tmp_path / 'alias'
    alias.symlink_to(executable)
    assert compare_creation([str(alias), '--flag'], [str(executable), '--flag'],
                            str(executable))['mismatch_fields'] == []


def test_native_binding_exact_values_returned_unchanged(monkeypatch, tmp_path):
    executable = str(tmp_path / 'browser')
    args = [executable, '--expected']
    value = SimpleNamespace(process=SimpleNamespace(args=args),
                            argv=(executable, '--actual'), executable=executable, start=12)
    module = SimpleNamespace(_Binding=lambda: value, _require=lambda ok: None)
    receipt = object.__new__(IdentityReceipt)
    receipt.path = None
    receipt.data = {'first_rejection': None, 'stages': []}
    receipt.instrument_registration(monkeypatch, module)
    assert module._Binding() is value
    assert receipt.data['native_binding_check']['first_mismatch_field'] == 'ARGV_AFTER_EXECUTABLE'
    assert receipt.data['bound_process_create_time'] == 12


def test_file_identity_and_cleanup_receipt(tmp_path):
    executable = tmp_path / 'browser'
    executable.write_bytes(b'fixture-executable')
    identity = file_identity(executable)
    assert identity['stable_read'] and identity['inode'] == executable.stat().st_ino
    receipt = object.__new__(IdentityReceipt)
    receipt.path = tmp_path / 'receipt.json'
    receipt.data = {'stages': []}
    receipt.cleanup(SimpleNamespace(poll=lambda: 0), tmp_path / 'removed-profile', True)
    result = json.loads(receipt.path.read_text())
    assert result['cleanup'] == {'child_exit_code': 0, 'profile_removed': True,
                                  'owned_process_group_gone': True}
