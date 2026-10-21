"""Test-only paired identity receipts. Never serialize raw argv, env or exceptions."""
from datetime import UTC, datetime
import hashlib
import inspect
import json
import os
from pathlib import Path
import platform
import subprocess


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def file_identity(path):
    resolved = Path(path).resolve(strict=True)
    with resolved.open('rb') as stream:
        before = os.fstat(stream.fileno())
        sha = hashlib.file_digest(stream, 'sha256').hexdigest()
        after = os.fstat(stream.fileno())
    stable = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) == (
        after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    return {'normalized_path': str(resolved), 'sha256': sha, 'device': after.st_dev,
            'inode': after.st_ino, 'bytes': after.st_size, 'stable_read': stable}


def argv_summary(args):
    # Even unexpected arguments are represented only by position, length and hash.
    return [{'index': i, 'length': len(a), 'sha256': digest(a)} for i, a in enumerate(args)]


def compare_creation(args, actual_args, executable):
    expected_exe = str(Path(args[0]).resolve())
    actual_exe = str(Path(executable).resolve())
    fields = []
    if tuple(args[1:]) != tuple(actual_args[1:]):
        fields.append('ARGV_AFTER_EXECUTABLE')
    if expected_exe != actual_exe:
        fields.append('EXECUTABLE_NORMALIZED_PATH')
    return {'EXPECTED': {'executable': expected_exe, 'argv': argv_summary(args)},
            'ACTUAL': {'executable': actual_exe, 'argv': argv_summary(actual_args)},
            'mismatch_fields': fields, 'first_mismatch_field': fields[0] if fields else 'NONE'}


def architecture(command):
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=3, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return 'UNAVAILABLE'
    words = result.stdout.split()
    allowed = {'arm64', 'arm64e', 'x86_64', 'x86_64h', 'i386'}
    return words if result.returncode == 0 and words and set(words) <= allowed else 'UNAVAILABLE'


class IdentityReceipt:
    """Observe the original binding values; preserve every native guard and result."""

    def __init__(self, directory, task, executable, profile):
        self.path = directory / (task + '-identity.json') if directory else None
        self.data = {'task': task, 'host_architecture': platform.machine(), 'stages': [],
                     'first_rejection': None, 'registration': 'NOT_REACHED',
                     'executable_before_spawn': file_identity(executable),
                     'binary_architectures': architecture(['/usr/bin/lipo', '-archs', executable])}
        s = profile.stat()
        self.data['profile_expected'] = {'path': str(profile), 'device': s.st_dev,
                                         'inode': s.st_ino, 'mode': oct(s.st_mode & 0o777)}
        self.record('BEFORE_SPAWN')

    def record(self, stage):
        self.data['stages'].append({'stage': stage,
                                   'observed_at': datetime.now(UTC).isoformat()})
        if self.path:
            # This receipt belongs exclusively to this test invocation and task.
            self.path.write_text(json.dumps(self.data, indent=2) + '\n')

    def process_started(self, process):
        self.data['popen_pid'] = process.pid
        self.data['creation_argv'] = argv_summary(process.args)
        self.record('POPEN_RETURNED')

    def observe_ready(self, process, profile, endpoint, session):
        import psutil
        observed = psutil.Process(process.pid)
        self.data['precheck'] = compare_creation(process.args, observed.cmdline(), observed.exe())
        self.data['executable_at_check'] = file_identity(observed.exe())
        self.data['process'] = {'EXPECTED': {'pid': process.pid, 'ppid': os.getpid(),
                                           'uid': os.getuid(), 'process_group': process.pid},
                                'ACTUAL': {'pid': observed.pid, 'ppid': observed.ppid(),
                                           'uid': observed.uids().real,
                                           'process_group': os.getpgid(observed.pid),
                                           'create_time': observed.create_time()}}
        self.data['execution_architecture'] = architecture(
            ['/bin/ps', '-p', str(process.pid), '-o', 'arch='])
        s = profile.stat()
        self.data['profile_actual'] = {'path': str(profile.resolve()), 'device': s.st_dev,
                                       'inode': s.st_ino, 'mode': oct(s.st_mode & 0o777)}
        expected = {'owner_task_id': self.data['task'], 'session_key': self.data['task'],
                    'endpoint_sha256': digest(endpoint), 'cdp_override': True}
        actual = {'owner_task_id': session['owner_task_id'], 'session_key': session['session_key'],
                  'endpoint_sha256': digest(session['cdp_url']),
                  'cdp_override': session['features']['cdp_override']}
        self.data['session'] = {'EXPECTED': expected, 'ACTUAL': actual,
                               'active_port_file_sha256': file_identity(
                                   profile / 'DevToolsActivePort')['sha256']}
        self.record('BEFORE_FIXTURE_PRECHECK')
        return not self.data['precheck']['mismatch_fields']

    def instrument_registration(self, monkeypatch, module):
        original_binding, original_require = module._Binding, module._require

        def binding(*args, **kwargs):
            value = original_binding(*args, **kwargs)
            # These are the exact immutable values used by the next composite guard.
            self.data['native_binding_check'] = compare_creation(
                value.process.args, value.argv, value.executable)
            self.data['bound_process_create_time'] = value.start
            self.record('NATIVE_BINDING_CONSTRUCTED')
            return value

        def require(ok):
            if not ok and self.data['first_rejection'] is None:
                frame = inspect.currentframe().f_back
                self.data['first_rejection'] = {
                    'function': frame.f_code.co_name, 'line': frame.f_lineno,
                    'safe_error_code': 'OWNED_BROWSER_IDENTITY_REJECTED'}
                del frame
                self.record('NATIVE_GUARD_REJECTED')
            return original_require(ok)

        monkeypatch.setattr(module, '_Binding', binding)
        monkeypatch.setattr(module, '_require', require)

    def cleanup(self, process, profile, groups_gone):
        self.data['cleanup'] = {'child_exit_code': process.poll(),
                                'profile_removed': not profile.exists(),
                                'owned_process_group_gone': groups_gone}
        self.record('FIXTURE_CLEANUP_COMPLETE')
