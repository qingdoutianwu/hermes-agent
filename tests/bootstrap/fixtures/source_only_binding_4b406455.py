"""Git-object source-only future-selector transaction (POSIX, no runtime actions).

All selector writers must use the same flock. This tool does not modify an
existing checkout, launcher, environment, config, or running process. A launcher
must explicitly resolve this selector at a later, independently authorized
start; creating a selector alone does not rewire an existing production launcher.
"""
from __future__ import annotations

import argparse
import ctypes
import fcntl
import hashlib
import json
import os
import posixpath
import re
import stat
import subprocess
import sys
import tempfile
from pathlib import Path


class BindingError(RuntimeError):
    pass


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'))+'\n').encode()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def git(repo, *args):
    return subprocess.check_output(['/usr/bin/git', '-C', str(repo), *args], stderr=subprocess.DEVNULL)


def _identity(value, size):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{%d}' % size, value):
        raise BindingError('E_SOURCE_IDENTITY_INVALID')


def source_manifest(repo, commit):
    _identity(commit, 40)
    tree = git(repo, 'rev-parse', commit+'^{tree}').decode().strip()
    files = []
    for record in git(repo, 'ls-tree', '-rz', commit).split(b'\0'):
        if not record:
            continue
        header, name = record.split(b'\t', 1)
        mode, kind, blob = header.decode().split()
        path = name.decode()
        if kind != 'blob' or mode not in {'100644','100755','120000'}:
            raise BindingError('E_SOURCE_ENTRY_UNSUPPORTED')
        data = git(repo, 'cat-file', 'blob', blob)
        if mode == '120000':
            target = data.decode()
            resolved = posixpath.normpath(posixpath.join(posixpath.dirname(path), target))
            if target.startswith('/') or resolved == '..' or resolved.startswith('../'):
                raise BindingError('E_SOURCE_SYMLINK_ESCAPE')
        files.append({'path':path,'git_mode':mode,'blob_sha1':blob,'sha256':digest(data),'bytes':len(data)})
    return {'schema_version':1,'source_commit':commit,'source_tree':tree,'files':files}


def _fsync_dir(path):
    fd=os.open(path,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)


def _publish_sealed(staging, version):
    # macOS rejects ordinary rename of some sealed directories. Preserve the
    # seal using the same renamex_np SWAP primitive as existing source installs.
    if sys.platform=='darwin':
        version.mkdir(mode=0o555)
        swap=ctypes.CDLL(None,use_errno=True).renamex_np
        swap.argtypes=[ctypes.c_char_p,ctypes.c_char_p,ctypes.c_uint];swap.restype=ctypes.c_int
        if swap(os.fsencode(staging),os.fsencode(version),2)!=0:
            raise BindingError('E_SOURCE_STAGE_PUBLISH:'+str(ctypes.get_errno()))
    else:
        os.rename(staging,version)


def _atomic(path, data, mode=0o444):
    fd,name=tempfile.mkstemp(prefix='.'+path.name+'.',dir=path.parent)
    temp=Path(name)
    try:
        with os.fdopen(fd,'wb') as handle:
            handle.write(data);handle.flush();os.fchmod(handle.fileno(),mode);os.fsync(handle.fileno())
        os.replace(temp,path);_fsync_dir(path.parent)
    finally:temp.unlink(missing_ok=True)


def _root(root):
    raw=Path(root)
    if not raw.is_absolute() or '..' in raw.parts or raw.is_symlink() or not raw.is_dir():
        raise BindingError('E_SOURCE_AUTHORITY_ROOT')
    root=raw.resolve()
    if root!=raw or root.stat().st_uid!=os.geteuid() or stat.S_IMODE(root.stat().st_mode)&0o022:
        raise BindingError('E_SOURCE_AUTHORITY_ROOT')
    for name in ['versions','transactions','future-source.json','.source-bind.lock']:
        if (root/name).is_symlink():raise BindingError('E_SOURCE_AUTHORITY_SYMLINK')
    return root


def verify_version(root, manifest):
    if root.is_symlink() or not root.is_dir() or stat.S_IMODE(root.stat().st_mode)!=0o555:
        raise BindingError('E_SOURCE_VERSION_MODE')
    expected={row['path'] for row in manifest['files']}
    actual={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() or p.is_symlink()}
    if actual!=expected:raise BindingError('E_SOURCE_FILE_SET')
    for row in manifest['files']:
        path=root/row['path'];info=path.lstat()
        if row['git_mode']=='120000':
            if not stat.S_ISLNK(info.st_mode):raise BindingError('E_SOURCE_FILE_MODE')
            data=os.readlink(path).encode()
        else:
            mode=0o555 if row['git_mode']=='100755' else 0o444
            if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode)!=mode:raise BindingError('E_SOURCE_FILE_MODE')
            data=path.read_bytes()
        if len(data)!=row['bytes'] or digest(data)!=row['sha256']:raise BindingError('E_SOURCE_BYTES')
        if hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()!=row['blob_sha1']:
            raise BindingError('E_SOURCE_BLOB')


def read_authority(root):
    root=_root(root);p=root/'future-source.json'
    if p.is_symlink() or not p.is_file() or stat.S_IMODE(p.stat().st_mode)!=0o444:
        raise BindingError('E_SOURCE_AUTHORITY_RECORD')
    return json.loads(p.read_bytes())


def resolve_future_source(root):
    """Read-only future-start consumer; never changes an already running process."""
    root=_root(root);record=read_authority(root);_identity(record['source_commit'],40)
    manifest_path=root/'versions'/(record['source_commit']+'.manifest.json')
    if manifest_path.is_symlink() or stat.S_IMODE(manifest_path.stat().st_mode)!=0o444:
        raise BindingError('E_SOURCE_MANIFEST_MODE')
    raw=manifest_path.read_bytes()
    if digest(raw)!=record['source_manifest_sha256']:raise BindingError('E_SOURCE_MANIFEST_HASH')
    manifest=json.loads(raw)
    if manifest['source_commit']!=record['source_commit'] or manifest['source_tree']!=record['source_tree']:
        raise BindingError('E_SOURCE_MANIFEST_IDENTITY')
    version=root/'versions'/record['source_commit'];verify_version(version,manifest)
    return version


def bind(*, repo, authority_root, base_commit, successor_commit, successor_tree,
         source_manifest_path, source_manifest_sha256, expected_current_authority,
         transaction_id, dry_run=False, inject=None):
    """The only writable boundary is this private source-authority namespace."""
    root=_root(authority_root);repo=Path(repo)
    for value in [base_commit,successor_commit,successor_tree]:_identity(value,40)
    _identity(source_manifest_sha256,64)
    if not re.fullmatch('[a-z][a-z0-9-]{7,79}',transaction_id):raise BindingError('E_SOURCE_TRANSACTION_ID')
    if git(repo,'rev-parse',successor_commit+'^').decode().strip()!=base_commit:
        raise BindingError('E_SOURCE_LINEAGE_MISMATCH')
    if git(repo,'rev-parse',successor_commit+'^{tree}').decode().strip()!=successor_tree:
        raise BindingError('E_SOURCE_TREE_MISMATCH')
    path=Path(source_manifest_path)
    if path.is_symlink() or not path.is_file() or stat.S_IMODE(path.stat().st_mode)!=0o444:
        raise BindingError('E_SOURCE_MANIFEST_MODE')
    raw=path.read_bytes()
    if digest(raw)!=source_manifest_sha256:raise BindingError('E_SOURCE_MANIFEST_HASH')
    manifest=json.loads(raw)
    if manifest!=source_manifest(repo,successor_commit):raise BindingError('E_SOURCE_MANIFEST_IDENTITY')
    current=read_authority(root)
    resolve_future_source(root)
    if current!=expected_current_authority or current.get('source_commit')!=base_commit:
        raise BindingError('E_SOURCE_AUTHORITY_CAS_MISMATCH')
    if dry_run:
        return {'state':'VERIFIED_DRY_RUN','authority_mutation_count':0,'runtime_activated':False}
    with (root/'.source-bind.lock').open('a+b') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        if read_authority(root)!=expected_current_authority:raise BindingError('E_SOURCE_AUTHORITY_CAS_MISMATCH')
        versions=root/'versions';journals=root/'transactions'
        versions.mkdir(exist_ok=True);journals.mkdir(exist_ok=True)
        version=versions/successor_commit
        journal=journals/(transaction_id+'.json')
        if journal.exists() or version.exists():raise BindingError('E_SOURCE_TRANSACTION_REPLAY')
        staging=versions/('.stage-'+transaction_id)
        record={'schema_version':1,'transaction_id':transaction_id,'phase':'PREPARED',
                'previous':current,'successor':{'source_commit':successor_commit,'source_tree':successor_tree,
                    'source_manifest_sha256':source_manifest_sha256},'runtime_activated':False,
                'staged_runtime_transition_pending':True}
        def state(phase):
            record['phase']=phase;_atomic(journal,encode(record))
        state('PREPARED')
        try:
            staging.mkdir()
            for row in manifest['files']:
                p=staging/row['path'];p.parent.mkdir(parents=True,exist_ok=True)
                data=git(repo,'cat-file','blob',row['blob_sha1'])
                if row['git_mode']=='120000':p.symlink_to(data.decode())
                else:
                    with p.open('xb') as handle:handle.write(data);handle.flush();os.fsync(handle.fileno())
                    p.chmod(0o555 if row['git_mode']=='100755' else 0o444)
            for p in sorted(staging.rglob('*'),key=lambda p:len(p.parts),reverse=True):
                if p.is_dir() and not p.is_symlink():
                    p.chmod(0o555);_fsync_dir(p)
            staging.chmod(0o555);_fsync_dir(staging);state('STAGED')
            if inject:inject('STAGED')
            verify_version(staging,manifest)
            _publish_sealed(staging,version);_fsync_dir(versions)
            _atomic(versions/(successor_commit+'.manifest.json'),raw)
            state('VERIFIED')
            if inject:inject('BEFORE_CAS')
            if read_authority(root)!=expected_current_authority:raise BindingError('E_SOURCE_AUTHORITY_CAS_MISMATCH')
            if inject:inject('BEFORE_COMMIT')
            _atomic(root/'future-source.json',encode(record['successor']))
            if read_authority(root)!=record['successor']:raise BindingError('E_SOURCE_AUTHORITY_POST_READBACK')
            state('COMMITTED')
            return record
        except BaseException:
            # A replace-then-error or crash is resolved from the actual selector,
            # never by rolling back a legitimately old running process.
            state('COMMITTED' if read_authority(root)==record['successor'] else 'ABORTED')
            raise


def reconcile_transaction(authority_root, transaction_id):
    """Recover a crashed journal from selector identity; never signal/rollback runtime."""
    root=_root(authority_root)
    if not re.fullmatch('[a-z][a-z0-9-]{7,79}',transaction_id):raise BindingError('E_SOURCE_TRANSACTION_ID')
    with (root/'.source-bind.lock').open('a+b') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        path=root/'transactions'/(transaction_id+'.json');record=json.loads(path.read_bytes())
        actual=read_authority(root)
        if actual==record['successor']:
            resolve_future_source(root);record['phase']='COMMITTED'
        elif actual==record['previous']:record['phase']='ABORTED'
        else:raise BindingError('E_SOURCE_RECOVERY_EXTERNAL_AUTHORITY')
        _atomic(path,encode(record));return record


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ['repo','authority-root','base-commit','successor-commit','successor-tree',
                 'source-manifest-path','source-manifest-sha256','expected-current-authority','transaction-id']:
        parser.add_argument('--'+name,required=True)
    parser.add_argument('--dry-run',action='store_true')
    args=vars(parser.parse_args());args['expected_current_authority']=json.loads(args['expected_current_authority'])
    print(json.dumps(bind(**args),sort_keys=True))


if __name__=='__main__':main()
