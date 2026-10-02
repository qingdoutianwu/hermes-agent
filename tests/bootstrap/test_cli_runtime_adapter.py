"""Real unchanged logger, atomic tool, exec CLI and isolated signal/fd proof."""
import importlib.machinery
import importlib.util
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys

import pytest

# Every selected source is an inert fixture and every child is reaped here.
# Use the repository's explicit lookalike contract; do not weaken the guard.
pytestmark = pytest.mark.spawns_gateway_lookalike

ROOT=Path(__file__).resolve().parents[2]
CONSUMER=ROOT/'bootstrap/hermes'
LOGGER=ROOT/'hermes_cli/stderr_timestamp.py'

def load(path,name):
    loader=importlib.machinery.SourceFileLoader(name,str(path))
    spec=importlib.util.spec_from_loader(name,loader);m=importlib.util.module_from_spec(spec);loader.exec_module(m);return m

adapter=load(CONSUMER,'stable_adapter')
tool=load(Path(__file__).parent/'fixtures/source_only_binding_4b406455.py','atomic_tool')

PROGRAM='''import os,sys,json,signal
from pathlib import Path
from version import VERSION
def output():
    print(json.dumps({'version':VERSION,'pid':os.getpid(),'argv':sys.argv[1:],
          'cwd':os.getcwd(),'env':{k:os.environ.get(k) for k in ['HERMES_HOME','PATH','PYTHONPATH','VIRTUAL_ENV','HOME','HERMES_LAUNCHD_LABEL']}}),flush=True)
print('stderr-original',file=sys.stderr,flush=True)
output()
mode=os.environ.get('FIXTURE_MODE','wait')
if mode=='exit':sys.exit(int(os.environ['FIXTURE_EXIT']))
if mode=='fd':
    os.write(int(os.environ['FIXTURE_FD']),b'fd-preserved');sys.exit(0)
if mode=='signal':
    def received(n,f):
        print('received-signal='+str(n),file=sys.stderr,flush=True)
        signal.signal(n,signal.SIG_DFL);os.kill(os.getpid(),n)
    for n in [signal.SIGTERM,signal.SIGINT,signal.SIGHUP,signal.SIGUSR1,signal.SIGUSR2]:signal.signal(n,received)
    print('signals-ready',flush=True)
for line in sys.stdin:output()
'''

@pytest.fixture
def source(tmp_path):
    tmp_path=tmp_path.resolve();repo=tmp_path/'repo';repo.mkdir()
    def git(*a):return subprocess.check_output(['/usr/bin/git','-C',str(repo),*a],stderr=subprocess.DEVNULL)
    git('init');git('config','user.name','Fixture');git('config','user.email','fixture@example.invalid')
    (repo/'hermes_cli').mkdir();(repo/'hermes_cli/__init__.py').write_text('')
    (repo/'hermes_cli/main.py').write_text(PROGRAM);(repo/'version.py').write_text("VERSION='A'\n")
    git('add','.');git('commit','-m','A');a=git('rev-parse','HEAD').decode().strip()
    (repo/'version.py').write_text("VERSION='B'\n");git('add','.');git('commit','-m','B');b=git('rev-parse','HEAD').decode().strip()
    root=tmp_path/'authority';root.mkdir(mode=0o700);ma=tool.source_manifest(repo,a)
    version=root/'versions'/a;version.mkdir(parents=True)
    for row in ma['files']:
        p=version/row['path'];p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(git('cat-file','blob',row['blob_sha1']));p.chmod(0o444)
    for p in sorted(version.rglob('*'),key=lambda x:len(x.parts),reverse=True):
        if p.is_dir():p.chmod(0o555)
    version.chmod(0o555);mp=root/'versions'/(a+'.manifest.json');mp.write_bytes(tool.encode(ma));mp.chmod(0o444)
    old={'source_commit':a,'source_tree':ma['source_tree'],'source_manifest_sha256':tool.digest(mp.read_bytes())}
    (root/'future-source.json').write_bytes(tool.encode(old));(root/'future-source.json').chmod(0o444)
    mb=tool.source_manifest(repo,b);nextmp=tmp_path/'b.json';nextmp.write_bytes(tool.encode(mb));nextmp.chmod(0o444)
    args=dict(repo=repo,authority_root=root,base_commit=a,successor_commit=b,successor_tree=mb['source_tree'],source_manifest_path=nextmp,source_manifest_sha256=tool.digest(nextmp.read_bytes()),expected_current_authority=old,transaction_id='cli-adapter-test-0001')
    return root,args

def command(root):
    return [sys.executable,'-I','-B',str(CONSUMER),'--authority-root',str(root),'--runtime-python',sys.executable,'--','gateway','run','--external-supervisor']

def start(root,tmp,wrapped=False,extra=None,pass_fds=()):
    env={**os.environ,'HERMES_HOME':str(tmp),'HERMES_LAUNCHD_LABEL':'ai.hermes.fixture','XPC_SERVICE_NAME':'ai.hermes.fixture'}
    env.update(extra or {})
    cmd=command(root)
    if wrapped:
        body="import sys,runpy;sys.path.insert(0,"+repr(str(ROOT))+ ");sys.argv=["+repr(str(LOGGER))+",*sys.argv[1:]];runpy.run_path("+repr(str(LOGGER))+",run_name='__main__')"
        cmd=[sys.executable,'-I','-B','-c',body,'--error-log',str(tmp/'stderr.log'),'--',*cmd]
    return subprocess.Popen(cmd,cwd=tmp,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,pass_fds=pass_fds)

def read(p):
    s=selectors.DefaultSelector();s.register(p.stdout,selectors.EVENT_READ)
    try:
        assert s.select(15),'startup timeout'
        line=p.stdout.readline();at=line.find('{');assert at>=0,line
        return json.loads(line[at:])
    finally:s.close()

def stop(p):
    p.stdin.close();assert p.wait(timeout=15)==0;p.stdout.close();p.stderr.close()

@pytest.mark.parametrize('code',[0,1,2,64,65,70,75,78,143])
def test_exit_layering_and_unchanged_logger(source,tmp_path,code):
    root,_=source
    for wrapped in [False,True]:
        p=start(root,tmp_path,wrapped,{'FIXTURE_MODE':'exit','FIXTURE_EXIT':str(code)})
        out,err=p.communicate(timeout=15)
        assert p.returncode==(0 if wrapped and code==78 else code)
        assert 'stderr-original' in (tmp_path/'stderr.log').read_text() if wrapped else 'stderr-original' in err
        if wrapped:assert out[:4].isdigit() and 'stderr-original' not in out

def test_real_atomic_A_B_and_pid_env_cwd(source,tmp_path):
    root,args=source;p1=start(root,tmp_path,True)
    try:
        before=read(p1);assert before['version']=='A'
        assert before['cwd']==str(tmp_path) and before['argv']==['gateway','run','--external-supervisor']
        assert before['env']['HERMES_HOME']==str(tmp_path) and before['env']['HERMES_LAUNCHD_LABEL']=='ai.hermes.fixture'
        assert tool.bind(**args)['phase']=='COMMITTED'
        p1.stdin.write('after\n');p1.stdin.flush();assert read(p1)['version']=='A'
        p2=start(root,tmp_path,True)
        try:assert read(p2)['version']=='B'
        finally:stop(p2)
        (root/'future-source.json').unlink();p1.stdin.write('missing\n');p1.stdin.flush();assert read(p1)['version']=='A'
    finally:stop(p1)
    p=start(root,tmp_path)
    out,err=p.communicate(timeout=15);assert p.returncode==os.EX_DATAERR and 'stderr-original' not in err

@pytest.mark.parametrize('sig',[signal.SIGTERM,signal.SIGINT,signal.SIGHUP,signal.SIGUSR1,signal.SIGUSR2])
def test_real_isolated_signal_forwarding(source,tmp_path,sig):
    root,_=source;p=start(root,tmp_path,True,{'FIXTURE_MODE':'signal'})
    one=read(p)
    # Readiness occurs after handlers install; no timing sleep/race assumption.
    assert 'signals-ready' in p.stdout.readline()
    os.kill(p.pid,sig)
    assert p.wait(timeout=15)==128+sig
    assert 'received-signal='+str(sig) in (tmp_path/'stderr.log').read_text()
    assert one['pid']!=p.pid  # logger parent remains, consumer execs its child
    p.stdin.close();p.stdout.close();p.stderr.close()

def test_exec_pid_fd_environment_preserved(source,tmp_path):
    root,_=source;r,w=os.pipe()
    try:
        p=start(root,tmp_path,False,{'FIXTURE_MODE':'fd','FIXTURE_FD':str(w),'PYTHONPATH':'inert-fixture-path','VIRTUAL_ENV':'inert-fixture-venv'},(w,))
        observed=read(p);assert observed['pid']==p.pid
        assert observed['env']['PYTHONPATH']=='inert-fixture-path' and observed['env']['VIRTUAL_ENV']=='inert-fixture-venv'
        assert p.wait(timeout=15)==0;os.close(w);w=None
        assert os.read(r,100)==b'fd-preserved'
        p.stdin.close();p.stdout.close();p.stderr.close()
    finally:
        os.close(r)
        if w is not None:os.close(w)

@pytest.mark.parametrize('failure',['missing','malformed','manifest','commit','tree','unsealed','entrypoint','cli','exec','version-missing','schema','authority-escape'])
def test_preflight_failures_not_gateway_78(source,tmp_path,failure,monkeypatch):
    root,args=source;version=root/'versions'/args['base_commit'];selector=root/'future-source.json'
    if failure=='missing':selector.unlink()
    elif failure=='malformed':tool._atomic(selector,b'{')
    elif failure in ['manifest','commit','tree']:
        x=tool.read_authority(root);x[{'manifest':'source_manifest_sha256','commit':'source_commit','tree':'source_tree'}[failure]]='0'*(64 if failure=='manifest' else 40);tool._atomic(selector,tool.encode(x))
    elif failure=='unsealed':version.chmod(0o755)
    elif failure=='version-missing':
        version.chmod(0o755);version.rename(version.with_name('missing'))
    elif failure=='schema':
        x=tool.read_authority(root);x['schema_version']=2;tool._atomic(selector,tool.encode(x))
    elif failure=='authority-escape':
        argv=['--authority-root',str(root/'..'),'--runtime-python',sys.executable,'--','gateway','run','--external-supervisor']
        assert adapter.main(argv)==os.EX_DATAERR
        return
    elif failure=='entrypoint':
        version.chmod(0o755);(version/'hermes_cli').chmod(0o755);(version/'hermes_cli/main.py').unlink()
    elif failure=='exec':monkeypatch.setattr(os,'execve',lambda *a:(_ for _ in ()).throw(OSError(8,'fixture exec',sys.executable)))
    argv=['--authority-root',str(root),'--runtime-python',sys.executable,'--',*(['bad'] if failure=='cli' else ['gateway','run','--external-supervisor'])]
    assert adapter.main(argv)==(os.EX_OSERR if failure=='exec' else os.EX_DATAERR)

def test_selector_exactly_one_read(source,monkeypatch):
    root,_=source;old=Path.read_bytes;calls=[]
    def count(p):
        if p==root/'future-source.json':calls.append(p)
        return old(p)
    monkeypatch.setattr(Path,'read_bytes',count)
    pin=adapter.resolve_once(root)
    assert pin['selector_read_count']==len(calls)==1
