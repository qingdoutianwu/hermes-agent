"""Native Cron child proof with a one-use ledger binding."""
from __future__ import annotations
import hashlib,json,os,select,stat,time
from pathlib import Path
MAX_BYTES,MAX_TTL=4096,30
_signers={}
class ProofError(RuntimeError):
 def __init__(self,code):self.code=code;super().__init__(code)
def install_signer(home,key,entry):
 home=str(Path(home).resolve())
 if home in _signers:raise ProofError("PROOF_SIGNER_ALREADY_BOUND")
 _signers[home]=(key,Path(entry).resolve(strict=True))
def canonical(v):return json.dumps(v,sort_keys=True,separators=(",",":"),allow_nan=False).encode()
def scrub_identity(env):
 for n in tuple(env):
  if n.startswith("HERMES_CRON_") or n in {"FINANCE_TRIGGER_TYPE","FINANCE_SCHEDULED_AT","FINANCE_DISPATCHED_AT"}:env.pop(n,None)
def _owned_identity(home,entry):
 from cron import executions
 from cron.execution_identity import current_cron_execution
 from cron.jobs import get_job
 from hermes_constants import get_hermes_home
 if str(get_hermes_home().resolve())!=home:raise ProofError("PROOF_HOME_BINDING_DENIED")
 ident=current_cron_execution()
 if ident is None:raise ProofError("SCHEDULER_ORIGIN_REQUIRED")
 row,job=executions.get_execution(ident.execution_id),get_job(ident.job_id)
 if not row or not job:raise ProofError("PROOF_NATIVE_ROW_REQUIRED")
 expected={"id":ident.execution_id,"job_id":ident.job_id,"source":"builtin","scheduled_instant":ident.scheduled_instant,"started_at":ident.started_at,"status":"running","pid":os.getpid(),"process_id":executions._PROCESS_ID}
 if any(row.get(k)!=v for k,v in expected.items()) or not ident.scheduled_instant:raise ProofError("PROOF_EXECUTION_OWNERSHIP_DENIED")
 if row.get("finished_at") is not None or not row.get("process_started_at"):raise ProofError("PROOF_EXECUTION_STATE_DENIED")
 if Path(job.get("script","")).resolve()!=entry or entry.is_symlink():raise ProofError("PROOF_ENTRY_BINDING_DENIED")
 return ident,row
class ChildIssue:
 def __init__(self,home,key,entry,ident,row):self.home,self.key,self.entry,self.ident,self.row=home,key,entry,ident,row
 def deliver(self,process):
  from cron import executions
  from hermes_cli.sqlite_util import add_column_if_missing
  ident,row=_owned_identity(self.home,self.entry)
  if ident!=self.ident or any(row[k]!=self.row[k] for k in ("pid","process_id","process_started_at","started_at","scheduled_instant")):raise ProofError("PROOF_OWNER_CHANGED")
  issued=time.time();body={"schema":"hermes-native-child-proof.v1","home":self.home,"profile":ident.profile,"job_id":ident.job_id,"execution_id":ident.execution_id,"source":row["source"],"scheduled_instant":row["scheduled_instant"],"started_at":row["started_at"],"owner_pid":row["pid"],"owner_process_id":row.get("process_id") or executions._PROCESS_ID,"owner_started_at":row["process_started_at"],"child_pid":process.pid,"entry":str(self.entry),"entry_sha256":hashlib.sha256(self.entry.read_bytes()).hexdigest(),"issued_at":issued,"expires_at":issued+MAX_TTL,"nonce":os.urandom(16).hex()}
  raw=canonical({"body":body,"signature":self.key.sign(canonical(body)).hex()});digest=hashlib.sha256(raw).hexdigest()
  with executions._transaction() as c:
   add_column_if_missing(c,"executions","child_proof_sha256","child_proof_sha256 TEXT");add_column_if_missing(c,"executions","child_proof_used","child_proof_used INTEGER NOT NULL DEFAULT 0")
   if c.execute("UPDATE executions SET child_proof_sha256=?,child_proof_used=0 WHERE id=? AND job_id=? AND status='running' AND source='builtin' AND process_id=? AND pid=? AND child_proof_sha256 IS NULL",(digest,ident.execution_id,ident.job_id,row.get("process_id") or executions._PROCESS_ID,os.getpid())).rowcount!=1:raise ProofError("PROOF_ISSUANCE_ALREADY_USED")
  process.stdin.write(raw.decode());process.stdin.close();process.stdin=None
def prepare_child(entry):
 from hermes_constants import get_hermes_home
 home=str(get_hermes_home().resolve());binding=_signers.get(home)
 if binding is None or Path(entry).resolve()!=binding[1]:return None
 key,resolved=binding;ident,row=_owned_identity(home,resolved);return ChildIssue(home,key,resolved,ident,row)
def read_stdin_proof():
 if not stat.S_ISFIFO(os.fstat(0).st_mode):raise ProofError("SCHEDULER_ORIGIN_REQUIRED")
 deadline=time.monotonic()+5;raw=bytearray()
 while True:
  left=deadline-time.monotonic()
  if left<=0 or not select.select([0],[],[],left)[0]:raise ProofError("PROOF_INPUT_TIMEOUT")
  block=os.read(0,MAX_BYTES+1-len(raw))
  if not block:break
  raw.extend(block)
  if len(raw)>MAX_BYTES:raise ProofError("PROOF_TOO_LARGE")
 if not raw:raise ProofError("SCHEDULER_ORIGIN_REQUIRED")
 return bytes(raw)
