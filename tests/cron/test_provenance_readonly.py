"""Read-only provenance contract tests."""
import json,sqlite3
from pathlib import Path
from cron.provenance_readonly import verify_receipt_provenance
JOB,EXEC,SLOT,RELEASE,PROOF="job-native","execution-native","2026-10-15T09:00:00+08:00","HK-ANOMALY-PROD-test","a"*64

def setup(p,profile="default"):
 with sqlite3.connect(p) as c:
  c.execute("CREATE TABLE executions(id TEXT,job_id TEXT,source TEXT,profile TEXT,status TEXT,scheduled_instant TEXT,child_proof_sha256 TEXT,child_proof_used INTEGER,finished_at TEXT)");c.execute("INSERT INTO executions VALUES(?,?,?, ?,?,?,?,1,?)",(EXEC,JOB,"builtin",profile,"completed",SLOT,PROOF,SLOT))
def rec(**x):return {"receipt_id":"r1","execution_origin":"NATURAL_CRON","cron_job_id":JOB,"execution_id":EXEC,"profile":"default","scheduled_at":SLOT,"release_binding":RELEASE,"child_proof_sha256":PROOF,**x}
def test_verified(tmp_path):
 p,r=tmp_path/"e",tmp_path/"r";setup(p);r.write_text(json.dumps(rec()));o=verify_receipt_provenance(r,ledger_path=p,expected_job_id=JOB,expected_profile="default",expected_release_id=RELEASE);assert o["trusted"]
def test_spoof(tmp_path):
 p,r=tmp_path/"e",tmp_path/"r";setup(p,"other");r.write_text(json.dumps(rec(child_proof_sha256="b"*64)));o=verify_receipt_provenance(r,ledger_path=p,expected_job_id=JOB,expected_profile="default",expected_release_id=RELEASE);assert not o["trusted"]
def test_duplicate(tmp_path):
 p,a,b=tmp_path/"e",tmp_path/"a",tmp_path/"b";setup(p);a.write_text(json.dumps(rec()));b.write_text(json.dumps(rec(receipt_id="r2")));o=verify_receipt_provenance(a,ledger_path=p,expected_job_id=JOB,expected_profile="default",expected_release_id=RELEASE,related_receipt_paths=[b]);assert "duplicate_execution_receipt" in o["reasons"]
def test_read_only(tmp_path):
 p,r=tmp_path/"e",tmp_path/"r";setup(p);r.write_text(json.dumps(rec()));before=p.read_bytes();verify_receipt_provenance(r,ledger_path=p,expected_job_id=JOB,expected_profile="default",expected_release_id=RELEASE);assert p.read_bytes()==before
