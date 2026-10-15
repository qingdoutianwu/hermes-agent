"""Read-only R3 provenance reconciliation."""
from __future__ import annotations
import json,sqlite3
from pathlib import Path

def _load(p):
    try: v=json.loads(Path(p).read_text())
    except (OSError,ValueError,TypeError): return None
    return v if isinstance(v,dict) else None

def _row(p,eid):
    try:
        with sqlite3.connect(f"file:{p}?mode=ro",uri=True) as c:
            c.row_factory=sqlite3.Row;c.execute("PRAGMA query_only=ON")
            cols={r[1] for r in c.execute("PRAGMA table_info(executions)")}
            if not {"child_proof_sha256","child_proof_used"}.issubset(cols): return None
            r=c.execute("SELECT id,job_id,source,profile,status,scheduled_instant,child_proof_sha256,child_proof_used,finished_at FROM executions WHERE id=?",(eid,)).fetchone()
            return dict(r) if r else None
    except (OSError,sqlite3.Error): return None

def verify_receipt_provenance(receipt_path:Path,*,ledger_path:Path,expected_job_id:str,expected_profile:str,expected_release_id:str,related_receipt_paths=()):
    r=_load(receipt_path)
    if r is None:return {"status":"PROVENANCE_UNVERIFIED","trusted":False,"reasons":["receipt_unreadable"]}
    row=_row(ledger_path,str(r.get("execution_id") or ""))
    checks={"NATIVE_EXECUTION_VERIFIED":bool(row and row.get("source")=="builtin" and row.get("status") in {"running","completed"} and row.get("finished_at") is not None and row.get("job_id")==expected_job_id and row.get("profile")==expected_profile),"NATIVE_JOB_ID_MATCH":bool(row and r.get("cron_job_id")==expected_job_id and row.get("job_id")==expected_job_id),"NATIVE_PROFILE_MATCH":bool(row and r.get("profile")==expected_profile and row.get("profile")==expected_profile),"SCHEDULED_INSTANT_VERIFIED":bool(row and r.get("scheduled_at") and row.get("scheduled_instant")==r.get("scheduled_at")),"BUSINESS_RECEIPT_LINK_VERIFIED":bool(row and row.get("child_proof_sha256")==r.get("child_proof_sha256") and row.get("child_proof_used")==1),"RELEASE_IDENTITY_MATCH":r.get("release_binding")==expected_release_id}
    reasons=["origin_not_natural_cron"] if r.get("execution_origin")!="NATURAL_CRON" else []
    reasons += [x for x,n in (("native_execution_unverified","NATIVE_EXECUTION_VERIFIED"),("job_id_mismatch","NATIVE_JOB_ID_MATCH"),("profile_mismatch","NATIVE_PROFILE_MATCH"),("scheduled_instant_mismatch","SCHEDULED_INSTANT_VERIFIED"),("child_proof_mismatch","BUSINESS_RECEIPT_LINK_VERIFIED"),("release_identity_mismatch","RELEASE_IDENTITY_MATCH")) if not checks[n]]
    for p in related_receipt_paths:
        o=_load(p)
        if o and o.get("receipt_id")!=r.get("receipt_id") and o.get("execution_id")==r.get("execution_id"): reasons.append("duplicate_execution_receipt")
    ok=not reasons and all(checks.values())
    return {"status":"PROVENANCE_VERIFIED" if ok else "PROVENANCE_UNVERIFIED","trusted":ok,**checks,"reasons":sorted(set(reasons)),"receipt_id":r.get("receipt_id"),"execution_id":r.get("execution_id")}
