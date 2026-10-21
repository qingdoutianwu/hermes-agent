"""R3 result shape; authority checks live only in provenance_readonly_r2."""
from cron.provenance_readonly_r2 import verify_receipt_provenance


def verify_r3(receipt_path, **kwargs):
    result = verify_receipt_provenance(receipt_path, **kwargs)
    checks = {name: result.pop(name, False) for name in (
        "NATIVE_EXECUTION_VERIFIED", "NATIVE_JOB_ID_MATCH", "NATIVE_PROFILE_MATCH",
        "SCHEDULED_INSTANT_VERIFIED", "BUSINESS_RECEIPT_LINK_VERIFIED", "RELEASE_IDENTITY_MATCH",
    )}
    return dict(result, checks=checks)
