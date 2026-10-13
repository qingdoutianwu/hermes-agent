"""Explicit CLI surface for the revision-checked future-only candidate."""
import json


def command(args):
    from cron.future_resume import inspect_job_revision, resume_future_only
    try:
        if args.cron_command == "revision":
            result = inspect_job_revision(args.job_id)
        else:
            result = resume_future_only(args.job_id, expected_revision=args.expected_revision,
                                        request_id=args.request_id)
    except (OSError, ValueError) as exc:
        result = {"success": False, "error": str(exc)}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("success", True) else 1
