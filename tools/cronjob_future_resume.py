"""Future-only actions on the existing Cron tool, never a new tool/database."""
import json


def action(args):
    from cron.future_resume import inspect_job_revision, resume_future_only
    if args["action"] == "revision":
        result = inspect_job_revision(args["job_id"])
    else:
        result = resume_future_only(args["job_id"], expected_revision=args.get("expected_revision"),
                                    request_id=args.get("request_id"))
    return json.dumps(result, ensure_ascii=False)
