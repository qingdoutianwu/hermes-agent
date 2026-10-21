# Finance Daily R54 owned Chromium lifecycle — isolated native candidate

This is NOT installed. The activated R54 release and its Business Core are unchanged.
The actual address/DNS classification behind the historical close rejection remains
NOT_PROVEN. This patch does not change DNS,IPv6,Fake-IP,proxy configuration or URL Safety.

## Existing behavior and exact change

The private-page guard on generic raw CDP is intentional (history830860306ded5aa9333e411a608fb6b5c2f76751).
It prevents content methods from bypassing the guarded browser surface. The observed
Finance adapter uses an application-created external Chromium,then a generic
Browser.close after task-scoped backend close. That second call is rejected before
dispatch when the current-page URL guard blocks. Generic raw Browser.close remains
guarded in this candidate;it is NOT added to its allowed-method collection.

The existing native transport `_cdp_call` gains only an internal send-completion
observer. All existing callers retain their previous signature behavior and guards.
`tools/browser_owned_lifecycle.py` is a small in-process capability API,not a registered
model tool,network service,new browser controller or CDP server.

## Ownership and dispatch

Trusted application code calls `register_owned_chromium` at creation with the real
Popen object,private Profile and already-discovered browser endpoint. It accepts no
PID-only claim,env authority flag or arbitrary URL. It checks:

- direct child parentage/current UID and process birth time;
- exact live executable/argv matching its recorded creation arguments;
- dedicated process group,private0700 owned non-symlink Profile under the active
  profile's runtime,and stable device/inode;
- exact original `--user-data-dir`,loopback CDP/address and ephemeral-port arguments;
- private Profile's regular owned DevToolsActivePort browser nonce and port;
- actual listener owned by that process according to psutil,not a DNS/argv substring guess.

An opaque Python handle,kept in a weak in-process map,cannot be reconstructed from
tool JSON. Duplicate live process/Profile/endpoint registration is refused. Before
close it rechecks the handle's profile scope,live birth/executable/argv/listener,
Profile/DevTools identities and active native Session task/owner/session_key/CDP
override binding. The exact Session object is bound at registration;replacement with
even an equal dictionary fails. The thin consumer creates the existing native task
Session before registration. Changed/foreign/unregistered identities fail before a send.

`close_owned_chromium(handle,task_id=...,timeout=...)` accepts no target URL,CDP
method,page ID or navigation params. It invokes only native `_cdp_call` with
Browser.close,empty params,no target/session/frame routing,and the verified literal
loopback endpoint. The transport's existing loopback direct-connect policy is reused.
No current-page DOM/URL/DNS is read to authorize resource teardown.

This is the normal trusted-Python/owning-UID trust envelope,not protection against
malicious operator/root/Python code mutating all process objects and private records.
Ownership read/permission failures reject;there is no insecure fallback connection.

## Response and exit are separate

The send observer sets dispatched only after WebSocket.send succeeds. The existing
CDP transport matches the response ID and rejects protocol errors. A received response
alone is insufficient:the registered Popen must wait/reap its exact original child
and observe exit0 before OWNED_PROCESS_EXIT_VERIFIED. The native proof explicitly
covers the direct child,not all descendants/Profile/CDP cleanup. Existing Finance
unconditional Session release,process-group quiescence,Profile deletion,NetLog and
resource gates remain necessary and unchanged.

Failure/timeout/disconnect/identity mismatch returns false/UNPROVEN without raw secret
exception text and never sends a signal or automatically retries an ambiguous close.
A completed close is idempotent for the same handle/task;another task cannot reuse it.
Only whole isolated Chromium is supported;this API never claims a page-only close
means an entire browser exited. Shared/human/remote browser endpoints cannot acquire
this direct-child/private-Profile/listener capability.

Sanitized close-channel diagnostics distinguish literalIPv4 owned-loopback control
from article DNS classification. Browser proxy args are unchanged. A fixture uses a
loopback discard HTTP proxy solely to prevent internet egress;production7897settings
are not touched. No unknown article address class is filled from historical guesses.

## Concrete Finance consumer and publication limits

`finance_adapter.patch` is an application-consumer candidate against Finance commit
15aee5aeb900ab3b1919d4cc87d727a3e07d8ce1. It registers the existing Popen/Profile/CDP
at creation and replaces only the owned-exit call with this capability API. It also
adapts two existing offline API mocks. No Business Core,research algorithm,proxy
startup flag,240sbudget,NetLog rule,quality threshold or existing cleanup gate changes.
The patch was applied and tested only in an outside-repository audit copy. It is NOT
applied to production or to the frozen Finance Git branch.
It is a zero-context unified patch;use `git apply --check --unidiff-zero` only after
verifying the exact frozen base file hashes. It is not permission to patch a live
Release. The payload is deliberately kept outside the activated application's tree.

This native API is additive:installing only it does NOT fix the currently activated
adapter,which still calls generic raw CDP. A separately reviewed Finance successor
must include the thin patch,updated module provenance/Portable Manifest and a newly
verified native-runtime binding. The active release pins browser_cdp_tool.py's hash;
changing production native files without qualifying that successor would correctly
make its old binding fail closed. Do not loosen hashes or silently retarget the old
Activation. Source installation/paired compatibility,activation and a real no-send
business retest remain separate authority. Retain old native and application identities
for a coherent rollback;old release pointer alone is not native-code rollback.

## Isolated verification

Use the repository's `scripts/run_tests.sh` with an already-available test interpreter,
isolated HOME and zero credentials. Targeted native tests cover owned/unowned handles,
task/Profile/CDP/birth/listener authority,private-page close versus generic safety,
ACK-without-exit,protocol error redaction,no automatic repeats and idempotency.
Existing CDP/URL Safety/override regressions run unchanged. Real macOS Fixture starts
two Chromium processes with separate temporary Profiles and about:blank,never news;
it proves closing one preserves the other,then verifies both process groups and
Profiles released. Finance222 offline tests run against the staged thin consumer with
network denied. Fixture success is not report quality,natural Scheduler or delivery PASS.

Stop after isolated qualification/local commit. No production install,Gateway or
Scheduler action,real finance collection,external send or GitHub push is authorized.

## Current qualification limitation

The final targeted native set passed132 tests and the staged thin-consumer business
set passed222 tests. However,an earlier resumed real Chromium Fixture was rejected
at the creation-argv/executable equality gate while registering its second browser.
The mismatch values were not in that earlier receipt. A diagnostic assertion now
retains both values on any future failure,but the single bounded follow-up passed
without reproducing the mismatch. No signal/URL/ownership gate was relaxed.

This is an OPEN registration-reliability qualification issue,not evidence that a
foreign browser was closed. The completed real fixtures verify exact send,response,
exit and sibling isolation. They do not prove the intermittent registration cause
fixed. Therefore this candidate is NOT ready for a controlled production install
recommendation despite the latest green tests. Keep earlier failed/interrupted logs.
The smallest remaining work is isolated reproduction of that exact creation identity
read,not a new browser architecture or another Finance report run.
