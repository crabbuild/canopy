# Immutable push outcome preparation

`PreparedCatalog::root_push_completion` prepares a joint catalog/ref/outcome input bounded by 8 KiB. It does not execute a publication command or acknowledge a push. The final atomic publisher, authorized completed-response query, streaming replay adapter, typed collector and production hard cutover remain required.

This factory requires a successful nonempty ref plan. Immutable outcome-only completion for a failed or empty-command receive remains required; the old inline outcome-only API is not a hard-cutover adapter.

The private factory obtains its native result from the current registered input checkpoint. It authenticates that checkpoint against the prepared catalog's physical input custody, reopens the exact native plan/report/options and uses the existing scoped signing-key lookup for a signed annotation. The native plan must match the private policy guard's original intent and evidence. All ref expectations are checked through the immutable ref transition against the selected joint generation; there is no SQL-ref fallback. For a preparation with no physical inputs, the checkpoint must have no native pack inventory. Its exact digest is still included in the final certificate.

The factory freezes three outcomes before signing:

| Choice | Response |
| --- | --- |
| Native | Exact original status, headers and immutable body descriptor |
| Publication rejected | Existing all-successful-ref rejection transformation, preserving native failures and progress |
| Signed certificate replayed | Same transformation with the existing certificate-replay reason |

An absent report-status response becomes an explicit HTTP 409 rejection. The final command must select a durable refusal when current policy/ACL or signed ownership rejects publication. A moving catalog CAS must instead preserve reconciliation/retry semantics. It must never retain an unselected native success as the client outcome.

## Representation and namespaces

`NativeOutcomeRoot` reuses `StoredInputRoot`, the shared immutable metadata representation. Its typed metadata reuses `GitHttpResponse<ArtifactDescriptor>` and links to the original `NativeResultRoot` for plan/options/signed audit information. A body-operation field identifies the response bytes' creating namespace. This avoids copying the large plan, original request, options or signed body into each alternative.

Every new outcome metadata artifact and rejection body belongs to the current admitted attempt. The native success body can remain in the original native creator namespace after a legitimate owner claim/adoption. Creating replacement artifacts in that old namespace would race its retention lifecycle, so preparation never does so. Registered custody and the adopted independent pin retain borrowed artifacts while preparation remains active or uncertain.

Completed retention must traverse the selected response body and the original native metadata's plan/options/signed annotation. It must not permanently retain the native metadata's original wire request/body merely because that descriptor remains present. For a rejection, the native success body also needs no completed-response retention unless a separate audit policy selects it. Active/uncertain input pins retain those private-input dependencies independently. The collector must implement and qualify these distinct typed traversals before any deletion is enabled.

An outcome metadata record cannot decode as native-result metadata. A decoded root is transport data, not a native witness or read capability. The completed-response API must select the root from durable actor/logical-operation/request identity under current read authorization; it must never accept a caller-selected root. That API is not implemented yet.

## Binding and final obligations

The completion certificate binds the exact catalog/base/pin/token/actor, policy intent, immutable ref snapshot, response UUID, ref generation, all three outcome descriptors and optional SHA-256 signed-certificate ownership facts. The largest permitted signing-key string is 4,096 bytes. Plans and response bytes are absent from the bounded input. After freezing every artifact, preparation rechecks checkpoint custody and live policy readiness before minting the completion-purpose certificate. The existing inline publisher refuses this purpose. An admitted service must retain this exact prepared input and its mutation identity through an uncertain command result; regenerating a new response UUID is not replay.

These checks are conditional preparation. The final admitted command still must authenticate the complete MAC and check its actual owner fence, current lease/pin/ACL, guard/epoch/dependencies, selected retained base and root CAS in the same transaction as signed ownership, joint generation and selected outcome writes. It must return the original selected outcome on exact completed replay before requiring new authority, preserve independent pins and roll back every late error. It must perform no remote reads, response rewriting, per-object/per-ref writes or whole-plan decoding.

Preparation still reopens a `Vec` plan/report and uses the existing report transformer. The 8 KiB bound applies to Cell transport and does not prove whole-operation RSS, CPU or end-to-end throughput. File-backed intent/report processing and hard OS containment remain mandatory, as do full-history Linux/Kubernetes/Chromium and 10,000-developer mixed-load qualification.
