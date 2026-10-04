# Initial staging receipt and cold restart

A Begin acknowledgement can be lost even though its input lease committed. The SDK stops resolving that mutation after its identity expires; this does not mean admission failed. Initial admission therefore retains its original receipt separately from the live lease. Recovering that receipt grants knowledge of the accepted command. Uploads still require current custody, and a restarted owner must execute Claim.

## Durable representation

The first accepted Begin stores a purpose-specific authenticated record in `pushes.initial_staging`. This reuses the existing logical request row, `CertificateEnvelope`, admitted mutation `Stamp` and `Recorded` result codec. It adds no queue, table or per-object metadata. The blob is at most 1 KiB and contains the tenant/application, exact Begin request, actual SDK identity/digest, original sequence and granted result. The original lease token includes the admitting incarnation, owner fence, attempt and creating namespace.

The private record, query, MAC decoding, original-result lookup and restart-token verifier are now shared with [first catalog preparation admission](initial-preparation-receipts.md). Each kind retains a separate purpose, column and typed result validator. Staging's original wire representation and admitted-sequence rule are preserved.

The receiver records the result in the same Cell transaction that allocates the namespace and inserts the operation and independent lease. A late encoding or SQL failure rolls back all of those writes and SDK acceptance. SQL guards retain the first receipt and forbid mutation, replacement or deletion. Ordinary expiry/reaping may remove custody rows without removing admission knowledge.

Root completion updates this pending logical request row using its actor/digest and null completion fields as conditions. It preserves admission metadata while recording the terminal selection. Joint and ref-free completion share the existing result builder. A matching admission-only row does not conflict with compaction, and admission-only rows do not make an otherwise empty repository non-pristine for initialization. Completed or unrelated outcomes retain their existing conflict checks.

## Recovery and permissions

The staging supervisor retains the original prepared command. After uncertainty it reads the domain receipt before SDK resolution. MAC, purpose, target, logical identity, original SDK stamp/digest and incarnation must match. A receipt belonging to another SDK identity cannot settle this command. The returned receipt uses the original commit sequence, not the lookup query's sequence.

The SQL query must observe a durable logical head; the SDK returns `PendingPublication` for an unproven local head. Lookup/validation errors retain the original evidence, job and 8 KiB command reservation. Absence of domain metadata falls back to resolving the same original SDK command. The retained supervisor never swaps expired or unknown SDK evidence for a newly minted Begin identity.

After recovering a known grant, the existing supervisor queries live staging custody at the original receipt watermark. Expiry, a changed active attempt or revoked Write access keeps workers fenced. Historical grant timestamps never refresh the local deadline.

`StagingAdmission::load` is a trusted service API for historical knowledge, independent of product authorization. It cannot create an upload task or a preparation resolver. The ordinary Ready factory refuses an already recorded logical admission. A cold caller uses `ready_claim` to prepare the existing Claim command, which checks current Write access and stamps the actual executing owner, sequence and new namespace. If the original operation was reaped or aborted, the receiver additionally authenticates and matches the exact initial receipt before recreating the operation. A completed logical push, conflicting identity, invalid receipt or exhausted quota refuses recreation. Claim does not adopt artifacts from the expired namespace or recreate its former pin.

## Validation and remaining integration

The original failing test confirms acceptance and a still-live artifact lease, waits for real SDK identity expiry, then recovers the original Begin. Lost acknowledgements and post-execution panics cover both Git object formats. Additional tests exercise final-write rollback and exact retry, local SQL destruction and fresh-owner restore, independent Claim namespaces, lease reaping, Write revocation, immutable rows, mismatched SDK identities, corrupt MACs, forged restart tokens, rollback of a restarted Claim, completed-request refusal and retained uncertainty/credits.

The full goal remains open. This record preserves the first accepted initial Begin. Denied Begin commands, competing/repeated raw Begin identities and subsequent Claim/Renew receipts still need the complete registered attempt journal for recovery beyond their SDK expiry. No pre-admission command snapshot is yet discoverable after process loss. Mandatory registration must remove raw unregistered execution paths. Retained input adoption/repreparation, production producers/readers, the fresh-schema hard cutover, full typed GC/backup/restore, continuous maintenance and full-history mixed-load qualification remain required. These receipt tests do not prove the large-team capacity target.

The local [custody journal](durable-custody-command-intents.md) now provides original pre-dispatch snapshots and positive/negative phases for staging Begin/Claim/Renew/Bind. Production has unbound the raw custody commands; this first-positive-only carrier remains in explicit domain/service qualification consumers pending conversion. Convert those consumers to the journal and remove the redundant carrier before the complete hard cutover is published.
