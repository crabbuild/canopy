# Shared dependency spools for native verification

PhysicalVerifier now retains one dependency file per metadata page, rather than one file per structural object. A page still contains at most 512 privately constructed VerifiedObject witnesses. This reduces dependency file descriptors from as many as 512 to one for the physical-verification page. Native Git processes, pack/index files, SQLite connections and other concurrent operations have separate resource costs; this change does not establish a whole-process file-descriptor limit.

The implementation reuses VerifiedObject, DiskSink, AdmittedFile, the existing Cellule DiskBudget and the fixed-width typed-edge encoding. It changes private witness ownership, not persisted catalog or metadata bytes. Structural dependencies remain occurrence records containing the child OID followed by its expected-kind byte. The maximum encoding buffer remains 512 records (16,896 bytes for SHA-256). Blobs and other objects with no emitted dependencies allocate no file or retained range.

## Producer and range ownership

Create one private EdgeSpool at the start of each physical metadata page. Acquire one append permit for the entire inspected object, including between its edge pages. Reject overlapping object writers; a file mutex alone would allow two objects' pages to interleave. The permit stays in the object's shared state, so queued blocking writes retain exclusivity after the requesting future is dropped. Complete native frame/hash verification remains the only way to construct a decoded witness.

On the first dependency page, assign the object's start offset from the admitted file length. Subsequent pages must begin at that object's previous end and the file's current end. Checked arithmetic enforces both the per-object dependency-byte ceiling and the accumulated file length. Before any write, reserve or grow disk credit. Seek explicitly to the admitted end: a prior witness replay may have moved the shared file cursor. Update file length, object length and digest only after the entire page is written.

Mark the storage failed before allocation, growth, length checking, seeking or writing. An unrecovered failure leaves it poisoned; retained witnesses reject replay and later appends reject. This prevents a partially written tail or a denied growth attempt from being silently adopted by a subsequent object. The operation fails closed and is discarded. An observer-canceled queued write may finish and leave an orphan range. Those bytes remain admitted but cannot produce the canceled object's witness. The writer permit releases only after all of that object's workers drain.

A successful witness owns the storage Arc, start offset, exact length and complete occurrence digest. Completion releases the object writer permit while preserving the immutable range. PhysicalVerifier drops the producer handle before transferring the whole page to the blocking metadata worker. Every witness and queued write keeps the complete admitted file alive independently of the producer. Dropping some witnesses never releases partial credit: their bytes still occupy the same file. Cleanup removes the file before releasing its entire reservation, reusing AdmittedFile's conservative credit retention on cleanup failure.

## Replay and SQLite retries

Replay holds the storage mutex for its range. Reject poisoned storage, zero or misaligned range lengths, overflowing/out-of-file ranges and any actual file length differing from the admitted append length. Seek to the private range offset and read exactly its declared bytes in pages of at most 512 dependencies. Validate every OID and kind and compare the complete range digest before the metadata transaction commits. Appended bytes from later objects are outside the range and cannot enter its graph.

Every SQLite growth retry repeats the complete range validation and digest calculation from the same start. MetadataBuilder keeps all witnesses through transaction rollback/replay and poisons sealing on an unrecovered error. No committed header/edge prefix survives a late digest failure. Shared file cursors, repeated replay and reversed witness order cannot change canonical dependency identity.

There is no global spool or repository-wide verification lock. Separate admitted physical operations use separate page files. A slow worker retains its file and admission until it exits. Whole-operation/native RSS, CPU/I/O and global file/process admission still need the shared production resource profile, and production handlers must adopt the verified pipeline before the hard cutover.

## Verification

A native SHA-1/SHA-256 fixture creates 512 distinct commits and retains all decoded witnesses in one dependency file under a 64 KiB disk budget. It interleaves earlier-range replay with later appends, checks exact offsets and parent/tree identities, replays in reverse order twice, and verifies full-file credit retention until the last witness drops.

The wide-tree metadata fixture uses the same page spool through admitted SQLite growth under a 2 MiB shared budget. A late corruption check modifies the second range at a nonzero offset after a valid 1,600-edge tree range; the entire metadata batch rolls back and sealing remains poisoned. A deterministic blocked-worker fixture confirms that cancellation prevents completion and new-writer admission until queued work drains, then checks that orphan bytes cannot enter a later witness. Additional checks reject denied growth, a real write failure after credit growth, later appends to poisoned storage, appended/truncated files and premature credit release. Existing isolated physical SHA-1/SHA-256, artifact-integrity and queued-assembly fixtures exercise the production PhysicalVerifier wiring.

These tests qualify dependency ownership, integrity and bounded page file count. They do not prove full-history import throughput, native descendant limits, large-team capacity or completed production cutover.
