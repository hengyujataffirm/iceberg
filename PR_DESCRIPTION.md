# Flink: Log committed offsets (checkpointIds) and snapshotId on commit

## Summary
Add INFO-level logging when an Iceberg snapshot/commit happens so that the **committed offsets** (Flink checkpoint IDs) and the resulting **snapshot ID** are recorded in the operator logs. This helps identify the source of data loss when it occurs by correlating Flink checkpoints with Iceberg table state.

## Motivation
When data loss is suspected, we need to answer:
- Which Flink checkpoint(s) were actually committed to the table?
- Which Iceberg snapshot corresponds to each commit?

Today this is only recoverable by scanning table metadata (snapshot summaries for `flink.max-committed-checkpoint-id`). Having the same information in the committer logs makes debugging and incident response faster.

## Data loss investigation: Flink job vs downstream

When data is missing from the table, use these logs to decide whether the loss is in the **Flink job** (data never committed) or **downstream** (data was committed; loss is in a consumer, ETL, or reporting layer).

1. **Get the last committed offset from Flink**  
   Search committer logs for `Committed offsets (checkpointIds):` for the affected table/branch. The highest checkpoint ID in those lines is the last offset the Flink sink committed to Iceberg.

2. **Get the snapshot that represents that commit**  
   Use the "Committed ... snapshotId: ..." lines to map that checkpoint ID to an Iceberg **snapshot ID**.

3. **Decide: Flink vs downstream**  
   - **Query the table as of that snapshot** (e.g. `SELECT ... FROM table VERSION AS OF snapshot_id`). If the expected data **is present** there, the Flink job committed it correctly → **loss is downstream** (consumer/ETL/reporting).  
   - If the expected data **is not present** in that snapshot, the Flink job never committed it → **loss is in the Flink job** (e.g. source not reading, sink/commit failures, or backpressure).  
   - Optional: compare the last committed checkpoint ID to source progress (e.g. Kafka offsets at that checkpoint) to narrow down whether the gap is in the source, the job, or the sink/commit path.

## Changes
- **IcebergCommitter** (Sink V2): After committing a batch, log `Committed offsets (checkpointIds): [...]` and extend the existing commit log line to include `snapshotId` (via `table.refresh()` + `table.snapshot(branch)`).
- **IcebergFilesCommitter** (legacy sink): Same — log committed checkpoint IDs after the batch and add `snapshotId` to the commit log line.
- **DynamicCommitter** (dynamic tables): Same — log committed checkpoint IDs per table/branch and add `snapshotId` to each commit log line.

## Example log output
```
Committed offsets (checkpointIds): [42, 43, 44] for table: db.table, branch: main
Committed append to table: db.table, branch: main, checkpointId: 44, snapshotId: 123456789 in 150 ms
```

## Testing
- No new tests; existing committer behavior unchanged except for additional logging.
- Suggested: run a streaming job and confirm logs appear at commit time.

---
*Authored with Cursor agent.*
