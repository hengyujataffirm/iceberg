# Flink: Log committed offsets (checkpointIds) and snapshotId on commit

## Summary
Add INFO-level logging when an Iceberg snapshot/commit happens so that the **committed offsets** (Flink checkpoint IDs) and the resulting **snapshot ID** are recorded in the operator logs. This helps identify the source of data loss when it occurs by correlating Flink checkpoints with Iceberg table state.

## Motivation
When data loss is suspected, we need to answer:
- Which Flink checkpoint(s) were actually committed to the table?
- Which Iceberg snapshot corresponds to each commit?

Today this is only recoverable by scanning table metadata (snapshot summaries for `flink.max-committed-checkpoint-id`). Having the same information in the committer logs makes debugging and incident response faster.

## Data loss investigation: Flink vs CLD to Snowflake

When data is **missing in Snowflake**, use these logs to determine whether the loss happened in the **Flink job** (data never made it to Iceberg) or in the **CLD-to-Snowflake** pipeline (data reached Iceberg but was not loaded into Snowflake).

1. **Identify the missing record and its Kafka offset**  
   Identify which record is missing in Snowflake (e.g. the **event_id**). In **Confluent UI**, find that message and note its **offset**.

2. **Get what Flink committed to Iceberg**  
   Search Flink committer logs for `Committed offsets (checkpointIds):` for the affected table/branch; the highest checkpoint ID is the last offset the Flink sink committed. Use the "Committed ... snapshotId: ..." lines to get the **Iceberg snapshot ID** that corresponds to that commit.

3. **Decide: Flink or CLD → Snowflake**  
   **Query the Iceberg table as of that snapshot** (e.g. `SELECT ... FROM iceberg_table VERSION AS OF snapshot_id`).  
   - If the missing data **is present** in Iceberg at that snapshot → the Flink job wrote it; **loss is in CLD to Snowflake**.  
   - If the missing data **is not present** in Iceberg at that snapshot → **loss is in the Flink job** (e.g. source, job, or sink/commit never included that data).

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
