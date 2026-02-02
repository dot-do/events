# ADR-0002: R2 for Event Storage (Lakehouse)

## Status

Accepted

## Context

events.do needs to store event data for long-term retention and analytics. The system handles multiple event types:

- **Raw events**: All ingested events (webhooks, RPC calls, CDC, WebSocket, custom)
- **CDC deltas**: Incremental change records for each collection
- **Compacted data**: Merged delta files into current-state snapshots
- **Catalog metadata**: Iceberg-style manifest files

We evaluated several storage options:

- **Cloudflare R2**: S3-compatible object storage with zero egress fees
- **AWS S3**: Industry-standard object storage
- **Cloudflare KV**: Key-value storage optimized for reads
- **D1 Database**: Serverless SQLite for structured data
- **External data warehouses**: Snowflake, BigQuery, Databricks

### Key Requirements

- Support for Parquet file format (columnar, efficient for analytics)
- DuckDB query compatibility for ad-hoc analysis
- Cost-effective at high event volumes (millions/day)
- Time-partitioned storage for efficient range queries
- Zero egress fees for query workloads
- S3-compatible API for ecosystem tooling

## Decision

We will use **Cloudflare R2** as the primary lakehouse storage layer with a hierarchical path structure:

```
events/                           # Raw event stream
  {year}/{month}/{day}/{hour}/
    {ulid}.parquet               # Individual event files
    _compact.parquet             # Hourly compacted file

{namespace}/                      # CDC data per namespace
  {collection}/
    deltas/
      {year}/{month}/{day}/
        {ulid}.parquet           # Incremental changes
    data.parquet                 # Current state (compacted)
    snapshots/
      {date}.parquet             # Point-in-time snapshots

catalog/                          # Iceberg-style metadata
  {table}/
    metadata/
      v{version}.json            # Table metadata
    manifests/
      {manifest-id}.avro         # Manifest files
```

Events are stored in Parquet format, enabling efficient columnar queries with DuckDB.

## Consequences

### Positive

1. **Zero egress fees**: R2 has no egress charges, making analytics queries cost-effective regardless of data volume scanned. Critical for lakehouse workloads.

2. **S3-compatible API**: Existing tools (DuckDB, Spark, pandas) work without modification. Parquet readers can access R2 directly via HTTPS.

3. **Native Cloudflare integration**: R2 bindings in Workers provide low-latency access. No external network calls for event writing.

4. **Flexible schema evolution**: Parquet self-describes columns. New event fields are automatically captured without schema migration.

5. **Time-travel queries**: Combined with CDC delta files and snapshots, R2 enables point-in-time recovery and historical analysis:
   ```sql
   SELECT * FROM read_parquet('r2://events/2024/01/31/**/*.parquet')
   WHERE type = 'collection.update' AND ts BETWEEN @start AND @end
   ```

6. **Compaction strategy**: Hierarchical structure supports efficient compaction (hourly -> daily -> monthly) reducing file count while maintaining query performance.

7. **Cost predictability**: R2 pricing is based on storage and operations, not egress. Event archival costs are predictable.

### Negative

1. **No native query engine**: R2 is storage-only. Query execution requires external tools (DuckDB WASM, external Spark clusters).

2. **Eventual consistency for listings**: `list` operations may not immediately reflect recent writes. Addressed by using deterministic paths and catalog metadata.

3. **Small file problem**: High event throughput creates many small files. Mitigated by buffering in `EventWriterDO` and periodic compaction.

4. **Limited transaction support**: R2 doesn't support atomic multi-file updates. Iceberg-style metadata provides eventual consistency guarantees.

5. **Storage class limitations**: No automatic tiering to cold storage. All data has the same storage cost regardless of access patterns.

### Risks

| Risk | Mitigation |
|------|------------|
| Small file proliferation | `EventWriterDO` buffers events; compaction worker merges files hourly |
| Query performance on raw files | Compacted files reduce file count; partition pruning via path structure |
| Data consistency during compaction | Iceberg-style manifests track valid files; readers use snapshots |
| Cost growth with retention | Implement retention policies; auto-delete old partitions |
| DuckDB WASM limitations | Offload large queries to external compute; implement query pagination |

## Implementation Notes

### Event Writing Pipeline

```
Ingest -> EventWriterDO (buffer) -> R2 (Parquet files)
                |
                v
          Scheduled Worker -> Compaction -> Merged Parquet
```

### Query Patterns

```sql
-- Recent events (uses hourly compacted files)
SELECT * FROM read_parquet('r2://events/2024/01/31/*/_compact.parquet')

-- CDC current state (uses compacted data.parquet)
SELECT * FROM read_parquet('r2://users/profiles/data.parquet')

-- Point-in-time recovery (uses snapshots)
SELECT * FROM read_parquet('r2://users/profiles/snapshots/2024-01-30.parquet')
```

## References

- [Cloudflare R2 Documentation](https://developers.cloudflare.com/r2/)
- [Apache Parquet Format](https://parquet.apache.org/)
- [Apache Iceberg Table Format](https://iceberg.apache.org/)
- [DuckDB Parquet Integration](https://duckdb.org/docs/data/parquet/overview)
