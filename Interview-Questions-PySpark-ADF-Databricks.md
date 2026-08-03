# Azure Data Engineering Interview Q&A Bank

Full interview questions and answers for **PySpark**, **Azure Data Factory (ADF)**, and **Azure Databricks (ADB)**, plus **production support tickets (P1–P4)**. Answers are written in a practical Azure Data Engineer tone — use them as speaking scripts, then adapt with your own project stories.

Each technology section contains **exactly 25** detailed Q&As (concepts + scenario-style problems mixed in).

---

## Table of Contents

1. [PySpark](#pyspark) (25 Q&A)
2. [Azure Data Factory (ADF)](#azure-data-factory-adf) (25 Q&A)
3. [Azure Databricks (ADB)](#azure-databricks-adb) (25 Q&A)
4. [Production Support Tickets (P1–P4)](#production-support-tickets-p1p4) (12 tickets)

---

## PySpark

### Q1. Explain Spark architecture: Driver, Executors, and Cluster Manager.

**Answer:**  
Spark applications have a **Driver** process that runs your main program, builds the logical and physical plan, schedules tasks, and tracks progress. **Executors** are worker processes that run tasks, hold shuffle data, and cache RDDs/DataFrames. A **Cluster Manager** (YARN, Kubernetes, or Databricks’ manager) allocates CPU and memory for the driver and executors.

Execution flow: your code → Driver creates a DAG → stages and tasks are scheduled → Executors compute partitions in parallel → results return to the Driver or are written to storage. On Databricks, the notebook or job process is the driver; worker VMs host executors. Understanding this model helps you diagnose driver OOM vs executor OOM, and why collecting large results to the driver fails.

---

### Q2. What is the difference between transformations and actions? Why does lazy evaluation matter?

**Answer:**  
**Transformations** (`select`, `filter`, `join`, `groupBy`) describe how to build a new DataFrame; they are lazy and return another DataFrame. **Actions** (`count`, `collect`, `show`, `write`) trigger execution and produce a result or side effect.

Spark records transformations as a lineage/DAG and waits until an action to optimize and execute the whole plan. That lets Catalyst combine projections, push down filters, and avoid unnecessary shuffles. Example: read Parquet → filter one date → select three columns → `count()` can push the filter and column prune into the scan instead of loading everything first. Transformations do **not** run immediately; only an action (or write/stream trigger) materializes work.

---

### Q3. DataFrame vs RDD — when would you use each?

**Answer:**  
**DataFrames** are the default for structured data: schema, Catalyst + Tungsten optimizations, SQL support, and strong analytics performance. **RDDs** are low-level distributed collections with fine-grained control but no automatic optimization.

Use DataFrames/Datasets for almost all modern Azure DE work. Mention RDDs only for rare cases — custom partitioning that DataFrames cannot express cleanly, or legacy code. Prefer built-in functions or carefully designed Pandas UDFs over dropping to RDD Python lambdas, which lose Catalyst benefits.

---

### Q4. How does Spark decide partitions, and what is a stage? When do you use `repartition` vs `coalesce`?

**Answer:**  
A DataFrame is split into **partitions**; each task processes one partition. Narrow transformations stay in the same stage; **wide** transformations that require shuffle (`groupBy`, `join`, `repartition`) introduce a stage boundary.

- `repartition(n)` does a full shuffle and can increase or decrease partitions; use to raise parallelism or redistribute by key.
- `coalesce(n)` avoids a full shuffle when **reducing** partitions; useful before writing fewer output files.

Too few partitions underutilizes the cluster; too many creates scheduling overhead and small files. Avoid `coalesce(1)` as a habit — it creates a single bottleneck partition. Prefer a moderate output file count, or let Delta OPTIMIZE compact later.

---

### Q5. Explain shuffle, data skew, and how you mitigate both.

**Answer:**  
A **shuffle** redistributes data so records with the same key land on the same partition (joins, aggregations, window partitions). It involves serialization, disk/network I/O, and often a sort — usually the biggest cost in Spark jobs.

**Skew** means a few keys dominate row counts, so some tasks run much longer. Symptoms: one task stuck at 99%, uneven shuffle read sizes in the Spark UI.

Mitigations: filter early; broadcast small dimensions; pre-aggregate; avoid unnecessary `repartition`; salt hot keys; enable AQE skew join; process hotspot keys separately; verify key cardinality before large joins. Scaling the cluster alone rarely fixes a single hot key.

---

### Q6. Explain broadcast joins vs sort-merge joins — how do you choose?

**Answer:**  
A **broadcast join** sends a small DataFrame to every executor so the large side is not shuffled. Use when one side fits comfortably in executor memory (often MB to low GB, depending on cluster and `spark.sql.autoBroadcastJoinThreshold`).

```python
from pyspark.sql.functions import broadcast
df = large.join(broadcast(dim), "id")
```

If both sides are large, Spark typically uses **sort-merge join** with a shuffle. Check the physical plan for `BroadcastHashJoin` vs `SortMergeJoin`. Broadcasting an oversized “dimension” causes executor OOM. Trade-off: broadcast saves shuffle on the big table but multiplies memory pressure on every executor.

---

### Q7. How do window functions and deduplication work at scale?

**Answer:**  
Window functions compute over a partition of rows without collapsing them. Define `partitionBy`, optional `orderBy`, and a frame. Common pattern — latest record per business key:

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

w = Window.partitionBy("customer_id").orderBy(col("updated_at").desc(), col("ingest_ts").desc())
latest = df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")
```

Other dedupe options: exact `dropDuplicates([...])`; aggregate with `max(updated_at)` when grain is clear. At scale, watch shuffle on the partition keys and skew on popular keys. In Delta, enforce uniqueness at MERGE time and validate with quality checks in Silver.

---

### Q8. What is Delta Lake? How do you use MERGE, time travel, OPTIMIZE, and VACUUM?

**Answer:**  
**Delta Lake** adds ACID transactions, schema enforcement/evolution, and scalable metadata on top of Parquet.

- **MERGE**: upsert/delete by matching keys (SCD, incremental facts).
- **Time travel**: query by version or timestamp for audit/recovery.
- **OPTIMIZE**: compact small files for faster reads.
- **ZORDER / liquid clustering**: improve data skipping on filter/join columns.
- **VACUUM**: remove unreferenced old files after retention — trades storage cleanup against time-travel depth.

Delta is the default storage layer for Silver/Gold (and often Bronze) in Azure lakehouse designs because it supports reliable incremental loads and concurrent readers/writers.

---

### Q9. How do you implement SCD Type 1 vs Type 2 in PySpark/Delta?

**Answer:**  
**SCD1** overwrites attributes in place (no history) via Delta `MERGE` (`WHEN MATCHED THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT`).

**SCD2** keeps history with effective dates / `is_current` flag:

1. Compare incoming vs current (`is_current = true`) rows using a hash of tracked columns.
2. Close changed rows (`end_date`, `is_current = false`).
3. Insert new versions for changes and new keys.

Discuss business keys, late-arriving updates, idempotent reruns, and validating that only one current row exists per key after each batch.

---

### Q10. What is the medallion architecture, and how does it map to Spark jobs?

**Answer:**  
Medallion organizes the lake into layers:

- **Bronze**: raw/append-mostly ingestion, minimal transformation, auditability (`_source_file`, `_ingest_ts`).
- **Silver**: cleaned, typed, deduped, conformed keys, SCD rules.
- **Gold**: business aggregates, star schemas, BI/ML feature tables.

Benefits: reprocess from raw, clear ownership, and separation of ingestion from business logic. Typical Spark design: Autoloader/Copy → Bronze Delta; scheduled MERGE jobs → Silver; aggregation notebooks → Gold. Enforce contracts and quality checks between layers rather than silently dropping bad rows.

---

### Q11. Pros and cons of Python UDFs — and what you use instead.

**Answer:**  
**Pros:** express custom logic not available as built-ins.  
**Cons:** Python UDFs break many Catalyst optimizations, cause row/batch Python↔JVM serialization, and are often much slower than native expressions.

Prefer Spark SQL built-ins, higher-order functions, or SQL expressions. If Python is required, use **Pandas UDFs** / `mapInPandas` for vectorized batches with an explicit return schema. Always justify why a UDF is necessary and measure before/after runtime.

---

### Q12. How do you inspect a query plan and tune a slow PySpark job?

**Answer:**  
Use `df.explain("formatted")` and the Spark UI. Look for scan size and pushed filters, join strategy, unexpected Cartesian products, excessive Exchanges (shuffles), skew, spill, and GC time.

Tuning approach:

1. Reproduce on a representative partition of data.
2. Identify the longest stage and bottleneck (scan vs shuffle vs write).
3. Reduce data early; broadcast small dims; fix skew.
4. Avoid UDFs/`collect`; right-size partitions and cluster.
5. For Delta: OPTIMIZE/ZORDER or liquid clustering; fix tiny-file writes.

End with a measurable win (runtime, shuffle GB, or cost).

---

### Q13. What is AQE, and how do Catalyst/Tungsten help DataFrames?

**Answer:**  
**Catalyst** optimizes queries (logical → optimized → physical). **Tungsten** improves execution (whole-stage codegen, efficient memory). DataFrames benefit from both; raw RDD Python lambdas generally do not.

**AQE** (Spark 3+) re-optimizes at runtime using post-shuffle statistics: coalesce shuffle partitions, change join strategy, and handle skewed joins. Enable/check with `spark.sql.adaptive.enabled` (on by default in recent Databricks runtimes). AQE helps, but you still design good keys, broadcast wisely, and manage file sizes.

---

### Q14. How would you design an incremental load with PySpark + Delta?

**Answer:**  
Typical pattern:

1. Read watermark/high-water mark from a control table.
2. Ingest new/changed source rows into Bronze.
3. Transform/dedupe to Silver.
4. `MERGE` into target on business keys.
5. Advance watermark **only after success**.

Idempotency matters: rerunning the same batch must not duplicate Gold metrics. Prefer MERGE or deterministic partition overwrite by business date. Handle late-arriving rows with a lookback window rather than a strict `>` watermark alone.

---

### Q15. Scenario — Slow join due to data skew. How do you diagnose and fix it?

**Answer:**  
**Situation:** A fact–dimension join runs for hours; Spark UI shows one task with huge shuffle read while others finish quickly.  
**Approach:** Confirm skew on the join key (e.g. null/`UNKNOWN`/`0` customer_id). Decide whether the small side can be broadcast; if not, isolate hot keys.  
**Implementation:**  
- Filter or bucket null/unknown keys separately.  
- Salt hot keys (`key + random(0..N)`), join, then drop salt.  
- Enable AQE skew join.  
- Broadcast true small dims.  
**Validation:** Task durations become even; stage time drops; row counts before/after join stay correct. Compare shuffle metrics and business totals for the batch date.

---

### Q16. Scenario — Driver/executor OOM from `collect` or aggressive cache. What do you do?

**Answer:**  
**Situation:** Job fails with driver OOM after `collect()`, or executors die after caching a huge Bronze DataFrame.  
**Approach:** Never pull large datasets to the driver; cache only reused, expensive, memory-fitting datasets.  
**Implementation:** Replace `collect` with `write` to Delta/temp table, `take`/`limit`, or aggregations. Use `persist` with a disk-aware level only when needed; `unpersist` after use. Break lineage by materializing intermediate Delta tables instead of caching multi-GB scans.  
**Validation:** Job completes without OOM; driver memory stable; only intentional samples appear in logs. Confirm downstream counts match expectations.

---

### Q17. Scenario — Late-arriving data after daily ETL closed. How do you redesign?

**Answer:**  
**Situation:** Orders dated D-2 arrive on day D after Gold for D-2 was already published.  
**Approach:** Separate “as-of business date” from “ingestion date”; support controlled reprocessing.  
**Implementation:** Keep Bronze append-only with ingest timestamp. Silver MERGE upserts by business key. Gold rebuilds affected date partitions or uses MERGE with a lookback (e.g. last 3–7 days). Maintain a watermark plus lookback; do not advance watermark past unprocessed late data if SLA requires completeness.  
**Validation:** Re-run for impacted dates; reconcile source vs Gold counts/amounts; document late-data SLA with stakeholders.

---

### Q18. Scenario — Small-file problem and partition explosion. How do you fix writes?

**Answer:**  
**Situation:** Queries on a Delta table are slow; storage shows hundreds of thousands of tiny Parquet files; table is partitioned by high-cardinality `user_id`.  
**Approach:** Fix partition strategy and compact files; reduce writer parallelism noise.  
**Implementation:** Partition by low/medium cardinality columns (e.g. `event_date`). Before write, `repartition`/`coalesce` to a target file count or set `maxRecordsPerFile`. Run OPTIMIZE (and ZORDER on selective columns) on hot tables. For streaming, increase trigger interval or use auto-optimize/compaction.  
**Validation:** File count drops; LIST and scan times improve; query runtime and cloud IOPS decrease without changing business results.

---

### Q19. Scenario — Bad UDF performance in production ETL. How do you remediate?

**Answer:**  
**Situation:** A daily job grew from 20 minutes to 3 hours after a Python UDF for phone normalization was added.  
**Approach:** Replace row-wise Python UDF with native Spark expressions or a vectorized Pandas UDF; verify plan no longer shows expensive Python eval everywhere.  
**Implementation:** Rewrite with `regexp_replace` / built-ins where possible. If library logic is required, use Pandas UDF with batch processing. Cache nothing extra; compare `explain` and stage CPU time.  
**Validation:** Runtime returns near baseline; CPU per stage drops; sample outputs match the old UDF for a regression set of rows.

---

### Q20. Scenario — Delta MERGE producing duplicates. How do you find root cause and stop it?

**Answer:**  
**Situation:** After MERGE, Silver has two `is_current=true` rows for the same customer.  
**Approach:** MERGE matches on keys present in the **source** batch; duplicate keys in the source (or overlapping concurrent MERGEs) create ambiguous/duplicate results.  
**Implementation:** Dedupe source with `row_number` before MERGE. Enforce one source row per merge key per batch. Serialize writers or use correct isolation for concurrent jobs. Add a post-MERGE quality check: `groupBy(key).count()` where `is_current` and fail if count > 1.  
**Validation:** Quarantine/fix duplicates; MERGE rerun is idempotent; uniqueness check passes every run.

---

### Q21. Scenario — Streaming checkpoint failure / corruption. How do you recover safely?

**Answer:**  
**Situation:** Structured Streaming query fails; restart complains about checkpoint inconsistency after a partial commit or accidental checkpoint delete.  
**Approach:** Treat checkpoint + sink as a pair; prefer replay-safe sinks (Delta).  
**Implementation:** Do not manually edit checkpoint files. If checkpoint is lost, start a new checkpoint path and reload from a safe offset/time only if the sink is idempotent (Delta MERGE/`foreachBatch` with deterministic keys). Restore checkpoint from backup if available. Fix underlying storage permissions/transient errors first.  
**Validation:** Stream resumes; no duplicate business keys in Silver (or duplicates are merged away); lag returns to SLA.

---

### Q22. Scenario — Null-heavy joins dropping or exploding rows. What is your approach?

**Answer:**  
**Situation:** Left join to dimension loses metrics or explodes rows; many fact keys are null.  
**Approach:** Define null semantics with the business; never assume SQL null-join behavior is “fine.”  
**Implementation:** Replace null keys with a durable unknown member (`-1` / `UNKNOWN`) in both fact and dim. Dedupe dimension on key before join. Use left join for facts; validate counts: `fact_count == joined_count` for many-to-one. Use left anti join to find unmatched keys and send to a quarantine/report.  
**Validation:** Grain preserved; unmatched rate tracked; Gold totals reconcile to source within agreed tolerance.

---

### Q23. Scenario — Daily ETL volume grew 10x. How do you optimize end-to-end?

**Answer:**  
**Situation:** Same pipeline design now processes 10× data and breaches the morning SLA.  
**Approach:** Profile first (scan vs shuffle vs write), then apply incremental design and physical layout fixes — not only larger clusters.  
**Implementation:**  
- Ensure true incremental reads (watermark/CDC), not full reloads.  
- Partition/prune by date; compact small files.  
- Broadcast small dims; fix skew; remove UDFs.  
- Materialize Silver; make Gold partition-overwrite by date.  
- Right-size job cluster / enable Photon where eligible.  
**Validation:** SLA met for 7 consecutive days; cost per TB stable or improved; row-count reconciliation green.

---

### Q24. How do you handle nulls, bad records, and reproducible results in pipelines?

**Answer:**  
Keep raw data in Bronze; enforce hard rules in Silver. Use schema enforcement, quarantine tables for corrupt rows, and explicit `na.fill`/`na.drop` only with business rules. Log rejected counts and samples.

For reproducibility: avoid unordered `first()`/`collect`; use explicit `orderBy` before windows; make MERGEs idempotent; parameterize `run_date`; avoid `rand`/`uuid` in business transforms unless seeded and intentional. Deterministic keys and run dates make backfills trustworthy.

---

### Q25. Parquet vs Delta vs CSV for landing zones — and DataFrame API vs Spark SQL?

**Answer:**  
- **CSV/JSON**: source-compatible landing, easy debug, poor scan performance.  
- **Parquet**: columnar, compressed, predicate/column pushdown without transaction log.  
- **Delta**: ACID, upserts, time travel, concurrent readers/writers — prefer for Silver/Gold and often Bronze via Autoloader.

DataFrame API and Spark SQL both go through Catalyst. Use DataFrame API for composable Python modules; Spark SQL for readable set logic and analyst-friendly expressions. Teams often mix both: SQL for transforms, Python for orchestration. Avoid building SQL via unchecked string concatenation; allowlist dynamic table names.

---

## Azure Data Factory (ADF)

### Q1. What is Azure Data Factory and where does it fit in an Azure DE architecture?

**Answer:**  
ADF is Azure’s managed **orchestration and data movement** service: pipelines, triggers, linked services, datasets, Integration Runtimes, and monitoring. It excels at ingesting from many sources, scheduling, dependency management, and invoking compute (Databricks, Functions, Synapse, Fabric, stored procedures).

It is not primarily a heavy distributed compute engine. Complex transformations usually run in **Databricks/Spark**, **Mapping Data Flows**, or SQL engines, while ADF coordinates. A clear ownership line — ADF orchestrates, Databricks transforms, ADLS/Delta stores — is what interviewers look for in architecture answers.

---

### Q2. Linked service vs dataset vs pipeline vs activity vs trigger?

**Answer:**  
- **Linked service**: connection/identity to a store or compute (ADLS, SQL, Databricks, Key Vault).  
- **Dataset**: pointer to data within that store (folder/table/file pattern) and optional schema.  
- **Pipeline**: container/workflow of activities.  
- **Activity**: unit of work (Copy, Lookup, ForEach, Databricks Notebook, If Condition).  
- **Trigger**: schedule/event that starts a pipeline run (schedule, tumbling window, storage event, manual).

Analogy: linked service = connection; dataset = table/path; pipeline = job definition; trigger = scheduler. Pipelines can call child pipelines via Execute Pipeline for parent-child orchestration.

---

### Q3. Explain Integration Runtime types and when you need SHIR.

**Answer:**  
- **Azure IR**: managed; cloud-to-cloud Copy and Data Flows when endpoints are reachable.  
- **Self-hosted IR (SHIR)**: agent in your network for on-prem SQL/files or private/VNet-isolated sources.  
- **Azure-SSIS IR**: lift-and-shift SSIS packages.

On-prem SQL Server → ADLS almost always needs SHIR (or a gateway pattern). For HA, install multiple SHIR nodes with the same IR name. Performance depends on SHIR VM size, network path, and source extract design — raising Copy DIU does not fix an undersized SHIR.

---

### Q4. How does Copy activity work, and how do you tune DIUs and throughput?

**Answer:**  
Copy moves data between source and sink with optional mappings, compression, and staging. Key knobs: **DIUs**, parallel copy, degree of parallelism, fault tolerance, and staging.

**DIUs** abstract Copy compute power. Higher DIU can increase throughput for cloud copies but costs more and will not help if the source cannot produce data faster or SHIR is the bottleneck. Practical tuning: selective source queries (watermark predicates), avoid unbounded selects, use binary copy when mapping is unnecessary, same-region resources, and scale DIU only after Monitor shows transfer — not source extract — as the limit.

---

### Q5. How do you implement incremental load / watermark patterns in ADF?

**Answer:**  
Store a watermark (timestamp/ID) in a control table. Steps:

1. Lookup last watermark.  
2. Copy/query source with `WHERE updated_at > @watermark` (plus lookback if needed).  
3. Write to lake/SQL.  
4. Update watermark to max value seen **only on success**.

Edge cases: late-arriving rows, clock skew, duplicates on retry, and partial success if watermark advances too early. Prefer CDC/change tracking when the source supports it. Pair ADF orchestration with Databricks MERGE for idempotent curated sinks.

---

### Q6. Lookup + ForEach metadata-driven pipelines — how do you design for many tables?

**Answer:**  
A control table lists entities (source table, sink path, watermark column, enabled flag, priority). Pipeline:

1. **Lookup** reads metadata.  
2. **ForEach** iterates with a tuned batch count.  
3. Inside: parameterized Copy / Databricks activity per entity.  
4. Log status, row counts, and errors per entity.

Benefits: onboard tables by inserting metadata, not cloning pipelines. Design for concurrency limits, error isolation, and separate heavy tables into their own lanes so one giant fact cannot block 99 small dims.

---

### Q7. Parameters vs variables — and how do values flow into datasets/linked services?

**Answer:**  
**Parameters** are external inputs (immutable during a run): `RunDate`, `Environment`, `TableName`. **Variables** are mutable pipeline state (Set/Append Variable).

Parameterize dataset paths and linked service properties per environment. Use `@pipeline().parameters.X`, `@activity('Lookup1').output...`, and `@pipeline().RunId`. Keep expressions readable; push complex routing into metadata tables rather than huge dynamic expressions. Guard empty Lookup results with If Condition before using `firstRow`.

---

### Q8. Mapping Data Flows vs Copy vs Databricks — when do you choose each?

**Answer:**  
- **Copy**: bulk movement, light mapping, ELT landing — simplest/cheapest for ingest.  
- **Mapping Data Flows**: visual Spark transforms in ADF for moderate logic without managing clusters yourself.  
- **Databricks**: complex PySpark/SQL, Delta MERGE, streaming, ML, reusable repos/tests, fine-grained performance control.

Balanced pattern: ADF orchestrates; Copy for ingest; Databricks for heavy transforms; Data Flows when the team wants low-code Spark and complexity stays moderate. Do not implement complex SCD2 with Copy alone.

---

### Q9. How do you secure ADF with Key Vault and Managed Identity?

**Answer:**  
Prefer **Managed Identity** on the ADF instance for ADLS, Azure SQL (AAD), Key Vault, and Databricks where supported — avoid passwords in linked services. Store secrets in **Azure Key Vault** and reference secret names.

Also apply least-privilege RBAC/ACLs on storage, private endpoints where required, restrict public access, and separate DEV/UAT/PROD (or strict parameterization). Never bake production secrets into ARM parameters in cleartext.

---

### Q10. What is schema drift, and how should production pipelines handle it?

**Answer:**  
**Schema drift** means columns appear, disappear, or change types unexpectedly. Mapping Data Flows can allow schema drift and auto-mapping; Copy mappings can be flexible; curated sinks (Delta/SQL) often break.

Production stance: allow controlled flexibility in Bronze landing, then enforce contracts in Silver with validation and alerts. Do not silently auto-map drifting columns into Gold marts. Alert on schema changes and route incompatible rows to quarantine when needed.

---

### Q11. Tumbling window vs schedule vs event triggers — differences and when to use each?

**Answer:**  
- **Schedule**: cron-like wall-clock runs.  
- **Tumbling window**: fixed non-overlapping windows with retry/backfill and window dependencies — strong for time-series batch SLAs.  
- **Event**: starts on Blob/ADLS create (near real-time file landing).

Choose tumbling when window correctness and backfill matter; schedule for simple cadences; event for file-arrival architectures. Avoid long Until-polling loops when an event trigger can express the same dependency more cleanly.

---

### Q12. How do you implement CI/CD and DEV/TEST/PROD promotion in ADF?

**Answer:**  
DEV factory is Git-integrated; publish produces ARM/Template artifacts; release pipeline deploys to TEST/PROD with **parameterization** (linked service URLs, Key Vault names, Databricks workspace IDs).

Rules: Git is source of truth — do not hand-edit PROD; secrets stay in Key Vault; global parameters carry environment name; separate storage accounts per env. A common failure mode is promoting a pipeline that still points at DEV storage or a DEV Databricks workspace because parameters were not overridden in the release stage.

---

### Q13. Monitoring, alerts, retries, timeouts, and idempotency — how do you operate ADF?

**Answer:**  
Use Monitor for runs, activity outputs, and IR health; send logs to Log Analytics; alert on failures, long durations, and SHIR offline. Standardize correlation IDs, table name, watermark, and row counts in logs.

Configure retries for transient faults; timeouts to avoid hung activities; fail fast on bad data with quarantine + alert. **Idempotency**: MERGE/upsert sinks, overwrite by partition, dedupe keys, and advance watermarks only after success so retries and double-fires do not corrupt Gold.

---

### Q14. Parent-child pipeline pattern — when and how?

**Answer:**  
A **parent** handles orchestration (metadata lookup, ForEach, overall status). **Child** pipelines encapsulate reusable work (ingest one table, load dims, load facts). Benefits: reuse, clearer retries, smaller pipelines, separation of platform vs domain logic.

Watch for excessive nesting, concurrency storms, and sloppy parameter plumbing. Classic pattern: parent waits for all dimension children to succeed, then launches fact children — dims-then-facts ordering.

---

### Q15. Scenario — ForEach one table fails mid-run. How do you design recovery?

**Answer:**  
**Situation:** Metadata-driven ForEach ingests 80/100 tables; table #81 fails; the rest may be skipped or left inconsistent depending on settings.  
**Approach:** Persist per-entity state; isolate failures; allow rerun of failed entities only.  
**Implementation:** Write `success/failed/rows/error` to a control/log table inside ForEach. Use batch count and consider not failing the entire ForEach on first error if business allows partial success (or use sequential lanes for critical tables). Rerun pipeline filtered to `status='failed'`. Do not globally advance a shared watermark.  
**Validation:** Failed tables reprocess cleanly; successful tables are not duplicated; ops dashboard shows entity-level status.

---

### Q16. Scenario — Watermark advanced but sink write incomplete. How do you prevent data loss?

**Answer:**  
**Situation:** Copy partially succeeded or downstream Databricks job failed, but Set Variable/stored proc already advanced the watermark — next run skips data.  
**Approach:** Treat watermark update as the final commit step in a success path only.  
**Implementation:** On success dependency: validate row counts / Databricks exit value → then update watermark. On failure: alert, leave watermark unchanged, write a repair batch. Prefer storing `batch_id` processed ranges for replay. For curated tables use idempotent MERGE so replay is safe.  
**Validation:** Inject a forced sink failure in TEST; confirm watermark does not move; replay restores completeness.

---

### Q17. Scenario — Self-hosted IR is down. What is your incident response?

**Answer:**  
**Situation:** All on-prem Copy activities fail; Monitor shows SHIR offline/unavailable.  
**Approach:** Restore IR connectivity/HA first; backlog handling second.  
**Implementation:** Check SHIR service on VM, disk/CPU, outbound connectivity to Azure, gateway keys, and Windows updates/reboots. Fail over to secondary SHIR node (same IR name). Communicate SLA impact; pause non-critical triggers if needed; backfill tumbling windows after recovery.  
**Validation:** IR shows online/healthy; test Copy succeeds; queued windows complete; no watermark corruption from retries.

---

### Q18. Scenario — Schema drift broke the sink. How do you fix and harden?

**Answer:**  
**Situation:** Source added/renamed columns; Copy or Data Flow mapping fails or writes incompatible types into SQL/Delta.  
**Approach:** Land flexible Bronze; enforce Silver contract; version mappings.  
**Implementation:** Enable drift only for landing. Add schema validation activity/notebook before curated load. Quarantine bad files; alert data producers. Update explicit mappings and Delta schema evolution rules deliberately (`mergeSchema` only when approved).  
**Validation:** Pipeline green; Silver schema matches contract; historical Bronze still readable; alert fires on future drift.

---

### Q19. Scenario — Late files arrive after daily load completed. How do you handle them?

**Answer:**  
**Situation:** Daily sales files expected by 02:00 arrive at 06:30 after Gold refresh finished.  
**Approach:** Detect late arrival; reprocess impacted dates without duplicating.  
**Implementation:** Event trigger or morning catch-up pipeline lists new files via Get Metadata. Load to Bronze by ingest date; Silver/Gold MERGE or partition overwrite for `sales_date`. Optionally keep a “late file” path and SLA metric.  
**Validation:** File checksums processed once (control table of filenames); Gold for that business date reconciles; no duplicate facts.

---

### Q20. Scenario — Duplicate loads on rerun / double trigger fire. How do you make pipelines safe?

**Answer:**  
**Situation:** Trigger retries or a manual rerun inserts the same day’s facts twice.  
**Approach:** Idempotent sinks + run tokens.  
**Implementation:** Use Delta MERGE or overwrite by `sales_date` partition. Maintain a processed-file registry (`filename`, `etag`, `status`). Advance watermark only after success. Make child pipelines safe to replay.  
**Validation:** Intentional double-run in TEST yields identical Gold counts; monitoring shows second run as no-op or upsert with zero net change.

---

### Q21. Scenario — Key Vault / Managed Identity auth failure. How do you troubleshoot?

**Answer:**  
**Situation:** Pipeline fails with authorization errors accessing Key Vault, ADLS, or SQL.  
**Approach:** Separate identity problems from secret problems.  
**Implementation:** Verify ADF MI has Key Vault Secrets User / storage RBAC / SQL AAD user roles. Confirm secret name and vault URI parameters per environment. Check firewall/private endpoint rules allowing ADF/SHIR. Rotate expired secrets; purge cached linked service errors by republishing if needed.  
**Validation:** Test connection on linked services; small Copy succeeds in each environment; access reviews document least privilege.

---

### Q22. Scenario — Tumbling window backlog after outage. How do you clear it safely?

**Answer:**  
**Situation:** Factory was down; 48 hourly tumbling windows are pending/backlogged.  
**Approach:** Decide sequential catch-up vs aggregate backfill based on SLA and source capability.  
**Implementation:** Increase concurrent window runs cautiously; or temporarily run a historical backfill pipeline for the range, then mark windows. Ensure each window’s sink is idempotent. Watch source OLTP load and SHIR CPU.  
**Validation:** All windows succeed in order/business correctness; no duplicate Gold; lag returns to zero; document max concurrent windows for next outage.

---

### Q23. Scenario — Parent-child dims-then-facts orchestration failed halfway. What next?

**Answer:**  
**Situation:** Dimension children succeeded; fact child failed; next day might double-count or miss referential integrity.  
**Approach:** Stage-based control table; never load facts before dims for that `run_date` are green.  
**Implementation:** Parent writes orchestration status (`dims_ok`, `facts_ok`). Facts check dim readiness. Rerun only fact child with same `run_date`. Facts use MERGE/partition overwrite.  
**Validation:** Dim keys cover fact FKs; fact rerun idempotent; BI sees consistent star schema for the date.

---

### Q24. Scenario — Copy 200 GB is too slow. How do you tune it?

**Answer:**  
**Situation:** Nightly 200 GB SQL → ADLS Copy breaches the window.  
**Approach:** Find bottleneck in Monitor (source vs sink vs transfer) before spending on DIU.  
**Implementation:** Selective incremental extract; indexed watermark column; partition parallel copy by key ranges/dates; stage if beneficial; scale SHIR/VM and place Azure IR in-region; raise DIU after source is proven healthy; compress; prefer Parquet/snappy sink. Split hot tables from the metadata pack.  
**Validation:** Sustained throughput increases; window completes with margin; source DTU/CPU remains acceptable.

---

### Q25. Scenario — DEV/TEST/PROD parameter promotion issue. What went wrong and how do you fix it?

**Answer:**  
**Situation:** Release to PROD succeeded but pipeline wrote to the TEST storage account / used DEV Databricks workspace.  
**Approach:** Treat environment configuration as a first-class release artifact.  
**Implementation:** ARM/template parameters per environment for linked services, Key Vault, storage, workspace URL. Use global parameter `Environment`. Block PROD publishes that lack parameter overrides in CI. Smoke-test connections post-deploy.  
**Validation:** PROD linked services resolve to PROD resources only; test pipeline writes a canary path in PROD landing; rollback plan documented.

---

## Azure Databricks (ADB)

### Q1. What is Azure Databricks and its core constructs?

**Answer:**  
Azure Databricks is a managed **Apache Spark lakehouse platform** on Azure: workspaces, clusters, jobs/workflows, notebooks/repos, Delta Lake, and Unity Catalog governance. It integrates with ADLS, ADF, Power BI, and MLflow.

In Azure DE architectures it is the primary engine for large-scale transforms, SCD/MERGE, streaming ingestion (Autoloader), and collaborative engineering. Position it as compute + lakehouse management sitting on ADLS storage, orchestrated by Jobs and/or ADF.

---

### Q2. All-purpose clusters vs job clusters — cost and production practice?

**Answer:**  
**All-purpose clusters** are interactive/shared for development; they stay up until terminated and get expensive if left running. **Job clusters** are created for a run and terminated afterward — preferred for production (isolation, cost control, reproducible sizing).

Best practice: develop on small all-purpose clusters or SQL warehouses; schedule production on job clusters under cluster policies with auto-termination and mandatory cost tags. A common cost incident is production notebooks scheduled against a large always-on all-purpose cluster.

---

### Q3. What is Databricks Runtime (DBR), and what are Photon and AQE used for?

**Answer:**  
**DBR** bundles Spark, Delta, libraries, and performance features. Pin production jobs to a tested LTS runtime; regression-test major upgrades.

**Photon** is a vectorized execution engine that can accelerate many SQL/DataFrame operations on compatible runtimes/warehouses — not a separate API. **AQE** re-optimizes shuffles/joins at runtime. Photon/AQE help eligible workloads, but UDF-heavy or poorly laid-out Delta tables still underperform. Verify with Spark UI and realistic data volumes.

---

### Q4. DBFS vs Unity Catalog vs Volumes — practical guidance?

**Answer:**  
Historically **DBFS** mounted object storage; many teams also use `abfss://` directly. **Unity Catalog (UC)** is the modern governance layer: catalogs → schemas → tables/views/functions/volumes, with centralized permissions and auditing.

**Volumes** are governed locations for non-tabular files (landing files, artifacts). Prefer UC tables/volumes and direct cloud paths over legacy unmanaged DBFS mounts for new work. Least privilege via UC grants is a core production expectation.

---

### Q5. Delta Lake on Databricks — why is it central? OPTIMIZE / ZORDER / VACUUM trade-offs?

**Answer:**  
Delta enables ACID MERGE, schema enforcement, time travel, streaming source/sink, and compaction. Most ADE designs use Delta for Silver/Gold and often Bronze.

- **OPTIMIZE**: fewer, larger files → faster reads.  
- **ZORDER**: colocates data for better data skipping (selective columns only).  
- **VACUUM**: cleans unreferenced files; reduces time-travel depth and must respect retention.

Schedule optimize on hot tables; avoid ZORDERing every column. Liquid clustering is a newer alternative where available. Trade-off: frequent OPTIMIZE costs compute; skipping it costs query time and LIST overhead.

---

### Q6. Jobs & Workflows vs ADF orchestration — how do you decide?

**Answer:**  
**Workflows/Jobs** chain notebooks, wheels, SQL, conditionals, retries, and schedules inside Databricks. **ADF** orchestrates across Azure services (Copy from on-prem, Key Vault, enterprise trigger patterns) and can call Databricks activities.

Many teams use ADF for cross-system scheduling and Databricks Workflows for in-lakehouse DAGs — or Workflows alone when Databricks is the center of gravity. Pass `run_date` / batch IDs as job parameters for lineage and idempotency.

---

### Q7. What is Autoloader, and how do you design checkpoints?

**Answer:**  
**Autoloader** (`cloudFiles`) incrementally ingests new files from cloud storage into Delta using directory listing or file notification modes. It tracks processed files and supports schema inference/evolution — the standard Bronze pattern.

Give each stream a unique durable **checkpoint** path; do not delete it casually. Keep schema location durable. Prefer notification mode at high scale. On failure, restart resumes from checkpoint if sink semantics are safe (Delta). Pair with Silver cleanses and monitoring for lag/schema drift.

---

### Q8. How do secrets, cluster policies, and pools work?

**Answer:**  
Use **secret scopes** (Azure Key Vault-backed preferred) with `dbutils.secrets.get`; prefer Managed Identity/UC storage credentials to reduce secret sprawl. Never print secrets.

**Cluster policies** enforce max node types, auto-termination, runtime versions, and tags — critical for FinOps. **Instance pools** keep warm VMs to cut job startup latency; useful for frequent short jobs, with idle capacity cost. Balance cold-start SLA against pool spend.

---

### Q9. Explain lakehouse + medallion design on Databricks (concrete talking points).

**Answer:**  
Lakehouse = ADLS storage + Delta reliability/performance + UC governance + SQL warehouses for BI + Spark for engineering/ML on the same data.

- Bronze: Autoloader raw Delta + ingestion metadata.  
- Silver: typed, deduped, SCD MERGE.  
- Gold: star schemas/aggregates for BI.

Use UC catalogs/schemas per layer/environment (`prod.silver.customers`). Add quality checks between layers. Contrast with copying everything into a proprietary warehouse: lakehouse keeps open formats with ACID as the system of record.

---

### Q10. How does Databricks integrate with ADF? What auth and cluster patterns do you use?

**Answer:**  
ADF linked service → Databricks Notebook/Job activity with base parameters (`run_date`, `table`). ADF schedules and handles multi-system dependencies; Databricks executes Spark/Delta work on **job clusters** in production (not interactive all-purpose clusters).

Prefer AAD/MSI patterns over long-lived PATs where possible. Align storage paths (`abfss`) so ADF and Databricks share the lake. Return clear notebook exit codes/row counts for ADF dependency decisions.

---

### Q11. When choose Databricks vs ADF Mapping Data Flow vs Microsoft Fabric?

**Answer:**  
- **ADF Data Flow**: low-code, moderate transforms, ADF-centric teams.  
- **Databricks**: complex engineering, large-scale Spark, advanced Delta/streaming/ML, repos/tests/wheels.  
- **Fabric**: unified Microsoft analytics SaaS (OneLake, Lakehouse, Pipelines, Power BI) when the org standardizes there.

Choose based on scale, skillset, governance, and existing estate. Many enterprises still run ADF + Databricks + ADLS successfully; Fabric may consolidate pieces over time.

---

### Q12. Streaming basics — watermarks, `foreachBatch`, and batch on the same table?

**Answer:**  
Structured Streaming: source → transform → sink with checkpoint. Delta sinks are common for reliable lakehouse streaming. Use watermarks for late data in windowed aggregations; use `foreachBatch` + MERGE for upsert streams.

Hybrid is normal: streaming Bronze (Autoloader) + scheduled micro-batch Silver MERGE + batch Gold. Concurrent streaming and batch writers need compatible isolation and non-overlapping partition responsibilities or MERGE-safe designs to avoid conflicts and duplicates.

---

### Q13. Cost control and naming/tagging maturity on Databricks?

**Answer:**  
Prefer job clusters + auto-termination; right-size nodes; enforce policies/tags (`project`, `env`, `owner`, `cost_center`); use Photon/SQL warehouses appropriately; reduce shuffle via incremental design and OPTIMIZE; use pools only when startup SLA justifies idle cost.

Name jobs `domain_layer_process` and tables `catalog.schema.table` with layer in schema. Concrete FinOps story: move prod from always-on all-purpose to job clusters and cut DBU cost materially.

---

### Q14. How do you implement SCD2 on Delta, and what about DLT/Lakeflow?

**Answer:**  
MERGE pattern: detect changes via hash against `is_current` rows → close old current rows → insert new versions. Emphasize idempotent batch keys and late-arriving updates. Alternatively `APPLY CHANGES` / DLT declarative pipelines provide expectations, lineage, and managed orchestration for medallion tables.

If you have not used DLT hands-on, relate it honestly to the same Bronze→Silver→Gold principles implemented with Jobs + notebooks + quality checks.

---

### Q15. How do you handle PII, security, and the admin vs engineer split?

**Answer:**  
UC grants, row filters/column masks where applicable, separate catalogs for sensitive data, storage encryption, secret scopes, private networking/VNet injection, audit logs. Avoid uncontrolled PII in open Bronze paths; tokenize/hash in Silver when allowed.

Admins own metastore/network/SCIM/policies/budgets. Engineers own table design, jobs, repo code, and SLA for pipelines they own. Engineers should not need workspace admin for daily work when UC is set up correctly.

---

### Q16. Scenario — Job cluster vs all-purpose cost blowup. How do you remediate?

**Answer:**  
**Situation:** Monthly Databricks bill spiked; interactive clusters run 24/7 running “prod” notebooks.  
**Approach:** Move production to Jobs + job clusters; enforce policies.  
**Implementation:** Create Jobs with job clusters sized to workload; set auto-termination on all-purpose; apply cluster policies blocking oversized interactive clusters; tag everything; educate team. Optionally SQL warehouses for BI.  
**Validation:** DBU trend down; prod runs still meet SLA; inventory shows no always-on prod all-purpose clusters.

---

### Q17. Scenario — Autoloader missing files. How do you diagnose?

**Answer:**  
**Situation:** Files landed in ADLS but never appeared in Bronze.  
**Approach:** Check path patterns, permissions, schema evolution failures, and checkpoint progress.  
**Implementation:** Verify `cloudFiles` path and glob; confirm UC/storage credentials; inspect stream logs for rescued/corrupt data; confirm notification vs listing mode configuration; ensure files are not landing outside the watched prefix; validate checkpoint advancing.  
**Validation:** Drop a canary file; see it in Bronze with metadata columns; lag metrics recover; backlog files backfilled intentionally.

---

### Q18. Scenario — Concurrent MERGE conflicts on a Delta table. What do you do?

**Answer:**  
**Situation:** Two jobs MERGE into the same Silver table; one fails with concurrent operation conflicts.  
**Approach:** Serialize writers or partition ownership; reduce conflict scope.  
**Implementation:** Single-writer-per-table pattern for that MERGE; split tables by domain; stagger schedules; use Databricks Workflows concurrency controls; shorter transactions; ensure jobs do not overlap on the same target partitions.  
**Validation:** Conflict errors stop; both workloads complete in planned order; row-level uniqueness checks pass.

---

### Q19. Scenario — Notebook works interactively but fails as a Job. Why?

**Answer:**  
**Situation:** Manual run succeeds; scheduled Job fails.  
**Approach:** Diff cluster config, permissions, widgets/parameters, and working directory/repos context.  
**Implementation:** Align DBR/libraries with job cluster; pass widgets via job parameters; avoid relying on interactive state/`%pip` side effects — bake libraries into cluster policy/init/wheel; fix UC permissions for the job’s run-as identity; remove hard-coded personal paths.  
**Validation:** Job succeeds unattended for multiple `run_date`s; same code path used in DEV Jobs before PROD.

---

### Q20. Scenario — Unity Catalog permission denied. How do you resolve?

**Answer:**  
**Situation:** Job fails with `PERMISSION_DENIED` on `USE CATALOG` / `SELECT` / `MODIFY`.  
**Approach:** Grant least privilege to the correct principal (user, group, or service principal run-as).  
**Implementation:** Identify run-as identity in Job settings. Grant on catalog/schema/table/volume as needed. Verify external location and storage credential. Avoid granting broad admin rights.  
**Validation:** Job SP can only access required schemas; canary query works; audit log shows intended grants.

---

### Q21. Scenario — AQE/Photon: when it helps and when it does not.

**Answer:**  
**Situation:** Leadership asks why Photon did not speed up a notebook dominated by Python UDFs and tiny files.  
**Approach:** Explain eligibility — Photon accelerates many native SQL/DataFrame operators; Python UDFs and terrible file layouts limit gains.  
**Implementation:** Rewrite UDFs to SQL; OPTIMIZE tables; ensure Photon-enabled runtime/warehouse; confirm operators in UI show Photon. Keep AQE on for skew/partition coalescing.  
**Validation:** Eligible queries show lower runtime/DBUs; UDF-heavy path improves only after rewrite; document which workloads are Photon-eligible.

---

### Q22. Scenario — Small files from many partitions / streaming micro-batches.

**Answer:**  
**Situation:** Streaming writes every minute created millions of tiny files; Gold queries timed out.  
**Approach:** Compaction + fewer, larger writes.  
**Implementation:** Auto Optimize/OPTIMIZE schedule; increase streaming trigger interval; `coalesce`/`maxRecordsPerFile`; reduce over-partitioning; consider liquid clustering.  
**Validation:** File counts drop; query scan time improves; streaming lag remains within SLA.

---

### Q23. Scenario — Cluster pool cold start causes SLA miss. How do you design pools?

**Answer:**  
**Situation:** Short job should finish by 06:00 but spends 12 minutes provisioning VMs.  
**Approach:** Use instance pools for frequent short jobs; size min idle wisely.  
**Implementation:** Attach job cluster to a pool with warm instances of the right node type/DBR family; set min idle to cover morning burst; monitor idle DBU cost vs SLA benefit; keep cluster policy aligned.  
**Validation:** Startup time drops; SLA met; pool idle cost accepted by FinOps with tags/budgets.

---

### Q24. Scenario — Design medallion for clickstream on ADLS.

**Answer:**  
**Situation:** High-volume clickstream JSON lands continuously; need analytics-ready models.  
**Approach:** Autoloader Bronze → clean Silver sessions/events → Gold marts.  
**Implementation:**  
1. Autoloader (`cloudFiles`) JSON → `bronze.clickstream_raw` with filename/ingest time.  
2. Silver: parse, dedupe event_id, conform user/session keys, quarantine bad payloads.  
3. Gold: daily user metrics, funnel tables; partition by `event_date`.  
4. UC permissions for analysts via Gold views; OPTIMIZE hot tables.  
5. Orchestrate with Workflows; alert on lag and null rates.  
**Validation:** End-to-end canary events appear in Gold within SLA; duplicate event_id rate near zero; cost per billion events tracked.

---

### Q25. How do you debug a failed Job, parameterize notebooks, and structure code for CI?

**Answer:**  
Debug path: Job run event log → failed task → Spark UI/driver logs → exception. Validate parameters, data existence, UC/storage permissions, init scripts. Rerun failed task only if downstream is idempotent.

Parameterize with widgets/job params (`run_date`, `env`, `source_table`); ADF passes `base_parameters`. For CI: Databricks Repos/Git, thin notebook entrypoints, business logic in Python wheels/modules, deploy Jobs via bundles/Terraform/API. This avoids notebook spaghetti and makes interactive-vs-job parity achievable.

---

## Production Support Tickets (P1–P4)

Production-style incidents spanning PySpark/Spark jobs, ADF pipelines, and Databricks. Use these as storytelling practice for on-call / L2 support interviews.

### Priority & SLA reference

| Priority | Meaning | Typical response / resolve target |
|----------|---------|-----------------------------------|
| **P1** | Critical production outage / data unavailable for core SLA | Respond immediately; resolve **1–4 hours** |
| **P2** | Major impairment; workaround limited | Respond quickly; resolve **8–24 hours** |
| **P3** | Partial impact / degraded performance | Resolve **2–3 days** |
| **P4** | Minor / request / hardening | Resolve **~1 week** |

### Ticket summary

| Ticket ID | Priority | Area | Symptom (short) | SLA target |
|-----------|----------|------|-----------------|------------|
| INC-1001 | P1 | ADF + SHIR | On-prem ingest down; SHIR offline | 1–4 hrs |
| INC-1002 | P1 | Databricks | Critical Gold job failing; exec dashboard empty | 1–4 hrs |
| INC-1003 | P1 | PySpark/Delta | MERGE job stuck; downstream locked / conflict storm | 1–4 hrs |
| INC-2001 | P2 | ADF | Watermark advanced; missing half day of orders | 8–24 hrs |
| INC-2002 | P2 | Databricks | Autoloader lag; Bronze behind by hours | 8–24 hrs |
| INC-2003 | P2 | PySpark | Daily ETL OOM after volume spike | 8–24 hrs |
| INC-3001 | P3 | ADF | One table in ForEach failing nightly | 2–3 days |
| INC-3002 | P3 | Databricks | UC permission errors for analyst group | 2–3 days |
| INC-3003 | P3 | PySpark/Delta | Small files causing slow BI queries | 2–3 days |
| INC-4001 | P4 | ADF | DEV→PROD param promotion cleanup | ~1 week |
| INC-4002 | P4 | Databricks | Cost tagging / job cluster policy | ~1 week |
| INC-4003 | P4 | PySpark | Replace Python UDF with built-ins | ~1 week |

---

### INC-1001 — P1 — SHIR offline, on-prem pipelines failing

| Field | Detail |
|-------|--------|
| **Ticket ID** | INC-1001 |
| **Priority** | P1 |
| **Description** | All ADF pipelines copying from on-prem SQL Server fail. Monitor shows Self-hosted IR status unavailable. Finance close feed missed first SLA checkpoint. |
| **Resolution time / SLA** | Target **2 hours** (P1 window 1–4 hrs) |
| **Root cause** | SHIR Windows service stopped after VM disk full from heap dumps/logs; secondary node was not installed (no HA). |

**Fix / resolution steps:**  
1. Declare incident; pause non-critical triggers to reduce noise.  
2. On SHIR VM: free disk, clear old logs, restart SHIR service; verify outbound connectivity to Azure ADF endpoints.  
3. Run test linked-service connection + small Copy.  
4. Resume critical pipelines; backfill missed watermark ranges with idempotent loads.  
5. Permanent fix: add second SHIR node, disk alerts, log rotation.  

**Validation:** IR healthy; critical pipelines green; control-table watermarks consistent; postmortem filed.

---

### INC-1002 — P1 — Databricks Gold job failing; executive dashboard empty

| Field | Detail |
|-------|--------|
| **Ticket ID** | INC-1002 |
| **Priority** | P1 |
| **Description** | Scheduled Databricks Job `finance_gold_daily_sales` fails since 05:10. Power BI dataset refresh shows empty/stale Gold. |
| **Resolution time / SLA** | Target **3 hours** (P1 window 1–4 hrs) |
| **Root cause** | Job run-as service principal lost `MODIFY` on `prod.gold.daily_sales` after UC schema re-acl; interactive run by engineer still worked with personal grants. |

**Fix / resolution steps:**  
1. Open Job run → stack trace confirms `PERMISSION_DENIED`.  
2. Identify Job **run as** service principal.  
3. Grant least-privilege UC rights on required catalog/schema/table.  
4. Re-run Job for `run_date=today` on job cluster.  
5. Trigger BI refresh; communicate restoration.  
6. Add CI check / integration test that executes Job as SP in TEST.  

**Validation:** Job success; Gold row counts for business date match Silver; dashboard populated.

---

### INC-1003 — P1 — Concurrent Delta MERGE conflict blocking critical path

| Field | Detail |
|-------|--------|
| **Ticket ID** | INC-1003 |
| **Priority** | P1 |
| **Description** | Silver customer MERGE retries for 90+ minutes with concurrent update conflicts; dependent fact job is blocked; SLA breach imminent. |
| **Resolution time / SLA** | Target **2–3 hours** |
| **Root cause** | Two overlapping workflows (streaming `foreachBatch` MERGE and batch SCD2 job) writing the same Delta table concurrently. |

**Fix / resolution steps:**  
1. Stop/pause the lower-priority writer (streaming upsert) temporarily.  
2. Allow batch SCD2 MERGE to complete; verify uniqueness of `is_current` rows.  
3. Redesign: single-writer pattern or split hot/cold paths; serialize via Workflow concurrency = 1 on that target.  
4. Replay streaming backlog with idempotent MERGE.  

**Validation:** No conflict errors; customer current-row uniqueness check passes; facts launch on time.

---

### INC-2001 — P2 — Watermark advanced after incomplete sink write

| Field | Detail |
|-------|--------|
| **Ticket ID** | INC-2001 |
| **Priority** | P2 |
| **Description** | Orders team reports ~12:00–18:00 orders missing in Silver. ADF shows pipeline succeeded. |
| **Resolution time / SLA** | Target **12–24 hours** (P2 window 8–24 hrs) |
| **Root cause** | Watermark update activity ran on “completion” path even when Databricks notebook returned failure/partial write; dependency condition was wrong. |

**Fix / resolution steps:**  
1. Compare source max(`updated_at`) vs Silver coverage for the day.  
2. Reset watermark to last known good value; prepare replay window.  
3. Fix pipeline: watermark update **on success only** of Notebook activity; validate row-count threshold.  
4. Re-run replay; MERGE idempotently into Silver.  
5. Add alert when source_count vs sink_count diverges.  

**Validation:** Missing hours restored; intentional failure test in TEST does not advance watermark.

---

### INC-2002 — P2 — Autoloader lag; Bronze hours behind

| Field | Detail |
|-------|--------|
| **Ticket ID** | INC-2002 |
| **Priority** | P2 |
| **Description** | Clickstream Bronze lag metric > 4 hours; Silver/Gold delayed. Files visible in ADLS landing. |
| **Resolution time / SLA** | Target **8–16 hours** |
| **Root cause** | Autoloader in directory listing mode on a high-file-count prefix; listing bottleneck after a landing-folder fan-out change. Plus schema evolution failure on a new column paused progress for a subset. |

**Fix / resolution steps:**  
1. Inspect streaming query logs/checkpoint offsets and rescued data.  
2. Fix schema evolution policy / update schema location intentionally.  
3. Scale cluster; consider file notification mode; tighten glob to date partitions.  
4. Backfill backlog; compact tiny files afterward.  

**Validation:** Canary file appears in Bronze within SLA; lag < agreed threshold for 24h.

---

### INC-2003 — P2 — PySpark daily ETL OOM after volume spike

| Field | Detail |
|-------|--------|
| **Ticket ID** | INC-2003 |
| **Priority** | P2 |
| **Description** | Nightly PySpark job fails with executor OOM after marketing event caused ~8× traffic. |
| **Resolution time / SLA** | Target **16–24 hours** |
| **Root cause** | Aggressive `cache()` of full Bronze day + broadcast of a dimension that grew past threshold + skewed join on campaign_id. |

**Fix / resolution steps:**  
1. Spark UI: identify OOM stage; remove unnecessary cache; materialize intermediate Delta instead.  
2. Disable bad broadcast; restore sort-merge; salt/AQE for skew.  
3. Temporarily scale job cluster to clear SLA; then optimize for steady-state cost.  
4. Add data-volume guards/alerts.  

**Validation:** Job completes on right-sized cluster; shuffle metrics healthy; reconciliation green.

---

### INC-3001 — P3 — Single table fails nightly inside metadata ForEach

| Field | Detail |
|-------|--------|
| **Ticket ID** | INC-3001 |
| **Priority** | P3 |
| **Description** | 99/100 tables succeed; `sales_ledger_hist` fails each night with SQL timeout; overall pipeline marked failed, noisy pages. |
| **Resolution time / SLA** | Target **2–3 days** |
| **Root cause** | Unbounded historical extract without watermark predicate; table grew and started timing out; ForEach failure policy fails parent. |

**Fix / resolution steps:**  
1. Add watermark/CDC for that entity in metadata.  
2. Move heavy table to dedicated child pipeline with longer timeout and tuned parallelism.  
3. Persist per-entity failure status; allow parent success with alert on failed entities if business accepts.  
4. Backfill history in controlled chunks.  

**Validation:** Entity succeeds incrementally for 3 nights; pages only on real failures.

---

### INC-3002 — P3 — Unity Catalog permission errors for analyst group

| Field | Detail |
|-------|--------|
| **Ticket ID** | INC-3002 |
| **Priority** | P3 |
| **Description** | Analysts cannot query `prod.gold.*` in SQL warehouse after catalog migration; engineering jobs unaffected. |
| **Resolution time / SLA** | Target **2 days** |
| **Root cause** | Grants applied to old hive_metastore tables; UC privileges not granted to `gg-bi-analysts` on new catalog/schema; row filter missing on one PII view. |

**Fix / resolution steps:**  
1. Map required schemas/views for BI.  
2. Grant `USE CATALOG` / `USE SCHEMA` / `SELECT` to group.  
3. Re-apply column mask/row filter on sensitive view.  
4. Document access request process.  

**Validation:** Analyst canary queries succeed; unauthorized catalog still denied; audit logged.

---

### INC-3003 — P3 — Small files degrading BI query performance

| Field | Detail |
|-------|--------|
| **Ticket ID** | INC-3003 |
| **Priority** | P3 |
| **Description** | DBSQL queries on `gold.session_metrics` increased from 20s to 6+ minutes; no code change. |
| **Resolution time / SLA** | Target **2–3 days** |
| **Root cause** | Streaming + over-partitioning wrote hundreds of thousands of tiny files; OPTIMIZE not scheduled. |

**Fix / resolution steps:**  
1. Measure file count / size via DESCRIBE DETAIL.  
2. Run OPTIMIZE (and ZORDER on `event_date`, `app_id` if selective).  
3. Tune streaming trigger / `maxRecordsPerFile`; schedule weekly OPTIMIZE job.  
4. Reassess partition columns.  

**Validation:** File count drops; p95 query latency returns near baseline; OPTIMIZE job monitored.

---

### INC-4001 — P4 — DEV/TEST/PROD parameter promotion hardening

| Field | Detail |
|-------|--------|
| **Ticket ID** | INC-4001 |
| **Priority** | P4 |
| **Description** | Near-miss: PROD release briefly pointed a linked service at TEST storage due to missing ARM parameter override. Caught in smoke test. |
| **Resolution time / SLA** | Target **1 week** |
| **Root cause** | Release pipeline lacked mandatory environment parameter validation and post-deploy connection tests. |

**Fix / resolution steps:**  
1. Add CI gates requiring env-specific parameter files.  
2. Post-deploy smoke: test connections + canary Copy to PROD landing only.  
3. Document promotion checklist; restrict PROD factory UI edits.  

**Validation:** Deliberate bad parameter fails CI; successful release smoke tests pass.

---

### INC-4002 — P4 — Cluster policy and cost tagging for Jobs

| Field | Detail |
|-------|--------|
| **Ticket ID** | INC-4002 |
| **Priority** | P4 |
| **Description** | FinOps requests enforcement: no untagged clusters; prod must use job clusters. |
| **Resolution time / SLA** | Target **1 week** |
| **Root cause** | No cluster policy; engineers created oversized interactive clusters for “quick prod fixes.” |

**Fix / resolution steps:**  
1. Create/enforce cluster policies (node limits, auto-termination, required tags).  
2. Migrate remaining prod schedules to Jobs.  
3. Educate team; dashboard idle DBU by owner tag.  

**Validation:** Policy blocks non-compliant clusters; tagged cost report coverage > 95%.

---

### INC-4003 — P4 — Replace slow Python UDF in PySpark ETL

| Field | Detail |
|-------|--------|
| **Ticket ID** | INC-4003 |
| **Priority** | P4 |
| **Description** | Non-SLA job runtime slowly grew from 25 to 70 minutes after phone-normalization UDF; optimize before next peak season. |
| **Resolution time / SLA** | Target **1 week** |
| **Root cause** | Row-wise Python UDF prevented native execution optimizations. |

**Fix / resolution steps:**  
1. Rewrite with Spark built-ins / selective Pandas UDF.  
2. Add unit tests for normalization cases.  
3. Compare stage times and outputs on sample + full backfill day.  
4. Deploy via Job; remove old UDF.  

**Validation:** Runtime ≤ 30 minutes on representative volume; regression tests pass.

---

## Quick revision checklist

Before a mock interview, be able to explain in ~2 minutes each:

1. Lazy evaluation + DAG/stages + shuffle/skew  
2. Broadcast join vs sort-merge + when broadcast hurts  
3. Delta MERGE + SCD2 + idempotent incremental loads  
4. ADF metadata-driven ForEach + watermark success-path  
5. Azure IR vs SHIR + HA  
6. Job cluster vs all-purpose + UC permissions  
7. Autoloader + checkpoints + medallion on Delta  
8. P1 vs P2 triage mindset (restore service → then harden)

---

*Use with: [Azure-Data-Engineering-30-Day-Interview-Plan.md](Azure-Data-Engineering-30-Day-Interview-Plan.md)*
