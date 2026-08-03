# Azure Data Engineering — 30-Day Interview Preparation Plan

**Goal:** Be interview-ready for Azure Data Engineer roles covering **PySpark → SQL + ADF → Microsoft Fabric**, with daily practice and revision.

**Start date:** Monday, August 3, 2026 → target ready by **Tuesday, September 1, 2026**.

**How to use this plan**
- Run this as **4 agile sprints** — board + full flow: [`sprints/README.md`](sprints/README.md).
- Study **3–4 hours/day** (focus + hands-on).
- Every day: complete the **tutorial notebook** → do the **assignment notebook** → write 5 interview answers.
- Notebooks live in [`notebooks/`](notebooks/README.md) (`day-NN/NN-tutorial.ipynb` + `NN-assignment.ipynb`), grouped by sprint.
- Keep a notes file: definitions, patterns, mistakes, and “explain in 2 minutes” scripts.
- End of each sprint: mock interview (45–60 min) + review + retro.

---

## 4-Sprint Structure (starts today)

| Sprint | Dates | Days | Goal | Folder |
|--------|-------|------|------|--------|
| **1 — PySpark** | Mon Aug 3 – Sun Aug 9, 2026 | 1–7 | Spark lifecycle + common PySpark coding | [sprints/sprint-01](sprints/sprint-01/README.md) |
| **2 — SQL + ADF fundamentals** | Mon Aug 10 – Sun Aug 16, 2026 | 8–14 | Medium SQL + parameterized ADF sketches | [sprints/sprint-02](sprints/sprint-02/README.md) |
| **3 — ADF deep dive** | Mon Aug 17 – Sun Aug 23, 2026 | 15–21 | Production incremental + metadata-driven ADF | [sprints/sprint-03](sprints/sprint-03/README.md) |
| **4 — Fabric + finals** | Mon Aug 24 – **Tue Sep 1**, 2026 | 22–30 | Fabric architecture + two full mocks (**9 days**) | [sprints/sprint-04](sprints/sprint-04/README.md) |

> Sprint 4 is intentionally **9 calendar days** so Fabric (22–28) and finals (29–30) both get full days.

**Start today (Sprint 1, Day 1):** [01-tutorial](notebooks/day-01/01-tutorial.ipynb) → [01-assignment](notebooks/day-01/01-assignment.ipynb) · standup: [daily-standup.md](sprints/sprint-01/daily-standup.md)

Each sprint folder has: `README` · `backlog` · `board` · `daily-standup` · `review` · `retro`.

---

## Plan Overview (topic phases)

| Phase | Days | Sprint | Focus |
|-------|------|--------|--------|
| **Week 1** | 1–7 | Sprint 1 | PySpark (core for Databricks / Spark interviews) |
| **Week 2** | 8–14 | Sprint 2 | SQL (T-SQL / analytics) + start ADF |
| **Week 3** | 15–21 | Sprint 3 | Azure Data Factory (pipelines, orchestration, patterns) |
| **Week 4** | 22–28 | Sprint 4 | Microsoft Fabric (Lakehouse, Warehouse, pipelines, OneLake) |
| **Final** | 29–30 | Sprint 4 | Full revision + mock interviews |

---

# Week 1 — PySpark (Days 1–7)

## Day 1 — Spark & PySpark Fundamentals
**Learn**
- Spark architecture: Driver, Executors, Cluster Manager
- Transformations vs Actions
- Lazy evaluation, DAG, stages, tasks
- SparkSession, DataFrame vs RDD

**Practice**
- Create a SparkSession
- Read CSV/JSON into DataFrame
- `select`, `filter`, `withColumn`, `drop`, `alias`

**Interview focus**
- Explain lazy evaluation with an example
- Difference between RDD, DataFrame, Dataset
- What happens when you call `count()` / `show()` / `collect()`

---

## Day 2 — DataFrame Operations & Schema
**Learn**
- Schema inference vs explicit schema
- Types: String, Integer, Timestamp, Array, Struct, Map
- Null handling, casting, renaming
- `printSchema`, `describe`, `summary`

**Practice**
- Define StructType schema
- Clean nulls, cast columns, rename consistently
- Nested JSON flatten (`explode`, `getField`)

**Interview focus**
- Why define schema instead of inferring?
- How do you handle nested JSON in PySpark?
- `dropDuplicates` vs `distinct`

---

## Day 3 — Joins, Aggregations, Window Functions
**Learn**
- Join types: inner, left, right, full, anti, semi, cross
- Broadcast join vs shuffle join
- Aggregations: `groupBy`, `agg`, `pivot`
- Window: `row_number`, `rank`, `dense_rank`, `lag`, `lead`, running totals

**Practice**
- Deduplicate with `row_number` over partition
- Running average / previous-day comparison
- Compare join strategies on a sample dataset

**Interview focus**
- When to use broadcast join?
- `rank` vs `dense_rank` vs `row_number`
- How would you find latest record per customer?

---

## Day 4 — Performance Tuning Essentials
**Learn**
- Partitioning, coalesce vs repartition
- Shuffle, skew, spill
- Caching / persist levels
- Catalyst optimizer & Tungsten (high level)
- AQE (Adaptive Query Execution) basics
- Predicate pushdown, column pruning

**Practice**
- Inspect plans with `explain()`
- Fix a skewed join (salting idea / AQE)
- Cache a reused DataFrame and measure impact

**Interview focus**
- Why is shuffle expensive?
- When to use `repartition` vs `coalesce`?
- How do you debug a slow Spark job?

---

## Day 5 — File Formats, Partitioning & Delta Basics
**Learn**
- Parquet vs CSV vs JSON vs ORC
- Partition columns, partition pruning
- Delta Lake: ACID, time travel, MERGE, OPTIMIZE, Z-ORDER (overview)
- Write modes: append, overwrite, overwriteSchema

**Practice**
- Write partitioned Parquet
- Simple Delta table: write, read, time travel
- `MERGE` upsert scenario (customers / orders)

**Interview focus**
- Why Parquet over CSV in production?
- What problem does Delta solve?
- Explain MERGE / upsert pattern

---

## Day 6 — Streaming & Real-World Patterns (Interview Depth)
**Learn**
- Structured Streaming basics: source → transform → sink
- Checkpointing, watermarking (concept)
- Common batch patterns: bronze/silver/gold, SCD Type 1/2 (high level)
- Error handling & bad records

**Practice**
- Design (on paper or notebook) a medallion pipeline
- Implement SCD Type 1 upsert with Delta
- Outline SCD Type 2 columns (valid_from, valid_to, is_current)

**Interview focus**
- Medallion architecture explained simply
- Batch vs streaming — when each?
- How do you ensure exactly-once / idempotent loads?

---

## Day 7 — Week 1 Review + PySpark Mock
**Do**
- Revise Days 1–6 notes (2 hours)
- Solve 8–10 coding problems:
  - Top N per group
  - Remove duplicates keep latest
  - Explode arrays + aggregate
  - Join + window KPI
  - Simple MERGE

**Mock interview (45 min)**
1. Architecture explanation
2. 2 coding problems (live)
3. Performance debugging scenario

**Checklist before Week 2**
- [ ] Can explain Spark job lifecycle
- [ ] Comfortable with joins + windows
- [ ] Can talk through one optimized pipeline end-to-end

---

# Week 2 — SQL + Start ADF (Days 8–14)

## Day 8 — SQL Foundations for Data Engineers
**Learn**
- SELECT, WHERE, ORDER BY, GROUP BY, HAVING
- JOINs (all types) + when to use each
- Subqueries vs CTEs
- NULL behavior (`IS NULL`, `COALESCE`, `NULLIF`)

**Practice**
- 15–20 problems: joins + aggregations
- Rewrite nested subqueries as CTEs

**Interview focus**
- WHERE vs HAVING
- INNER vs LEFT JOIN — real examples
- How NULLs affect joins and aggregates

---

## Day 9 — Advanced SQL (Windows, Analytics)
**Learn**
- Window functions (same mental model as Spark)
- Running totals, moving averages
- `QUALIFY`-style patterns (or filter with subquery if T-SQL)
- Set ops: UNION / UNION ALL / INTERSECT / EXCEPT
- CASE expressions for bucketing

**Practice**
- Latest order per customer
- Month-over-month growth
- Gaps and islands (basic version)

**Interview focus**
- Explain window frame (`ROWS` vs `RANGE`) at high level
- Dedup strategy in SQL
- Write a query for “employees earning more than dept average”

---

## Day 10 — Performance, Indexing & Data Modeling SQL
**Learn**
- Primary key, foreign key, indexes (clustered/nonclustered basics)
- Star vs snowflake schema
- Fact vs dimension tables
- Normalization vs denormalization for analytics
- Query plans (conceptual): scans vs seeks

**Practice**
- Design a star schema for retail sales
- Write 5 BI-style queries against that model

**Interview focus**
- Why star schema for warehousing?
- Slowly Changing Dimensions (Type 1 vs Type 2)
- How do you improve a slow SQL query?

---

## Day 11 — T-SQL / Azure SQL Practical Patterns
**Learn**
- Temp tables vs table variables vs CTEs
- MERGE statement
- Stored procedures (when used in ETL)
- Transactions, idempotent loads
- Incremental load patterns (watermark / last_updated)

**Practice**
- Implement incremental load logic in SQL
- MERGE for upsert
- Soft delete pattern

**Interview focus**
- Full load vs incremental load
- How to make a pipeline restart-safe
- MERGE pitfalls (duplicates, matching keys)

---

## Day 12 — ADF Fundamentals
**Learn**
- ADF components: pipeline, activity, dataset, linked service, integration runtime
- Copy activity basics
- Parameters vs variables
- Triggers: schedule, tumbling window, event
- Pipeline run monitoring

**Practice**
- Design (whiteboard) a pipeline: Blob → ADLS → Azure SQL
- List linked services needed for a typical project
- Parameterize source path and table name

**Interview focus**
- Dataset vs linked service
- Self-hosted IR vs Azure IR — when?
- How do parameters make pipelines reusable?

---

## Day 13 — ADF Control Flow & Orchestration
**Learn**
- Activities: ForEach, If Condition, Until, Lookup, Get Metadata, Execute Pipeline
- Dependency conditions: Succeeded / Failed / Completed
- Error handling: retry, timeout, fail activity
- Metadata-driven pipeline concept

**Practice**
- Design a ForEach over file list / table list
- Parent-child pipeline pattern sketch
- Failure notification flow (Logic App / email concept)

**Interview focus**
- How do you process 100 tables dynamically?
- Lookup + ForEach pattern
- How do you handle partial failures?

---

## Day 14 — Week 2 Review + SQL/ADF Mock
**Do**
- Revise SQL + ADF notes
- 10 SQL interview problems (timed)
- Draw 1 end-to-end ADF architecture for a case study

**Mock interview (45 min)**
1. Star schema design (10 min)
2. 2 SQL problems (20 min)
3. ADF pipeline design scenario (15 min)

**Checklist before Week 3**
- [ ] Strong on joins, windows, incremental SQL
- [ ] Can explain ADF building blocks clearly
- [ ] Can design a parameterized orchestration pipeline

---

# Week 3 — Azure Data Factory Deep Dive (Days 15–21)

## Day 15 — Copy Activity Mastery
**Learn**
- Mapping, schema drift
- Staging, PolyBase / BULK options (high level)
- Incremental copy patterns (watermark column)
- Parallel copy, DIU concepts (interview-level)
- Binary vs tabular copy

**Practice**
- Design incremental copy from SQL → ADLS
- Handle late-arriving files
- File naming + folder date partitioning strategy

**Interview focus**
- How do you implement incremental load in ADF?
- What is schema drift and when is it useful?
- Full refresh vs delta load decision

---

## Day 16 — Data Flows (Mapping Data Flows)
**Learn**
- Mapping Data Flow vs Copy Activity
- Transformations: Select, Derived Column, Aggregate, Join, Exists, Surrogate Key, Sink
- Debug mode vs pipeline run
- When to use Data Flow vs Databricks/Spark

**Practice**
- Design a Data Flow: clean → join dim → write fact
- Surrogate key + SCD Type 1 flow sketch

**Interview focus**
- ADF Data Flow vs Spark notebook — trade-offs
- How do you unit-test / debug a Data Flow?
- Cost/performance considerations

---

## Day 17 — Integration Runtimes, Security & Connectivity
**Learn**
- Azure IR, Self-hosted IR, Azure-SSIS IR
- Private endpoints / VNet concepts (interview level)
- Managed Identity, Key Vault for secrets
- Linked service authentication options

**Practice**
- Document a secure landing-zone style ADF setup
- List secrets that belong in Key Vault

**Interview focus**
- When is Self-hosted IR required?
- How do you avoid storing secrets in ADF?
- On-prem SQL → cloud ingestion pattern

---

## Day 18 — CI/CD, Monitoring & Production Practices
**Learn**
- Dev/Test/Prod, ARM/Bicep or Azure DevOps deployment (overview)
- Alerts, Log Analytics, pipeline run history
- Idempotency, restartability, concurrency control
- SLAs and dependency management

**Practice**
- Write a runbook: “pipeline failed at copy — how to recover”
- Define retry policy for flaky sources

**Interview focus**
- How do you promote ADF changes across environments?
- How do you monitor and alert on failures?
- How do you avoid duplicate loads on rerun?

---

## Day 19 — End-to-End Case Study (ADF + Storage + SQL)
**Build (design + document)**
Scenario: Daily sales files land in ADLS Gen2 → validate → transform → load warehouse dims/facts → log control table

**Deliverables**
1. Architecture diagram
2. Pipeline list + activity sequence
3. Incremental logic
4. Error handling strategy
5. 2-minute spoken explanation

**Interview focus**
- Walk through the design without notes
- Justify tool choices

---

## Day 20 — Common ADF Interview Scenarios
**Drill these scenarios (write answers)**
1. Dynamically ingest all tables from a source DB
2. Process only new files since last run
3. Load dimensions then facts with dependencies
4. Handle late files and reprocessing
5. Large file performance tuning
6. Cross-environment parameterization

**Practice**
- Record yourself answering 3 scenarios (2–3 min each)

---

## Day 21 — Week 3 Review + ADF Mock
**Do**
- Quick revision of Days 12–20
- Redraw architecture from memory
- Flashcards: IR, triggers, activities, CI/CD, Key Vault

**Mock interview (60 min)**
1. Component definitions (rapid fire)
2. Design a metadata-driven ingestion platform
3. Debugging a failed production pipeline

**Checklist before Week 4**
- [ ] Can design ADF pipelines end-to-end
- [ ] Clear on incremental + metadata-driven patterns
- [ ] Can discuss security and monitoring

---

# Week 4 — Microsoft Fabric (Days 22–28)

## Day 22 — Fabric Landscape & OneLake
**Learn**
- What is Microsoft Fabric (unified analytics platform)
- Workspaces, capacities, items
- OneLake concept (“OneDrive for data”)
- Fabric vs Azure “classic” stack (ADF + Databricks + Synapse) — when each appears in interviews

**Practice**
- Draw Fabric architecture for a mid-size company
- Map old Azure services → Fabric equivalents

**Interview focus**
- What problem does Fabric solve?
- What is OneLake?
- Fabric vs Synapse vs Databricks (balanced answer)

---

## Day 23 — Lakehouse in Fabric
**Learn**
- Lakehouse item: files + tables
- Delta tables in Fabric
- Bronze / Silver / Gold in Fabric
- Shortcuts (OneLake shortcuts) concept
- Spark notebooks in Fabric

**Practice**
- Design medallion layers in a Fabric Lakehouse
- Notebook flow: ingest → clean → curated table

**Interview focus**
- Lakehouse vs Warehouse — when to use which?
- What are shortcuts and why use them?
- How is Delta used in Fabric Lakehouse?

---

## Day 24 — Fabric Warehouse & SQL Analytics
**Learn**
- Fabric Warehouse basics
- T-SQL on warehouse
- Star schema in Fabric
- Direct Lake vs Import vs DirectQuery (Power BI modes — interview awareness)
- Security basics: workspace roles, RLS concept

**Practice**
- Design dims/facts for a Fabric Warehouse
- Write 5 analytical SQL queries for interview practice

**Interview focus**
- Lakehouse vs Warehouse decision criteria
- How does Power BI typically connect in Fabric projects?
- Explain Direct Lake at a high level

---

## Day 25 — Fabric Data Pipelines & Dataflows
**Learn**
- Fabric pipelines (ADF-like experience)
- Dataflow Gen2 overview
- Orchestrating notebooks + pipelines
- Scheduling and monitoring in Fabric
- Reuse of ADF skills inside Fabric

**Practice**
- Design a Fabric pipeline: land files → notebook transform → warehouse load
- Compare Dataflow Gen2 vs Spark notebook for a transform

**Interview focus**
- How is Fabric pipeline similar/different from ADF?
- When prefer Dataflow Gen2 vs notebook?
- How do you orchestrate multi-step Fabric jobs?

---

## Day 26 — Real-Time, Governance & Fabric Ecosystem
**Learn (interview-level)**
- Eventstream / real-time analytics overview
- Purview / governance concepts in Fabric world
- Endorsement, lineage (high level)
- Capacity units / cost awareness (basic)
- Domains / workspace organization

**Practice**
- Write a 1-page “Fabric governance checklist”
- Case: raw data in ADLS, analytics in Fabric via shortcut

**Interview focus**
- How do you organize workspaces for Dev/Test/Prod?
- How do you avoid duplicate data copies with OneLake shortcuts?
- Cost control talking points

---

## Day 27 — End-to-End Fabric Case Study
**Build (design + speak)**
Scenario: Retail company migrates from ADF + ADLS + SQL to Fabric

**Cover**
1. Landing → Lakehouse medallion
2. Curated model in Warehouse or Lakehouse SQL
3. Pipeline orchestration
4. Power BI consumption
5. Security + monitoring

**Deliverable**
- Architecture diagram + 3-minute walkthrough script

---

## Day 28 — Week 4 Review + Fabric Mock
**Do**
- Revise Fabric vocabulary
- Compare answers: Fabric vs ADF+Databricks for 3 scenarios
- Flashcards: OneLake, Lakehouse, Warehouse, shortcuts, Direct Lake, pipelines

**Mock interview (60 min)**
1. “Explain Fabric to a hiring manager”
2. Design a Fabric lakehouse platform
3. Migration scenario from classic Azure DE stack

**Checklist before final days**
- [ ] Clear Fabric story end-to-end
- [ ] Can map ADF skills to Fabric pipelines
- [ ] Can justify Lakehouse vs Warehouse

---

# Final Stretch — Days 29–30

## Day 29 — Full Stack Revision (All Topics)
**Morning — Rapid revision**
- PySpark: architecture, joins, windows, Delta, performance
- SQL: windows, incremental, star schema, MERGE
- ADF: IR, Copy, ForEach, metadata-driven, Key Vault
- Fabric: OneLake, Lakehouse, Warehouse, pipelines, shortcuts

**Afternoon — Question bank (write short answers)**
Aim for **40 answers** across:
- Architecture / design
- Coding (PySpark + SQL)
- Scenario-based (failures, late data, scale)
- Behavioral: “Tell me about a pipeline you built”

**Create your “Top 20” cheat sheet**
One page only: strongest talking points + code patterns.

---

## Day 30 — Final Mock Interviews + Weak Spot Fix
**Morning**
- Mock 1 (60 min): PySpark + SQL heavy
- Mock 2 (60 min): ADF + Fabric design heavy

**Afternoon**
- Fix weakest 3 topics only
- Re-answer 10 questions out loud
- Prepare 3 stories using STAR:
  1. Performance tuning win
  2. Pipeline failure / incident
  3. Design decision (tool choice / trade-off)

**Final readiness checklist**
- [ ] 2-minute intro as Azure Data Engineer
- [ ] 1 end-to-end project story (ADF or Fabric)
- [ ] 1 PySpark optimization story
- [ ] 1 SQL modeling story (star schema / SCD)
- [ ] Questions ready to ask interviewer

---

# Daily Template (Use Every Day)

```text
Date: ____   Day #: ____   Topic: ____
1) Concepts learned (bullet points)
2) Hands-on practice done
3) 5 interview Q&A written
4) Blockers / doubts
5) Tomorrow’s focus
```

---

# Must-Know Interview Themes (All 30 Days)

1. **End-to-end data pipeline design** (ingest → transform → serve)
2. **Incremental load / watermark / MERGE / idempotency**
3. **Medallion architecture** (Bronze / Silver / Gold)
4. **Performance** (Spark shuffles, SQL plans, ADF copy tuning)
5. **Data modeling** (star schema, SCD1/SCD2)
6. **Orchestration** (ADF / Fabric pipelines, dependencies, retries)
7. **Security** (Key Vault, Managed Identity, least privilege)
8. **Quality & observability** (validation, logging, alerts, reprocessing)
9. **Tool choice** (Spark vs Data Flow vs SQL; Lakehouse vs Warehouse)
10. **Clear communication** (whiteboard + 2-minute explanations)

---

# Suggested Practice Resources (Pick 1–2, Don’t Overload)

- **PySpark:** Databricks Academy free content / local Spark notebooks
- **SQL:** LeetCode SQL, StrataScratch, or Mode SQL tutorials
- **ADF:** Microsoft Learn — Azure Data Factory learning path
- **Fabric:** Microsoft Learn — Microsoft Fabric fundamentals + Lakehouse tutorials
- **Mock practice:** Explain designs on paper, then speak them aloud daily

---

# Sprint Success Criteria

| Sprint | Dates | You are ready if you can… |
|--------|-------|---------------------------|
| 1 | Aug 3–9 | Code common PySpark transforms and explain Spark performance basics |
| 2 | Aug 10–16 | Solve medium SQL problems and sketch parameterized ADF pipelines |
| 3 | Aug 17–23 | Design production-grade ADF incremental + metadata-driven flows |
| 4 | Aug 24–Sep 1 | Architect a Fabric Lakehouse solution, compare to classic Azure DE, and pass two full mocks |

Track Done in each sprint’s [`review.md`](sprints/README.md).

---

Good luck — stay consistent, speak answers out loud, and prioritize hands-on over passive reading.
