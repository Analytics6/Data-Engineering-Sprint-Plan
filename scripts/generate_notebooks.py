"""Generate tutorial + assignment Jupyter notebooks for the 30-day Azure DE plan."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "notebooks"


def md(text: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": text.strip() + "\n"}


def code(text: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": text.strip() + "\n",
    }


def nb(cells: list[dict]) -> dict:
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "pygments_lexer": "ipython3"},
        },
        "cells": cells,
    }


def write_nb(day: int, kind: str, cells: list[dict]) -> None:
    folder = ROOT / f"day-{day:02d}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{day:02d}-{kind}.ipynb"
    path.write_text(json.dumps(nb(cells), indent=1), encoding="utf-8")
    print("Wrote", path.relative_to(ROOT.parent))


SPARK_SETUP = [
    md(
        "### Environment setup\n"
        "Skip pip install on Databricks/Fabric. Locally you may need: `pip install pyspark pandas`."
    ),
    code("# %pip install pyspark==3.5.1 pandas -q"),
    code(
        "from pyspark.sql import SparkSession\n"
        "from pyspark.sql import functions as F\n"
        "from pyspark.sql import types as T\n"
        "from pyspark.sql.window import Window\n\n"
        "spark = (\n"
        "    SparkSession.builder\n"
        "    .appName('AzureDE-InterviewPrep')\n"
        "    .master('local[*]')\n"
        "    .config('spark.sql.shuffle.partitions', '4')\n"
        "    .getOrCreate()\n"
        ")\n"
        "spark"
    ),
]


def sql_cell(sql: str) -> dict:
    return code("spark.sql('''\n" + sql.strip() + "\n''').show()")


DAYS: dict[int, dict] = {}


def add(day: int, title: str, topic: str, tutorial: list, assignment: list) -> None:
    DAYS[day] = {
        "title": title,
        "topic": topic,
        "tutorial": tutorial,
        "assignment": assignment,
    }


# ========================= WEEK 1: PYSPARK =========================

add(
    1,
    "Spark & PySpark Fundamentals",
    "PySpark",
    [
        md(
            "# Day 01 Tutorial — Spark & PySpark Fundamentals\n\n"
            "**Goal:** Understand Spark architecture and basic DataFrame operations.\n\n"
            "## Learning objectives\n"
            "- Driver, Executors, Cluster Manager\n"
            "- Transformations vs Actions + lazy evaluation\n"
            "- Create SparkSession and explore a DataFrame"
        ),
        *SPARK_SETUP,
        md(
            "## 1. Architecture (interview mental model)\n\n"
            "| Component | Role |\n"
            "|-----------|------|\n"
            "| **Driver** | Runs main code, builds DAG, schedules tasks |\n"
            "| **Executor** | Runs tasks and stores data |\n"
            "| **Cluster Manager** | Allocates resources |\n\n"
            "**Lazy evaluation:** Transformations build a plan; Actions trigger execution."
        ),
        md("## 2. Create a DataFrame"),
        code(
            "data = [\n"
            "    (1, 'Alice', 'Sales', 70000),\n"
            "    (2, 'Bob', 'Engineering', 90000),\n"
            "    (3, 'Carol', 'Sales', 72000),\n"
            "    (4, 'Dan', 'Engineering', 95000),\n"
            "    (5, 'Eve', 'HR', 65000),\n"
            "]\n"
            "cols = ['emp_id', 'name', 'department', 'salary']\n"
            "df = spark.createDataFrame(data, cols)\n"
            "df.printSchema()\n"
            "df.show()"
        ),
        md("## 3. Transformations (lazy) vs Actions (eager)"),
        code(
            "high_paid = (\n"
            "    df.filter(F.col('salary') > 70000)\n"
            "      .select('name', 'department', 'salary')\n"
            "      .withColumn('salary_k', F.round(F.col('salary') / 1000, 1))\n"
            ")\n"
            "print('Plan built; nothing executed yet.')\n"
            "high_paid.explain(True)\n"
            "high_paid.show()\n"
            "print('Count:', high_paid.count())"
        ),
        md("## 4. Common operations"),
        code(
            "(\n"
            "    df.select(F.col('name').alias('employee_name'), 'department', 'salary')\n"
            "      .filter(F.col('department') == 'Sales')\n"
            "      .show()\n"
            ")\n\n"
            "df.withColumn(\n"
            "    'band',\n"
            "    F.when(F.col('salary') >= 90000, 'A')\n"
            "     .when(F.col('salary') >= 70000, 'B')\n"
            "     .otherwise('C'),\n"
            ").show()"
        ),
        md(
            "## Interview talking points\n"
            "1. DataFrames have schema + Catalyst optimizer (prefer over RDDs for analytics).\n"
            "2. `show` / `count` / `collect` are actions; avoid `collect` on large data.\n"
            "3. Lazy evaluation chains transforms into one optimized job at action time."
        ),
        md("## Mini practice"),
        code(
            "# TODO: Engineering employees with salary_k and band\n"
            "# Attempt first, then check solution cell"
        ),
        code(
            "(\n"
            "    df.filter(F.col('department') == 'Engineering')\n"
            "      .withColumn('salary_k', F.round(F.col('salary') / 1000, 1))\n"
            "      .withColumn('band', F.when(F.col('salary') >= 90000, 'A').otherwise('B'))\n"
            "      .select('name', 'salary', 'salary_k', 'band')\n"
            "      .show()\n"
            ")"
        ),
    ],
    [
        md(
            "# Day 01 Assignment — PySpark Fundamentals\n\n"
            "**Time box:** 60-90 minutes"
        ),
        *SPARK_SETUP,
        md("## Dataset"),
        code(
            "orders = spark.createDataFrame(\n"
            "    [\n"
            "        (1001, 'c01', 'IN', 250.0, '2024-01-05'),\n"
            "        (1002, 'c02', 'US', 40.0, '2024-01-06'),\n"
            "        (1003, 'c01', 'IN', 120.0, '2024-01-07'),\n"
            "        (1004, 'c03', 'UK', 500.0, '2024-01-07'),\n"
            "        (1005, 'c02', 'US', 15.0, '2024-01-08'),\n"
            "        (1006, 'c04', 'IN', 300.0, '2024-01-08'),\n"
            "    ],\n"
            "    ['order_id', 'customer_id', 'country', 'amount', 'order_date'],\n"
            ")\n"
            "orders.show()"
        ),
        md(
            "## Tasks\n"
            "### A1. Filter IN orders; columns order_id, customer_id, amount_inr; sort amount desc.\n"
            "### A2. Add order_size: Small <100, Medium <300, else Large.\n"
            "### A3. Comment which ops are transformations vs actions; explore with show/count.\n"
            "### A4. Answer: lazy evaluation; RDD vs DataFrame; why collect() is dangerous.\n"
            "### Stretch. Function high_value_orders(df, min_amount)."
        ),
        code("# A1"),
        code("# A2"),
        code("# A3"),
        md("### A4 Answers\n1. ...\n2. ...\n3. ..."),
        code("# Stretch"),
    ],
)

add(
    2,
    "DataFrame Operations & Schema",
    "PySpark",
    [
        md(
            "# Day 02 Tutorial — DataFrame Operations & Schema\n\n"
            "**Goal:** Explicit schemas, null handling, nested JSON flatten."
        ),
        *SPARK_SETUP,
        md("## Infer vs explicit schema"),
        code(
            "schema = T.StructType([\n"
            "    T.StructField('id', T.IntegerType(), True),\n"
            "    T.StructField('name', T.StringType(), False),\n"
            "    T.StructField('salary', T.DoubleType(), True),\n"
            "])\n"
            "df = spark.createDataFrame(\n"
            "    [(1, 'Alice', 70000.0), (2, 'Bob', None), (3, 'Carol', 72000.0)],\n"
            "    schema,\n"
            ")\n"
            "df.printSchema()\n"
            "df.show()"
        ),
        md("## Nulls, cast, rename"),
        code(
            "(\n"
            "    df.withColumnRenamed('name', 'employee_name')\n"
            "      .na.fill({'salary': 0.0})\n"
            "      .na.drop(subset=['employee_name'])\n"
            "      .show()\n"
            ")"
        ),
        md("## Nested structs and arrays"),
        code(
            "nested_schema = T.StructType([\n"
            "    T.StructField('id', T.IntegerType(), False),\n"
            "    T.StructField('address', T.StructType([\n"
            "        T.StructField('city', T.StringType(), True),\n"
            "        T.StructField('pin', T.StringType(), True),\n"
            "    ]), True),\n"
            "    T.StructField('skills', T.ArrayType(T.StringType()), True),\n"
            "])\n"
            "nested = spark.createDataFrame(\n"
            "    [\n"
            "        (1, {'city': 'Pune', 'pin': '411001'}, ['spark', 'sql']),\n"
            "        (2, {'city': 'London', 'pin': 'SW1A'}, ['adf', 'fabric']),\n"
            "    ],\n"
            "    nested_schema,\n"
            ")\n"
            "nested.select('id', 'address.city', 'address.pin').show()\n"
            "nested.select('id', F.explode('skills').alias('skill')).show()"
        ),
        md(
            "## Interview points\n"
            "- Explicit schema stabilizes pipelines.\n"
            "- Use explode / explode_outer for arrays.\n"
            "- dropDuplicates(cols) vs distinct()."
        ),
    ],
    [
        md("# Day 02 Assignment — Schema & Nested Data"),
        *SPARK_SETUP,
        code(
            "events = spark.createDataFrame(\n"
            "    [\n"
            "        ('{\"customer_id\":\"c1\",\"profile\":{\"city\":\"Pune\",\"age\":28},"
            "\"tags\":[\"new\",\"in\"],\"amount\":\"100\"}',),\n"
            "        ('{\"customer_id\":\"c2\",\"profile\":{\"city\":\"Delhi\",\"age\":null},"
            "\"tags\":[\"vip\"],\"amount\":null}',),\n"
            "        ('{\"customer_id\":\"c3\",\"profile\":{\"city\":\"Pune\",\"age\":35},"
            "\"tags\":[],\"amount\":\"250.5\"}',),\n"
            "    ],\n"
            "    ['raw_json'],\n"
            ")\n"
            "events.show(truncate=False)"
        ),
        md(
            "## Tasks\n"
            "### A1. Parse JSON with explicit schema.\n"
            "### A2. Flatten city/age; cast amount; fill null amount with 0.\n"
            "### A3. explode_outer tags.\n"
            "### A4. Why explicit schema over inferSchema in production?"
        ),
        code("# A1"),
        code("# A2"),
        code("# A3"),
        md("### A4\n..."),
    ],
)

add(
    3,
    "Joins, Aggregations, Window Functions",
    "PySpark",
    [
        md(
            "# Day 03 Tutorial — Joins, Aggregations, Windows\n\n"
            "**Goal:** Join types, aggregations, ranking patterns."
        ),
        *SPARK_SETUP,
        code(
            "customers = spark.createDataFrame(\n"
            "    [('c1', 'Alice'), ('c2', 'Bob'), ('c3', 'Carol'), ('c4', 'Dan')],\n"
            "    ['customer_id', 'name'],\n"
            ")\n"
            "orders = spark.createDataFrame(\n"
            "    [\n"
            "        ('o1', 'c1', 100.0, '2024-01-01'),\n"
            "        ('o2', 'c1', 200.0, '2024-01-03'),\n"
            "        ('o3', 'c2', 50.0, '2024-01-02'),\n"
            "        ('o4', 'c5', 80.0, '2024-01-04'),\n"
            "    ],\n"
            "    ['order_id', 'customer_id', 'amount', 'order_date'],\n"
            ")"
        ),
        md("## Joins"),
        code(
            "customers.join(orders, 'customer_id', 'inner').show()\n"
            "customers.join(orders, 'customer_id', 'left').show()\n"
            "customers.join(orders, 'customer_id', 'left_anti').show()\n"
            "customers.join(orders, 'customer_id', 'left_semi').show()"
        ),
        md("## Aggregations and broadcast"),
        code(
            "from pyspark.sql.functions import broadcast\n"
            "orders.groupBy('customer_id').agg(\n"
            "    F.count('*').alias('order_cnt'),\n"
            "    F.sum('amount').alias('total_amount'),\n"
            ").show()\n"
            "broadcast(customers).join(orders, 'customer_id').show()"
        ),
        md("## Windows"),
        code(
            "w = Window.partitionBy('customer_id').orderBy(F.col('order_date').desc())\n"
            "orders.withColumn('rn', F.row_number().over(w)).show()\n"
            "orders.withColumn('rn', F.row_number().over(w)).filter(F.col('rn') == 1).show()"
        ),
        md(
            "## Interview\n"
            "- Broadcast small dimensions.\n"
            "- row_number vs rank vs dense_rank."
        ),
    ],
    [
        md("# Day 03 Assignment — Joins & Windows"),
        *SPARK_SETUP,
        code(
            "sales = spark.createDataFrame(\n"
            "    [\n"
            "        ('c1', '2024-01-01', 100),\n"
            "        ('c1', '2024-01-02', 150),\n"
            "        ('c1', '2024-01-02', 150),\n"
            "        ('c2', '2024-01-01', 80),\n"
            "        ('c2', '2024-01-03', 200),\n"
            "        ('c3', '2024-01-01', 50),\n"
            "    ],\n"
            "    ['customer_id', 'sale_date', 'amount'],\n"
            ")\n"
            "dim = spark.createDataFrame(\n"
            "    [('c1', 'East'), ('c2', 'West'), ('c3', 'East'), ('c4', 'North')],\n"
            "    ['customer_id', 'region'],\n"
            ")"
        ),
        md(
            "## Tasks\n"
            "### A1. Left join + anti join for customers with no sales.\n"
            "### A2. Totals by region.\n"
            "### A3. lag previous amount + day-over-day change.\n"
            "### A4. Dedup customer+date with row_number.\n"
            "### A5. When broadcast dim?"
        ),
        code("# A1"),
        code("# A2"),
        code("# A3"),
        code("# A4"),
        md("### A5\n..."),
    ],
)

add(
    4,
    "Performance Tuning Essentials",
    "PySpark",
    [
        md(
            "# Day 04 Tutorial — Performance Tuning Essentials\n\n"
            "**Goal:** Plans, shuffle, partitioning, cache."
        ),
        *SPARK_SETUP,
        code(
            "df = spark.range(0, 100000).withColumn('grp', (F.col('id') % 10).cast('int'))\n"
            "df2 = (\n"
            "    spark.range(0, 100000)\n"
            "    .withColumnRenamed('id', 'id2')\n"
            "    .withColumn('grp', (F.col('id2') % 10).cast('int'))\n"
            ")"
        ),
        md("## Explain plans"),
        code("df.join(df2, 'grp').explain('formatted')"),
        md("## Repartition vs coalesce"),
        code(
            "print('partitions', df.rdd.getNumPartitions())\n"
            "wide = df.repartition(8, 'grp')\n"
            "narrow = wide.coalesce(2)\n"
            "print(wide.rdd.getNumPartitions(), narrow.rdd.getNumPartitions())"
        ),
        md("## Cache"),
        code(
            "base = df.filter(F.col('id') < 50000)\n"
            "base.cache()\n"
            "print(base.count())\n"
            "print(base.groupBy('grp').count().count())\n"
            "base.unpersist()"
        ),
        md(
            "## Interview checklist\n"
            "- Shuffle is expensive network movement.\n"
            "- repartition shuffles; coalesce mainly reduces partitions.\n"
            "- Debug via Spark UI, explain, skew/spill signals."
        ),
    ],
    [
        md("# Day 04 Assignment — Tuning Scenarios"),
        *SPARK_SETUP,
        md(
            "## Tasks\n"
            "### A1. Join two DataFrames; note Exchange in explain.\n"
            "### A2. Broadcast join plan comparison.\n"
            "### A3. Partition counts after repartition/coalesce.\n"
            "### A4. How do you debug a slow Spark job?\n"
            "### A5. One skew example + mitigation."
        ),
        code("# A1"),
        code("# A2"),
        code("# A3"),
        md("### A4\n...\n\n### A5\n..."),
    ],
)

add(
    5,
    "File Formats, Partitioning & Delta Basics",
    "PySpark",
    [
        md(
            "# Day 05 Tutorial — File Formats, Partitioning & Delta Basics\n\n"
            "**Goal:** Parquet partitions and Delta MERGE concepts.\n\n"
            "> Delta needs delta-spark locally; built-in on Databricks/Fabric."
        ),
        *SPARK_SETUP,
        code(
            "from pathlib import Path\n"
            "out = Path('data/day05')\n"
            "out.mkdir(parents=True, exist_ok=True)\n"
            "df = spark.createDataFrame(\n"
            "    [\n"
            "        (1, 'IN', '2024-01-01', 10.0),\n"
            "        (2, 'IN', '2024-01-01', 20.0),\n"
            "        (3, 'US', '2024-01-02', 15.0),\n"
            "        (4, 'US', '2024-01-02', 25.0),\n"
            "    ],\n"
            "    ['id', 'country', 'dt', 'amount'],\n"
            ")"
        ),
        md("## Partitioned Parquet"),
        code(
            "path = str(out / 'sales_parquet')\n"
            "df.write.mode('overwrite').partitionBy('country', 'dt').parquet(path)\n"
            "spark.read.parquet(path).filter(F.col('country') == 'IN').show()"
        ),
        md("## Delta MERGE pattern (optional)"),
        code(
            "try:\n"
            "    target = str(out / 'customers_delta')\n"
            "    customers = spark.createDataFrame(\n"
            "        [(1, 'Alice', 'Pune'), (2, 'Bob', 'Delhi')], ['id', 'name', 'city']\n"
            "    )\n"
            "    customers.write.format('delta').mode('overwrite').save(target)\n"
            "    updates = spark.createDataFrame(\n"
            "        [(2, 'Bob', 'Mumbai'), (3, 'Carol', 'London')], ['id', 'name', 'city']\n"
            "    )\n"
            "    from delta.tables import DeltaTable\n"
            "    dt = DeltaTable.forPath(spark, target)\n"
            "    (dt.alias('t').merge(updates.alias('s'), 't.id = s.id')\n"
            "      .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())\n"
            "    spark.read.format('delta').load(target).show()\n"
            "except Exception as e:\n"
            "    print('Delta not configured:', type(e).__name__, str(e)[:180])\n"
            "    print('Still learn MERGE for interviews.')"
        ),
        md(
            "## Interview\n"
            "- Parquet over CSV for analytics.\n"
            "- Delta: ACID, time travel, MERGE.\n"
            "- Partition on filter-friendly columns (often date)."
        ),
    ],
    [
        md("# Day 05 Assignment — Formats & Upserts"),
        *SPARK_SETUP,
        md(
            "## Tasks\n"
            "### A1. Write partitioned Parquet by year/month from a date column.\n"
            "### A2. Read with partition filter; optional explain.\n"
            "### A3. Write MERGE pseudocode for SCD1 customer upsert.\n"
            "### A4. Parquet vs CSV; Delta value; OPTIMIZE/Z-ORDER overview."
        ),
        code("# A1"),
        code("# A2"),
        md("### A3\n```sql\n-- MERGE ...\n```\n\n### A4\n..."),
    ],
)

add(
    6,
    "Streaming & Real-World Patterns",
    "PySpark",
    [
        md(
            "# Day 06 Tutorial — Medallion Architecture & SCD Patterns\n\n"
            "**Goal:** Bronze/silver/gold and SCD1; outline SCD2."
        ),
        *SPARK_SETUP,
        md(
            "## Medallion\n"
            "- **Bronze:** raw append\n"
            "- **Silver:** clean/dedupe\n"
            "- **Gold:** business marts"
        ),
        code(
            "bronze = spark.createDataFrame(\n"
            "    [\n"
            "        (1, 'Alice', 'Pune', '2024-01-01 10:00:00'),\n"
            "        (1, 'Alice', 'Mumbai', '2024-01-02 09:00:00'),\n"
            "        (2, 'Bob', 'Delhi', '2024-01-01 11:00:00'),\n"
            "        (2, 'Bob', 'Delhi', '2024-01-01 11:00:00'),\n"
            "    ],\n"
            "    ['customer_id', 'name', 'city', 'updated_at'],\n"
            ")\n"
            "w = Window.partitionBy('customer_id').orderBy(F.col('updated_at').desc())\n"
            "silver = bronze.withColumn('rn', F.row_number().over(w)).filter('rn = 1').drop('rn')\n"
            "silver.show()"
        ),
        md("## SCD Type 1"),
        code(
            "dim = spark.createDataFrame(\n"
            "    [(1, 'Alice', 'Pune'), (2, 'Bob', 'Delhi')],\n"
            "    ['customer_id', 'name', 'city'],\n"
            ")\n"
            "scd1 = (\n"
            "    dim.alias('d').join(silver.alias('s'), 'customer_id', 'left')\n"
            "    .select(\n"
            "        F.col('customer_id'),\n"
            "        F.coalesce('s.name', 'd.name').alias('name'),\n"
            "        F.coalesce('s.city', 'd.city').alias('city'),\n"
            "    )\n"
            ")\n"
            "scd1.show()"
        ),
        md(
            "## SCD2 outline\n"
            "Use valid_from, valid_to, is_current. Close old row on change; insert new current row.\n\n"
            "## Streaming concepts\n"
            "Source -> transform -> sink + checkpoint; watermark for late data bounds."
        ),
    ],
    [
        md("# Day 06 Assignment — Medallion + SCD"),
        *SPARK_SETUP,
        md(
            "## Tasks\n"
            "### A1. Bronze->silver orders dedupe by order_id latest ingest_ts.\n"
            "### A2. Gold daily revenue by country.\n"
            "### A3. Build SCD2 sample history DataFrame for city changes.\n"
            "### A4. Batch vs streaming use cases.\n"
            "### A5. How to make loads idempotent?"
        ),
        code("# A1"),
        code("# A2"),
        code("# A3"),
        md("### A4\n...\n\n### A5\n..."),
    ],
)

add(
    7,
    "Week 1 Review + PySpark Mock",
    "PySpark",
    [
        md(
            "# Day 07 Tutorial — Week 1 Review Lab\n\n"
            "**Goal:** Rehearse highest-frequency PySpark patterns."
        ),
        *SPARK_SETUP,
        code(
            "emp = spark.createDataFrame(\n"
            "    [\n"
            "        (1, 'Alice', 'Sales', 70000, '2020-01-01'),\n"
            "        (2, 'Bob', 'Sales', 70000, '2019-06-01'),\n"
            "        (3, 'Carol', 'Eng', 90000, '2021-03-01'),\n"
            "        (4, 'Dan', 'Eng', 95000, '2018-01-01'),\n"
            "        (5, 'Eve', 'Eng', 90000, '2022-01-01'),\n"
            "    ],\n"
            "    ['id', 'name', 'dept', 'salary', 'hire_date'],\n"
            ")\n"
            "w = Window.partitionBy('dept').orderBy(F.col('salary').desc(), F.col('hire_date').asc())\n"
            "emp.withColumn('rn', F.row_number().over(w)).filter(F.col('rn') <= 2).show()\n"
            "w_avg = Window.partitionBy('dept')\n"
            "emp.withColumn('dept_avg', F.avg('salary').over(w_avg))"
            ".filter(F.col('salary') > F.col('dept_avg')).show()"
        ),
        md(
            "## Mock structure (45 min)\n"
            "1. Spark job lifecycle\n"
            "2. Coding: latest record per key\n"
            "3. Coding: join + KPI\n"
            "4. Performance debugging scenario"
        ),
    ],
    [
        md("# Day 07 Assignment — PySpark Mock Exam\n\nTimed **60 minutes**."),
        *SPARK_SETUP,
        code(
            "clicks = spark.createDataFrame(\n"
            "    [\n"
            "        ('u1', '2024-01-01 10:00:00', 'home'),\n"
            "        ('u1', '2024-01-01 10:05:00', 'product'),\n"
            "        ('u1', '2024-01-01 10:05:00', 'product'),\n"
            "        ('u2', '2024-01-01 11:00:00', 'home'),\n"
            "        ('u2', '2024-01-02 09:00:00', 'checkout'),\n"
            "        ('u3', '2024-01-02 12:00:00', 'home'),\n"
            "    ],\n"
            "    ['user_id', 'event_ts', 'page'],\n"
            ")\n"
            "users = spark.createDataFrame(\n"
            "    [('u1', 'IN'), ('u2', 'US'), ('u3', 'IN'), ('u4', 'UK')],\n"
            "    ['user_id', 'country'],\n"
            ")"
        ),
        md(
            "## Exam\n"
            "### Q1. Deduplicate user+timestamp+page.\n"
            "### Q2. Last event per user.\n"
            "### Q3. Distinct active users by country.\n"
            "### Q4. Users with zero events.\n"
            "### Q5. Design bronze/silver/gold for clickstream on Azure."
        ),
        code("# Q1"),
        code("# Q2"),
        code("# Q3"),
        code("# Q4"),
        md("### Q5\n..."),
    ],
)


# ========================= WEEK 2: SQL + ADF =========================

add(
    8,
    "SQL Foundations for Data Engineers",
    "SQL",
    [
        md(
            "# Day 08 Tutorial — SQL Foundations\n\n"
            "**Goal:** SELECT/JOIN/CTE skills with Spark SQL."
        ),
        *SPARK_SETUP,
        code(
            "spark.createDataFrame(\n"
            "    [(1, 'Alice', 10), (2, 'Bob', 20), (3, 'Carol', 10), (4, 'Dan', None)],\n"
            "    ['emp_id', 'name', 'dept_id'],\n"
            ").createOrReplaceTempView('employees')\n"
            "spark.createDataFrame(\n"
            "    [(10, 'Sales'), (20, 'Eng'), (30, 'HR')],\n"
            "    ['dept_id', 'dept_name'],\n"
            ").createOrReplaceTempView('departments')\n"
            "spark.createDataFrame(\n"
            "    [(1, 100), (1, 200), (2, 50), (4, 80)],\n"
            "    ['emp_id', 'amount'],\n"
            ").createOrReplaceTempView('sales')"
        ),
        sql_cell(
            "SELECT e.name, d.dept_name, s.amount\n"
            "FROM employees e\n"
            "LEFT JOIN departments d ON e.dept_id = d.dept_id\n"
            "LEFT JOIN sales s ON e.emp_id = s.emp_id\n"
            "ORDER BY e.name"
        ),
        sql_cell(
            "WITH totals AS (\n"
            "  SELECT emp_id, SUM(amount) AS total_amount\n"
            "  FROM sales\n"
            "  GROUP BY emp_id\n"
            "  HAVING SUM(amount) >= 100\n"
            ")\n"
            "SELECT e.name, t.total_amount\n"
            "FROM totals t\n"
            "JOIN employees e ON e.emp_id = t.emp_id"
        ),
        md(
            "## Interview\n"
            "- WHERE filters rows; HAVING filters groups.\n"
            "- LEFT JOIN keeps unmatched left rows.\n"
            "- Prefer CTEs for clarity."
        ),
    ],
    [
        md("# Day 08 Assignment — SQL Foundations"),
        *SPARK_SETUP,
        code(
            "spark.createDataFrame(\n"
            "    [\n"
            "        (101, 'c1', 'IN', 100, '2024-01-01'),\n"
            "        (102, 'c1', 'IN', 200, '2024-01-02'),\n"
            "        (103, 'c2', 'US', 50, '2024-01-01'),\n"
            "        (104, 'c3', 'US', 500, '2024-01-03'),\n"
            "        (105, 'c2', 'US', 70, '2024-01-04'),\n"
            "    ],\n"
            "    ['order_id', 'customer_id', 'country', 'amount', 'order_date'],\n"
            ").createOrReplaceTempView('orders')"
        ),
        md(
            "## Tasks\n"
            "### A1. Total amount and count by country.\n"
            "### A2. Customers with total >= 200 (CTE + HAVING).\n"
            "### A3. Orders above overall average amount.\n"
            "### A4. WHERE vs HAVING example."
        ),
        code("spark.sql('''\n-- A1\n''').show()"),
        code("spark.sql('''\n-- A2\n''').show()"),
        code("spark.sql('''\n-- A3\n''').show()"),
        md("### A4\n..."),
    ],
)

add(
    9,
    "Advanced SQL — Windows & Analytics",
    "SQL",
    [
        md("# Day 09 Tutorial — Advanced SQL Windows & Analytics"),
        *SPARK_SETUP,
        code(
            "spark.createDataFrame(\n"
            "    [\n"
            "        ('c1', '2024-01-01', 100),\n"
            "        ('c1', '2024-01-02', 150),\n"
            "        ('c1', '2024-01-03', 120),\n"
            "        ('c2', '2024-01-01', 80),\n"
            "        ('c2', '2024-01-02', 90),\n"
            "    ],\n"
            "    ['customer_id', 'dt', 'amount'],\n"
            ").createOrReplaceTempView('daily_sales')"
        ),
        sql_cell(
            "SELECT customer_id, dt, amount,\n"
            "       LAG(amount) OVER (PARTITION BY customer_id ORDER BY dt) AS prev_amount,\n"
            "       SUM(amount) OVER (PARTITION BY customer_id ORDER BY dt\n"
            "         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,\n"
            "       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS amount_rn\n"
            "FROM daily_sales\n"
            "ORDER BY customer_id, dt"
        ),
        sql_cell(
            "WITH ranked AS (\n"
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY dt DESC) AS rn\n"
            "  FROM daily_sales\n"
            ")\n"
            "SELECT customer_id, dt, amount FROM ranked WHERE rn = 1"
        ),
    ],
    [
        md("# Day 09 Assignment — Analytical SQL"),
        *SPARK_SETUP,
        code(
            "spark.createDataFrame(\n"
            "    [\n"
            "        ('e1', 'Sales', 70000),\n"
            "        ('e2', 'Sales', 80000),\n"
            "        ('e3', 'Sales', 80000),\n"
            "        ('e4', 'Eng', 90000),\n"
            "        ('e5', 'Eng', 95000),\n"
            "        ('e6', 'Eng', 85000),\n"
            "    ],\n"
            "    ['emp_id', 'dept', 'salary'],\n"
            ").createOrReplaceTempView('emp')"
        ),
        md(
            "## Tasks\n"
            "### A1. Employees above dept average.\n"
            "### A2. RANK and DENSE_RANK by dept salary.\n"
            "### A3. Running total of salary by dept.\n"
            "### A4. LAG month-over-month style change on your sample.\n"
            "### A5. RANK vs DENSE_RANK vs ROW_NUMBER."
        ),
        code("# A1"),
        code("# A2"),
        code("# A3"),
        code("# A4"),
        md("### A5\n..."),
    ],
)

add(
    10,
    "SQL Performance, Indexing & Data Modeling",
    "SQL",
    [
        md(
            "# Day 10 Tutorial — Data Modeling for Analytics\n\n"
            "**Goal:** Star schema, facts/dims, SCD interview fluency."
        ),
        md(
            "## Star schema\n"
            "- Fact = events + measures + FKs\n"
            "- Dimensions = descriptive context\n"
            "- SCD1 overwrite; SCD2 history with validity columns"
        ),
        *SPARK_SETUP,
        code(
            "spark.createDataFrame([(1, 'Alice', 'East'), (2, 'Bob', 'West')],\n"
            "    ['customer_key', 'customer_name', 'region']).createOrReplaceTempView('dim_customer')\n"
            "spark.createDataFrame([(10, 'Tea', 'Beverages'), (20, 'Mug', 'Home')],\n"
            "    ['product_key', 'product_name', 'category']).createOrReplaceTempView('dim_product')\n"
            "spark.createDataFrame(\n"
            "    [(100, 1, 10, 20240101, 2, 40.0), (101, 2, 20, 20240101, 1, 15.0),\n"
            "     (102, 1, 20, 20240102, 3, 45.0)],\n"
            "    ['sales_key', 'customer_key', 'product_key', 'date_key', 'qty', 'amount']\n"
            ").createOrReplaceTempView('fact_sales')"
        ),
        sql_cell(
            "SELECT c.region, p.category, SUM(f.amount) AS revenue\n"
            "FROM fact_sales f\n"
            "JOIN dim_customer c ON f.customer_key = c.customer_key\n"
            "JOIN dim_product p ON f.product_key = p.product_key\n"
            "GROUP BY c.region, p.category\n"
            "ORDER BY revenue DESC"
        ),
    ],
    [
        md("# Day 10 Assignment — Model a Retail Star Schema"),
        *SPARK_SETUP,
        md(
            "## Tasks\n"
            "### A1. Design fact_sales + dims (columns, PK/FK).\n"
            "### A2. Implement sample tables as temp views.\n"
            "### A3. SQL: top products; revenue by store/month.\n"
            "### A4. SCD1 vs SCD2 attribute choices.\n"
            "### A5. Approach for a slow SQL query."
        ),
        code("# A2"),
        code("# A3"),
        md("### A1 / A4 / A5\n..."),
    ],
)

add(
    11,
    "T-SQL / Azure SQL Practical Patterns",
    "SQL",
    [
        md(
            "# Day 11 Tutorial — Incremental Load & MERGE Patterns\n\n"
            "**Goal:** Watermark extracts and upsert thinking."
        ),
        *SPARK_SETUP,
        code(
            "spark.createDataFrame(\n"
            "    [(1, 'Alice', 'Pune', '2024-01-01'), (2, 'Bob', 'Delhi', '2024-01-01')],\n"
            "    ['id', 'name', 'city', 'last_updated'],\n"
            ").createOrReplaceTempView('dim_customer_tgt')\n"
            "spark.createDataFrame(\n"
            "    [(2, 'Bob', 'Mumbai', '2024-01-05'), (3, 'Carol', 'London', '2024-01-05')],\n"
            "    ['id', 'name', 'city', 'last_updated'],\n"
            ").createOrReplaceTempView('dim_customer_src')\n"
            "watermark = '2024-01-01'\n"
            "spark.sql(\n"
            "    \"SELECT * FROM dim_customer_src WHERE last_updated > '{0}'\".format(watermark)\n"
            ").show()"
        ),
        md("## Upsert pattern without Delta"),
        code(
            "src = spark.table('dim_customer_src')\n"
            "tgt = spark.table('dim_customer_tgt')\n"
            "unchanged = tgt.join(src, 'id', 'left_anti')\n"
            "upserted = unchanged.unionByName(src)\n"
            "upserted.orderBy('id').show()\n"
            "print('In Azure SQL / Delta interviews, describe MERGE for this upsert.')"
        ),
        md(
            "## Interview patterns\n"
            "- Full vs incremental\n"
            "- Idempotent reruns\n"
            "- MERGE key pitfalls (duplicates in source)"
        ),
    ],
    [
        md("# Day 11 Assignment — Incremental + Idempotent Loads"),
        *SPARK_SETUP,
        md(
            "## Tasks\n"
            "### A1. Watermark extract using control table etl_watermark.\n"
            "### A2. Upsert customers.\n"
            "### A3. Soft-delete with is_deleted.\n"
            "### A4. Full vs incremental; restart-safe; MERGE pitfalls."
        ),
        code("# A1"),
        code("# A2"),
        code("# A3"),
        md("### A4\n..."),
    ],
)

add(
    12,
    "ADF Fundamentals",
    "ADF",
    [
        md(
            "# Day 12 Tutorial — Azure Data Factory Fundamentals\n\n"
            "**Goal:** ADF building blocks + parameterized copy design.\n\n"
            "> Cloud UI/ARM service — this notebook trains design fluency for interviews."
        ),
        md(
            "## Core components\n\n"
            "| Component | Purpose |\n"
            "|-----------|---------|\n"
            "| Linked Service | Connection/auth |\n"
            "| Dataset | Path/table shape over a linked service |\n"
            "| Pipeline | Activity orchestration |\n"
            "| Activity | Unit of work |\n"
            "| Integration Runtime | Compute/network bridge |\n"
            "| Trigger | Schedule / tumbling / event |"
        ),
        code(
            "import pandas as pd\n"
            "pd.DataFrame([\n"
            "    {\n"
            "        'pipeline': 'pl_copy_sales',\n"
            "        'source_ls': 'ls_adls',\n"
            "        'source_path': 'raw/sales/{yyyy}/{mm}/{dd}/',\n"
            "        'sink_ls': 'ls_azuresql',\n"
            "        'sink_table': 'stg.sales',\n"
            "        'trigger': 'schedule_daily_0200',\n"
            "    }\n"
            "])"
        ),
        md(
            "## Rehearse\n"
            "1. Dataset vs Linked Service\n"
            "2. Azure IR vs Self-hosted IR\n"
            "3. Why parameterize paths/tables"
        ),
    ],
    [
        md("# Day 12 Assignment — ADF Fundamentals Design Pack"),
        md(
            "## Tasks\n"
            "### A1. Component inventory for on-prem SQL -> ADLS -> Azure SQL (SHIR).\n"
            "### A2. Parameter list + example values.\n"
            "### A3. Trigger choice for daily / file arrival / tumbling.\n"
            "### A4. 2-minute explanation script.\n"
            "### A5. Short quiz answers."
        ),
        code("import pandas as pd\n# A1"),
        code("# A2"),
        md("### A3\n...\n\n### A4\n...\n\n### A5\n..."),
    ],
)

add(
    13,
    "ADF Control Flow & Orchestration",
    "ADF",
    [
        md(
            "# Day 13 Tutorial — ADF Control Flow & Orchestration\n\n"
            "**Goal:** Lookup -> ForEach metadata-driven pattern + error handling."
        ),
        md(
            "## Key activities\n"
            "Get Metadata, Lookup, ForEach, If/Switch, Until, Execute Pipeline\n\n"
            "Dependency conditions: Succeeded / Failed / Completed / Skipped"
        ),
        code(
            "config = [\n"
            "    {'src_table': 'dbo.Customers', 'sink_table': 'stg.customers', 'watermark_col': 'ModifiedDate'},\n"
            "    {'src_table': 'dbo.Orders', 'sink_table': 'stg.orders', 'watermark_col': 'OrderDate'},\n"
            "    {'src_table': 'dbo.Products', 'sink_table': 'stg.products', 'watermark_col': 'ModifiedDate'},\n"
            "]\n"
            "for row in config:\n"
            "    print('Copy {0} -> {1} on {2}'.format(row['src_table'], row['sink_table'], row['watermark_col']))"
        ),
        md(
            "## Parent-child pattern\n"
            "Parent Lookup config -> ForEach -> Execute Pipeline child with parameters."
        ),
    ],
    [
        md("# Day 13 Assignment — Metadata-Driven Orchestration"),
        md(
            "## Tasks\n"
            "### A1. Control table schema for 100-table ingestion.\n"
            "### A2. Parent/child pseudo steps.\n"
            "### A3. Retry/timeout strategy for flaky on-prem source.\n"
            "### A4. Partial ForEach failure + alerting.\n"
            "### A5. Notification flow diagram."
        ),
        code("import pandas as pd\n# A1"),
        md("### A2-A5\n..."),
    ],
)

add(
    14,
    "Week 2 Review — SQL + ADF Mock",
    "SQL+ADF",
    [
        md("# Day 14 Tutorial — SQL + ADF Review Lab"),
        *SPARK_SETUP,
        code(
            "spark.createDataFrame(\n"
            "    [(1, 'c1', 100, '2024-01-01'), (2, 'c1', 200, '2024-01-02'), (3, 'c2', 50, '2024-01-01')],\n"
            "    ['order_id', 'customer_id', 'amount', 'dt'],\n"
            ").createOrReplaceTempView('orders')"
        ),
        sql_cell(
            "WITH daily AS (\n"
            "  SELECT customer_id, dt, SUM(amount) AS amt\n"
            "  FROM orders GROUP BY customer_id, dt\n"
            ")\n"
            "SELECT *, LAG(amt) OVER (PARTITION BY customer_id ORDER BY dt) AS prev_amt\n"
            "FROM daily"
        ),
        md(
            "## ADF rapid-fire checklist\n"
            "Linked service, dataset, IR, trigger, parameters, Lookup, ForEach, Copy, monitoring"
        ),
    ],
    [
        md("# Day 14 Assignment — Midterm Mock (SQL + ADF)\n\n**Timed 60 minutes**"),
        *SPARK_SETUP,
        md(
            "## Part A SQL\n"
            "1. Star schema revenue by region/category\n"
            "2. Latest order per customer\n"
            "3. Incremental watermark select\n\n"
            "## Part B ADF\n"
            "50 on-prem tables -> ADLS bronze -> Azure SQL gold: components, metadata, graph, IR, errors, 2-min script"
        ),
        code("# Part A"),
        md("### Part B\n..."),
    ],
)


# ========================= WEEK 3: ADF =========================

add(
    15,
    "Copy Activity Mastery",
    "ADF",
    [
        md(
            "# Day 15 Tutorial — ADF Copy Activity Mastery\n\n"
            "**Goal:** Incremental copy, schema drift, folder partitions."
        ),
        md(
            "## Incremental patterns\n"
            "1. Watermark column\n"
            "2. Date folder partitions\n"
            "3. CDC / change tracking\n\n"
            "Performance levers: parallel copy, DIUs, partitioned reads, staging"
        ),
        code(
            "import pandas as pd\n"
            "from datetime import datetime, timedelta\n"
            "control = pd.DataFrame([{'pipeline': 'sales_copy', 'watermark': '2024-01-07 00:00:00'}])\n"
            "source_rows = pd.DataFrame([\n"
            "    {'id': 1, 'modified': '2024-01-06 10:00:00', 'amount': 10},\n"
            "    {'id': 2, 'modified': '2024-01-07 12:00:00', 'amount': 20},\n"
            "    {'id': 3, 'modified': '2024-01-08 09:00:00', 'amount': 30},\n"
            "])\n"
            "wm = control.loc[0, 'watermark']\n"
            "source_rows[source_rows['modified'] > wm]"
        ),
        code(
            "def partitions_for_range(start, end):\n"
            "    s = datetime.fromisoformat(start)\n"
            "    e = datetime.fromisoformat(end)\n"
            "    out = []\n"
            "    cur = s\n"
            "    while cur <= e:\n"
            "        out.append(cur.strftime('raw/sales/%Y/%m/%d/'))\n"
            "        cur += timedelta(days=1)\n"
            "    return out\n\n"
            "partitions_for_range('2024-01-01', '2024-01-03')"
        ),
    ],
    [
        md("# Day 15 Assignment — Incremental Copy Design"),
        md(
            "## Tasks\n"
            "### A1. Watermark control table + post-success update steps.\n"
            "### A2. Late file reprocess plan.\n"
            "### A3. Schema drift vs strict mapping.\n"
            "### A4. 200GB extract performance plan.\n"
            "### A5. Full refresh vs delta decision matrix."
        ),
        code("import pandas as pd\n# A1"),
        md("### A2-A5\n..."),
    ],
)

add(
    16,
    "Mapping Data Flows",
    "ADF",
    [
        md(
            "# Day 16 Tutorial — Mapping Data Flows\n\n"
            "**Goal:** Transformations and Data Flow vs Spark choice."
        ),
        md(
            "## Useful transforms\n"
            "Select, Derived Column, Filter, Aggregate, Join, Exists, Surrogate Key, Sink\n\n"
            "| Need | Prefer |\n|------|--------|\n"
            "| Lift-and-shift | Copy |\n"
            "| Light UI transforms | Mapping Data Flow |\n"
            "| Heavy/complex | Databricks/Fabric Spark |"
        ),
        code(
            "flow = {\n"
            "    'source': 'stg_orders',\n"
            "    'steps': [\n"
            "        'Cast + Derived Columns',\n"
            "        'Filter bad rows',\n"
            "        'Join dim_customer',\n"
            "        'Surrogate Key',\n"
            "        'Sink fact_orders',\n"
            "    ],\n"
            "}\n"
            "flow"
        ),
    ],
    [
        md("# Day 16 Assignment — Data Flow Design"),
        md(
            "## Tasks\n"
            "### A1. Data Flow for sales fact.\n"
            "### A2. SCD1 customer update steps.\n"
            "### A3. Data Flow vs Spark for 500GB SCD pipeline.\n"
            "### A4. Debugging bad joins.\n"
            "### A5. Cost/performance talking points."
        ),
        md("### Answers\n..."),
    ],
)

add(
    17,
    "IRs, Security & Connectivity",
    "ADF",
    [
        md(
            "# Day 17 Tutorial — Integration Runtimes, Security & Connectivity\n\n"
            "**Goal:** IR selection + secure connectivity story."
        ),
        md(
            "## IRs\n"
            "- Azure IR: cloud-to-cloud\n"
            "- Self-hosted IR: on-prem / private network\n"
            "- Azure-SSIS IR: SSIS lift\n\n"
            "## Security\n"
            "Managed Identity, Key Vault, Private Endpoints, least privilege"
        ),
        code(
            "import pandas as pd\n"
            "pd.DataFrame([\n"
            "    {'source': 'Azure SQL Public', 'ir': 'Azure IR', 'auth': 'Managed Identity'},\n"
            "    {'source': 'On-prem SQL', 'ir': 'Self-hosted IR', 'auth': 'SQL/Windows + KV'},\n"
            "    {'source': 'ADLS Gen2', 'ir': 'Azure IR', 'auth': 'Managed Identity'},\n"
            "    {'source': 'SAP datacenter', 'ir': 'Self-hosted IR', 'auth': 'KV secret'},\n"
            "])"
        ),
    ],
    [
        md("# Day 17 Assignment — Secure Landing Zone"),
        md(
            "## Tasks\n"
            "### A1. Secure ADF setup inventory.\n"
            "### B. Secrets in Key Vault vs non-secrets.\n"
            "### C. Three SHIR-mandatory examples.\n"
            "### D. Private endpoint explanation.\n"
            "### E. Security anti-patterns."
        ),
        code("import pandas as pd\n# A1"),
        md("### B-E\n..."),
    ],
)

add(
    18,
    "CI/CD, Monitoring & Production Practices",
    "ADF",
    [
        md(
            "# Day 18 Tutorial — CI/CD, Monitoring & Production Practices\n\n"
            "**Goal:** Environments, deployment, monitoring, recovery."
        ),
        md(
            "## Environments\n"
            "Dev -> Test/UAT -> Prod via Git + ARM/Azure DevOps\n\n"
            "## Production qualities\n"
            "Idempotency, retries, concurrency, SLAs, runbooks, alerts"
        ),
        code(
            "import pandas as pd\n"
            "pd.DataFrame([{\n"
            "    'run_id': 'abc-123',\n"
            "    'pipeline': 'pl_sales',\n"
            "    'status': 'Failed',\n"
            "    'activity': 'Copy_Orders',\n"
            "    'error': 'Timeout connecting to SHIR',\n"
            "}])"
        ),
    ],
    [
        md("# Day 18 Assignment — Ops Readiness"),
        md(
            "## Tasks\n"
            "### A1. Recovery runbook for partial copy failure.\n"
            "### A2. Retry policy defaults.\n"
            "### A3. CI/CD promotion sketch.\n"
            "### A4. Three alerting rules.\n"
            "### A5. Idempotent daily sales design."
        ),
        md("### Answers\n..."),
    ],
)

add(
    19,
    "End-to-End ADF Case Study",
    "ADF",
    [
        md(
            "# Day 19 Tutorial — End-to-End Case Study Workshop\n\n"
            "Daily sales files -> validate -> transform -> dims/facts -> control logging."
        ),
        md(
            "## Reference outline\n"
            "1. Ingest to bronze\n"
            "2. Validate metadata/quality\n"
            "3. Transform to silver\n"
            "4. Load dims/facts\n"
            "5. Update watermark + logs\n"
            "6. Alert on failure"
        ),
        code(
            "design = {\n"
            "    'datasets': ['ds_adls_raw_sales', 'ds_sql_stg_sales', 'ds_sql_fact_sales'],\n"
            "    'pipelines': ['pl_ingest_sales', 'pl_transform_sales', 'pl_master_sales'],\n"
            "    'control_tables': ['etl_watermark', 'etl_run_log', 'etl_file_inventory'],\n"
            "    'sla': 'gold ready by 06:00',\n"
            "}\n"
            "design"
        ),
    ],
    [
        md(
            "# Day 19 Assignment — Case Study Pack\n\n"
            "### A1. Architecture diagram\n"
            "### A2. Pipeline list + activity sequence\n"
            "### A3. Incremental logic\n"
            "### A4. Error handling\n"
            "### A5. 2-minute spoken script"
        ),
        md(
            "```mermaid\n"
            "flowchart LR\n"
            "  A[ADLS raw] --> B[ADF ingest]\n"
            "  B --> C[Bronze]\n"
            "  C --> D[Transform]\n"
            "  D --> E[Gold SQL]\n"
            "```"
        ),
        md("### A2-A5\n..."),
    ],
)

add(
    20,
    "Common ADF Interview Scenarios",
    "ADF",
    [
        md(
            "# Day 20 Tutorial — ADF Scenario Drills\n\n"
            "Answer format: Situation -> Design -> Trade-offs -> Failure handling"
        ),
        code(
            "scenarios = [\n"
            "    'metadata-driven 100 tables',\n"
            "    'new files only',\n"
            "    'dims before facts',\n"
            "    'late file reprocess',\n"
            "    '200GB tuning',\n"
            "    'dev/test/prod params',\n"
            "]\n"
            "for i, s in enumerate(scenarios, 1):\n"
            "    print('{0}. {1}'.format(i, s))"
        ),
    ],
    [
        md(
            "# Day 20 Assignment — Six Scenario Writeups\n\n"
            "For each: Design, ADF components, Risks, Monitoring. ~10 min each."
        ),
        md(
            "### 1. Metadata-driven 100 tables\n...\n\n"
            "### 2. New files only\n...\n\n"
            "### 3. Dims then facts\n...\n\n"
            "### 4. Late files\n...\n\n"
            "### 5. Large file tuning\n...\n\n"
            "### 6. Cross-env params\n..."
        ),
    ],
)

add(
    21,
    "Week 3 Review + ADF Mock",
    "ADF",
    [
        md("# Day 21 Tutorial — ADF Week Review"),
        md(
            "## Flashcards\n"
            "1. Linked Service vs Dataset\n"
            "2. Azure IR vs SHIR\n"
            "3. Tumbling vs schedule trigger\n"
            "4. Lookup + ForEach\n"
            "5. Watermark incremental\n"
            "6. Key Vault + Managed Identity\n"
            "7. Data Flow vs Spark\n"
            "8. Idempotency on rerun"
        ),
        code(
            "flash = {\n"
            "    'dataset': 'data shape/path over a linked service',\n"
            "    'shir': 'on-prem or private network connectivity',\n"
            "    'tumbling': 'fixed non-overlapping windows with backlog catchup',\n"
            "}\n"
            "flash"
        ),
    ],
    [
        md(
            "# Day 21 Assignment — ADF Mock Interview Board\n\n"
            "### Round 1 Rapid fire definitions\n"
            "### Round 2 Design metadata-driven platform (200 tables)\n"
            "### Round 3 Debug failed on-prem Copy"
        ),
        md("### Round 1\n...\n\n### Round 2\n...\n\n### Round 3\n..."),
    ],
)


# ========================= WEEK 4: FABRIC =========================

add(
    22,
    "Fabric Landscape & OneLake",
    "Fabric",
    [
        md(
            "# Day 22 Tutorial — Microsoft Fabric Landscape & OneLake\n\n"
            "**Goal:** Explain Fabric + map classic Azure DE to Fabric items."
        ),
        md(
            "## Fabric in one minute\n"
            "Unified analytics around **OneLake**: lakehouse, warehouse, pipelines, notebooks, real-time, Power BI.\n\n"
            "Key terms: Capacity, Workspace, OneLake, Items"
        ),
        code(
            "import pandas as pd\n"
            "pd.DataFrame([\n"
            "    {'classic_azure': 'ADLS Gen2', 'fabric': 'OneLake'},\n"
            "    {'classic_azure': 'Databricks/Spark', 'fabric': 'Lakehouse notebooks'},\n"
            "    {'classic_azure': 'ADF', 'fabric': 'Fabric pipelines / Dataflow Gen2'},\n"
            "    {'classic_azure': 'Synapse dedicated SQL', 'fabric': 'Fabric Warehouse'},\n"
            "    {'classic_azure': 'Power BI + separate lake', 'fabric': 'Direct Lake on OneLake'},\n"
            "])"
        ),
    ],
    [
        md("# Day 22 Assignment — Fabric Positioning"),
        md(
            "## Tasks\n"
            "### A1. Architecture for mid-size retailer.\n"
            "### A2. Map 8 classic services to Fabric.\n"
            "### A3. 90-second What is Fabric pitch.\n"
            "### A4. When still recommend Databricks + ADF?\n"
            "### A5. OneLake in plain English."
        ),
        code("import pandas as pd\n# A2"),
        md("### A1, A3-A5\n..."),
    ],
)

add(
    23,
    "Lakehouse in Fabric",
    "Fabric",
    [
        md(
            "# Day 23 Tutorial — Fabric Lakehouse\n\n"
            "**Goal:** Medallion in Lakehouse, shortcuts, Delta tables."
        ),
        md(
            "## Concepts\n"
            "- Lakehouse = files + Delta tables + Spark/SQL\n"
            "- Shortcuts avoid full data copies\n"
            "- Medallion bronze/silver/gold tables"
        ),
        *SPARK_SETUP,
        code(
            "bronze = spark.createDataFrame(\n"
            "    [(1, 'c1', '100', '2024-01-01'), (2, 'c1', 'x', '2024-01-01'), (3, 'c2', '50', '2024-01-02')],\n"
            "    ['order_id', 'customer_id', 'amount_str', 'dt'],\n"
            ")\n"
            "silver = bronze.filter(F.col('amount_str').rlike(r'^\\d+(\\.\\d+)?$')).withColumn(\n"
            "    'amount', F.col('amount_str').cast('double')\n"
            ")\n"
            "gold = silver.groupBy('dt').agg(F.sum('amount').alias('revenue'))\n"
            "silver.show()\n"
            "gold.show()"
        ),
    ],
    [
        md("# Day 23 Assignment — Lakehouse Design + Transform"),
        *SPARK_SETUP,
        md(
            "## Tasks\n"
            "### A1. Lakehouse layout for clickstream.\n"
            "### A2. Implement bronze->silver->gold in Spark.\n"
            "### A3. Three shortcut use cases.\n"
            "### A4. Lakehouse vs Warehouse decision.\n"
            "### A5. Delta reliability talking points."
        ),
        code("# A2"),
        md("### A1, A3-A5\n..."),
    ],
)

add(
    24,
    "Fabric Warehouse & SQL Analytics",
    "Fabric",
    [
        md(
            "# Day 24 Tutorial — Fabric Warehouse & SQL Analytics\n\n"
            "**Goal:** Star schema + Direct Lake awareness."
        ),
        md(
            "## Warehouse + Power BI modes\n"
            "- Import / DirectQuery / **Direct Lake**\n"
            "- Workspace roles and RLS awareness"
        ),
        *SPARK_SETUP,
        code(
            "spark.createDataFrame([(1, 'East'), (2, 'West')], ['customer_key', 'region'])" 
            ".createOrReplaceTempView('dim_customer')\n"
            "spark.createDataFrame([(1, 1, 20240101, 50.0)],\n"
            "    ['sales_key', 'customer_key', 'date_key', 'amount']).createOrReplaceTempView('fact_sales')"
        ),
        sql_cell(
            "SELECT c.region, SUM(f.amount) AS revenue\n"
            "FROM fact_sales f\n"
            "JOIN dim_customer c ON f.customer_key = c.customer_key\n"
            "GROUP BY c.region"
        ),
    ],
    [
        md("# Day 24 Assignment — Warehouse Modeling"),
        *SPARK_SETUP,
        md(
            "## Tasks\n"
            "### A1. Dims/facts for subscriptions business.\n"
            "### A2. Sample tables + 5 analytical SQL queries.\n"
            "### A3. Explain Direct Lake.\n"
            "### A4. Lakehouse vs Warehouse choices.\n"
            "### A5. Regional RLS approach."
        ),
        code("# A2"),
        md("### A1, A3-A5\n..."),
    ],
)

add(
    25,
    "Fabric Data Pipelines & Dataflows",
    "Fabric",
    [
        md(
            "# Day 25 Tutorial — Fabric Pipelines & Dataflow Gen2\n\n"
            "**Goal:** Transfer ADF skills; choose Dataflow Gen2 vs notebooks."
        ),
        md(
            "## Transfer\n"
            "Fabric pipelines ~= ADF concepts.\n"
            "Dataflow Gen2 = low-code Power Query style.\n"
            "Notebooks for heavier engineering."
        ),
        code(
            "orchestration = {\n"
            "    'pipeline': 'pl_retail_daily',\n"
            "    'activities': [\n"
            "        'Land files to lakehouse bronze',\n"
            "        'Notebook silver transform',\n"
            "        'SQL/Notebook gold load',\n"
            "        'Optional semantic model refresh',\n"
            "    ],\n"
            "}\n"
            "orchestration"
        ),
    ],
    [
        md("# Day 25 Assignment — Fabric Orchestration Design"),
        md(
            "## Tasks\n"
            "### A1. Pipeline design land -> notebook -> warehouse.\n"
            "### A2. Dataflow Gen2 vs notebook for 3 examples.\n"
            "### A3. ADF -> Fabric concept mapping table.\n"
            "### A4. Schedule + monitoring plan.\n"
            "### A5. 2-minute ADF-to-Fabric talk."
        ),
        code("import pandas as pd\n# A3"),
        md("### A1, A2, A4, A5\n..."),
    ],
)

add(
    26,
    "Real-Time, Governance & Fabric Ecosystem",
    "Fabric",
    [
        md(
            "# Day 26 Tutorial — Governance, Real-Time Awareness & Cost\n\n"
            "**Goal:** Governance, Eventstream overview, capacity awareness."
        ),
        md(
            "## Topics\n"
            "- Eventstream / Real-Time Intelligence overview\n"
            "- Workspaces, domains, endorsement, lineage\n"
            "- Shortcuts to avoid copies\n"
            "- Capacity monitoring\n"
            "- Dev/Test/Prod deployment"
        ),
        code(
            "checklist = [\n"
            "    'Workspace naming standards',\n"
            "    'Prod least privilege',\n"
            "    'Endorse certified gold tables',\n"
            "    'Lineage for critical reports',\n"
            "    'No secrets in notebooks',\n"
            "    'Capacity monitoring plan',\n"
            "    'Dev/Test/Prod path',\n"
            "]\n"
            "for i, item in enumerate(checklist, 1):\n"
            "    print('{0}. {1}'.format(i, item))"
        ),
    ],
    [
        md("# Day 26 Assignment — Governance Pack"),
        md(
            "## Tasks\n"
            "### A1. Governance checklist with owners.\n"
            "### A2. ADLS -> Fabric via shortcut architecture.\n"
            "### A3. Dev/Test/Prod workspace strategy.\n"
            "### A4. Five cost-control bullets.\n"
            "### A5. Eventstream vs batch choice."
        ),
        md("### Answers\n..."),
    ],
)

add(
    27,
    "End-to-End Fabric Case Study",
    "Fabric",
    [
        md(
            "# Day 27 Tutorial — Fabric Migration Case Study\n\n"
            "Retail migrates from ADF + ADLS + SQL to Fabric."
        ),
        md(
            "## Target story\n"
            "OneLake landing/shortcuts -> Lakehouse medallion -> Warehouse/SQL gold -> "
            "Fabric pipelines -> Power BI Direct Lake -> security + monitoring"
        ),
        code(
            "import pandas as pd\n"
            "pd.DataFrame([\n"
            "    {'wave': 1, 'scope': 'Bronze + one domain gold'},\n"
            "    {'wave': 2, 'scope': 'ADF orchestration to Fabric pipelines'},\n"
            "    {'wave': 3, 'scope': 'BI cutover to Direct Lake'},\n"
            "    {'wave': 4, 'scope': 'Decommission redundant copies'},\n"
            "])"
        ),
    ],
    [
        md(
            "# Day 27 Assignment — Migration Deliverables\n\n"
            "### A1 Architecture\n### A2 Medallion plan\n### A3 Pipelines\n"
            "### A4 Power BI plan\n### A5 Security/monitoring\n### A6 3-minute script"
        ),
        md(
            "```mermaid\n"
            "flowchart TB\n"
            "  ADLS[Existing ADLS] -->|shortcut| OL[OneLake]\n"
            "  OL --> LH[Lakehouse medallion]\n"
            "  LH --> WH[Warehouse marts]\n"
            "  LH --> PBI[Power BI Direct Lake]\n"
            "  PIP[Fabric pipelines] --> LH\n"
            "```"
        ),
        md("### A2-A6\n..."),
    ],
)

add(
    28,
    "Week 4 Review + Fabric Mock",
    "Fabric",
    [
        md("# Day 28 Tutorial — Fabric Review Cards"),
        code(
            "import pandas as pd\n"
            "pd.DataFrame([\n"
            "    {'scenario': 'Heavy streaming + advanced ML', 'lean': 'Often Databricks / careful Fabric design'},\n"
            "    {'scenario': 'Unified BI + lakehouse domains', 'lean': 'Fabric strong fit'},\n"
            "    {'scenario': 'Existing ADF + SHIR estate', 'lean': 'ADF remains; hybrid possible'},\n"
            "])"
        ),
        md(
            "## Terms\n"
            "OneLake, Lakehouse, Warehouse, Shortcut, Direct Lake, Pipeline, Dataflow Gen2, Capacity, Workspace roles"
        ),
    ],
    [
        md(
            "# Day 28 Assignment — Fabric Mock Board\n\n"
            "### Q1 Explain Fabric to hiring manager\n"
            "### Q2 Design lakehouse for 3 domains\n"
            "### Q3 Migration waves/risks/rollback\n"
            "### Q4 Rapid fire 10 terms"
        ),
        md("### Answers\n..."),
    ],
)


# ========================= FINAL =========================

add(
    29,
    "Full Stack Revision",
    "Revision",
    [
        md("# Day 29 Tutorial — Full Stack Rapid Revision"),
        *SPARK_SETUP,
        md("## Drill 1 — latest record"),
        code(
            "df = spark.createDataFrame(\n"
            "    [('c1', '2024-01-01', 10), ('c1', '2024-01-03', 20), ('c2', '2024-01-02', 5)],\n"
            "    ['customer_id', 'dt', 'amt'],\n"
            ")\n"
            "w = Window.partitionBy('customer_id').orderBy(F.col('dt').desc())\n"
            "df.withColumn('rn', F.row_number().over(w)).filter('rn = 1').show()"
        ),
        md("## Drill 2 — running total"),
        code(
            "df.createOrReplaceTempView('t')"
        ),
        sql_cell(
            "SELECT customer_id, dt, amt,\n"
            "       SUM(amt) OVER (PARTITION BY customer_id ORDER BY dt) AS running_amt\n"
            "FROM t"
        ),
        md(
            "## Drill 3 — speak 2 minutes each\n"
            "1. Medallion on Azure\n"
            "2. ADF metadata-driven ingestion\n"
            "3. Fabric OneLake + Direct Lake"
        ),
    ],
    [
        md(
            "# Day 29 Assignment — 40-Question Blitz + Top 20 Sheet\n\n"
            "Write short answers; then compress into a Top 20 cheat sheet."
        ),
        md(
            "## Architecture (10)\n"
            "Medallion, star schema, SCD1/2, idempotent load, watermark, OneLake, "
            "Lakehouse vs Warehouse, SHIR, bronze vs gold quality, batch vs streaming\n\n"
            "## Coding (10)\n"
            "Latest dedupe, broadcast join, anti join, running total, MERGE, LAG MoM, "
            "top N, explode JSON, repartition vs coalesce, fact grain\n\n"
            "## Scenarios (10)\n"
            "Late files, skew, ForEach partial fail, schema drift, cost spike, duplicate facts, "
            "CDC vs watermark, Fabric migration, Key Vault, restart after partial load\n\n"
            "## Behavioral (10)\n"
            "Intro, hardest pipeline, conflict, mistake, perf win, mentoring, priorities, "
            "stakeholder comms, learning plan, questions for interviewer"
        ),
        md("### Your answers\n...\n\n### Top 20 cheat sheet\n..."),
    ],
)

add(
    30,
    "Final Mocks + Weak Spot Fix",
    "Revision",
    [
        md(
            "# Day 30 Tutorial — Final Interview Day Prep\n\n"
            "## Mocks\n"
            "- Mock A: PySpark + SQL\n"
            "- Mock B: ADF + Fabric\n\n"
            "## STAR stories\n"
            "1. Performance win\n"
            "2. Incident/failure\n"
            "3. Tool-choice trade-off\n\n"
            "## Intro template\n"
            "I'm a data engineer focused on Azure analytics. I build ingestion/transform "
            "pipelines with SQL/PySpark, orchestrate with ADF/Fabric, and model data with "
            "medallion + star schema patterns. Recently I ..."
        ),
        *SPARK_SETUP,
        code(
            "data = spark.createDataFrame(\n"
            "    [('a', 1, 10), ('a', 2, 5), ('b', 1, 7), ('b', 1, 7)],\n"
            "    ['k', 'day', 'v'],\n"
            ")\n"
            "w = Window.partitionBy('k').orderBy(F.col('day').desc())\n"
            "data.dropDuplicates(['k', 'day', 'v']).withColumn('rn', F.row_number().over(w)).filter('rn=1').show()"
        ),
    ],
    [
        md(
            "# Day 30 Assignment — Final Assessment\n\n"
            "## Mock A (60 min)\n"
            "Lifecycle, PySpark coding, SQL, performance narrative\n\n"
            "## Mock B (60 min)\n"
            "ADF platform design, Fabric design, trade-offs, security/monitoring\n\n"
            "## Closing checklist\n"
            "- [ ] 2-min intro\n"
            "- [ ] Project story\n"
            "- [ ] PySpark optimization story\n"
            "- [ ] SQL modeling story\n"
            "- [ ] Questions for interviewer\n"
            "- [ ] Weakest 3 topics reviewed"
        ),
        *SPARK_SETUP,
        code("# Mock A coding space"),
        md(
            "### Mock A written\n...\n\n### Mock B written\n...\n\n"
            "### STAR stories\n1.\n2.\n3.\n\n### Checklist notes\n..."
        ),
    ],
)


def build_all() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    assert len(DAYS) == 30, "Expected 30 days, found {0}".format(len(DAYS))

    for day, meta in sorted(DAYS.items()):
        write_nb(day, "tutorial", meta["tutorial"])
        write_nb(day, "assignment", meta["assignment"])

    lines = [
        "# Notebooks Index — 30-Day Azure Data Engineering Interview Prep",
        "",
        "Each day has:",
        "- `NN-tutorial.ipynb` — guided lesson",
        "- `NN-assignment.ipynb` — practice / mock tasks",
        "",
        "## How to run",
        "1. Open in Jupyter, VS Code, Databricks, or Fabric.",
        "2. Local PySpark days: `pip install pyspark pandas`.",
        "3. ADF/Fabric days are design-heavy — complete markdown answers.",
        "",
        "| Day | Topic | Tutorial | Assignment |",
        "|-----|-------|----------|------------|",
    ]
    for day, meta in sorted(DAYS.items()):
        lines.append(
            "| {day:02d} | {topic}: {title} | "
            "[tutorial](day-{day:02d}/{day:02d}-tutorial.ipynb) | "
            "[assignment](day-{day:02d}/{day:02d}-assignment.ipynb) |".format(
                day=day, topic=meta["topic"], title=meta["title"]
            )
        )
    lines += [
        "",
        "## Suggested daily flow",
        "1. Complete the tutorial notebook",
        "2. Attempt the assignment without notes",
        "3. Write 5 interview answers from that day",
        "4. Mark doubts for weekly mock review",
        "",
    ]
    (ROOT / "README.md").write_text("\n".join(lines), encoding="utf-8")
    print("Wrote", ROOT / "README.md")
    print("Generated {0} notebooks for {1} days.".format(len(DAYS) * 2, len(DAYS)))


if __name__ == "__main__":
    build_all()
