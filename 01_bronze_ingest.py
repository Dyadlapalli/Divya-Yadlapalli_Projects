# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: metadata-driven ingestion
# MAGIC Loops over every enabled source in `config/pipeline_config.json` and lands it **as-is** in a Bronze Delta table,
# MAGIC adding audit columns. Adding a new source means adding a config entry, not writing a new notebook.
# MAGIC
# MAGIC | Format | How it is read |
# MAGIC |---|---|
# MAGIC | `csv` | Files from the landing volume, all columns as strings |
# MAGIC | `jdbc` | SQL Server table, credentials from a Databricks secret scope |

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F


def secret(ref: str) -> str:
    """Read a secret given as 'scope/key'. Credentials never live in the repo."""
    scope, key = ref.split("/", 1)
    return dbutils.secrets.get(scope, key)


def read_source(src: dict):
    fmt = src["format"]
    if fmt == "csv":
        # Bronze keeps raw values: read everything as string, cast later in Silver.
        return (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .csv(f"{LANDING_PATH}/{src['path']}")
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )
    if fmt == "jdbc":
        j = src["jdbc"]
        return (
            spark.read.format("jdbc")
            .option("url", secret(j["url_secret"]))
            .option("user", secret(j["user_secret"]))
            .option("password", secret(j["password_secret"]))
            .option("dbtable", j["table"])
            .load()
            .withColumn("_source_file", F.lit(j["table"]))
        )
    raise ValueError(f"Unsupported format: {fmt}")


def ingest(src: dict) -> int:
    df = (
        read_source(src)
        .withColumn("_source_system", F.lit(src["source_system"]))
        .withColumn("_ingest_ts", F.current_timestamp())
    )
    target = table("bronze", src["name"])

    # Full loads replace the table; incremental loads append (Silver de-duplicates).
    mode = "append" if src["load_type"] == "incremental" else "overwrite"
    (
        df.write.format("delta")
        .mode(mode)
        .option("mergeSchema", True)
        .saveAsTable(target)
    )
    return df.count()

# COMMAND ----------

results = []
for src in enabled_sources():
    rows = ingest(src)
    results.append((src["name"], src["load_type"], rows))
    print(f"Bronze  {src['name']:<16} {src['load_type']:<12} {rows:>8,} rows")

display(spark.createDataFrame(results, "source string, load_type string, rows long"))
