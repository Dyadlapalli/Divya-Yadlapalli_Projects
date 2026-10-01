# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: clean, conform, de-duplicate
# MAGIC Every rule comes from the config, so all sources go through the same code:
# MAGIC
# MAGIC 1. **Standardize** text (trim, lower/upper case) and turn blanks into nulls
# MAGIC 2. **Cast** columns to the types listed in `columns` (bad values become null instead of failing the job)
# MAGIC 3. **Validate** `required` columns; failing rows go to `silver.<name>_quarantine` with a reason
# MAGIC 4. **De-duplicate** on the `primary_key`, keeping the latest Bronze record
# MAGIC 5. **MERGE** into the Silver Delta table (upsert)

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def standardize(df: DataFrame, src: dict) -> DataFrame:
    for c in df.columns:
        if c.startswith("_"):
            continue
        df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c)))
    for c in src.get("trim", []):
        df = df.withColumn(c, F.trim(F.col(c)))
    for c in src.get("lowercase", []):
        df = df.withColumn(c, F.lower(F.col(c)))
    for c in src.get("uppercase", []):
        df = df.withColumn(c, F.upper(F.col(c)))
    return df


def cast_columns(df: DataFrame, src: dict) -> DataFrame:
    # try_cast returns null for unparseable values, which the validation step then catches
    for c, dtype in src.get("columns", {}).items():
        df = df.withColumn(c, F.expr(f"try_cast(`{c}` AS {dtype})"))
    return df


def validate(df: DataFrame, src: dict):
    required = src.get("required", [])
    if not required:
        return df, df.limit(0).withColumn("_reject_reason", F.lit(None).cast("string"))
    missing = [F.when(F.col(c).isNull(), F.lit(c)) for c in required]
    df = df.withColumn("_missing", F.concat_ws(",", *missing))
    good = df.filter(F.col("_missing") == "").drop("_missing")
    bad = (
        df.filter(F.col("_missing") != "")
        .withColumn("_reject_reason", F.concat(F.lit("missing: "), F.col("_missing")))
        .drop("_missing")
    )
    return good, bad


def deduplicate(df: DataFrame, src: dict) -> DataFrame:
    w = Window.partitionBy(*src["primary_key"]).orderBy(F.col("_ingest_ts").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def merge_into_silver(df: DataFrame, src: dict):
    target = table("silver", src["name"])
    df = df.withColumn("_silver_updated_ts", F.current_timestamp())
    if not table_exists(target):
        df.write.format("delta").saveAsTable(target)
        return
    keys = " AND ".join(f"t.{k} = s.{k}" for k in src["primary_key"])
    (
        DeltaTable.forName(spark, target).alias("t")
        .merge(df.alias("s"), keys)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

summary = []
for src in enabled_sources():
    bronze = spark.table(table("bronze", src["name"]))
    cleaned = cast_columns(standardize(bronze, src), src)
    good, bad = validate(cleaned, src)
    good = deduplicate(good, src)

    merge_into_silver(good, src)
    bad.write.format("delta").mode("overwrite").option("mergeSchema", True) \
        .saveAsTable(table("silver", f"{src['name']}_quarantine"))

    summary.append((src["name"], bronze.count(), good.count(), bad.count()))
    print(f"Silver  {src['name']:<16} bronze={summary[-1][1]:>7,}  silver={summary[-1][2]:>7,}  quarantined={summary[-1][3]:>4,}")

display(spark.createDataFrame(summary, "source string, bronze_rows long, silver_rows long, quarantined long"))
