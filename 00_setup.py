# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates the Unity Catalog objects and copies the sample CSVs from the repo into the landing volume.
# MAGIC
# MAGIC Run once before the pipeline. Requires permission to create catalogs (or change `catalog` in the config to one you own).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for layer in SCHEMAS.values():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{layer}")

# Landing zone for raw files: /Volumes/<catalog>/raw/landing
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.raw")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.landing")

# COMMAND ----------

# Copy sample data shipped in the repo (data/raw/*.csv) into the landing volume.
repo_data = os.path.abspath(os.path.join(os.getcwd(), "..", "data", "raw"))

for src in enabled_sources():
    if src["format"] != "csv":
        continue
    local_file = os.path.join(repo_data, src["path"])
    target = f"{LANDING_PATH}/{src['path']}"
    dbutils.fs.cp(f"file:{local_file}", target)
    print(f"Copied {src['path']} -> {target}")

# COMMAND ----------

display(dbutils.fs.ls(LANDING_PATH))
