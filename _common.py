# Databricks notebook source
# MAGIC %md
# MAGIC # Shared helpers
# MAGIC Loaded by the other notebooks with `%run ./_common`. Reads the pipeline config and provides small helpers.

# COMMAND ----------

import json
import os

CONFIG_PATH = os.path.abspath(os.path.join(os.getcwd(), "..", "config", "pipeline_config.json"))

with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

CATALOG = CONFIG["catalog"]
SCHEMAS = CONFIG["schemas"]
LANDING_PATH = CONFIG["landing_path"]


def enabled_sources():
    """Sources switched on in the config."""
    return [s for s in CONFIG["sources"] if s.get("enabled", True)]


def table(layer: str, name: str) -> str:
    """Fully qualified table name, e.g. neobank.silver.customers."""
    return f"{CATALOG}.{SCHEMAS[layer]}.{name}"


def table_exists(full_name: str) -> bool:
    return spark.catalog.tableExists(full_name)


print(f"Loaded config: {CONFIG_PATH}")
print(f"Enabled sources: {[s['name'] for s in enabled_sources()]}")
