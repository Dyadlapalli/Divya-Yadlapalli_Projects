# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Data quality checks
# MAGIC Simple checks that run after each load. The job fails if any check fails, so bad data never reaches dashboards silently.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

s = lambda name: table("silver", name)
g = lambda name: table("gold", name)

checks = {
    "Silver customers have unique IDs":
        f"SELECT COUNT(*) - COUNT(DISTINCT customer_id) FROM {s('customers')}",
    "Silver transactions have unique IDs":
        f"SELECT COUNT(*) - COUNT(DISTINCT txn_id) FROM {s('transactions')}",
    "Every account belongs to a known customer":
        f"""SELECT COUNT(*) FROM {s('accounts')} a
            LEFT ANTI JOIN {s('customers')} c ON a.customer_id = c.customer_id""",
    "No negative transaction amounts":
        f"SELECT COUNT(*) FROM {s('transactions')} WHERE amount < 0",
    "Credit scores between 300 and 850":
        f"SELECT COUNT(*) FROM {s('credit_bureau')} WHERE credit_score NOT BETWEEN 300 AND 850",
    "Gold customer_360 matches Silver customer count":
        f"SELECT ABS((SELECT COUNT(*) FROM {g('customer_360')}) - (SELECT COUNT(*) FROM {s('customers')}))",
}

results = []
for name, sql in checks.items():
    failures = spark.sql(sql).collect()[0][0]
    results.append((name, int(failures), "PASS" if failures == 0 else "FAIL"))

display(spark.createDataFrame(results, "check string, failing_rows long, result string"))

failed = [r for r in results if r[2] == "FAIL"]
if failed:
    raise Exception(f"{len(failed)} data quality check(s) failed: {[r[0] for r in failed]}")
print("All data quality checks passed.")
