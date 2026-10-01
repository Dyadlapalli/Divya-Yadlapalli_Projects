# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: business-ready tables
# MAGIC Three tables built for dashboards and the Genie space:
# MAGIC
# MAGIC | Table | Grain | Used for |
# MAGIC |---|---|---|
# MAGIC | `gold.customer_360` | one row per customer | customer activity, balances, credit profile, risk tier |
# MAGIC | `gold.daily_transactions` | day × channel × type | volumes, spend trends, decline rates |
# MAGIC | `gold.loan_portfolio_risk` | product × delinquency bucket | portfolio exposure and credit risk |

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

s = lambda name: table("silver", name)
g = lambda name: table("gold", name)

# COMMAND ----------

# MAGIC %md ## Customer 360

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {g('customer_360')} AS
WITH acct AS (
  SELECT customer_id,
         COUNT(*)                                        AS accounts,
         SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) AS active_accounts,
         SUM(balance)                                    AS total_balance
  FROM {s('accounts')}
  GROUP BY customer_id
),
txn AS (
  SELECT a.customer_id,
         COUNT(*)                                                           AS txn_count_90d,
         SUM(CASE WHEN t.txn_type = 'debit' THEN t.amount ELSE 0 END)       AS spend_90d,
         MAX(t.txn_ts)                                                      AS last_txn_ts
  FROM {s('transactions')} t
  JOIN {s('accounts')} a ON t.account_id = a.account_id
  WHERE t.status = 'posted'
    AND t.txn_ts >= (SELECT MAX(txn_ts) FROM {s('transactions')}) - INTERVAL 90 DAYS
  GROUP BY a.customer_id
),
credit AS (
  SELECT customer_id, credit_score, total_debt, delinquencies_24m
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY report_date DESC) AS rn
    FROM {s('credit_bureau')}
  ) WHERE rn = 1
),
loan AS (
  SELECT customer_id,
         COUNT(*)                 AS loans,
         SUM(outstanding_balance) AS loan_outstanding,
         MAX(days_past_due)       AS max_days_past_due
  FROM {s('loans')}
  GROUP BY customer_id
)
SELECT c.customer_id,
       c.first_name, c.last_name, c.state, c.segment, c.signup_date,
       COALESCE(acct.accounts, 0)           AS accounts,
       COALESCE(acct.active_accounts, 0)    AS active_accounts,
       COALESCE(acct.total_balance, 0)      AS total_balance,
       COALESCE(txn.txn_count_90d, 0)       AS txn_count_90d,
       COALESCE(txn.spend_90d, 0)           AS spend_90d,
       txn.last_txn_ts,
       credit.credit_score,
       credit.delinquencies_24m,
       COALESCE(loan.loans, 0)              AS loans,
       COALESCE(loan.loan_outstanding, 0)   AS loan_outstanding,
       COALESCE(loan.max_days_past_due, 0)  AS max_days_past_due,
       CASE
         WHEN COALESCE(loan.max_days_past_due, 0) >= 90 OR credit.credit_score < 580 THEN 'High'
         WHEN COALESCE(loan.max_days_past_due, 0) >= 30 OR credit.credit_score < 670 THEN 'Medium'
         ELSE 'Low'
       END AS risk_tier,
       current_timestamp() AS _gold_updated_ts
FROM {s('customers')} c
LEFT JOIN acct   ON c.customer_id = acct.customer_id
LEFT JOIN txn    ON c.customer_id = txn.customer_id
LEFT JOIN credit ON c.customer_id = credit.customer_id
LEFT JOIN loan   ON c.customer_id = loan.customer_id
""")

# COMMAND ----------

# MAGIC %md ## Daily transactions

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {g('daily_transactions')} AS
SELECT CAST(txn_ts AS DATE)                                      AS txn_date,
       channel,
       txn_type,
       COUNT(*)                                                  AS txn_count,
       SUM(CASE WHEN status = 'posted' THEN amount ELSE 0 END)   AS posted_amount,
       AVG(CASE WHEN status = 'posted' THEN amount END)          AS avg_ticket,
       ROUND(AVG(CASE WHEN status = 'declined' THEN 1.0 ELSE 0 END) * 100, 2) AS decline_rate_pct,
       current_timestamp()                                       AS _gold_updated_ts
FROM {s('transactions')}
GROUP BY CAST(txn_ts AS DATE), channel, txn_type
""")

# COMMAND ----------

# MAGIC %md ## Loan portfolio risk

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {g('loan_portfolio_risk')} AS
WITH latest_score AS (
  SELECT customer_id, credit_score
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY report_date DESC) AS rn
    FROM {s('credit_bureau')}
  ) WHERE rn = 1
)
SELECT l.product,
       CASE
         WHEN l.days_past_due = 0  THEN '1. Current'
         WHEN l.days_past_due < 30 THEN '2. 1-29 DPD'
         WHEN l.days_past_due < 60 THEN '3. 30-59 DPD'
         WHEN l.days_past_due < 90 THEN '4. 60-89 DPD'
         ELSE '5. 90+ DPD'
       END                               AS delinquency_bucket,
       COUNT(*)                          AS loans,
       SUM(l.principal)                  AS principal,
       SUM(l.outstanding_balance)        AS outstanding_balance,
       ROUND(AVG(l.interest_rate), 2)    AS avg_interest_rate,
       ROUND(AVG(cs.credit_score), 0)    AS avg_credit_score,
       current_timestamp()               AS _gold_updated_ts
FROM {s('loans')} l
LEFT JOIN latest_score cs ON l.customer_id = cs.customer_id
GROUP BY 1, 2
""")

# COMMAND ----------

for t in ["customer_360", "daily_transactions", "loan_portfolio_risk"]:
    print(f"Gold    {t:<22} {spark.table(g(t)).count():>8,} rows")
