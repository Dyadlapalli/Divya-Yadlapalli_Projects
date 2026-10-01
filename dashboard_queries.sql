-- NeoBank dashboard queries (Databricks SQL)
-- Each query backs one tile on the NeoBank Executive dashboard.

-- 1. KPI tiles: customers, deposits, loan book, high-risk share
SELECT COUNT(*)                                                    AS customers,
       SUM(total_balance)                                          AS total_deposits,
       SUM(loan_outstanding)                                       AS loan_book,
       ROUND(AVG(CASE WHEN risk_tier = 'High' THEN 1.0 ELSE 0 END) * 100, 1) AS high_risk_pct
FROM neobank.gold.customer_360;

-- 2. Monthly spend trend by channel (posted debits)
SELECT DATE_TRUNC('month', txn_date) AS month,
       channel,
       SUM(posted_amount)            AS spend
FROM neobank.gold.daily_transactions
WHERE txn_type = 'debit'
GROUP BY 1, 2
ORDER BY 1, 2;

-- 3. Decline rate by channel
SELECT channel,
       SUM(txn_count)                                              AS transactions,
       ROUND(SUM(decline_rate_pct * txn_count) / SUM(txn_count), 2) AS decline_rate_pct
FROM neobank.gold.daily_transactions
GROUP BY channel
ORDER BY decline_rate_pct DESC;

-- 4. Loan exposure by product and delinquency bucket
SELECT product, delinquency_bucket, loans, outstanding_balance, avg_credit_score
FROM neobank.gold.loan_portfolio_risk
ORDER BY product, delinquency_bucket;

-- 5. Customers by risk tier and segment
SELECT segment, risk_tier, COUNT(*) AS customers, ROUND(AVG(credit_score)) AS avg_credit_score
FROM neobank.gold.customer_360
GROUP BY segment, risk_tier
ORDER BY segment, risk_tier;

-- 6. Top 20 high-risk customers by loan exposure (watch list)
SELECT customer_id, first_name, last_name, segment, credit_score,
       loan_outstanding, max_days_past_due
FROM neobank.gold.customer_360
WHERE risk_tier = 'High'
ORDER BY loan_outstanding DESC
LIMIT 20;
