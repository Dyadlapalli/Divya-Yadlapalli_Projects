# Genie space setup

A Databricks **AI/BI Genie** space lets business users ask questions about the Gold tables in plain English.

## Steps

1. In Databricks, open **Genie** → **New**.
2. Add these tables:
   - `neobank.gold.customer_360`
   - `neobank.gold.daily_transactions`
   - `neobank.gold.loan_portfolio_risk`
3. Pick a SQL warehouse (Serverless is easiest).
4. Paste the instructions below into **Instructions**.
5. Add the sample questions so users see what they can ask.

## Instructions for Genie

```
You answer questions for NeoBank's operations and risk teams.
- "Deposits" means SUM(total_balance) from customer_360.
- "Loan book" or "exposure" means SUM(outstanding_balance) from loan_portfolio_risk.
- "Delinquent" means days past due >= 30 (buckets 3, 4 and 5).
- "High-risk customer" means risk_tier = 'High' in customer_360.
- "Spend" means posted debit amounts in daily_transactions.
- Show money with 2 decimals and percentages with 1 decimal.
```

## Sample questions

- What is our total loan book by product?
- How many high-risk customers do we have in each segment?
- Which channel has the highest decline rate this year?
- Show monthly card spend for 2025.
- List the top 10 customers by loan exposure with 90+ days past due.
- What share of the Personal loan book is delinquent?
