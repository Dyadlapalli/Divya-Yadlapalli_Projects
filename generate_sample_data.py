"""Generate synthetic NeoBank data for the lakehouse demo.

All data is fake and randomly generated. Run from the repo root:

    python scripts/generate_sample_data.py

Writes CSV files to data/raw/.
"""

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

random.seed(42)

OUT = Path(__file__).resolve().parent.parent / "data" / "raw"
OUT.mkdir(parents=True, exist_ok=True)

N_CUSTOMERS = 500
START = date(2024, 1, 1)
END = date(2025, 12, 31)

FIRST = ["Ava", "Liam", "Noah", "Emma", "Olivia", "Mason", "Sophia", "Ethan", "Mia", "Lucas",
         "Aria", "Ravi", "Priya", "Arjun", "Meera", "Diego", "Sofia", "Chen", "Mei", "Omar"]
LAST = ["Smith", "Johnson", "Patel", "Garcia", "Kim", "Nguyen", "Brown", "Lee", "Reddy", "Lopez",
        "Wilson", "Khan", "Martin", "Clark", "Rao", "Davis", "Moore", "Singh", "Hall", "Young"]
STATES = ["IL", "TX", "CA", "NY", "FL", "WA", "GA", "OH", "NC", "AZ"]
SEGMENTS = ["Retail", "Retail", "Retail", "Premium", "Student"]
CHANNELS = ["card", "card", "card", "ach", "p2p", "atm"]
MCC = ["grocery", "dining", "fuel", "travel", "utilities", "online_retail", "healthcare", "entertainment"]


def rand_date(a: date, b: date) -> date:
    return a + timedelta(days=random.randint(0, (b - a).days))


def write(name, header, rows):
    with open(OUT / f"{name}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"{name}.csv: {len(rows):,} rows")


# Customers -------------------------------------------------------------
customers = []
for i in range(1, N_CUSTOMERS + 1):
    fn, ln = random.choice(FIRST), random.choice(LAST)
    customers.append([
        f"C{i:05d}", fn, ln, f"{fn.lower()}.{ln.lower()}{i}@example.com",
        random.choice(STATES), rand_date(START, date(2025, 6, 30)).isoformat(),
        random.choice(SEGMENTS),
    ])
# a few messy rows so the Silver layer has something to clean
customers.append(["C00007", " Ava ", "Smith", "AVA.SMITH7@EXAMPLE.COM", "il", "2024-02-01", "Retail"])
customers.append([f"C{N_CUSTOMERS + 1:05d}", "Test", "User", "", "TX", "2025-01-15", ""])
write("customers", ["customer_id", "first_name", "last_name", "email", "state", "signup_date", "segment"], customers)

# Accounts --------------------------------------------------------------
accounts = []
a_id = 1
for c in customers[:N_CUSTOMERS]:
    signup = date.fromisoformat(c[5])
    for acct_type in random.sample(["Checking", "Savings"], k=random.choice([1, 1, 2])):
        accounts.append([
            f"A{a_id:06d}", c[0], acct_type, rand_date(signup, END).isoformat(),
            random.choices(["Active", "Dormant", "Closed"], weights=[85, 10, 5])[0],
            round(random.lognormvariate(8, 1.1), 2),
        ])
        a_id += 1
write("accounts", ["account_id", "customer_id", "account_type", "open_date", "status", "balance"], accounts)

# Transactions ----------------------------------------------------------
txns = []
t_id = 1
for acct in accounts:
    if acct[4] == "Closed":
        continue
    opened = date.fromisoformat(acct[3])
    for _ in range(random.randint(5, 40)):
        d = rand_date(opened, END)
        ts = datetime(d.year, d.month, d.day, random.randint(0, 23), random.randint(0, 59))
        is_credit = random.random() < 0.25
        txns.append([
            f"T{t_id:08d}", acct[0], ts.isoformat(sep=" "),
            round(random.uniform(500, 4000) if is_credit else random.lognormvariate(3.5, 1.0), 2),
            "credit" if is_credit else "debit",
            "ach" if is_credit else random.choice(CHANNELS),
            "" if is_credit else random.choice(MCC),
            random.choices(["posted", "declined", "reversed"], weights=[95, 4, 1])[0],
        ])
        t_id += 1
txns.append(txns[10][:])  # exact duplicate to exercise de-duplication
write("transactions", ["txn_id", "account_id", "txn_ts", "amount", "txn_type", "channel", "merchant_category", "status"], txns)

# Loans -----------------------------------------------------------------
loans = []
for i, c in enumerate(random.sample(customers[:N_CUSTOMERS], 300), start=1):
    product = random.choice(["Personal", "Auto", "Credit Card"])
    principal = {"Personal": random.randint(2, 30), "Auto": random.randint(10, 60), "Credit Card": random.randint(1, 15)}[product] * 1000
    orig = rand_date(date.fromisoformat(c[5]), date(2025, 9, 30))
    dpd = random.choices([0, 15, 45, 75, 120], weights=[80, 8, 6, 3, 3])[0]
    loans.append([
        f"L{i:05d}", c[0], product, principal, round(random.uniform(5.5, 24.9), 2),
        orig.isoformat(), random.choice([12, 24, 36, 48, 60]),
        round(principal * random.uniform(0.2, 1.0), 2), dpd,
    ])
write("loans", ["loan_id", "customer_id", "product", "principal", "interest_rate", "origination_date",
                "term_months", "outstanding_balance", "days_past_due"], loans)

# Credit bureau ---------------------------------------------------------
bureau = []
for c in customers[:N_CUSTOMERS]:
    for report in ["2025-06-30", "2025-12-31"]:
        bureau.append([
            c[0], report, max(300, min(850, int(random.gauss(690, 70)))),
            round(random.lognormvariate(9.5, 1.0), 2), random.choices([0, 1, 2, 3], weights=[75, 15, 7, 3])[0],
        ])
write("credit_bureau", ["customer_id", "report_date", "credit_score", "total_debt", "delinquencies_24m"], bureau)
