import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

np.random.seed(42)

BASE_PATH = Path('./data/raw')
BASE_PATH.mkdir(parents=True, exist_ok=True)

N_CUSTOMERS = 5000
TODAY = datetime(2023,1,1)

print("="*80)
print("STEP 1/2: Generating Customer Profile Data")
print("=" * 80)

def generate_customers():
    account_ids = [f"ACC{str(i).zfill(5)}" for i in range(1, N_CUSTOMERS+1)]
    ages = np.random.randint(21,60, size = N_CUSTOMERS)
    incomes = np.random.randint(20000,200000, size = N_CUSTOMERS)
    genders = np.random.choice(['male','female','others'], size = N_CUSTOMERS, p=[0.5,0.4,0.1])
    employment_types = np.random.choice(['salaried','self_employed','contract'],size = N_CUSTOMERS, p= [0.6,0.25,0.15])
    prior_loans = np.random.poisson(lam=1.0,size=N_CUSTOMERS)
    credit_scores = np.clip(np.random.normal(650,70,size=N_CUSTOMERS),300,900)

    df_customers = pd.DataFrame({
        'account_id': account_ids,
        'age': ages,
        'income': incomes,
        'gender':genders,
        'employment_type': employment_types,
        'prior_loans_count': prior_loans,
        'credit_score': credit_scores.round(0).astype(int)
    })

    output_path = BASE_PATH/'customer_profile.parquet'
    df_customers.to_parquet(output_path, index = False)

    return df_customers

def calculate_emi(principal, annual_rate, tenure_months):
    r = annual_rate / (12 * 100)
    n = tenure_months
    if r == 0:
        return principal / n
    numerator = principal * r * (1 + r) ** n
    denominator = (1 + r) ** n - 1
    return numerator / denominator

def generate_loans(df_customers):
    n_loans = int(N_CUSTOMERS * 1.2)
    loan_ids = [f"LN{str(i).zfill(6)}" for i in range(1, n_loans + 1)]
    borrower_account_ids = np.random.choice(
        df_customers["account_id"],
        size=n_loans,
        replace=True
    )
    loan_amounts = np.random.randint(50000, 2000000, size=n_loans)
    tenures = np.random.choice([12, 24, 36], size=n_loans, p=[0.3, 0.4, 0.3])
    interest_rates = np.random.uniform(10, 24, size=n_loans)
    disb_dates = [
        TODAY - timedelta(days=int(x))
        for x in np.random.randint(0, 365 * 3, size=n_loans)
    ]
    loan_types = np.random.choice(
        ["personal", "business", "vehicle"],
        size=n_loans,
        p=[0.6, 0.2, 0.2]
    )

    emis = [
        calculate_emi(p, r, t)
        for p, r, t in zip(loan_amounts, interest_rates, tenures)
    ]

    df_loans = pd.DataFrame({
        "loan_id": loan_ids,
        "account_id": borrower_account_ids,
        "disbursement_date": disb_dates,
        "loan_amount": loan_amounts,
        "tenure_months": tenures,
        "interest_rate": interest_rates.round(2),
        "emi_amount": np.round(emis, 2),
        "loan_type": loan_types,
    })

    output_path = BASE_PATH / "loan_master.parquet"
    df_loans.to_parquet(output_path, index=False)

if __name__ == "__main__":
    print("\n")
    print("🚀 LOAN DEFAULT PREDICTION - DATA GENERATION")
    print("Customer Profile + Loan Master")
    print()

    df_cust = generate_customers()
    generate_loans(df_cust)

    print("=" * 80)
    print("✅ SUCCESS! Data generated successfully")
    print("=" * 80)
    print()