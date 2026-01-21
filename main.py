"""
MAIN ORCHESTRATOR
-----------------
Runs full synthetic data generation pipeline:
1. Customers + Loans
2. Repayments
3. Transactions
"""

import sys
from datetime import datetime

from src.generate_customer_loans import generate_customers, generate_loans
from src.generate_repayment import generate_repayments
from src.generate_transactions import generate_transactions


def main():
    print("=" * 60)
    print("SYNTHETIC FINANCIAL DATA GENERATION PIPELINE")
    print(f"Run started at: {datetime.now()}")
    print("=" * 60)
    print()

    # --------------------------------------------------
    # STEP 1: Generate Customers
    # --------------------------------------------------
    print("STEP 1: Generating customers")
    df_customers = generate_customers()
    print(f"✓ Customers generated: {len(df_customers)}")
    print()

    # --------------------------------------------------
    # STEP 2: Generate Loans
    # --------------------------------------------------
    print("STEP 2: Generating loans")
    df_loans = generate_loans(df_customers)
    print(f"✓ Loans generated: {len(df_loans)}")
    print()

    # --------------------------------------------------
    # STEP 3: Generate Repayments
    # --------------------------------------------------
    print("STEP 3: Generating repayment schedules")
    df_repayments = generate_repayments()
    print(f"✓ Repayment records generated: {len(df_repayments)}")
    print()

    # --------------------------------------------------
    # STEP 4: Generate Transactions
    # --------------------------------------------------
    print("STEP 4: Generating wallet transactions")
    df_transactions = generate_transactions()
    print(f"✓ Transactions generated: {len(df_transactions)}")
    print()

    print("=" * 60)
    print("DATA GENERATION COMPLETED SUCCESSFULLY")
    print(f"Run finished at: {datetime.now()}")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ Pipeline failed")
        print(str(e))
        sys.exit(1)
