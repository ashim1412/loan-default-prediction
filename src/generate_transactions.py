import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

np.random.seed(42)

BASE_PATH = Path("./data/raw")

print("=" * 80)
print("GENERATING TRANSACTION.....")
print("=" * 80)
print()

# Transaction type distribution
TRANSACTION_TYPES = {
    "salary": 0.10,
    "merchant": 0.40,
    "p2p": 0.30,
    "cashout": 0.10,
    "load": 0.10
}

print("Transaction Type Distribution:")
for ttype, prob in TRANSACTION_TYPES.items():
    print(f"  - {ttype}: {prob*100:.0f}%")
print()

def generate_transactions():
    """
    Generating Transaction for each customer.
    """

    # Load customer data
    print("Loading customer data...")
    customers = pd.read_parquet(BASE_PATH / "customer_profile.parquet")
    print(f"✓ Loaded {len(customers)} customers")
    print()

    
    records = []
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2025, 1, 1)
    date_range = (end_date - start_date).days
    
    txn_id = 1
    total_txns = 0
    salary_count = 0
    merchant_count = 0
    p2p_count = 0
    cashout_count = 0
    load_count = 0
    
    print("Generating transaction records...")
    
    for idx, cust in customers.iterrows():
        if (idx + 1) % 500 == 0:
            print(f"  Processed {idx + 1}/{len(customers)} customers...")
        
        account_id = cust["account_id"]
        income = cust["income"]
        
        # Starting balance: 2-6 months of income
        balance = np.random.randint(int(income * 2), int(income * 6))
        
        # Number of transactions for this customer
        n_txn = np.random.randint(300, 2000)
        
        for _ in range(n_txn):
            # Random date in 2-year window
            random_days = np.random.randint(0, date_range)
            txn_timestamp = start_date + timedelta(days=random_days)
            
            # Transaction type
            ttype = np.random.choice(
                list(TRANSACTION_TYPES.keys()),
                p=list(TRANSACTION_TYPES.values())
            )
            
            if ttype == "salary":
                amount = np.random.randint(
                    int(income * 0.8),
                    int(income * 1.2)
                )
                salary_flag = 1
                salary_count += 1
                
            elif ttype == "merchant":
                amount = np.random.randint(500, 20000)
                salary_flag = 0
                merchant_count += 1
                
            elif ttype == "p2p":
                amount = np.random.randint(1000, 50000)
                salary_flag = 0
                p2p_count += 1
                
            elif ttype == "cashout":
                amount = np.random.randint(2000, 20000)
                salary_flag = 0
                cashout_count += 1
                
            else:  # load
                amount = np.random.randint(5000, 50000)
                salary_flag = 0
                load_count += 1
            
            balance_before = balance
            
            if ttype in ["salary", "load"]:
                balance += amount
            else:
                balance -= amount
            
            if balance < 0:
                balance = np.random.randint(100, 5000)
            
            records.append({
                "txn_id": f"TXN{str(txn_id).zfill(8)}",
                "account_id": account_id,
                "txn_timestamp": txn_timestamp,
                "txn_type": ttype,
                "amount": float(amount),
                "balance_before": float(balance_before),
                "balance_after": float(balance),
                "salary_flag": salary_flag
            })
            
            txn_id += 1
            total_txns += 1
    
    print(f"✓ Generated {total_txns:,} transaction records")
    print()
    
    df_txn = pd.DataFrame(records)
    
    output_path = BASE_PATH / "transaction.parquet"
    df_txn.to_parquet(output_path, index=False)
    
    return df_txn

if __name__ == "__main__":
    print()
    print("LOAN DEFAULT PREDICTION - TRANSACTION GENERATION")
    print()

    df_txn = generate_transactions()
