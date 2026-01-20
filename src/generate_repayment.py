import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

np.random.seed(42)

BASE_PATH = Path("../data/raw/")

print("=" * 80)
print("REPAYMENT DATA GENERATOR")
print("=" * 80)
print()

# Load loan data
print("Loading loan data...")
loans = pd.read_parquet(BASE_PATH/'loan_master.parquet')
print(f"✓ Loaded {len(loans)} loans")
print()

# Payment behavior distribution 
PAYMENT_BEHAVIORS = {
    "on_time": 0.75,
    "late_15": 0.10,
    "late_30": 0.08,
    "late_90": 0.03,
    "default": 0.04
}

print("Payment Behavior Distribution:")
for behavior, prob in PAYMENT_BEHAVIORS.items():
    print(f"  - {behavior}: {prob*100:.0f}%")
print()

def generate_repayments():
    """
    For each loan, generating monthly emi records from disbursemnet to tenure end.
    """

    records = []
    total_installments = 0
    default_count = 0
    on_time_count = 0
    late_count = 0
    partial_count = 0

    print("Genrating payment record....")

    for idx, row in loans.iterrows():
        if (idx + 1) % 1000 == 0:
            print(f"Processed {idx + 1}/{len(loans)} loans")

            loan_id = row["loan_id"]
            account_id = row["account_id"]
            dibursement_date = pd.to_datetime(row["disbursement_date"])
            emi_amount = row["emi_amount"]
            tenure_months = row["tenure_months"]

            already_defaulted = False

            for installment_no in range(1, tenure_months+1):
                total_installments += 1

                due_date = dibursement_date + pd.DateOffset(months = installment_no)

                if already_defaulted:
                    actual_payment_date = pd.NaT
                    payment_amount = 0.0
                    dpd = 999
                    partial_flag = 0
                else:
                    behavior == np.random.choice(
                        list(PAYMENT_BEHAVIORS.keys()),
                        p=list(PAYMENT_BEHAVIORS.values())
                    )

                    if behavior == "default":
                        actual_payment_date = pd.NaT
                        payment_amount = 0.0
                        dpd = 999
                        partial_flag = 0
                        already_defaulted = True
                        default_count += 1

                    elif behavior == 'on_time':
                        delay_days = 0
                        actual_payment_date = due_date+ timedelta(days = int(delay_days))
                        dpd = 0
                        on_time_count += 1
                    
                    else:
                        delay_ranges = {
                            "late_15": (1,15),
                            "late_30": (15,30),
                            "late_90": (30,90)
                        }

                        min_delay, max_delay = delay_ranges[behavior]
                        delay_days = np.random.randint(min_delay, max_delay)

                        actual_payment_date = due_date + timedelta(days = int(delay_days))
                        dpd = delay_days
                        late_count += 1

                        if np.random.rand() < 0.15 and behavior != "default":

                            payment_factor = np.random.uniform(0.5, 0.9)
                            payment_amount = float(emi_amount * payment_factor)

                            partial_flag = 1
                            partial_count += 1
                        else:

                            payment_amount = float(emi_amount) if behavior != "default" else 0.0 
                            partial_flag = 0


                records.append({

                "loan_id": loan_id,
                "account_id": account_id,
                "installment_no": installment_no,
                "due_date": due_date,
                "actual_payment_date": actual_payment_date,
                "payment_amount": payment_amount,
                "dpd": dpd,
                "partial_payment_flag": partial_flag
                })

    print(f"✓ Generated {total_installments} installment records")
    print()
    
    df_rep = pd.DataFrame(records)
    
    output_path = BASE_PATH / "repayment.parquet"
    df_rep.to_parquet(output_path, index=False)

    return df_rep

if __name__ == "__main__":
    print()
    print("LOAN DEFAULT PREDICTION - REPAYMENT GENERATION")
    print("Payment Records with DPD")
    print()
    
    df_rep = generate_repayments()



