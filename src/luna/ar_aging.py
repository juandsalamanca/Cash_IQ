import pandas as pd
import numpy as np
from src.general_preprocessing import safe_strip, to_numeric

# ============================================================
# AR AGING -> projected collections inflow lines
# ============================================================

def get_assumptions(AR_AGING_PATH, AR_BUCKET_ASSUMPTIONS, PROJ_WEEK1_START, proj_week_starts, combined):

    ar = pd.read_excel(AR_AGING_PATH)
    ar.columns = [str(c).strip() for c in ar.columns]

    name_col = next((c for c in ["Customer", "Name", "Company", "Customer Name"] if c in ar.columns), None)

    bucket_map = {}
    for c in ar.columns:
        c_norm = c.replace(" ", "").replace("-", "").replace("_", "").lower()
        if c_norm == "current": bucket_map["Current"] = c
        elif "130" in c_norm or "1to30" in c_norm: bucket_map["1-30"] = c
        elif "3160" in c_norm or "31to60" in c_norm: bucket_map["31-60"] = c
        elif "6190" in c_norm or "61to90" in c_norm: bucket_map["61-90"] = c
        elif "over90" in c_norm or "91" in c_norm or "90+" in c_norm: bucket_map["91+"] = c

    ar_assump_rows = []
    try:
        if name_col and bucket_map:
            tmp = ar[[name_col] + list(bucket_map.values())].copy()
            tmp[name_col] = safe_strip(tmp[name_col].fillna(""))

            for b, col in bucket_map.items():
                tmp[col] = to_numeric(tmp[col]).fillna(0.0)

            for _, r in tmp.iterrows():
                cust = r[name_col].strip() or "AR - Unnamed Customer"
                proj = pd.Series(0.0, index=proj_week_starts)

                for bucket, col in bucket_map.items():
                    amt = float(r[col])
                    if amt == 0:
                        continue
                    p, mean_weeks = AR_BUCKET_ASSUMPTIONS.get(bucket, (0.5, 6))
                    collectible = amt * p

                    weights = np.array([0.25, 0.50, 0.25])
                    offsets = np.array([max(0, mean_weeks-1), mean_weeks, mean_weeks+1])

                    for off, wt in zip(offsets, weights):
                        wk = PROJ_WEEK1_START + pd.Timedelta(weeks=int(off))
                        if wk in proj.index:
                            proj.loc[wk] += collectible * float(wt)

                    ar_assump_rows.append({
                        "Customer": cust,
                        "Bucket": bucket,
                        "AR_Amount": amt,
                        "Collect_Prob": p,
                        "Expected_Collectible": collectible,
                        "Mean_Weeks_To_Collect": mean_weeks
                    })

                if proj.sum() != 0:
                    key = (f"Collections - {cust}", "AR Aging", "AR")
                    if key not in combined.index:
                        combined.loc[key, :] = 0.0
                    combined.loc[key, proj_week_starts] += proj.values

        ar_assumptions_df = pd.DataFrame(ar_assump_rows)
    except Exception as e:
        print(f"Error processing AR aging assumptions: {e}")
        ar_assumptions_df = pd.DataFrame(columns=["Customer", "Bucket", "AR_Amount", "Collect_Prob", "Expected_Collectible", "Mean_Weeks_To_Collect"])

    return ar, ar_assumptions_df