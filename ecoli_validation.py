import pandas as pd
import numpy as np

def run_ecoli_validation(df):
    df = df.copy()
    df["ECOLI_ValidationNotes"] = ""
    df["ECOLI_ChangeNotes"] = ""

    all_zero_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col]) and (df[col].fillna(0) == 0).all()]

    def log_change(idx, text):
        df.at[idx, "ECOLI_ChangeNotes"] += text + "; "

    def log_issue(idx, text):
        df.at[idx, "ECOLI_ValidationNotes"] += text + "; "

    col_temp = "Incubation temperature is 33° C +/- 3° C"
    if col_temp in df.columns and col_temp not in all_zero_cols:
        df[col_temp] = pd.to_numeric(df[col_temp], errors="coerce")
        mask = (df[col_temp] < 30) | (df[col_temp] > 36)
        df.loc[mask, "ECOLI_ValidationNotes"] += "Incubation temperature not in 30–36°C range; "
        df.loc[mask, col_temp] = np.nan

    col_time = "Incubation time is between 28-31 hours"
    if col_time in df.columns and col_time not in all_zero_cols:
        df[col_time] = pd.to_numeric(df[col_time], errors="coerce")
        mask = (df[col_time] < 28) | (df[col_time] > 31)
        df.loc[mask, "ECOLI_ValidationNotes"] += "Incubation time not in 28–31h range; "
        df.loc[mask, col_time] = np.nan

    for col in ["Sample 1: Colonies Counted", "Sample 2: Colonies Counted"]:
        if col in df.columns and col not in all_zero_cols:
            mask = df[col] > 200
            df.loc[mask, "ECOLI_ValidationNotes"] += f"{col} > 200 colonies; "
            df.loc[mask, col] = np.nan

    col_blank = "No colony growth on Field Blank"
    if col_blank in df.columns and col_blank not in all_zero_cols:
        bad_blank = df[col_blank].astype(str).str.lower().isin(["no", "false", "n"])
        df.loc[bad_blank, "ECOLI_ValidationNotes"] += "Colony growth detected in field blank; "

    col_ecoli = "E. Coli Average"
    if col_ecoli in df.columns and col_ecoli not in all_zero_cols:
        mask = df[col_ecoli] == 0
        df.loc[mask, "ECOLI_ValidationNotes"] += "E. coli = 0; "
        df.loc[mask, col_ecoli] = np.nan

        df["E. Coli Rounded"] = df[col_ecoli].round(0).astype("Int64")
        def round_sig_figs(n):
            try:
                if n == 0 or pd.isna(n): return n
                return round(n, -int(np.floor(np.log10(abs(n)))) + 1)
            except:
                return n
        df["E. Coli Rounded (2SF)"] = df["E. Coli Rounded"].apply(round_sig_figs)
        for idx in df.index:
            orig = df.at[idx, col_ecoli]
            rounded = df.at[idx, "E. Coli Rounded (2SF)"]
            if not pd.isna(orig) and not pd.isna(rounded):
                log_change(idx, f"E. coli {orig} → {rounded} (rounded to 2 significant figures)")

    def check_dilution(row, prefix):
        try:
            count = row[f"{prefix}: Colonies Counted"]
            dilution = row[f"{prefix}: Dilution Factor (Manual)"]
            volume = row[f"{prefix}: Sample Size (mL)"]
            reported = row[f"{prefix}: Colony Forming Units per 100mL"]
            calculated = (count * dilution * 100) / volume
            return abs(calculated - reported) <= 10
        except:
            return True

    for prefix in ["Sample 1", "Sample 2"]:
        cols = [f"{prefix}: Colonies Counted", f"{prefix}: Dilution Factor (Manual)", f"{prefix}: Sample Size (mL)", f"{prefix}: Colony Forming Units per 100mL"]
        if all(c in df.columns and c not in all_zero_cols for c in cols):
            valid = df.apply(lambda row: check_dilution(row, prefix), axis=1)
            df.loc[~valid, "ECOLI_ValidationNotes"] += f"{prefix} CFU formula mismatch; "

    return df
