import pandas as pd
import numpy as np

def run_advanced_validation(df):
    df = df.copy()
    df["ADVANCED_ValidationNotes"] = ""
    df["ADVANCED_ChangeNotes"] = ""

    all_zero_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col]) and (df[col].fillna(0) == 0).all()]

    for col in all_zero_cols:
        df["ADVANCED_ChangeNotes"] += f"Skipped checks for unmeasured parameter: {col}; "

    def log_change(idx, text):
        df.at[idx, "ADVANCED_ChangeNotes"] += text + "; "

    def log_issue(idx, text):
        df.at[idx, "ADVANCED_ValidationNotes"] += text + "; "

    phosphate_cols = [col for col in df.columns if "Phosphate" in col and "Value" in col and col not in all_zero_cols]
    for col in phosphate_cols:
        if "mg/L" not in col and "ppm" not in col:
            for idx in df.index:
                log_issue(idx, f"{col} not labeled in mg/L")

    nitrate_cols = [col for col in df.columns if "Nitrate-Nitrogen" in col and "Value" in col and col not in all_zero_cols]
    for col in nitrate_cols:
        if "mg/L" not in col and "ppm" not in col:
            for idx in df.index:
                log_issue(idx, f"{col} not labeled in mg/L")

    turbidity_cols = [col for col in df.columns if "Turbidity" in col and "Result" in col and col not in all_zero_cols]
    for col in turbidity_cols:
        if "NTU" not in col and "JTU" in col:
            for idx in df.index:
                log_issue(idx, f"{col} appears to be in JTU not NTU")

    col_discharge = "Discharge Recorded"
    if col_discharge in df.columns and col_discharge not in all_zero_cols:
        def fix_discharge(val):
            try:
                val = float(val)
                if val < 10:
                    new_val = round(val, 1)
                    return new_val, None if abs(val - new_val) < 0.05 else f"{val} → {new_val} (should have 1 decimal)"
                else:
                    new_val = round(val)
                    return new_val, None if val.is_integer() else f"{val} → {new_val} (should be integer)"
            except:
                return val, "Invalid or non-numeric discharge value"

        for idx in df.index:
            val = df.at[idx, col_discharge]
            fixed, issue = fix_discharge(val)
            if issue:
                log_issue(idx, f"Discharge format issue: {issue}")
            if not pd.isna(fixed) and fixed != val:
                log_change(idx, f"Discharge {val} → {fixed}")
                df.at[idx, col_discharge] = fixed

    unit_col = "ResultMeasure/MeasureUnitCode"
    param_col = "CharacteristicName"
    if unit_col in df.columns and param_col in df.columns:
        for idx in df.index:
            param = str(df.at[idx, param_col]).lower()
            unit = str(df.at[idx, unit_col]).lower()

            if "phosphate" in param and unit not in ["mg/l", "ppm"]:
                log_issue(idx, f"Phosphate unit invalid: {unit}")
            elif "nitrate" in param and unit not in ["mg/l", "ppm"]:
                log_issue(idx, f"Nitrate-Nitrogen unit invalid: {unit}")
            elif "turbidity" in param and unit != "ntu":
                log_issue(idx, f"Turbidity unit should be NTU, found: {unit}")
            elif "streamflow" in param and unit != "ft2/sec":
                log_issue(idx, f"Streamflow unit should be ft2/sec, found: {unit}")
            elif "discharge" in param and unit != "ft2/sec":
                log_issue(idx, f"Discharge unit should be ft2/sec, found: {unit}")

    return df
