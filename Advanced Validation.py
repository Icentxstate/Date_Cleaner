import pandas as pd
import numpy as np

def run_riparian_validation(df):
    df = df.copy()
    df["RIPARIAN_ValidationNotes"] = ""
    df["RIPARIAN_ChangeNotes"] = ""

    def log_change(idx, msg):
        df.at[idx, "RIPARIAN_ChangeNotes"] += msg + "; "

    def log_issue(idx, msg):
        df.at[idx, "RIPARIAN_ValidationNotes"] += msg + "; "

    indicator_cols = [
        "Energy Dissipation", "New Plant Colonization", "Stabilizing Vegetation",
        "Age Diversity", "Species Diversity", "Plant Vigor", "Water Storage",
        "Bank/Channel Erosion", "Sediment Deposition"
    ]
    available_cols = [col for col in indicator_cols if col in df.columns]

    zeroed_columns = []
    for col in available_cols:
        try:
            numeric_col = pd.to_numeric(df[col], errors="coerce").fillna(0)
            if numeric_col.eq(0).all():
                zeroed_columns.append(col)
        except:
            continue

    for col in zeroed_columns:
        df["RIPARIAN_ChangeNotes"] += f"Skipped checks for unmeasured parameter: {col}; "

    if "Bank Evaluated" in df.columns:
        for idx, val in df["Bank Evaluated"].items():
            if pd.isna(val) or str(val).strip() == "":
                log_issue(idx, "Bank evaluation missing")

    for idx, row in df.iterrows():
        for col in available_cols:
            if col in zeroed_columns:
                continue
            if pd.isna(row[col]) or str(row[col]).strip() == "":
                comments = str(row.get("Comments", "")).strip().lower()
                if comments in ["", "n/a", "na", "none"]:
                    log_issue(idx, f"{col} missing without explanation")
                else:
                    df.at[idx, col] = np.nan

    image_col = "Image of site was submitted"
    if image_col in df.columns:
        for idx, val in df[image_col].items():
            raw = str(val).strip().lower()
            if raw in ["no", "false", "n", "", "nan"]:
                log_issue(idx, "Site image not submitted")
            elif raw in ["yes", "true", "y"]:
                standard = "Yes"
                if str(val).strip() != standard:
                    log_change(idx, f"Image value standardized: '{val}' → '{standard}'")
                    df.at[idx, image_col] = standard

    return df
