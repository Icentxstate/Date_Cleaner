import pandas as pd
import numpy as np
def run_core_validation(df):
    df = df.copy()
    df["CORE_Notes"] = ""
    df["CORE_ChangeNotes"] = ""
    row_delete_indices = set()

    def log_change(col, idx, new_val, reason):
        df.at[idx, "CORE_ChangeNotes"] += f"{col} → {new_val} ({reason}); "

    if "Sample Depth (meters)" in df.columns and "Total Depth (meters)" in df.columns:
        for idx, row in df.iterrows():
            sample = row["Sample Depth (meters)"]
            total = row["Total Depth (meters)"]
            if not (sample == 0.3 or np.isclose(sample, total / 2, atol=0.05)):
                df.at[idx, "CORE_Notes"] += "Sample Depth not 0.3m or mid-depth; "

    if "Flow Severity" in df.columns and "Total Depth (meters)" in df.columns:
        mask = (df["Total Depth (meters)"] == 0) & (df["Flow Severity"] != 6)
        df.loc[mask, "CORE_Notes"] += "Zero Depth with non-dry flow; "
        row_delete_indices.update(df[mask].index.tolist())

    do1 = "Dissolved Oxygen (mg/L) 1st titration"
    do2 = "Dissolved Oxygen (mg/L) 2nd titration"
    if do1 in df.columns and do2 in df.columns:
        diff = (df[do1] - df[do2]).abs()
        mask = diff > 0.5
        df.loc[mask, "CORE_Notes"] += "DO Difference > 0.5; "
        df["DO1 Rounded"] = df[do1].round(1)
        df["DO2 Rounded"] = df[do2].round(1)
        for idx in df.index:
            log_change("DO1", idx, df.at[idx, "DO1 Rounded"], "Rounded to 0.1")
            log_change("DO2", idx, df.at[idx, "DO2 Rounded"], "Rounded to 0.1")

    secchi = "Secchi Disk Transparency - Average"
    if secchi in df.columns:
        df.loc[~df[secchi].apply(lambda v: len(str(v).replace(".", "").lstrip("0")) <= 2), "CORE_Notes"] += "Secchi not 2 significant figures; "
        df.loc[df[secchi] > df["Total Depth (meters)"], "CORE_Notes"] += "Secchi > Depth; "

    cond_col = "Conductivity (?S/cm)"
    if cond_col in df.columns and "Standard Value" in df.columns:
        cond = df[cond_col]
        std = df["Standard Value"]
        good = (cond >= 0.8 * std) & (cond <= 1.2 * std)
        df.loc[~good, "CORE_Notes"] += "Conductivity outside ±20%; "
        df.loc[~good, cond_col] = np.nan

    if cond_col in df.columns:
        df["TDS Calculated"] = df[cond_col] * 0.65
        for idx in df.index:
            log_change("TDS", idx, df.at[idx, "TDS Calculated"], "Estimated TDS = Conductivity × 0.65")

    if "Sampling Time" in df.columns:
        df["Sampling Time"] = pd.to_datetime(df["Sampling Time"], errors='coerce')
        if "Post-Test Calibration" in df.columns:
            df["Post-Test Calibration"] = pd.to_datetime(df["Post-Test Calibration"], errors='coerce')
            delta = (df["Sampling Time"] - df["Post-Test Calibration"]).abs().dt.total_seconds() / 3600
            df.loc[delta > 24, "CORE_Notes"] += "Post-calibration >24h; "
        if "Pre-Test Calibration" in df.columns:
            df["Pre-Test Calibration"] = pd.to_datetime(df["Pre-Test Calibration"], errors='coerce')
            delta = (df["Sampling Time"] - df["Pre-Test Calibration"]).abs().dt.total_seconds() / 3600
            df.loc[delta > 24, "CORE_Notes"] += "Pre-calibration >24h; "

    if "pH (standard units)" in df.columns:
        df["pH Rounded"] = df["pH (standard units)"].round(1)
        for idx in df.index:
            log_change("pH", idx, df.at[idx, "pH Rounded"], "Rounded to 0.1")

    if "Water Temperature (° C)" in df.columns:
        df["Water Temp Rounded"] = df["Water Temperature (° C)"].round(1)
        for idx in df.index:
            log_change("Temp", idx, df.at[idx, "Water Temp Rounded"], "Rounded to 0.1")

    if cond_col in df.columns:
        df.loc[~df[cond_col].apply(lambda val: len(str(int(round(val)))) <= 3 if val > 100 else float(val).is_integer()), "CORE_Notes"] += "Conductivity format error; "

    if "Salinity (ppt)" in df.columns:
        df["Salinity Formatted"] = df["Salinity (ppt)"].apply(lambda val: "< 2.0" if val < 2.0 else round(val, 1) if pd.notna(val) else "")
        for idx in df.index:
            log_change("Salinity", idx, df.at[idx, "Salinity Formatted"], "Formatted for display")

    if "Time Spent Sampling/Traveling" in df.columns:
        mask = ~df["Time Spent Sampling/Traveling"].apply(lambda x: isinstance(x, (int, float, np.integer, np.floating)))
        df.loc[mask, "CORE_Notes"] += "Time format not numeric; "

    if "Roundtrip Distance Traveled" in df.columns:
        mask = ~df["Roundtrip Distance Traveled"].apply(lambda x: isinstance(x, (int, float, np.integer, np.floating)))
        df.loc[mask, "CORE_Notes"] += "Distance format not numeric; "

    df_cleaned = df.drop(index=row_delete_indices)

    return df_cleaned