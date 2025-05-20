import streamlit as st
import pandas as pd
import numpy as np
import tempfile
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import openpyxl
import os

st.set_page_config(layout="wide")
st.title("🧪 Water Quality Data Validation App")

# ==== Tabs ====
tabs = st.tabs([
    "📁 Upload File",
    "1️⃣ GENERAL Validation",
    "2️⃣ CORE Validation",
    "3️⃣ ECOLI Validation",
    "4️⃣ ADVANCED Validation",
    "5️⃣ RIPARIAN Validation",
    "📦 Final Output"
])

# ------------------------ 1. Upload Tab ------------------------
with tabs[0]:
    st.header("📁 Upload Your Excel File")
    uploaded_file = st.file_uploader("Please upload your Excel file:", type=["xlsx"])

    if uploaded_file:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded_file.read())
            input_path = tmp.name
        st.success("✅ File uploaded successfully. You can now proceed to the next tabs.")
    else:
        st.warning("To continue, please upload an Excel file.")

# ------------------------ 2. GENERAL Validation Tab ------------------------
with tabs[1]:
    st.header("1️⃣ GENERAL Validation")

    if uploaded_file:
        run_button = st.button("✅ Run GENERAL Validation")
        if run_button:
            import openpyxl
            from openpyxl import load_workbook
            from openpyxl.styles import PatternFill

            df = pd.read_excel(input_path)
            df["ValidationNotes"] = ""
            df["ValidationColorKey"] = ""
            df["TransformNotes"] = ""

            # Core parameters for row-level deletion if all missing or zero
            core_params = [
                "pH (standard units)", 
                "Dissolved Oxygen (mg/L) Average",
                "Water Temperature (° C)", 
                "Conductivity (?S/cm)", 
                "Salinity (ppt)"
            ]

            row_delete_indices = set()

            # Check watershed site count
            if "Group or Affiliation" in df.columns and "Site ID: Site Name" in df.columns:
                site_counts = df.groupby("Group or Affiliation")["Site ID: Site Name"].nunique()
                invalid_ws = site_counts[site_counts < 3].index
                mask = df["Group or Affiliation"].isin(invalid_ws)
                df.loc[mask, "ValidationNotes"] += "Less than 3 sites in watershed; "
                df.loc[mask, "ValidationColorKey"] += "watershed_or_events;"
                row_delete_indices.update(df[mask].index.tolist())

            # Check site event count
            if "Site ID: Site Name" in df.columns and "Sample Date" in df.columns:
                df["Sample Date"] = pd.to_datetime(df["Sample Date"], errors='coerce')
                event_counts = df.groupby("Site ID: Site Name")["Sample Date"].nunique()
                low_event_sites = event_counts[event_counts < 10].index
                mask = df["Site ID: Site Name"].isin(low_event_sites)
                df.loc[mask, "ValidationNotes"] += "Fewer than 10 events; "
                df.loc[mask, "ValidationColorKey"] += "watershed_or_events;"
                row_delete_indices.update(df[mask].index.tolist())

            # Invalid Sample Date
            mask = df["Sample Date"].isna()
            df.loc[mask, "ValidationNotes"] += "Missing or invalid Sample Date; "
            df.loc[mask, "ValidationColorKey"] += "time;"
            row_delete_indices.update(df[mask].index.tolist())

            # Invalid Sample Time
            def invalid_time_format(t):
                try:
                    hour = int(str(t).split(":")[0])
                    return False
                except:
                    return True

            if "Sample Time Final Format" in df.columns:
                mask = df["Sample Time Final Format"].apply(invalid_time_format)
                df.loc[mask, "ValidationNotes"] += "Unparsable Sample Time; "
                df.loc[mask, "ValidationColorKey"] += "time;"
                row_delete_indices.update(df[mask].index.tolist())

            # Missing core parameters
            for idx, row in df.iterrows():
                if all((pd.isna(row[p]) or row[p] == 0) for p in core_params if p in df.columns):
                    df.at[idx, "ValidationNotes"] += "All core parameters missing or invalid; "
                    df.at[idx, "ValidationColorKey"] += "range;"
                    row_delete_indices.add(idx)

            # Standard ranges (EPA/TCEQ)
            standard_ranges = {
                "pH (standard units)": (6.5, 9.0),
                "Dissolved Oxygen (mg/L) Average": (5.0, 14.0),
                "Conductivity (?S/cm)": (50, 1500),
                "Salinity (ppt)": (0, 35),
                "Water Temperature (° C)": (0, 35),
                "Air Temperature (° C)": (-10, 50),
                "Turbidity": (0, 1000),
                "E. Coli Average": (1, 235),
                "Secchi Disk Transparency - Average": (0.2, 5),
                "Nitrate-Nitrogen VALUE (ppm or mg/L)": (0, 10),
                "Orthophosphate": (0, 0.5),
                "DO (%)": (80, 120),
                "Total Phosphorus (mg/L)": (0, 0.05)
            }

            for col, (min_val, max_val) in standard_ranges.items():
                if col in df.columns:
                    mask = (df[col] < min_val) | (df[col] > max_val)
                    df.loc[mask, "ValidationNotes"] += f"{col} out of range [{min_val}-{max_val}]; "
                    df.loc[mask, "ValidationColorKey"] += "range;"
                    df.loc[mask, col] = np.nan

            # Contextual outliers
            for col in standard_ranges:
                if col in df.columns and "Site ID: Site Name" in df.columns:
                    grouped = df[[col, "Site ID: Site Name"]].dropna().groupby("Site ID: Site Name")
                    means = grouped.transform('mean')[col]
                    stds = grouped.transform('std')[col]
                    z_scores = (df[col] - means) / stds
                    mask = abs(z_scores) > 3
                    df.loc[mask, "ValidationNotes"] += f"{col} is a contextual outlier (>3 std); "
                    df.loc[mask, "ValidationColorKey"] += "contextual_outlier;"
                    df.loc[mask, col] = np.nan

            if "Chemical Reagents Used" in df.columns:
                mask = df["Chemical Reagents Used"].astype(str).str.contains("expired", case=False, na=False)
                df.loc[mask, "ValidationNotes"] += "Expired reagents used; "
                df.loc[mask, "ValidationColorKey"] += "expired;"
                df.loc[mask, "Chemical Reagents Used"] = np.nan

            if "Comments" in df.columns:
                empty = df["Comments"].isna() | (df["Comments"].astype(str).str.strip() == "")
                flagged = df["ValidationNotes"] != ""
                mask = flagged & empty
                df.loc[mask, "ValidationNotes"] += "No explanation in Comments; "
                df.loc[mask, "ValidationColorKey"] += "comments;"

            replaced = df.replace(to_replace=r'(?i)\b(valid|invalid)\b', value='', regex=True)
            changed = replaced != df
            df.update(replaced)
            df.loc[changed.any(axis=1), "TransformNotes"] += "Removed 'valid/invalid'; "

            if "Site ID: Site Name" in df.columns and "Sample Date" in df.columns:
                df.sort_values(by=["Site ID: Site Name", "Sample Date"], inplace=True)

            df_clean = df.drop(index=list(row_delete_indices))

            clean_path = input_path.replace(".xlsx", "_cleaned_GENERAL.xlsx")
            annotated_path = input_path.replace(".xlsx", "_annotated_GENERAL.xlsx")
            df_clean.to_excel(clean_path, index=False)
            df.to_excel(annotated_path, index=False)

            st.success("✅ GENERAL validation complete.")
            st.download_button("📅 Download cleaned file", data=open(clean_path, 'rb').read(), file_name="cleaned_GENERAL.xlsx")
            st.download_button("📅 Download annotated file", data=open(annotated_path, 'rb').read(), file_name="annotated_GENERAL.xlsx")


# ------------------------ 3. CORE Validation Tab ------------------------
with tabs[2]:
    st.header("2️⃣ CORE Validation")

    uploaded_file_core = st.file_uploader("📂 Upload the cleaned_GENERAL.xlsx file for CORE Validation", type=["xlsx"], key="core_upload")

    if uploaded_file_core:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded_file_core.read())
            input_path = tmp.name

        df = pd.read_excel(input_path)
        df["CORE_Notes"] = ""
        df["CORE_ChangeNotes"] = ""

        run_core = st.button("✅ Run CORE Validation")
        if run_core:
            row_delete_indices = set()
            core_columns = [
                "Sample Depth (meters)", "Total Depth (meters)",
                "Dissolved Oxygen (mg/L) 1st titration", "Dissolved Oxygen (mg/L) 2nd titration",
                "Secchi Disk Transparency - Average", "Conductivity (?S/cm)",
                "Standard Value", "Chemical Reagents Used"
            ]

            def log_change(col, idx, new_val, reason):
                df.at[idx, "CORE_ChangeNotes"] += f"{col} → {new_val} ({reason}); "

            # Sample depth validation
            if "Sample Depth (meters)" in df.columns and "Total Depth (meters)" in df.columns:
                for idx, row in df.iterrows():
                    sample = row["Sample Depth (meters)"]
                    total = row["Total Depth (meters)"]
                    if not (sample == 0.3 or np.isclose(sample, total / 2, atol=0.05)):
                        df.at[idx, "CORE_Notes"] += "Sample Depth not 0.3m or mid-depth; "

            # Flow severity vs depth = 0
            if "Flow Severity" in df.columns and "Total Depth (meters)" in df.columns:
                mask = (df["Total Depth (meters)"] == 0) & (df["Flow Severity"] != 6)
                df.loc[mask, "CORE_Notes"] += "Zero Depth with non-dry flow; "
                row_delete_indices.update(df[mask].index.tolist())

            # DO titration difference > 0.5
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

            # Secchi Disk significant figures and comparison to depth
            secchi = "Secchi Disk Transparency - Average"
            zeroed_columns = []
            if secchi in df.columns:
                numeric_col = pd.to_numeric(df[secchi], errors="coerce").fillna(0)
                if numeric_col.eq(0).all():
                    zeroed_columns.append(secchi)
                df.loc[~df[secchi].apply(lambda v: len(str(v).replace(".", "").lstrip("0")) <= 2), "CORE_Notes"] += "Secchi not 2 significant figures; "
                df.loc[df[secchi] > df["Total Depth (meters)"], "CORE_Notes"] += "Secchi > Depth; "

            # Conductivity validation ±20% of Standard Value
            cond_col = "Conductivity (?S/cm)"
            if cond_col in df.columns and "Standard Value" in df.columns:
                cond = df[cond_col]
                std = df["Standard Value"]
                good = (cond >= 0.8 * std) & (cond <= 1.2 * std)
                df.loc[~good, "CORE_Notes"] += "Conductivity outside ±20%; "
                df.loc[~good, cond_col] = np.nan

            # Estimated TDS
            if cond_col in df.columns:
                df["TDS Calculated"] = df[cond_col] * 0.65
                for idx in df.index:
                    log_change("TDS", idx, df.at[idx, "TDS Calculated"], "Estimated TDS = Conductivity × 0.65")

            # Calibration date-time difference > 24h
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

            # Rounding pH and Temp
            if "pH (standard units)" in df.columns:
                df["pH Rounded"] = df["pH (standard units)"].round(1)
                for idx in df.index:
                    log_change("pH", idx, df.at[idx, "pH Rounded"], "Rounded to 0.1")
            if "Water Temperature (° C)" in df.columns:
                df["Water Temp Rounded"] = df["Water Temperature (° C)"].round(1)
                for idx in df.index:
                    log_change("Temp", idx, df.at[idx, "Water Temp Rounded"], "Rounded to 0.1")

            # Format validations
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

            # حذف کامل ردیف‌هایی که باید حذف بشن (بر اساس شرایط خاص)
            df_clean = df.drop(index=row_delete_indices)

            clean_path = input_path.replace(".xlsx", "_cleaned_CORE.xlsx")
            annotated_path = input_path.replace(".xlsx", "_annotated_CORE.xlsx")
            df_clean.to_excel(clean_path, index=False)
            df.to_excel(annotated_path, index=False)

            st.success("✅ CORE validation files generated.")
            st.download_button("📥 Download cleaned file", data=open(clean_path, 'rb').read(), file_name="cleaned_CORE.xlsx")
            st.download_button("📥 Download annotated file", data=open(annotated_path, 'rb').read(), file_name="annotated_CORE.xlsx")


# ------------------------ 4. ECOLI Validation Tab ------------------------
with tabs[3]:
    st.header("3️⃣ ECOLI Validation")

    uploaded_file_ecoli = st.file_uploader("📂 Upload the cleaned_GENERAL.xlsx file for ECOLI Validation", type=["xlsx"], key="ecoli_upload")

    if uploaded_file_ecoli:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded_file_ecoli.read())
            input_path = tmp.name

        df = pd.read_excel(input_path)
        df["ECOLI_ValidationNotes"] = ""
        df["ECOLI_ChangeNotes"] = ""

        run_ecoli = st.button("✅ Run ECOLI Validation")
        if run_ecoli:
            all_zero_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col]) and (df[col].fillna(0) == 0).all()]

            def log_change(idx, text):
                df.at[idx, "ECOLI_ChangeNotes"] += text + "; "

            def log_issue(idx, text):
                df.at[idx, "ECOLI_ValidationNotes"] += text + "; "

            # Temperature check
            col_temp = "Incubation temperature is 33° C +/- 3° C"
            if col_temp in df.columns and col_temp not in all_zero_cols:
                df[col_temp] = pd.to_numeric(df[col_temp], errors="coerce")
                mask = (df[col_temp] < 30) | (df[col_temp] > 36)
                df.loc[mask, "ECOLI_ValidationNotes"] += "Incubation temperature not in 30–36°C range; "
                df.loc[mask, col_temp] = np.nan

            # Time check
            col_time = "Incubation time is between 28-31 hours"
            if col_time in df.columns and col_time not in all_zero_cols:
                df[col_time] = pd.to_numeric(df[col_time], errors="coerce")
                mask = (df[col_time] < 28) | (df[col_time] > 31)
                df.loc[mask, "ECOLI_ValidationNotes"] += "Incubation time not in 28–31h range; "
                df.loc[mask, col_time] = np.nan

            # Colony count check
            for col in ["Sample 1: Colonies Counted", "Sample 2: Colonies Counted"]:
                if col in df.columns and col not in all_zero_cols:
                    mask = df[col] > 200
                    df.loc[mask, "ECOLI_ValidationNotes"] += f"{col} > 200 colonies; "
                    df.loc[mask, col] = np.nan

            # Field blank check
            col_blank = "No colony growth on Field Blank"
            if col_blank in df.columns and col_blank not in all_zero_cols:
                bad_blank = df[col_blank].astype(str).str.lower().isin(["no", "false", "n"])
                df.loc[bad_blank, "ECOLI_ValidationNotes"] += "Colony growth detected in field blank; "

            # E. Coli = 0
            col_ecoli = "E. Coli Average"
            if col_ecoli in df.columns and col_ecoli not in all_zero_cols:
                mask = df[col_ecoli] == 0
                df.loc[mask, "ECOLI_ValidationNotes"] += "E. coli = 0; "
                df.loc[mask, col_ecoli] = np.nan

            # Rounding to 2 significant figures
            def round_sig_figs(n):
                try:
                    if n == 0 or pd.isna(n): return n
                    return round(n, -int(np.floor(np.log10(abs(n)))) + 1)
                except:
                    return n

            if col_ecoli in df.columns and col_ecoli not in all_zero_cols:
                df["E. Coli Rounded"] = df[col_ecoli].round(0).astype("Int64")
                df["E. Coli Rounded (2SF)"] = df["E. Coli Rounded"].apply(round_sig_figs)
                for idx in df.index:
                    orig = df.at[idx, col_ecoli]
                    rounded = df.at[idx, "E. Coli Rounded (2SF)"]
                    if not pd.isna(orig) and not pd.isna(rounded):
                        log_change(idx, f"E. coli {orig} → {rounded} (rounded to 2 significant figures)")

            # Dilution factor validation
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
                cols = [f"{prefix}: Colonies Counted", f"{prefix}: Dilution Factor (Manual)",
                        f"{prefix}: Sample Size (mL)", f"{prefix}: Colony Forming Units per 100mL"]
                if all(c in df.columns and c not in all_zero_cols for c in cols):
                    valid = df.apply(lambda row: check_dilution(row, prefix), axis=1)
                    df.loc[~valid, "ECOLI_ValidationNotes"] += f"{prefix} CFU formula mismatch; "

            # Final clean rows: keep only rows with no validation issues
            df_clean = df[df["ECOLI_ValidationNotes"].str.strip() == ""]

            clean_path = input_path.replace(".xlsx", "_cleaned_ECOLI.xlsx")
            annotated_path = input_path.replace(".xlsx", "_annotated_ECOLI.xlsx")
            df_clean.to_excel(clean_path, index=False)
            df.to_excel(annotated_path, index=False)

            st.success("✅ ECOLI validation files generated.")
            st.download_button("📥 Download cleaned file", data=open(clean_path, 'rb').read(), file_name="cleaned_ECOLI.xlsx")
            st.download_button("📥 Download annotated file", data=open(annotated_path, 'rb').read(), file_name="annotated_ECOLI.xlsx")


# ------------------------ 5. ADVANCED Validation Tab ------------------------
with tabs[4]:
    st.header("4️⃣ ADVANCED Validation")

    uploaded_file_adv = st.file_uploader("📂 Upload the cleaned_GENERAL_cleaned_ECOLI.xlsx file for ADVANCED Validation", type=["xlsx"], key="adv_upload")

    if uploaded_file_adv:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded_file_adv.read())
            input_path = tmp.name

        df = pd.read_excel(input_path)
        df["ADVANCED_ValidationNotes"] = ""
        df["ADVANCED_ChangeNotes"] = ""

        run_adv = st.button("✅ Run ADVANCED Validation")
        if run_adv:
            all_zero_cols = [
                col for col in df.columns
                if pd.api.types.is_numeric_dtype(df[col]) and (df[col].fillna(0) == 0).all()
            ]

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

            # حذف فقط سلول‌هایی که ایراد دارند، و حفظ سایر اطلاعات
            df_clean = df[df["ADVANCED_ValidationNotes"].str.strip() == ""]

            clean_path = input_path.replace(".xlsx", "_cleaned_ADVANCED.xlsx")
            annotated_path = input_path.replace(".xlsx", "_annotated_ADVANCED.xlsx")
            df_clean.to_excel(clean_path, index=False)
            df.to_excel(annotated_path, index=False)

            st.success("✅ ADVANCED validation files generated.")
            st.download_button("📥 Download cleaned file", data=open(clean_path, 'rb').read(), file_name="cleaned_ADVANCED.xlsx")
            st.download_button("📥 Download annotated file", data=open(annotated_path, 'rb').read(), file_name="annotated_ADVANCED.xlsx")

# ------------------------ 6. RIPARIAN Validation Tab ------------------------
with tabs[5]:
    st.header("5️⃣ RIPARIAN Validation")

    uploaded_file_rip = st.file_uploader("📂 Upload the cleaned_GENERAL_cleaned_ECOLI_cleaned_ADVANCED.xlsx file for RIPARIAN Validation", type=["xlsx"], key="rip_upload")

    if uploaded_file_rip:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded_file_rip.read())
            input_path = tmp.name

        df = pd.read_excel(input_path)
        df["RIPARIAN_ValidationNotes"] = ""
        df["RIPARIAN_ChangeNotes"] = ""

        run_rip = st.button("✅ Run RIPARIAN Validation")
        if run_rip:
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
                            df.at[idx, col] = np.nan  # Clean just the cell

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

            df_clean = df[df["RIPARIAN_ValidationNotes"].str.strip() == ""]

            clean_path = input_path.replace(".xlsx", "_cleaned_RIPARIAN.xlsx")
            annotated_path = input_path.replace(".xlsx", "_annotated_RIPARIAN.xlsx")
            df_clean.to_excel(clean_path, index=False)
            df.to_excel(annotated_path, index=False)

            st.success("✅ RIPARIAN validation files generated.")
            st.download_button("📥 Download cleaned file", data=open(clean_path, 'rb').read(), file_name="cleaned_RIPARIAN.xlsx")
            st.download_button("📥 Download annotated file", data=open(annotated_path, 'rb').read(), file_name="annotated_RIPARIAN.xlsx")

# ------------------------ 7. Final Output Tab ------------------------
with tabs[6]:
    st.header("📦 Final Output: Run All Validations Sequentially")

    uploaded_master_file = st.file_uploader("📂 Upload the original Excel file (raw input)", type=["xlsx"], key="final_master_file")

    if uploaded_master_file:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded_master_file.read())
            input_path = tmp.name

        run_all = st.button("🌀 Run Full Validation Sequence")

        if run_all:
            try:
                def run_general_validation(input_path):
                    df = pd.read_excel(input_path)
                    # (Add your GENERAL validation logic here from Tab 1)
                    cleaned_path = input_path.replace(".xlsx", "_cleaned_GENERAL.xlsx")
                    annotated_path = input_path.replace(".xlsx", "_annotated_GENERAL.xlsx")
                    df.to_excel(annotated_path, index=False)
                    df_clean = df[df["ValidationNotes"].str.strip() == ""]  # or your final cleaned logic
                    df_clean.to_excel(cleaned_path, index=False)
                    return cleaned_path, annotated_path

                def run_core_validation(input_path):
                    df = pd.read_excel(input_path)
                    # (Add your CORE validation logic here from Tab 2)
                    cleaned_path = input_path.replace(".xlsx", "_cleaned_CORE.xlsx")
                    annotated_path = input_path.replace(".xlsx", "_annotated_CORE.xlsx")
                    df.to_excel(annotated_path, index=False)
                    df_clean = df[df["CORE_Notes"].str.strip() == ""]
                    df_clean.to_excel(cleaned_path, index=False)
                    return cleaned_path, annotated_path

                def run_ecoli_validation(input_path):
                    df = pd.read_excel(input_path)
                    # (Add your ECOLI validation logic here from Tab 3)
                    cleaned_path = input_path.replace(".xlsx", "_cleaned_ECOLI.xlsx")
                    annotated_path = input_path.replace(".xlsx", "_annotated_ECOLI.xlsx")
                    df.to_excel(annotated_path, index=False)
                    df_clean = df[df["ECOLI_ValidationNotes"].str.strip() == ""]
                    df_clean.to_excel(cleaned_path, index=False)
                    return cleaned_path, annotated_path

                def run_advanced_validation(input_path):
                    df = pd.read_excel(input_path)
                    # (Add your ADVANCED validation logic here from Tab 4)
                    cleaned_path = input_path.replace(".xlsx", "_cleaned_ADVANCED.xlsx")
                    annotated_path = input_path.replace(".xlsx", "_annotated_ADVANCED.xlsx")
                    df.to_excel(annotated_path, index=False)
                    df_clean = df[df["ADVANCED_ValidationNotes"].str.strip() == ""]
                    df_clean.to_excel(cleaned_path, index=False)
                    return cleaned_path, annotated_path

                def run_riparian_validation(input_path):
                    df = pd.read_excel(input_path)
                    # (Add your RIPARIAN validation logic here from Tab 5)
                    cleaned_path = input_path.replace(".xlsx", "_cleaned_RIPARIAN.xlsx")
                    annotated_path = input_path.replace(".xlsx", "_annotated_RIPARIAN.xlsx")
                    df.to_excel(annotated_path, index=False)
                    df_clean = df[df["RIPARIAN_ValidationNotes"].str.strip() == ""]
                    df_clean.to_excel(cleaned_path, index=False)
                    return cleaned_path, annotated_path

                # Run validations in sequence
                gen_clean, gen_anno = run_general_validation(input_path)
                core_clean, core_anno = run_core_validation(gen_clean)
                ecoli_clean, ecoli_anno = run_ecoli_validation(core_clean)
                adv_clean, adv_anno = run_advanced_validation(ecoli_clean)
                rip_clean, rip_anno = run_riparian_validation(adv_clean)

                # Final output files
                final_cleaned_path = "final_cleaned_validated_output.xlsx"
                final_annotated_path = "final_annotated_validated_output.xlsx"
                pd.read_excel(rip_clean).to_excel(final_cleaned_path, index=False)
                pd.read_excel(rip_anno).to_excel(final_annotated_path, index=False)

                st.success("✅ All validations completed successfully!")
                st.download_button("📥 Download Final Cleaned File", data=open(final_cleaned_path, 'rb').read(), file_name="final_cleaned_validated_output.xlsx")
                st.download_button("📥 Download Final Annotated File", data=open(final_annotated_path, 'rb').read(), file_name="final_annotated_validated_output.xlsx")

            except Exception as e:
                st.error(f"❌ Error during validation sequence: {e}")
