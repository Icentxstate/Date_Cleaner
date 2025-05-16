import streamlit as st
import pandas as pd
import numpy as np
import tempfile
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import os

st.set_page_config(layout="wide")
st.title("🧪 Water Quality Data Validation App")

# ==== تب‌ها ====
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
    uploaded_file = st.file_uploader("لطفاً فایل Excel را آپلود کنید:", type=["xlsx"])

    if uploaded_file:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded_file.read())
            input_path = tmp.name
        st.success("✅ فایل با موفقیت بارگذاری شد. حالا می‌توانید تب‌های بعدی را اجرا کنید.")
    else:
        st.warning("برای ادامه، ابتدا یک فایل Excel بارگذاری کنید.")

# ------------------------ 2. GENERAL Validation Tab ------------------------
with tabs[1]:
    st.header("1️⃣ GENERAL Validation")

    if uploaded_file:
        run_button = st.button("✅ اجرای اعتبارسنجی GENERAL")
        if run_button:
            df = pd.read_excel(input_path)
            df["ValidationNotes"] = ""
            df["ValidationColorKey"] = ""
            df["TransformNotes"] = ""

            color_map = {
                "flagged": "FF9999",
                "watershed_or_events": "FFCC99",
                "range": "FFFF99",
                "time": "99CCFF",
                "comments": "CCCCCC",
                "expired": "E6CCFF",
                "contextual_outlier": "FF66B2"
            }
            fills = {k: PatternFill(start_color=v, end_color=v, fill_type="solid") for k, v in color_map.items()}

            df.drop_duplicates(inplace=True)

            for col in ['Flag', 'Validation', 'QA/QC']:
                if col in df.columns:
                    mask = df[col].notna()
                    df.loc[mask, "ValidationNotes"] += f"Flagged in {col}; "
                    df.loc[mask, "ValidationColorKey"] += "flagged;"

            if "Group or Affiliation" in df.columns and "Site ID: Site Name" in df.columns:
                site_counts = df.groupby("Group or Affiliation")["Site ID: Site Name"].nunique()
                invalid_ws = site_counts[site_counts < 3].index
                mask = df["Group or Affiliation"].isin(invalid_ws)
                df.loc[mask, "ValidationNotes"] += "Less than 3 sites in watershed; "
                df.loc[mask, "ValidationColorKey"] += "watershed_or_events;"

            if "Site ID: Site Name" in df.columns and "Sample Date" in df.columns:
                df["Sample Date"] = pd.to_datetime(df["Sample Date"], errors='coerce')
                event_counts = df.groupby("Site ID: Site Name")["Sample Date"].nunique()
                low_event_sites = event_counts[event_counts < 10].index
                mask = df["Site ID: Site Name"].isin(low_event_sites)
                df.loc[mask, "ValidationNotes"] += "Fewer than 10 events; "
                df.loc[mask, "ValidationColorKey"] += "watershed_or_events;"

            standard_ranges = {
                "pH (standard units)": (3, 11),
                "Dissolved Oxygen (mg/L) Average": (0.5, 20),
                "Conductivity (?S/cm)": (10, 10000),
                "Salinity (ppt)": (0, 40),
                "Water Temperature (° C)": (0, 40),
                "Air Temperature (° C)": (-10, 50),
                "Turbidity": (0, 1000),
                "E. Coli Average": (1, 2000),
                "Secchi Disk Transparency - Average": (0.1, 5),
                "Nitrate-Nitrogen VALUE (ppm or mg/L)": (0, 10),
                "Orthophosphate": (0, 5)
            }
            for col, (min_val, max_val) in standard_ranges.items():
                if col in df.columns:
                    mask = (df[col] < min_val) | (df[col] > max_val)
                    df.loc[mask, "ValidationNotes"] += f"{col} out of range [{min_val}-{max_val}]; "
                    df.loc[mask, "ValidationColorKey"] += "range;"

            for col in standard_ranges:
                if col in df.columns and "Site ID: Site Name" in df.columns:
                    site_col = "Site ID: Site Name"
                    grouped = df[[site_col, col]].dropna().groupby(site_col)
                    means = grouped[col].transform('mean')
                    stds = grouped[col].transform('std')
                    z_scores = (df[col] - means) / stds
                    mask = abs(z_scores) > 3
                    df.loc[mask, "ValidationNotes"] += f"{col} is a contextual outlier (>3 std); "
                    df.loc[mask, "ValidationColorKey"] += "contextual_outlier;"

            def time_check(t):
                try:
                    hour = int(str(t).split(":" )[0])
                    return not (hour < 12 or hour >= 16)
                except:
                    return True

            if "Sample Time Final Format" in df.columns:
                mask = df["Sample Time Final Format"].apply(time_check)
                df.loc[mask, "ValidationNotes"] += "Sample time not within preferred range; "
                df.loc[mask, "ValidationColorKey"] += "time;"

            if "Comments" in df.columns:
                empty = df["Comments"].isna() | (df["Comments"].astype(str).str.strip() == "")
                flagged = df["ValidationNotes"] != ""
                mask = flagged & empty
                df.loc[mask, "ValidationNotes"] += "No explanation in Comments; "
                df.loc[mask, "ValidationColorKey"] += "comments;"

            if "Chemical Reagents Used" in df.columns:
                mask = df["Chemical Reagents Used"].astype(str).str.contains("expired", case=False, na=False)
                df.loc[mask, "ValidationNotes"] += "Expired reagents used; "
                df.loc[mask, "ValidationColorKey"] += "expired;"

            replaced = df.replace(to_replace=r'(?i)\b(valid|invalid)\b', value='', regex=True)
            changed = replaced != df
            df.update(replaced)
            df.loc[changed.any(axis=1), "TransformNotes"] += "Removed 'valid/invalid'; "

            if "Site ID: Site Name" in df.columns and "Sample Date" in df.columns:
                df.sort_values(by=["Site ID: Site Name", "Sample Date"], inplace=True)

            df_clean = df[df["ValidationNotes"].str.strip() == ""]

            clean_path = input_path.replace(".xlsx", "_cleaned_GENERAL.xlsx")
            annotated_path = input_path.replace(".xlsx", "_annotated_GENERAL.xlsx")
            df_clean.to_excel(clean_path, index=False)
            df.to_excel(annotated_path, index=False)

            st.success("✅ فایل‌های GENERAL ایجاد شدند.")
            st.download_button("📥 دانلود فایل cleaned", data=open(clean_path, 'rb').read(), file_name="cleaned_GENERAL.xlsx")
            st.download_button("📥 دانلود فایل annotated", data=open(annotated_path, 'rb').read(), file_name="annotated_GENERAL.xlsx")

# ------------------------ 3. CORE Validation Tab ------------------------
with tabs[2]:
    st.header("2️⃣ CORE Validation")

    uploaded_file_core = st.file_uploader("بارگذاری فایل cleaned_GENERAL.xlsx برای CORE", type=["xlsx"], key="core_upload")

    if uploaded_file_core:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded_file_core.read())
            input_path = tmp.name

        df = pd.read_excel(input_path)
        df["CORE_Notes"] = ""
        df["CORE_ChangeNotes"] = ""

        run_core = st.button("✅ اجرای اعتبارسنجی CORE")
        if run_core:
            colors = {
                "Sample Depth": "FFB6C1",
                "Zero Depth": "FA8072",
                "DO Difference": "FFD700",
                "DO Rounded": "F08080",
                "Secchi SigFig": "8FBC8F",
                "Secchi > Depth": "DA70D6",
                "Conductivity ±20%": "87CEFA",
                "TDS": "D3D3D3",
                "Calibration Δt": "FFA07A",
                "pH Rounded": "F0E68C",
                "Temp Rounded": "E6E6FA",
                "Conductivity Format": "CD5C5C",
                "Salinity Format": "7FFFD4",
                "Time Format": "FF69B4",
                "Distance Format": "FF8C00",
                "Pre-Cal Δt": "FFE4B5"
            }

            def log_change(col, idx, new_val, reason):
                df.at[idx, "CORE_ChangeNotes"] += f"{col} → {new_val} ({reason}); "

            if "Sample Depth (meters)" in df.columns and "Total Depth (meters)" in df.columns:
                for idx, row in df.iterrows():
                    sample = row["Sample Depth (meters)"]
                    total = row["Total Depth (meters)"]
                    if not (sample == 0.3 or np.isclose(sample, total / 2, atol=0.05)):
                        df.at[idx, "CORE_Notes"] += "Sample Depth; "

            if "Flow Severity" in df.columns and "Total Depth (meters)" in df.columns:
                mask = (df["Total Depth (meters)"] == 0) & (df["Flow Severity"] != 6)
                df.loc[mask, "CORE_Notes"] += "Zero Depth; "

            do1 = "Dissolved Oxygen (mg/L) 1st titration"
            do2 = "Dissolved Oxygen (mg/L) 2nd titration"
            if do1 in df.columns and do2 in df.columns:
                diff = (df[do1] - df[do2]).abs()
                df.loc[diff > 0.5, "CORE_Notes"] += "DO Difference; "
                df["DO1 Rounded"] = df[do1].round(1)
                df["DO2 Rounded"] = df[do2].round(1)
                for idx in df.index:
                    log_change("DO1", idx, df.at[idx, "DO1 Rounded"], "Rounded to 0.1")
                    log_change("DO2", idx, df.at[idx, "DO2 Rounded"], "Rounded to 0.1")

            def sig_figs(val):
                try:
                    digits = str(val).replace(".", "").lstrip("0")
                    return len(digits) <= 2
                except:
                    return True

            secchi = "Secchi Disk Transparency - Average"
            zeroed_columns = []
            if secchi in df.columns:
                numeric_col = pd.to_numeric(df[secchi], errors="coerce").fillna(0)
                if numeric_col.eq(0).all():
                    zeroed_columns.append(secchi)
                df.loc[~df[secchi].apply(sig_figs), "CORE_Notes"] += "Secchi SigFig; "
                df.loc[df[secchi] > df["Total Depth (meters)"], "CORE_Notes"] += "Secchi > Depth; "

            cond_col = "Conductivity (?S/cm)"
            if cond_col in df.columns and "Standard Value" in df.columns:
                cond = df[cond_col]
                std = df["Standard Value"]
                good = (cond >= 0.8 * std) & (cond <= 1.2 * std)
                df.loc[~good, "CORE_Notes"] += "Conductivity ±20%; "

            if cond_col in df.columns:
                df["TDS Calculated"] = df[cond_col] * 0.65
                for idx in df.index:
                    log_change("TDS", idx, df.at[idx, "TDS Calculated"], "Calculated")

            if "Sampling Time" in df.columns:
                df["Sampling Time"] = pd.to_datetime(df["Sampling Time"], errors='coerce')
                if "Post-Test Calibration" in df.columns:
                    df["Post-Test Calibration"] = pd.to_datetime(df["Post-Test Calibration"], errors='coerce')
                    delta = (df["Sampling Time"] - df["Post-Test Calibration"]).abs().dt.total_seconds() / 3600
                    df.loc[delta > 24, "CORE_Notes"] += "Calibration Δt; "
                if "Pre-Test Calibration" in df.columns:
                    df["Pre-Test Calibration"] = pd.to_datetime(df["Pre-Test Calibration"], errors='coerce')
                    delta = (df["Sampling Time"] - df["Pre-Test Calibration"]).abs().dt.total_seconds() / 3600
                    df.loc[delta > 24, "CORE_Notes"] += "Pre-Cal Δt; "

            if "pH (standard units)" in df.columns:
                df["pH Rounded"] = df["pH (standard units)"].round(1)
                for idx in df.index:
                    log_change("pH", idx, df.at[idx, "pH Rounded"], "Rounded to 0.1")

            if "Water Temperature (° C)" in df.columns:
                df["Water Temp Rounded"] = df["Water Temperature (° C)"].round(1)
                for idx in df.index:
                    log_change("Temp", idx, df.at[idx, "Water Temp Rounded"], "Rounded to 0.1")

            def check_cond_format(val):
                if pd.isna(val): return True
                if val > 100:
                    return len(str(int(round(val)))) <= 3
                else:
                    return float(val).is_integer()

            if cond_col in df.columns:
                df.loc[~df[cond_col].apply(check_cond_format), "CORE_Notes"] += "Conductivity Format; "

            def salinity_format(val):
                if pd.isna(val): return ""
                elif val < 2.0:
                    return "< 2.0"
                else:
                    return round(val, 1)

            if "Salinity (ppt)" in df.columns:
                df["Salinity Formatted"] = df["Salinity (ppt)"].apply(salinity_format)
                for idx in df.index:
                    log_change("Salinity", idx, df.at[idx, "Salinity Formatted"], "Formatted")

            if "Time Spent Sampling/Traveling" in df.columns:
                non_minutes = df["Time Spent Sampling/Traveling"].apply(lambda x: not isinstance(x, (int, float, np.integer, np.floating)))
                df.loc[non_minutes, "CORE_Notes"] += "Time Format; "

            if "Roundtrip Distance Traveled" in df.columns:
                non_miles = df["Roundtrip Distance Traveled"].apply(lambda x: not isinstance(x, (int, float, np.integer, np.floating)))
                df.loc[non_miles, "CORE_Notes"] += "Distance Format; "

            def has_real_issues(idx):
                note = str(df.at[idx, "CORE_Notes"]).strip()
                if not note:
                    return True
                if "Secchi > Depth" in note and secchi in zeroed_columns:
                    return True
                return False

            df_clean = df[[has_real_issues(idx) for idx in df.index]]

            clean_path = input_path.replace(".xlsx", "_cleaned_CORE.xlsx")
            annotated_path = input_path.replace(".xlsx", "_annotated_CORE.xlsx")
            df_clean.to_excel(clean_path, index=False)
            df.to_excel(annotated_path, index=False)

            st.success("✅ فایل‌های CORE ایجاد شدند.")
            st.download_button("📥 دانلود فایل cleaned", data=open(clean_path, 'rb').read(), file_name="cleaned_CORE.xlsx")
            st.download_button("📥 دانلود فایل annotated", data=open(annotated_path, 'rb').read(), file_name="annotated_CORE.xlsx")

# ------------------------ 4. ECOLI Validation Tab ------------------------
with tabs[3]:
    st.header("3️⃣ ECOLI Validation")

    uploaded_file_ecoli = st.file_uploader("بارگذاری فایل cleaned_GENERAL.xlsx برای ECOLI", type=["xlsx"], key="ecoli_upload")

    if uploaded_file_ecoli:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded_file_ecoli.read())
            input_path = tmp.name

        df = pd.read_excel(input_path)
        df["ECOLI_ValidationNotes"] = ""
        df["ECOLI_ChangeNotes"] = ""

        run_ecoli = st.button("✅ اجرای اعتبارسنجی ECOLI")
        if run_ecoli:
            all_zero_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col]) and (df[col].fillna(0) == 0).all()]

            def log_change(idx, text):
                df.at[idx, "ECOLI_ChangeNotes"] += text + "; "

            def log_issue(idx, text):
                df.at[idx, "ECOLI_ValidationNotes"] += text + "; "

            col_temp = "Incubation temperature is 33° C +/- 3° C"
            if col_temp in df.columns and col_temp not in all_zero_cols:
                df[col_temp] = pd.to_numeric(df[col_temp], errors="coerce")
                df.loc[(df[col_temp] < 30) | (df[col_temp] > 36), "ECOLI_ValidationNotes"] += "Incubation temp not 30–36°C; "

            col_time = "Incubation time is between 28-31 hours"
            if col_time in df.columns and col_time not in all_zero_cols:
                df[col_time] = pd.to_numeric(df[col_time], errors="coerce")
                df.loc[(df[col_time] < 28) | (df[col_time] > 31), "ECOLI_ValidationNotes"] += "Incubation time not 28–31h; "

            for col in ["Sample 1: Colonies Counted", "Sample 2: Colonies Counted"]:
                if col in df.columns and col not in all_zero_cols:
                    df.loc[df[col] > 200, "ECOLI_ValidationNotes"] += f"{col} > 200 colonies; "

            col_blank = "No colony growth on Field Blank"
            if col_blank in df.columns and col_blank not in all_zero_cols:
                bad_blank = df[col_blank].astype(str).str.lower().isin(["no", "false", "n"])
                df.loc[bad_blank, "ECOLI_ValidationNotes"] += "Colony growth on field blank; "

            col_ecoli = "E. Coli Average"
            if col_ecoli in df.columns:
                if col_ecoli in all_zero_cols:
                    pass
                else:
                    df.loc[df[col_ecoli] == 0, "ECOLI_ValidationNotes"] += "E. coli = 0; "
                    df = df[df[col_ecoli] != 0]

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
                        log_change(idx, f"E. coli {orig} → {rounded} (rounded to 2 SF)")

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
                    df.loc[~valid, "ECOLI_ValidationNotes"] += f"{prefix} CFU mismatch; "

            df_clean = df[df["ECOLI_ValidationNotes"] == ""]

            clean_path = input_path.replace(".xlsx", "_cleaned_ECOLI.xlsx")
            annotated_path = input_path.replace(".xlsx", "_annotated_ECOLI.xlsx")
            df_clean.to_excel(clean_path, index=False)
            df.to_excel(annotated_path, index=False)

            wb = load_workbook(annotated_path)
            ws = wb.active
            val_idx = [cell.value for cell in ws[1]].index("ECOLI_ValidationNotes")
            red_fill = PatternFill(start_color="FFFF0000", end_color="FFFF0000", fill_type="solid")

            for row in ws.iter_rows(min_row=2):
                note = row[val_idx].value
                if note and str(note).strip():
                    for cell in row:
                        cell.fill = red_fill

            wb.save(annotated_path)

            st.success("✅ فایل‌های ECOLI ایجاد شدند.")
            st.download_button("📥 دانلود فایل cleaned", data=open(clean_path, 'rb').read(), file_name="cleaned_ECOLI.xlsx")
            st.download_button("📥 دانلود فایل annotated", data=open(annotated_path, 'rb').read(), file_name="annotated_ECOLI.xlsx")

# ------------------------ 5. ADVANCED Validation Tab ------------------------
with tabs[4]:
    st.header("4️⃣ ADVANCED Validation")

    uploaded_file_adv = st.file_uploader("بارگذاری فایل cleaned_GENERAL_cleaned_ECOLI.xlsx برای ADVANCED", type=["xlsx"], key="adv_upload")

    if uploaded_file_adv:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded_file_adv.read())
            input_path = tmp.name

        df = pd.read_excel(input_path)
        df["ADVANCED_ValidationNotes"] = ""
        df["ADVANCED_ChangeNotes"] = ""

        run_adv = st.button("✅ اجرای اعتبارسنجی ADVANCED")
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

            def row_has_real_issue(idx):
                note = str(df.at[idx, "ADVANCED_ValidationNotes"]).lower().strip()
                if not note:
                    return True
                for col in all_zero_cols:
                    if col.lower() in note:
                        return True
                return False

            df_clean = df[[row_has_real_issue(idx) for idx in df.index]]

            clean_path = input_path.replace(".xlsx", "_cleaned_ADVANCED.xlsx")
            annotated_path = input_path.replace(".xlsx", "_annotated_ADVANCED.xlsx")
            df_clean.to_excel(clean_path, index=False)
            df.to_excel(annotated_path, index=False)

            wb = load_workbook(annotated_path)
            ws = wb.active
            val_idx = [cell.value for cell in ws[1]].index("ADVANCED_ValidationNotes")
            red_fill = PatternFill(start_color="FFFF0000", end_color="FFFF0000", fill_type="solid")

            for row in ws.iter_rows(min_row=2):
                note = row[val_idx].value
                if note and str(note).strip():
                    for cell in row:
                        cell.fill = red_fill

            wb.save(annotated_path)

            st.success("✅ فایل‌های ADVANCED ایجاد شدند.")
            st.download_button("📥 دانلود فایل cleaned", data=open(clean_path, 'rb').read(), file_name="cleaned_ADVANCED.xlsx")
            st.download_button("📥 دانلود فایل annotated", data=open(annotated_path, 'rb').read(), file_name="annotated_ADVANCED.xlsx")
# ------------------------ 6. RIPARIAN Validation Tab ------------------------
with tabs[5]:
    st.header("5️⃣ RIPARIAN Validation")

    uploaded_file_rip = st.file_uploader("بارگذاری فایل cleaned_GENERAL_cleaned_ECOLI_cleaned_ADVANCED.xlsx برای RIPARIAN", type=["xlsx"], key="rip_upload")

    if uploaded_file_rip:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded_file_rip.read())
            input_path = tmp.name

        df = pd.read_excel(input_path)
        df["RIPARIAN_ValidationNotes"] = ""
        df["RIPARIAN_ChangeNotes"] = ""

        run_rip = st.button("✅ اجرای اعتبارسنجی RIPARIAN")
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

            def row_has_only_zeroed_issues(idx):
                note = str(df.at[idx, "RIPARIAN_ValidationNotes"]).strip().lower()
                if not note:
                    return True
                for col in available_cols:
                    if col in zeroed_columns:
                        continue
                    if pd.isna(df.at[idx, col]) or str(df.at[idx, col]).strip() == "":
                        return False
                return True

            clean_df = df[[row_has_only_zeroed_issues(idx) for idx in df.index]]

            clean_path = input_path.replace(".xlsx", "_cleaned_RIPARIAN.xlsx")
            annotated_path = input_path.replace(".xlsx", "_annotated_RIPARIAN.xlsx")
            clean_df.to_excel(clean_path, index=False)
            df.to_excel(annotated_path, index=False)

            wb = load_workbook(annotated_path)
            ws = wb.active
            val_col_idx = [cell.value for cell in ws[1]].index("RIPARIAN_ValidationNotes")
            red_fill = PatternFill(start_color="FFFF0000", end_color="FFFF0000", fill_type="solid")

            for row in ws.iter_rows(min_row=2):
                note = row[val_col_idx].value
                if note and str(note).strip():
                    for cell in row:
                        cell.fill = red_fill

            wb.save(annotated_path)

            st.success("✅ فایل‌های RIPARIAN ایجاد شدند.")
            st.download_button("📥 دانلود فایل cleaned", data=open(clean_path, 'rb').read(), file_name="cleaned_RIPARIAN.xlsx")
            st.download_button("📥 دانلود فایل annotated", data=open(annotated_path, 'rb').read(), file_name="annotated_RIPARIAN.xlsx")


# ------------------------ 7. Final Output Tab ------------------------
with tabs[6]:
    st.header("📦 Final Output")

    st.markdown("در این بخش می‌توانید همه فایل‌های cleaned و annotated تولیدشده در مراحل قبل را بارگذاری و با هم ادغام کنید.")

    uploaded_general_cleaned = st.file_uploader("📂 فایل cleaned GENERAL", type=["xlsx"], key="final_general_cleaned")
    uploaded_core_cleaned = st.file_uploader("📂 فایل cleaned CORE", type=["xlsx"], key="final_core_cleaned")
    uploaded_ecoli_cleaned = st.file_uploader("📂 فایل cleaned ECOLI", type=["xlsx"], key="final_ecoli_cleaned")
    uploaded_advanced_cleaned = st.file_uploader("📂 فایل cleaned ADVANCED", type=["xlsx"], key="final_advanced_cleaned")
    uploaded_riparian_cleaned = st.file_uploader("📂 فایل cleaned RIPARIAN", type=["xlsx"], key="final_riparian_cleaned")

    uploaded_general_annotated = st.file_uploader("📂 فایل annotated GENERAL", type=["xlsx"], key="final_general_annotated")
    uploaded_core_annotated = st.file_uploader("📂 فایل annotated CORE", type=["xlsx"], key="final_core_annotated")
    uploaded_ecoli_annotated = st.file_uploader("📂 فایل annotated ECOLI", type=["xlsx"], key="final_ecoli_annotated")
    uploaded_advanced_annotated = st.file_uploader("📂 فایل annotated ADVANCED", type=["xlsx"], key="final_advanced_annotated")
    uploaded_riparian_annotated = st.file_uploader("📂 فایل annotated RIPARIAN", type=["xlsx"], key="final_riparian_annotated")

    if st.button("🌀 ترکیب و ساخت فایل نهایی"):
        cleaned_files = [uploaded_general_cleaned, uploaded_core_cleaned, uploaded_ecoli_cleaned, uploaded_advanced_cleaned, uploaded_riparian_cleaned]
        annotated_files = [uploaded_general_annotated, uploaded_core_annotated, uploaded_ecoli_annotated, uploaded_advanced_annotated, uploaded_riparian_annotated]

        if not all(cleaned_files + annotated_files):
            st.error("⛔ لطفاً تمام فایل‌های cleaned و annotated را بارگذاری کنید.")
        else:
            try:
                from functools import reduce
                dfs_cleaned = [pd.read_excel(file) for file in cleaned_files]
                dfs_annotated = [pd.read_excel(file) for file in annotated_files]

                df_final_cleaned = reduce(lambda left, right: pd.merge(left, right, how="outer"), dfs_cleaned)
                df_final_annotated = reduce(lambda left, right: pd.merge(left, right, how="outer"), dfs_annotated)

                cleaned_path = "final_cleaned_validated_output.xlsx"
                annotated_path = "final_annotated_validated_output.xlsx"

                df_final_cleaned.to_excel(cleaned_path, index=False)
                df_final_annotated.to_excel(annotated_path, index=False)

                st.success("✅ فایل‌های نهایی ترکیب‌شده ساخته شدند!")
                st.download_button("📥 دانلود فایل cleaned", data=open(cleaned_path, 'rb').read(), file_name="final_cleaned_validated_output.xlsx")
                st.download_button("📥 دانلود فایل annotated", data=open(annotated_path, 'rb').read(), file_name="final_annotated_validated_output.xlsx")
            except Exception as e:
                st.error(f"❌ خطا در ترکیب فایل‌ها: {e}")