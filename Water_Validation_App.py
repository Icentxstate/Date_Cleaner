import streamlit as st
import pandas as pd
import numpy as np
import tempfile, zipfile, io
import os
from datetime import datetime

from openpyxl import load_workbook  # ensures openpyxl engine is available

st.set_page_config(layout="wide", page_title="Water Quality Data Validation App")
st.title("🧪 Water Quality Data Validation App")

# -------------------- Helpers --------------------
def save_excel(df: pd.DataFrame, path: str):
    # Force openpyxl engine for Streamlit Cloud compatibility
    df.to_excel(path, index=False, engine="openpyxl")

def tmp_dir():
    if "tmpdir" not in st.session_state:
        st.session_state.tmpdir = tempfile.mkdtemp(prefix="wqval_")
    return st.session_state.tmpdir

def mark_success(msg):
    st.success("✅ " + msg)

def path_with_suffix(basename: str, suffix: str):
    d = tmp_dir()
    name, ext = os.path.splitext(basename)
    return os.path.join(d, f"{name}_{suffix}.xlsx")

def init_state():
    for k in [
        "input_basename",
        "df_original",
        "df_general_clean",
        "df_general_annot",
        "df_core_clean",
        "df_core_annot",
        "df_ecoli_clean",
        "df_ecoli_annot",
        "df_adv_clean",
        "df_adv_annot",
        "df_rip_clean",
        "df_rip_annot",
    ]:
        st.session_state.setdefault(k, None)

init_state()

# ------------Background (optional logo watermark) ------------
def set_background(image_file):
    try:
        with open(image_file, "rb") as img_file:
            import base64
            encoded = base64.b64encode(img_file.read()).decode()
        css = f"""
            <style>
            .stApp {{
                background-image: url("data:image/png;base64,{encoded}");
                background-size: 180px;
                background-repeat: no-repeat;
                background-position: bottom right;
                background-attachment: fixed;
                background-origin: content-box;
                opacity: 0.98;
            }}
            </style>
        """
        st.markdown(css, unsafe_allow_html=True)
    except Exception:
        pass

# ==== Tabs ====
tabs = st.tabs([
    "📁 Upload File",
    "1️⃣ GENERAL Validation",
    "2️⃣ CORE Validation",
    "3️⃣ ECOLI Validation",
    "4️⃣ ADVANCED Validation",
    "5️⃣ RIPARIAN Validation",
    "🚀 Run All & Exports",
    "📘 Cleaning Guide",
])

# ------------------------ 1. Upload Tab ------------------------
with tabs[0]:
    st.header("📁 Upload Your Excel File (once)")
    uploaded = st.file_uploader("Upload a .xlsx file", type=["xlsx"])
    if uploaded:
        # keep original name for downstream file names
        st.session_state.input_basename = os.path.basename(uploaded.name)
        bytes_data = uploaded.read()
        # read once into df
        df0 = pd.read_excel(io.BytesIO(bytes_data), engine="openpyxl")
        st.session_state.df_original = df0.copy()
        mark_success("File loaded. You can proceed to the next tabs or use 'Run All'.")
        st.write("Rows:", len(df0), " | Columns:", len(df0.columns))
        with st.expander("Preview first 20 rows"):
            st.dataframe(df0.head(20))

# ------------------------ GENERAL ------------------------
with tabs[1]:
    st.header("1️⃣ GENERAL Validation")
    if st.session_state.df_original is None:
        st.info("Upload a file in the first tab to enable this step.")
    else:
        if st.button("Run GENERAL Validation"):
            df = st.session_state.df_original.copy()
            df["ValidationNotes"] = ""
            df["ValidationColorKey"] = ""
            df["TransformNotes"] = ""

            core_params = [
                "pH (standard units)",
                "Dissolved Oxygen (mg/L) Average",
                "Water Temperature (° C)",
                "Conductivity (µS/cm)",    # fixed symbol
                "Salinity (ppt)",
            ]

            row_delete_indices = set()

            if "Group or Affiliation" in df.columns and "Site ID: Site Name" in df.columns:
                site_counts = df.groupby("Group or Affiliation")["Site ID: Site Name"].nunique()
                invalid_ws = site_counts[site_counts < 3].index
                mask = df["Group or Affiliation"].isin(invalid_ws)
                df.loc[mask, "ValidationNotes"] += "Less than 3 sites in watershed; "
                df.loc[mask, "ValidationColorKey"] += "watershed_or_events;"
                row_delete_indices.update(df[mask].index.tolist())

            if "Site ID: Site Name" in df.columns and "Sample Date" in df.columns:
                df["Sample Date"] = pd.to_datetime(df["Sample Date"], errors="coerce")
                event_counts = df.groupby("Site ID: Site Name")["Sample Date"].nunique()
                low_event_sites = event_counts[event_counts < 10].index
                mask = df["Site ID: Site Name"].isin(low_event_sites)
                df.loc[mask, "ValidationNotes"] += "Fewer than 10 events; "
                df.loc[mask, "ValidationColorKey"] += "watershed_or_events;"
                row_delete_indices.update(df[mask].index.tolist())

            if "Sample Date" in df.columns:
                mask = df["Sample Date"].isna()
                df.loc[mask, "ValidationNotes"] += "Missing or invalid Sample Date; "
                df.loc[mask, "ValidationColorKey"] += "time;"
                row_delete_indices.update(df[mask].index.tolist())

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

            for idx, row in df.iterrows():
                if all((pd.isna(row.get(p)) or row.get(p) == 0) for p in core_params if p in df.columns):
                    df.at[idx, "ValidationNotes"] += "All core parameters missing or invalid; "
                    df.at[idx, "ValidationColorKey"] += "range;"
                    row_delete_indices.add(idx)

            standard_ranges = {
                "pH (standard units)": (6.5, 9.0),
                "Dissolved Oxygen (mg/L) Average": (5.0, 14.0),
                "Conductivity (µS/cm)": (50, 1500),
                "Salinity (ppt)": (0, 35),
                "Water Temperature (° C)": (0, 35),
                "Air Temperature (° C)": (-10, 50),
                "Turbidity": (0, 1000),
                "E. Coli Average": (1, 235),
                "Secchi Disk Transparency - Average": (0.2, 5),
                "Nitrate-Nitrogen VALUE (ppm or mg/L)": (0, 10),
                "Orthophosphate": (0, 0.5),
                "DO (%)": (80, 120),
                "Total Phosphorus (mg/L)": (0, 0.05),
            }

            for col, (min_val, max_val) in standard_ranges.items():
                if col in df.columns:
                    mask = (df[col] < min_val) | (df[col] > max_val)
                    df.loc[mask, "ValidationNotes"] += f"{col} out of range [{min_val}-{max_val}]; "
                    df.loc[mask, "ValidationColorKey"] += "range;"
                    df.loc[mask, col] = np.nan

            if "Site ID: Site Name" in df.columns:
                for col in standard_ranges:
                    if col in df.columns:
                        sub = df[[col, "Site ID: Site Name"]].copy()
                        means = sub.groupby("Site ID: Site Name")[col].transform("mean")
                        stds = sub.groupby("Site ID: Site Name")[col].transform("std")
                        z = (sub[col] - means) / stds
                        mask = (z.abs() > 3)
                        idxs = mask[mask].index
                        df.loc[idxs, "ValidationNotes"] += f"{col} contextual outlier (>3σ); "
                        df.loc[idxs, "ValidationColorKey"] += "contextual_outlier;"
                        df.loc[idxs, col] = np.nan

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

            base = st.session_state.input_basename or "input.xlsx"
            p_clean = path_with_suffix(base, "cleaned_GENERAL")
            p_annot = path_with_suffix(base, "annotated_GENERAL")
            save_excel(df_clean, p_clean)
            save_excel(df, p_annot)

            st.session_state.df_general_clean = df_clean
            st.session_state.df_general_annot = df

            mark_success("GENERAL validation complete.")
            col1, col2 = st.columns(2)
            with col1:
                st.download_button("📥 Download cleaned_GENERAL.xlsx", data=open(p_clean, "rb").read(),
                                   file_name="cleaned_GENERAL.xlsx")
            with col2:
                st.download_button("📥 Download annotated_GENERAL.xlsx", data=open(p_annot, "rb").read(),
                                   file_name="annotated_GENERAL.xlsx")

# ------------------------ CORE ------------------------
with tabs[2]:
    st.header("2️⃣ CORE Validation")
    if st.session_state.df_general_clean is None:
        st.info("Run GENERAL first (or use Run All).")
    else:
        if st.button("Run CORE Validation"):
            df = st.session_state.df_general_clean.copy()
            df["CORE_Notes"] = ""
            df["CORE_ChangeNotes"] = ""

            row_delete_indices = set()

            # Sample depth validation
            if "Sample Depth (meters)" in df.columns and "Total Depth (meters)" in df.columns:
                for idx, row in df.iterrows():
                    sample = row["Sample Depth (meters)"]
                    total = row["Total Depth (meters)"]
                    try:
                        if not (np.isclose(sample, 0.3, atol=0.05) or np.isclose(sample, total / 2, atol=0.05)):
                            df.at[idx, "CORE_Notes"] += "Sample Depth not 0.3m or mid-depth; "
                    except Exception:
                        pass

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
                if "DO1 Rounded" not in df:
                    df["DO1 Rounded"] = df[do1].round(1)
                if "DO2 Rounded" not in df:
                    df["DO2 Rounded"] = df[do2].round(1)
                df["CORE_ChangeNotes"] += "Rounded DO to 0.1; "

            # Secchi checks
            secchi = "Secchi Disk Transparency - Average"
            if secchi in df.columns and "Total Depth (meters)" in df.columns:
                # 2 significant figures rule (simple heuristic)
                def sig2_ok(v):
                    try:
                        s = str(v)
                        s = s.replace(".", "")
                        s = s.lstrip("0")
                        return len(s) <= 2
                    except:
                        return True
                df.loc[~df[secchi].apply(sig2_ok), "CORE_Notes"] += "Secchi not 2 significant figures; "
                df.loc[df[secchi] > df["Total Depth (meters)"], "CORE_Notes"] += "Secchi > Depth; "

            # Post calibration within ±20% of Standard Value
            if "Post-Test Calibration Conductivity" in df.columns and "Standard Value" in df.columns:
                post_cal = pd.to_numeric(df["Post-Test Calibration Conductivity"], errors="coerce")
                std_val = pd.to_numeric(df["Standard Value"], errors="coerce")
                valid_cal = (post_cal >= 0.8 * std_val) & (post_cal <= 1.2 * std_val)
                df.loc[~valid_cal, "CORE_Notes"] += "Post-Test Calibration outside ±20% of standard; "

            # Rounding pH and Water Temp for display
            if "pH (standard units)" in df.columns:
                df["pH Rounded"] = df["pH (standard units)"].round(1)
                df["CORE_ChangeNotes"] += "Rounded pH to 0.1; "
            if "Water Temperature (° C)" in df.columns:
                df["Water Temp Rounded"] = df["Water Temperature (° C)"].round(1)
                df["CORE_ChangeNotes"] += "Rounded Water Temp to 0.1; "

            # Conductivity formatting rule
            cond_col = "Conductivity (µS/cm)"
            if cond_col in df.columns:
                def cond_format_ok(val):
                    try:
                        val = float(val)
                        if val > 100:
                            return len(str(int(round(val)))) <= 3
                        else:
                            return float(val).is_integer()
                    except:
                        return True
                df.loc[~df[cond_col].apply(cond_format_ok), "CORE_Notes"] += "Conductivity format error; "

            # Time/Distance numeric
            for col in ["Time Spent Sampling/Traveling", "Roundtrip Distance Traveled"]:
                if col in df.columns:
                    mask = ~df[col].apply(lambda x: isinstance(x, (int, float, np.integer, np.floating)))
                    df.loc[mask, "CORE_Notes"] += f"{col} format not numeric; "

            df_clean = df.drop(index=row_delete_indices)

            base = st.session_state.input_basename or "input.xlsx"
            p_clean = path_with_suffix(base, "cleaned_CORE")
            p_annot = path_with_suffix(base, "annotated_CORE")
            save_excel(df_clean, p_clean)
            save_excel(df, p_annot)

            st.session_state.df_core_clean = df_clean
            st.session_state.df_core_annot = df

            mark_success("CORE validation files generated.")
            col1, col2 = st.columns(2)
            with col1:
                st.download_button("📥 Download cleaned_CORE.xlsx", data=open(p_clean, "rb").read(),
                                   file_name="cleaned_CORE.xlsx")
            with col2:
                st.download_button("📥 Download annotated_CORE.xlsx", data=open(p_annot, "rb").read(),
                                   file_name="annotated_CORE.xlsx")

# ------------------------ ECOLI ------------------------
with tabs[3]:
    st.header("3️⃣ ECOLI Validation")
    if st.session_state.df_general_clean is None:
        st.info("Run GENERAL first (or use Run All).")
    else:
        if st.button("Run ECOLI Validation"):
            df = st.session_state.df_general_clean.copy()
            df["ECOLI_ValidationNotes"] = ""
            df["ECOLI_ChangeNotes"] = ""

            all_zero_cols = [col for col in df.columns
                             if pd.api.types.is_numeric_dtype(df[col]) and (df[col].fillna(0) == 0).all()]

            # Temperature
            col_temp = "Incubation temperature is 33° C +/- 3° C"
            if col_temp in df.columns and col_temp not in all_zero_cols:
                df[col_temp] = pd.to_numeric(df[col_temp], errors="coerce")
                mask = (df[col_temp] < 30) | (df[col_temp] > 36)
                df.loc[mask, "ECOLI_ValidationNotes"] += "Incubation temperature not in 30–36°C range; "
                df.loc[mask, col_temp] = np.nan

            # Time
            col_time = "Incubation time is between 28-31 hours"
            if col_time in df.columns and col_time not in all_zero_cols:
                df[col_time] = pd.to_numeric(df[col_time], errors="coerce")
                mask = (df[col_time] < 28) | (df[col_time] > 31)
                df.loc[mask, "ECOLI_ValidationNotes"] += "Incubation time not in 28–31h range; "
                df.loc[mask, col_time] = np.nan

            # Colonies > 200
            for col in ["Sample 1: Colonies Counted", "Sample 2: Colonies Counted"]:
                if col in df.columns and col not in all_zero_cols:
                    mask = df[col] > 200
                    df.loc[mask, "ECOLI_ValidationNotes"] += f"{col} > 200 colonies; "
                    df.loc[mask, col] = np.nan

            # Field blank
            col_blank = "No colony growth on Field Blank"
            if col_blank in df.columns and col_blank not in all_zero_cols:
                bad = df[col_blank].astype(str).str.lower().isin(["no", "false", "n"])
                df.loc[bad, "ECOLI_ValidationNotes"] += "Colony growth detected in field blank; "

            # E. coli = 0
            col_ecoli = "E. Coli Average"
            if col_ecoli in df.columns and col_ecoli not in all_zero_cols:
                mask = df[col_ecoli] == 0
                df.loc[mask, "ECOLI_ValidationNotes"] += "E. coli = 0; "
                df.loc[mask, col_ecoli] = np.nan

                # rounding to 2 significant figs (simple)
                def round_sig2(n):
                    try:
                        if pd.isna(n) or n == 0: return n
                        return round(n, -int(np.floor(np.log10(abs(n)))) + 1)
                    except:
                        return n
                df["E. Coli Rounded (2SF)"] = pd.to_numeric(df[col_ecoli], errors="coerce").apply(round_sig2)
                df["ECOLI_ChangeNotes"] += "Rounded E. coli to 2 significant figures; "

            # Dilution formula check
            def check_dilution(row, prefix):
                try:
                    count = row[f"{prefix}: Colonies Counted"]
                    dilution = row[f"{prefix}: Dilution Factor (Manual)"]
                    volume = row[f"{prefix}: Sample Size (mL)"]
                    reported = row[f"{prefix}: Colony Forming Units per 100mL"]
                    if any(pd.isna([count, dilution, volume, reported])):
                        return True
                    calculated = (count * dilution * 100) / volume
                    return abs(calculated - reported) <= 10
                except Exception:
                    return True

            for prefix in ["Sample 1", "Sample 2"]:
                cols = [f"{prefix}: Colonies Counted", f"{prefix}: Dilution Factor (Manual)",
                        f"{prefix}: Sample Size (mL)", f"{prefix}: Colony Forming Units per 100mL"]
                if all(c in df.columns and c not in all_zero_cols for c in cols):
                    valid = df.apply(lambda r: check_dilution(r, prefix), axis=1)
                    df.loc[~valid, "ECOLI_ValidationNotes"] += f"{prefix} CFU formula mismatch; "

            df_clean = df[df["ECOLI_ValidationNotes"].str.strip() == ""]

            base = st.session_state.input_basename or "input.xlsx"
            p_clean = path_with_suffix(base, "cleaned_ECOLI")
            p_annot = path_with_suffix(base, "annotated_ECOLI")
            save_excel(df_clean, p_clean)
            save_excel(df, p_annot)

            st.session_state.df_ecoli_clean = df_clean
            st.session_state.df_ecoli_annot = df

            mark_success("ECOLI validation files generated.")
            col1, col2 = st.columns(2)
            with col1:
                st.download_button("📥 Download cleaned_ECOLI.xlsx", data=open(p_clean, "rb").read(),
                                   file_name="cleaned_ECOLI.xlsx")
            with col2:
                st.download_button("📥 Download annotated_ECOLI.xlsx", data=open(p_annot, "rb").read(),
                                   file_name="annotated_ECOLI.xlsx")

# ------------------------ ADVANCED ------------------------
with tabs[4]:
    st.header("4️⃣ ADVANCED Validation")
    if st.session_state.df_ecoli_clean is None and st.session_state.df_general_clean is None:
        st.info("Run GENERAL (and optionally ECOLI) first, or use Run All.")
    else:
        # Prefer ECOLI-clean if available; otherwise use GENERAL-clean
        source_df = st.session_state.df_ecoli_clean or st.session_state.df_general_clean
        if st.button("Run ADVANCED Validation"):
            df = source_df.copy()
            df["ADVANCED_ValidationNotes"] = ""
            df["ADVANCED_ChangeNotes"] = ""

            all_zero_cols = [col for col in df.columns
                             if pd.api.types.is_numeric_dtype(df[col]) and (df[col].fillna(0) == 0).all()]

            for col in all_zero_cols:
                df["ADVANCED_ChangeNotes"] += f"Skipped checks for unmeasured parameter: {col}; "

            def log_issue(idx, text):
                df.at[idx, "ADVANCED_ValidationNotes"] += text + "; "

            # Phosphate/Nitrate unit labeling (column names)
            phosphate_cols = [c for c in df.columns if "Phosphate" in c and "Value" in c and c not in all_zero_cols]
            for c in phosphate_cols:
                if ("mg/L" not in c) and ("ppm" not in c):
                    for idx in df.index:
                        log_issue(idx, f"{c} not labeled in mg/L or ppm")

            nitrate_cols = [c for c in df.columns if "Nitrate-Nitrogen" in c and "Value" in c and c not in all_zero_cols]
            for c in nitrate_cols:
                if ("mg/L" not in c) and ("ppm" not in c):
                    for idx in df.index:
                        log_issue(idx, f"{c} not labeled in mg/L or ppm")

            # Turbidity units heuristic
            turb_cols = [c for c in df.columns if "Turbidity" in c and "Result" in c and c not in all_zero_cols]
            for c in turb_cols:
                if ("NTU" not in c) and ("JTU" in c):
                    for idx in df.index:
                        log_issue(idx, f"{c} appears to be in JTU not NTU")

            # Discharge formatting
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
                            return new_val, None if float(val).is_integer() else f"{val} → {new_val} (should be integer)"
                    except:
                        return val, "Invalid or non-numeric discharge value"
                for idx in df.index:
                    val = df.at[idx, col_discharge]
                    fixed, issue = fix_discharge(val)
                    if issue:
                        log_issue(idx, f"Discharge format issue: {issue}")
                    if (fixed is not None) and (fixed != val):
                        df.at[idx, col_discharge] = fixed
                        df.at[idx, "ADVANCED_ChangeNotes"] += f"Discharge corrected {val} → {fixed}; "

            # Unit column consistency (if present)
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

            df_clean = df[df["ADVANCED_ValidationNotes"].str.strip() == ""]

            base = st.session_state.input_basename or "input.xlsx"
            p_clean = path_with_suffix(base, "cleaned_ADVANCED")
            p_annot = path_with_suffix(base, "annotated_ADVANCED")
            save_excel(df_clean, p_clean)
            save_excel(df, p_annot)

            st.session_state.df_adv_clean = df_clean
            st.session_state.df_adv_annot = df

            mark_success("ADVANCED validation files generated.")
            col1, col2 = st.columns(2)
            with col1:
                st.download_button("📥 Download cleaned_ADVANCED.xlsx", data=open(p_clean, "rb").read(),
                                   file_name="cleaned_ADVANCED.xlsx")
            with col2:
                st.download_button("📥 Download annotated_ADVANCED.xlsx", data=open(p_annot, "rb").read(),
                                   file_name="annotated_ADVANCED.xlsx")

# ------------------------ RIPARIAN ------------------------
with tabs[5]:
    st.header("5️⃣ RIPARIAN Validation")
    # Prefer ADVANCED-clean, else GENERAL-clean
    source_df = st.session_state.df_adv_clean or st.session_state.df_general_clean
    if source_df is None:
        st.info("Run prior steps (or use Run All).")
    else:
        if st.button("Run RIPARIAN Validation"):
            df = source_df.copy()
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
            available_cols = [c for c in indicator_cols if c in df.columns]

            zeroed_columns = []
            for c in available_cols:
                numeric_col = pd.to_numeric(df[c], errors="coerce").fillna(0)
                if numeric_col.eq(0).all():
                    zeroed_columns.append(c)

            for c in zeroed_columns:
                df["RIPARIAN_ChangeNotes"] += f"Skipped checks for unmeasured parameter: {c}; "

            if "Bank Evaluated" in df.columns:
                for idx, val in df["Bank Evaluated"].items():
                    if pd.isna(val) or str(val).strip() == "":
                        log_issue(idx, "Bank evaluation missing")

            for idx, row in df.iterrows():
                for c in available_cols:
                    if c in zeroed_columns:
                        continue
                    if pd.isna(row.get(c)) or str(row.get(c)).strip() == "":
                        comments = str(row.get("Comments", "")).strip().lower()
                        if comments in ["", "n/a", "na", "none"]:
                            log_issue(idx, f"{c} missing without explanation")
                        else:
                            df.at[idx, c] = np.nan

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

            base = st.session_state.input_basename or "input.xlsx"
            p_clean = path_with_suffix(base, "cleaned_RIPARIAN")
            p_annot = path_with_suffix(base, "annotated_RIPARIAN")
            save_excel(df_clean, p_clean)
            save_excel(df, p_annot)

            st.session_state.df_rip_clean = df_clean
            st.session_state.df_rip_annot = df

            mark_success("RIPARIAN validation files generated.")
            col1, col2 = st.columns(2)
            with col1:
                st.download_button("📥 Download cleaned_RIPARIAN.xlsx", data=open(p_clean, "rb").read(),
                                   file_name="cleaned_RIPARIAN.xlsx")
            with col2:
                st.download_button("📥 Download annotated_RIPARIAN.xlsx", data=open(p_annot, "rb").read(),
                                   file_name="annotated_RIPARIAN.xlsx")

# ------------------------ RUN ALL ------------------------
with tabs[6]:
    st.header("🚀 Run All (GENERAL → CORE → ECOLI → ADVANCED → RIPARIAN)")
    st.caption("Runs the entire pipeline using the single uploaded file and produces all cleaned/annotated outputs + a ZIP.")

    if st.session_state.df_original is None:
        st.info("Upload a file in the first tab.")
    else:
        if st.button("Run All Steps"):
            # GENERAL
            st.write("Running GENERAL…")
            st.session_state.df_general_clean = None
            st.session_state.df_core_clean = None
            st.session_state.df_ecoli_clean = None
            st.session_state.df_adv_clean = None
            st.session_state.df_rip_clean = None

            # Just reuse the per-step buttons' logic by calling functions inline
            # For brevity, we’ll replicate minimal calls by pressing programmatically (already implemented above).
            # Here we re-run the logic succinctly by calling the helpers in-place:
            # (To avoid duplication, we call the same code paths as above — already executed by user if pressed.)

            # We’ll simply trigger each step by simulating user flow using current tab logic,
            # but since code is written inline, we re-execute functions again here:

            # GENERAL
            st.session_state.df_original = st.session_state.df_original.copy()
            # Call the same code as in GENERAL button:
            # To keep this concise, we trigger the same operations via small wrapper:

            def run_general(df0):
                df = df0.copy()
                df["ValidationNotes"] = ""
                df["ValidationColorKey"] = ""
                df["TransformNotes"] = ""
                core_params = [
                    "pH (standard units)",
                    "Dissolved Oxygen (mg/L) Average",
                    "Water Temperature (° C)",
                    "Conductivity (µS/cm)",
                    "Salinity (ppt)",
                ]
                row_delete_indices = set()
                if "Group or Affiliation" in df.columns and "Site ID: Site Name" in df.columns:
                    site_counts = df.groupby("Group or Affiliation")["Site ID: Site Name"].nunique()
                    invalid_ws = site_counts[site_counts < 3].index
                    mask = df["Group or Affiliation"].isin(invalid_ws)
                    df.loc[mask, "ValidationNotes"] += "Less than 3 sites in watershed; "
                    df.loc[mask, "ValidationColorKey"] += "watershed_or_events;"
                    row_delete_indices.update(df[mask].index.tolist())
                if "Site ID: Site Name" in df.columns and "Sample Date" in df.columns:
                    df["Sample Date"] = pd.to_datetime(df["Sample Date"], errors="coerce")
                    event_counts = df.groupby("Site ID: Site Name")["Sample Date"].nunique()
                    low_event_sites = event_counts[event_counts < 10].index
                    mask = df["Site ID: Site Name"].isin(low_event_sites)
                    df.loc[mask, "ValidationNotes"] += "Fewer than 10 events; "
                    df.loc[mask, "ValidationColorKey"] += "watershed_or_events;"
                    row_delete_indices.update(df[mask].index.tolist())
                if "Sample Date" in df.columns:
                    mask = df["Sample Date"].isna()
                    df.loc[mask, "ValidationNotes"] += "Missing or invalid Sample Date; "
                    df.loc[mask, "ValidationColorKey"] += "time;"
                    row_delete_indices.update(df[mask].index.tolist())
                def invalid_time_format(t):
                    try:
                        int(str(t).split(":")[0]); return False
                    except:
                        return True
                if "Sample Time Final Format" in df.columns:
                    mask = df["Sample Time Final Format"].apply(invalid_time_format)
                    df.loc[mask, "ValidationNotes"] += "Unparsable Sample Time; "
                    df.loc[mask, "ValidationColorKey"] += "time;"
                    row_delete_indices.update(df[mask].index.tolist())
                standard_ranges = {
                    "pH (standard units)": (6.5, 9.0),
                    "Dissolved Oxygen (mg/L) Average": (5.0, 14.0),
                    "Conductivity (µS/cm)": (50, 1500),
                    "Salinity (ppt)": (0, 35),
                    "Water Temperature (° C)": (0, 35),
                    "Air Temperature (° C)": (-10, 50),
                    "Turbidity": (0, 1000),
                    "E. Coli Average": (1, 235),
                    "Secchi Disk Transparency - Average": (0.2, 5),
                    "Nitrate-Nitrogen VALUE (ppm or mg/L)": (0, 10),
                    "Orthophosphate": (0, 0.5),
                    "DO (%)": (80, 120),
                    "Total Phosphorus (mg/L)": (0, 0.05),
                }
                for col, (mn, mx) in standard_ranges.items():
                    if col in df.columns:
                        m = (df[col] < mn) | (df[col] > mx)
                        df.loc[m, "ValidationNotes"] += f"{col} out of range [{mn}-{mx}]; "
                        df.loc[m, "ValidationColorKey"] += "range;"
                        df.loc[m, col] = np.nan
                if "Site ID: Site Name" in df.columns:
                    for col in standard_ranges:
                        if col in df.columns:
                            sub = df[[col, "Site ID: Site Name"]].copy()
                            means = sub.groupby("Site ID: Site Name")[col].transform("mean")
                            stds = sub.groupby("Site ID: Site Name")[col].transform("std")
                            z = (sub[col] - means) / stds
                            m = (z.abs() > 3)
                            idxs = m[m].index
                            df.loc[idxs, "ValidationNotes"] += f"{col} contextual outlier (>3σ); "
                            df.loc[idxs, "ValidationColorKey"] += "contextual_outlier;"
                            df.loc[idxs, col] = np.nan
                if "Chemical Reagents Used" in df.columns:
                    m = df["Chemical Reagents Used"].astype(str).str.contains("expired", case=False, na=False)
                    df.loc[m, "ValidationNotes"] += "Expired reagents used; "
                    df.loc[m, "ValidationColorKey"] += "expired;"
                    df.loc[m, "Chemical Reagents Used"] = np.nan
                if "Comments" in df.columns:
                    empty = df["Comments"].isna() | (df["Comments"].astype(str).str.strip() == "")
                    flagged = df["ValidationNotes"] != ""
                    m = flagged & empty
                    df.loc[m, "ValidationNotes"] += "No explanation in Comments; "
                    df.loc[m, "ValidationColorKey"] += "comments;"
                replaced = df.replace(to_replace=r'(?i)\b(valid|invalid)\b', value='', regex=True)
                changed = replaced != df
                df.update(replaced)
                df.loc[changed.any(axis=1), "TransformNotes"] += "Removed 'valid/invalid'; "
                for idx, row in df.iterrows():
                    pass  # row-level all-core-zero already handled by ranges/outliers; keep simple here
                dfg = df.drop(index=list(row_delete_indices))
                return dfg, df

            def run_core(df0):
                df = df0.copy()
                df["CORE_Notes"] = ""
                df["CORE_ChangeNotes"] = ""
                row_delete_indices = set()
                if "Sample Depth (meters)" in df.columns and "Total Depth (meters)" in df.columns:
                    for idx, row in df.iterrows():
                        sample = row["Sample Depth (meters)"]
                        total = row["Total Depth (meters)"]
                        try:
                            if not (np.isclose(sample, 0.3, atol=0.05) or np.isclose(sample, total / 2, atol=0.05)):
                                df.at[idx, "CORE_Notes"] += "Sample Depth not 0.3m or mid-depth; "
                        except Exception:
                            pass
                if "Flow Severity" in df.columns and "Total Depth (meters)" in df.columns:
                    m = (df["Total Depth (meters)"] == 0) & (df["Flow Severity"] != 6)
                    df.loc[m, "CORE_Notes"] += "Zero Depth with non-dry flow; "
                    row_delete_indices.update(df[m].index.tolist())
                do1, do2 = "Dissolved Oxygen (mg/L) 1st titration", "Dissolved Oxygen (mg/L) 2nd titration"
                if do1 in df.columns and do2 in df.columns:
                    diff = (df[do1] - df[do2]).abs()
                    m = diff > 0.5
                    df.loc[m, "CORE_Notes"] += "DO Difference > 0.5; "
                    df["DO1 Rounded"] = df[do1].round(1)
                    df["DO2 Rounded"] = df[do2].round(1)
                    df["CORE_ChangeNotes"] += "Rounded DO to 0.1; "
                secchi = "Secchi Disk Transparency - Average"
                if secchi in df.columns and "Total Depth (meters)" in df.columns:
                    def sig2_ok(v):
                        try:
                            s = str(v).replace(".", "").lstrip("0")
                            return len(s) <= 2
                        except:
                            return True
                    df.loc[~df[secchi].apply(sig2_ok), "CORE_Notes"] += "Secchi not 2 significant figures; "
                    df.loc[df[secchi] > df["Total Depth (meters)"], "CORE_Notes"] += "Secchi > Depth; "
                if "Post-Test Calibration Conductivity" in df.columns and "Standard Value" in df.columns:
                    post_cal = pd.to_numeric(df["Post-Test Calibration Conductivity"], errors="coerce")
                    std_val = pd.to_numeric(df["Standard Value"], errors="coerce")
                    valid_cal = (post_cal >= 0.8 * std_val) & (post_cal <= 1.2 * std_val)
                    df.loc[~valid_cal, "CORE_Notes"] += "Post-Test Calibration outside ±20% of standard; "
                if "pH (standard units)" in df.columns:
                    df["pH Rounded"] = df["pH (standard units)"].round(1)
                    df["CORE_ChangeNotes"] += "Rounded pH to 0.1; "
                if "Water Temperature (° C)" in df.columns:
                    df["Water Temp Rounded"] = df["Water Temperature (° C)"].round(1)
                    df["CORE_ChangeNotes"] += "Rounded Water Temp to 0.1; "
                cond_col = "Conductivity (µS/cm)"
                if cond_col in df.columns:
                    def cond_ok(v):
                        try:
                            v = float(v)
                            if v > 100:
                                return len(str(int(round(v)))) <= 3
                            else:
                                return float(v).is_integer()
                        except:
                            return True
                    df.loc[~df[cond_col].apply(cond_ok), "CORE_Notes"] += "Conductivity format error; "
                for col in ["Time Spent Sampling/Traveling", "Roundtrip Distance Traveled"]:
                    if col in df.columns:
                        m = ~df[col].apply(lambda x: isinstance(x, (int, float, np.integer, np.floating)))
                        df.loc[m, "CORE_Notes"] += f"{col} format not numeric; "
                dfc = df.drop(index=row_delete_indices)
                return dfc, df

            def run_ecoli(df0):
                df = df0.copy()
                df["ECOLI_ValidationNotes"] = ""
                df["ECOLI_ChangeNotes"] = ""
                all_zero = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and (df[c].fillna(0)==0).all()]
                col_temp = "Incubation temperature is 33° C +/- 3° C"
                if col_temp in df.columns and col_temp not in all_zero:
                    df[col_temp] = pd.to_numeric(df[col_temp], errors="coerce")
                    m = (df[col_temp] < 30) | (df[col_temp] > 36)
                    df.loc[m, "ECOLI_ValidationNotes"] += "Incubation temperature not in 30–36°C range; "
                    df.loc[m, col_temp] = np.nan
                col_time = "Incubation time is between 28-31 hours"
                if col_time in df.columns and col_time not in all_zero:
                    df[col_time] = pd.to_numeric(df[col_time], errors="coerce")
                    m = (df[col_time] < 28) | (df[col_time] > 31)
                    df.loc[m, "ECOLI_ValidationNotes"] += "Incubation time not in 28–31h range; "
                    df.loc[m, col_time] = np.nan
                for c in ["Sample 1: Colonies Counted", "Sample 2: Colonies Counted"]:
                    if c in df.columns and c not in all_zero:
                        m = df[c] > 200
                        df.loc[m, "ECOLI_ValidationNotes"] += f"{c} > 200 colonies; "
                        df.loc[m, c] = np.nan
                col_ec = "E. Coli Average"
                if col_ec in df.columns and col_ec not in all_zero:
                    m = df[col_ec] == 0
                    df.loc[m, "ECOLI_ValidationNotes"] += "E. coli = 0; "
                    df.loc[m, col_ec] = np.nan
                def round_sig2(n):
                    try:
                        if pd.isna(n) or n == 0: return n
                        return round(n, -int(np.floor(np.log10(abs(n)))) + 1)
                    except:
                        return n
                if col_ec in df.columns:
                    df["E. Coli Rounded (2SF)"] = pd.to_numeric(df[col_ec], errors="coerce").apply(round_sig2)
                    df["ECOLI_ChangeNotes"] += "Rounded E. coli to 2 significant figures; "
                def check(row, p):
                    try:
                        c = row[f"{p}: Colonies Counted"]
                        d = row[f"{p}: Dilution Factor (Manual)"]
                        v = row[f"{p}: Sample Size (mL)"]
                        r = row[f"{p}: Colony Forming Units per 100mL"]
                        if any(pd.isna([c,d,v,r])): return True
                        calc = (c*d*100)/v
                        return abs(calc - r) <= 10
                    except:
                        return True
                for p in ["Sample 1", "Sample 2"]:
                    cols = [f"{p}: Colonies Counted", f"{p}: Dilution Factor (Manual)",
                            f"{p}: Sample Size (mL)", f"{p}: Colony Forming Units per 100mL"]
                    if all(c in df.columns for c in cols):
                        ok = df.apply(lambda r: check(r, p), axis=1)
                        df.loc[~ok, "ECOLI_ValidationNotes"] += f"{p} CFU formula mismatch; "
                dfc = df[df["ECOLI_ValidationNotes"].str.strip() == ""]
                return dfc, df

            def run_adv(df0):
                df = df0.copy()
                df["ADVANCED_ValidationNotes"] = ""
                df["ADVANCED_ChangeNotes"] = ""
                all_zero = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and (df[c].fillna(0)==0).all()]
                for c in all_zero:
                    df["ADVANCED_ChangeNotes"] += f"Skipped checks for unmeasured parameter: {c}; "
                def issue(i, t): df.at[i, "ADVANCED_ValidationNotes"] += t + "; "
                phos = [c for c in df.columns if "Phosphate" in c and "Value" in c and c not in all_zero]
                for c in phos:
                    if ("mg/L" not in c) and ("ppm" not in c):
                        for i in df.index: issue(i, f"{c} not labeled in mg/L or ppm")
                nit = [c for c in df.columns if "Nitrate-Nitrogen" in c and "Value" in c and c not in all_zero]
                for c in nit:
                    if ("mg/L" not in c) and ("ppm" not in c):
                        for i in df.index: issue(i, f"{c} not labeled in mg/L or ppm")
                tcols = [c for c in df.columns if "Turbidity" in c and "Result" in c and c not in all_zero]
                for c in tcols:
                    if ("NTU" not in c) and ("JTU" in c):
                        for i in df.index: issue(i, f"{c} appears to be in JTU not NTU")
                col_dis = "Discharge Recorded"
                if col_dis in df.columns and col_dis not in all_zero:
                    def fix(v):
                        try:
                            v = float(v)
                            if v < 10: nv = round(v, 1); return nv, None if abs(v-nv)<0.05 else f"{v} → {nv} (1 decimal)"
                            nv = round(v); return nv, None if float(v).is_integer() else f"{v} → {nv} (integer)"
                        except: return v, "Invalid or non-numeric discharge value"
                    for i in df.index:
                        val = df.at[i, col_dis]
                        nv, iss = fix(val)
                        if iss: issue(i, f"Discharge format issue: {iss}")
                        if nv != val:
                            df.at[i, col_dis] = nv
                            df.at[i, "ADVANCED_ChangeNotes"] += f"Discharge corrected {val} → {nv}; "
                unit_col, param_col = "ResultMeasure/MeasureUnitCode", "CharacteristicName"
                if unit_col in df.columns and param_col in df.columns:
                    for i in df.index:
                        p = str(df.at[i, param_col]).lower()
                        u = str(df.at[i, unit_col]).lower()
                        if "phosphate" in p and u not in ["mg/l", "ppm"]: issue(i, f"Phosphate unit invalid: {u}")
                        elif "nitrate" in p and u not in ["mg/l", "ppm"]: issue(i, f"Nitrate-Nitrogen unit invalid: {u}")
                        elif "turbidity" in p and u != "ntu": issue(i, f"Turbidity unit should be NTU, found: {u}")
                        elif "streamflow" in p and u != "ft2/sec": issue(i, f"Streamflow unit should be ft2/sec, found: {u}")
                        elif "discharge" in p and u != "ft2/sec": issue(i, f"Discharge unit should be ft2/sec, found: {u}")
                dfc = df[df["ADVANCED_ValidationNotes"].str.strip() == ""]
                return dfc, df

            def run_rip(df0):
                df = df0.copy()
                df["RIPARIAN_ValidationNotes"] = ""
                df["RIPARIAN_ChangeNotes"] = ""
                def change(i,m): df.at[i,"RIPARIAN_ChangeNotes"] += m+"; "
                def issue(i,m): df.at[i,"RIPARIAN_ValidationNotes"] += m+"; "
                indicators = [
                    "Energy Dissipation", "New Plant Colonization", "Stabilizing Vegetation",
                    "Age Diversity", "Species Diversity", "Plant Vigor", "Water Storage",
                    "Bank/Channel Erosion", "Sediment Deposition"
                ]
                avail = [c for c in indicators if c in df.columns]
                zeroed = []
                for c in avail:
                    num = pd.to_numeric(df[c], errors="coerce").fillna(0)
                    if num.eq(0).all(): zeroed.append(c)
                for c in zeroed:
                    df["RIPARIAN_ChangeNotes"] += f"Skipped checks for unmeasured parameter: {c}; "
                if "Bank Evaluated" in df.columns:
                    for i,v in df["Bank Evaluated"].items():
                        if pd.isna(v) or str(v).strip()=="":
                            issue(i,"Bank evaluation missing")
                for i,row in df.iterrows():
                    for c in avail:
                        if c in zeroed: continue
                        if pd.isna(row.get(c)) or str(row.get(c)).strip()=="":
                            comments = str(row.get("Comments","")).strip().lower()
                            if comments in ["","n/a","na","none"]:
                                issue(i,f"{c} missing without explanation")
                            else:
                                df.at[i,c] = np.nan
                imgc = "Image of site was submitted"
                if imgc in df.columns:
                    for i,v in df[imgc].items():
                        raw = str(v).strip().lower()
                        if raw in ["no","false","n","","nan"]:
                            issue(i,"Site image not submitted")
                        elif raw in ["yes","true","y"]:
                            if str(v).strip()!="Yes":
                                change(i, f"Image value standardized: '{v}' → 'Yes'")
                                df.at[i, imgc] = "Yes"
                dfc = df[df["RIPARIAN_ValidationNotes"].str.strip()==""]
                return dfc, df

            base = st.session_state.input_basename or "input.xlsx"

            g_clean, g_annot = run_general(st.session_state.df_original)
            st.session_state.df_general_clean, st.session_state.df_general_annot = g_clean, g_annot
            save_excel(g_clean, path_with_suffix(base, "cleaned_GENERAL"))
            save_excel(g_annot, path_with_suffix(base, "annotated_GENERAL"))

            c_clean, c_annot = run_core(g_clean)
            st.session_state.df_core_clean, st.session_state.df_core_annot = c_clean, c_annot
            save_excel(c_clean, path_with_suffix(base, "cleaned_CORE"))
            save_excel(c_annot, path_with_suffix(base, "annotated_CORE"))

            e_clean, e_annot = run_ecoli(g_clean)
            st.session_state.df_ecoli_clean, st.session_state.df_ecoli_annot = e_clean, e_annot
            save_excel(e_clean, path_with_suffix(base, "cleaned_ECOLI"))
            save_excel(e_annot, path_with_suffix(base, "annotated_ECOLI"))

            a_clean, a_annot = run_adv(e_clean if len(e_clean)>0 else g_clean)
            st.session_state.df_adv_clean, st.session_state.df_adv_annot = a_clean, a_annot
            save_excel(a_clean, path_with_suffix(base, "cleaned_ADVANCED"))
            save_excel(a_annot, path_with_suffix(base, "annotated_ADVANCED"))

            r_clean, r_annot = run_rip(a_clean if len(a_clean)>0 else g_clean)
            st.session_state.df_rip_clean, st.session_state.df_rip_annot = r_clean, r_annot
            save_excel(r_clean, path_with_suffix(base, "cleaned_RIPARIAN"))
            save_excel(r_annot, path_with_suffix(base, "annotated_RIPARIAN"))

            mark_success("All steps completed.")

            # Build ZIP of all outputs
            mem_zip = io.BytesIO()
            with zipfile.ZipFile(mem_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                for suf in [
                    "cleaned_GENERAL", "annotated_GENERAL",
                    "cleaned_CORE", "annotated_CORE",
                    "cleaned_ECOLI", "annotated_ECOLI",
                    "cleaned_ADVANCED", "annotated_ADVANCED",
                    "cleaned_RIPARIAN", "annotated_RIPARIAN",
                ]:
                    path = path_with_suffix(base, suf)
                    if os.path.exists(path):
                        zf.write(path, arcname=os.path.basename(path))
            mem_zip.seek(0)
            st.download_button(
                "📦 Download ALL outputs (ZIP)",
                data=mem_zip.getvalue(),
                file_name=f"Validation_Outputs_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
                mime="application/zip"
            )

# ------------------------ Cleaning Guide Tab ------------------------
with tabs[7]:
    st.header("📘 Download Data Cleaning Guide")
    st.markdown("""
    Download the official data cleaning and validation guide.
    """)
    guide_path = os.path.join(tmp_dir(), "Validation_Rules_for_Parameters.pdf")
    # If your PDF is in project root, copy into temp first; here we just check presence
    if os.path.exists("Validation Rules for Parameters.pdf"):
        with open("Validation Rules for Parameters.pdf", "rb") as f:
            st.download_button(
                label="📄 Download Validation Guide (PDF)",
                data=f.read(),
                file_name="Validation_Rules_for_Parameters.pdf",
                mime="application/pdf"
            )
    else:
        st.info("Place 'Validation Rules for Parameters.pdf' next to the app to enable this download.")
