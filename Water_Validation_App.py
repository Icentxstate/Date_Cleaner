# Water_Validation_App.py
# Streamlit app for automated Water Quality Data Validation (CRP/TST-style)

import io
import numpy as np
import pandas as pd
import streamlit as st
from datetime import datetime

# -----------------------------------------------------------------------------
# Page config
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Water Quality Data Validation App",
    layout="wide"
)

st.title("Water Quality Data Validation App")

# -----------------------------------------------------------------------------
# 1. CONFIG – COLUMN NAMES (edit here if your headers differ slightly)
# -----------------------------------------------------------------------------

COLUMN_MAP = {
    "site": ["Site ID", "Site ID: Site Name", "Site ID: Site Name ", "Site"],
    "sample_date": ["Sample Date", "Date"],
    "sample_time": ["Sample Time Final Format", "Sample Time", "Time"],
    "watershed": ["Watershed", "Watershed Name"],  # optional

    # CORE
    "sample_depth": ["Sample Depth (meters)", "Sample Depth (m)"],
    "total_depth": ["Total Depth (meters)", "Total Depth (m)"],
    "secchi": ["Secchi Disk Transparency - Average", "Secchi Transparency - Average"],
    "secchi_mod": ["Secchi Disk Modifier", "Secchi Modifier"],
    "tube": ["Transparency Tube (meters)", "Transparency Tube (m)"],
    "tube_mod": ["Transparency Tube Modifier", "Transparency Tube Qualifier"],
    "do_avg": ["Dissolved Oxygen (mg/L) Average", "Dissolved Oxygen (mg/L) avg"],
    "do_1": ["Dissolved Oxygen (mg/L) 1st titration"],
    "do_2": ["Dissolved Oxygen (mg/L) 2nd titration"],
    "air_temp": ["Air Temperature (° C)", "Air Temp (° C)"],
    "water_temp": ["Water Temperature (° C)", "Water Temp (° C)"],
    "ph": ["pH (standard units)", "pH"],
    "cond": ["Conductivity (?S/cm)", "Conductivity (µS/cm)", "Conductivity (uS/cm)"],
    "tds": ["Total Dissolved Solids (mg/L)", "TDS (mg/L)"],
    "salinity": ["Salinity (ppt)"],
    "flow_severity": ["Flow Severity", "Flow severity"],
    "rain_acc": ["Rainfall Accumulation", "Total Rainfall (inches)", "Total Rainfall"],
    "days_since_rain": ["Days Since Last Significant Rainfall"],

    # QC FLAGS (optional)
    "valid_flag": ["Validation", "Valid/Invalid", "Data Quality"],

    # E. COLI
    "ecoli_avg": ["E. Coli Average", "E. coli Average"],
    "ecoli_cfu1": ["Sample 1: Colony Forming Units per 100mL"],
    "ecoli_cfu2": ["Sample 2: Colony Forming Units per 100mL"],
    "ecoli_colonies1": ["Sample 1: Colonies Counted"],
    "ecoli_colonies2": ["Sample 2: Colonies Counted"],
    "ecoli_size1": ["Sample 1: Sample Size (mL)"],
    "ecoli_size2": ["Sample 2: Sample Size (mL)"],
    "ecoli_dil1": ["Sample 1: Dilution Factor (Manual)"],
    "ecoli_dil2": ["Sample 2: Dilution Factor (Manual)"],
    "ecoli_temp": ["Sample Temp (° C)", "Incubation Temperature (°C)"],
    "ecoli_hold": ["Sample Hold Time", "Incubation Period (hours)"],
    "ecoli_blank_qc": ["Field Blank QC", "No colony growth on Field Blank"],
    "ecoli_incubation_qc": [
        "Incubation time is between 24 hours",
        "Incubation Period QC"
    ],
    "ecoli_optimal_colony": ["Optimal colony number is achieved (<200)"],

    # ADVANCED
    "orthophosphate": ["Orthophosphate", "Phosphate (mg/L)"],
    "orthophosphate_f": ["Filtered (Orthophosphate)"],
    "nitrate_n": ["Nitrate-Nitrogen VALUE (ppm or mg/L)", "Nitrate-Nitrogen (mg/L)"],
    "nitrate_f": ["Filtered (Nitrate-Nitrogen)"],
    "nitrate": ["Nitrate"],
    "turbidity": ["Turbidity Result (JTU)", "Turbidity (NTU)", "Turbidity"],
    "cross_section": ["Waterbody Cross Section"],
    "water_depth": ["Water Depth"],
    "downstream_10ft": ["10-foot Downstream Measurement"],
    "discharge": ["Discharge Recorded", "Streamflow (ft2/sec)", "Discharge (cfs)"],

    # RIPARIAN (common fields)
    "bank_evaluated": ["Bank Evaluated", "Bank evaluated is completed"],
    "riparian_image": ["Image Submitted", "Image of site was submitted"],
}


def find_col(df, candidates):
    """Return the first column in df that matches the candidate list."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


# -----------------------------------------------------------------------------
# 2. COLUMN CATEGORIZATION
# -----------------------------------------------------------------------------

def categorize_columns(df):
    """Return dict of category -> list_of_columns based on known headers."""
    cols = df.columns.tolist()

    core_cols = []
    ecoli_cols = []
    adv_cols = []
    riparian_cols = []
    general_cols = []

    core_keys = [
        "sample_depth", "total_depth", "secchi", "secchi_mod", "tube", "tube_mod",
        "do_avg", "do_1", "do_2", "air_temp", "water_temp", "ph", "cond", "tds",
        "salinity", "flow_severity", "rain_acc", "days_since_rain"
    ]
    ecoli_keys = [
        "ecoli_avg", "ecoli_cfu1", "ecoli_cfu2", "ecoli_colonies1",
        "ecoli_colonies2", "ecoli_size1", "ecoli_size2", "ecoli_dil1",
        "ecoli_dil2", "ecoli_temp", "ecoli_hold", "ecoli_blank_qc",
        "ecoli_incubation_qc", "ecoli_optimal_colony"
    ]
    adv_keys = [
        "orthophosphate", "orthophosphate_f", "nitrate_n", "nitrate_f",
        "nitrate", "turbidity", "cross_section", "water_depth",
        "downstream_10ft", "discharge"
    ]
    rip_keys = ["bank_evaluated", "riparian_image"]

    used_cols = set()

    def add_cols(keys, target_list):
        for key in keys:
            c = find_col(df, COLUMN_MAP.get(key, []))
            if c:
                target_list.append(c)
                used_cols.add(c)

    add_cols(core_keys, core_cols)
    add_cols(ecoli_keys, ecoli_cols)
    add_cols(adv_keys, adv_cols)
    add_cols(rip_keys, riparian_cols)

    # everything else => general
    for c in cols:
        if c not in used_cols:
            general_cols.append(c)

    return {
        "core": core_cols,
        "ecoli": ecoli_cols,
        "advanced": adv_cols,
        "riparian": riparian_cols,
        "general": general_cols,
    }


# -----------------------------------------------------------------------------
# 3. GENERAL CLEANING
# -----------------------------------------------------------------------------

def parse_datetime(df):
    """Add unified datetime columns (_parsed_date, _parsed_time) if possible."""
    date_col = find_col(df, COLUMN_MAP["sample_date"])
    time_col = find_col(df, COLUMN_MAP["sample_time"])

    if date_col is None:
        return df, None, None

    df["_parsed_date"] = pd.to_datetime(df[date_col], errors="coerce")

    if time_col and df[time_col].notna().any():
        def _parse_t(x):
            if pd.isna(x):
                return None
            x = str(x).strip()
            for fmt in ["%H:%M", "%H:%M:%S", "%I:%M %p"]:
                try:
                    return datetime.strptime(x, fmt).time()
                except Exception:
                    continue
            return None

        df["_parsed_time"] = df[time_col].apply(_parse_t)
    else:
        df["_parsed_time"] = None

    return df, date_col, time_col


def general_cleaning(df):
    """Apply GENERAL rules that are directly data-based."""
    df = df.copy()

    # Remove exact duplicate rows
    df = df.drop_duplicates().reset_index(drop=True)

    # Parse date & time
    df, date_col, time_col = parse_datetime(df)

    # Replace "valid"/"invalid" with blank (string/object columns only)
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].replace(
            {
                "valid": "",
                "Valid": "",
                "VALID": "",
                "invalid": "",
                "Invalid": "",
                "INVALID": "",
            }
        )

    # Sampling time-of-day QC – flag only (no deletions)
    if "_parsed_time" in df.columns and df["_parsed_time"].notna().any():
        times = df["_parsed_time"].dropna().apply(lambda t: t.hour + t.minute / 60.0)
        if len(times) > 0:
            median_hour = times.median()
            df["_sample_hour"] = df["_parsed_time"].apply(
                lambda t: t.hour + t.minute / 60.0 if pd.notna(t) else np.nan
            )
            df["QC_TimeOfDay_OK"] = np.abs(df["_sample_hour"] - median_hour) <= 4
        else:
            df["QC_TimeOfDay_OK"] = np.nan
    else:
        df["QC_TimeOfDay_OK"] = np.nan

    # Sort by Site + Date + Time for neatness
    site_col = find_col(df, COLUMN_MAP["site"])
    sort_cols = []
    if site_col:
        sort_cols.append(site_col)
    if "_parsed_date" in df.columns:
        sort_cols.append("_parsed_date")
    if "_parsed_time" in df.columns:
        sort_cols.append("_parsed_time")

    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    return df


# -----------------------------------------------------------------------------
# 4. CORE CLEANING
# -----------------------------------------------------------------------------

def clean_core(df):
    """Apply CORE rules (depth, temp, DO, pH, cond, TDS, etc.)."""
    df = df.copy()

    flow_col = find_col(df, COLUMN_MAP["flow_severity"])
    sample_depth_col = find_col(df, COLUMN_MAP["sample_depth"])
    total_depth_col = find_col(df, COLUMN_MAP["total_depth"])
    secchi_col = find_col(df, COLUMN_MAP["secchi"])
    tube_col = find_col(df, COLUMN_MAP["tube"])
    tube_mod_col = find_col(df, COLUMN_MAP["tube_mod"])
    do_avg_col = find_col(df, COLUMN_MAP["do_avg"])
    do1_col = find_col(df, COLUMN_MAP["do_1"])
    do2_col = find_col(df, COLUMN_MAP["do_2"])
    air_col = find_col(df, COLUMN_MAP["air_temp"])
    water_col = find_col(df, COLUMN_MAP["water_temp"])
    ph_col = find_col(df, COLUMN_MAP["ph"])
    cond_col = find_col(df, COLUMN_MAP["cond"])
    tds_col = find_col(df, COLUMN_MAP["tds"])

    # --- Total Depth ---
    if total_depth_col:
        depth = pd.to_numeric(df[total_depth_col], errors="coerce")
        # Remove codes like 999
        depth = depth.mask(depth >= 998, np.nan)

        if flow_col:
            flow = df[flow_col].astype(str).str.strip().str.lower()
            # 0 depth is only allowed if flow indicates dry / no water
            mask_zero_bad = (depth == 0) & (~flow.isin(["dry", "no water", "6"]))
            depth = depth.mask(mask_zero_bad, np.nan)

        df[total_depth_col] = depth

    # --- Sample Depth QC (0.3m or half total depth) ---
    if sample_depth_col and total_depth_col:
        sdepth = pd.to_numeric(df[sample_depth_col], errors="coerce")
        tdepth = pd.to_numeric(df[total_depth_col], errors="coerce")
        cond_03 = np.isclose(sdepth, 0.3, atol=0.05)
        cond_half = np.isclose(sdepth, 0.5 * tdepth, atol=0.05)
        df["QC_SampleDepth_OK"] = cond_03 | cond_half
        df.loc[
            (sdepth.notna()) & (~df["QC_SampleDepth_OK"]),
            "QC_SampleDepth_OK"
        ] = False

    # --- Secchi vs Total Depth ---
    if secchi_col and total_depth_col:
        secchi = pd.to_numeric(df[secchi_col], errors="coerce")
        tdepth = pd.to_numeric(df[total_depth_col], errors="coerce")
        secchi = secchi.mask(
            (secchi.notna()) & (tdepth.notna()) & (secchi > tdepth),
            np.nan
        )

        def round_sig(x, sig=2):
            if pd.isna(x) or x == 0:
                return x
            return float(f"{float(x):.{sig}g}")

        secchi = secchi.apply(round_sig)
        df[secchi_col] = secchi

    # --- Transparency Tube ---
    if tube_col:
        tube = pd.to_numeric(df[tube_col], errors="coerce")
        over_mask = tube > 1.2
        tube = tube.mask(over_mask, np.nan)

        def round_sig2(x):
            if pd.isna(x) or x == 0:
                return x
            return float(f"{float(x):.2g}")

        tube = tube.apply(round_sig2)
        df[tube_col] = tube

        if tube_mod_col:
            tube_mod = df[tube_mod_col].astype(str)
            # If original tube >1.2, we mark modifier as >1.2m
            tube_mod = tube_mod.mask(tube.isna() & over_mask, ">1.2m")
            df[tube_mod_col] = tube_mod

    # --- Dissolved Oxygen duplicate titration ---
    if do_avg_col and do1_col and do2_col:
        do1 = pd.to_numeric(df[do1_col], errors="coerce")
        do2 = pd.to_numeric(df[do2_col], errors="coerce")
        diff = (do1 - do2).abs()
        df["QC_DO_dup_within_0.5"] = diff <= 0.5
        do_avg = (do1 + do2) / 2.0
        do_avg = do_avg.mask(diff > 0.5, np.nan)
        df[do1_col] = do1.round(1)
        df[do2_col] = do2.round(1)
        df[do_avg_col] = do_avg.round(1)

    # --- Temperature (Air & Water) ---
    for col in [air_col, water_col]:
        if not col:
            continue
        temp = pd.to_numeric(df[col], errors="coerce")
        # Reasonable physical range
        temp = temp.mask((temp < -5) | (temp > 50), np.nan)
        df[col] = temp.round(1)

    # --- pH ---
    if ph_col:
        ph = pd.to_numeric(df[ph_col], errors="coerce")
        ph = ph.mask((ph < 0) | (ph > 14), np.nan)
        # Flag extreme pH as NaN (e.g., <2 or >12)
        ph = ph.mask((ph < 2) | (ph > 12), np.nan)
        df[ph_col] = ph.round(1)

    # --- Conductivity ---
    if cond_col:
        cond = pd.to_numeric(df[cond_col], errors="coerce")
        cond = cond.mask(cond < 0, np.nan)
        mask_low = cond < 100
        df.loc[mask_low, cond_col] = cond[mask_low].round(0)

        def round_sig3(x):
            if pd.isna(x) or x == 0:
                return x
            return float(f"{float(x):.3g}")

        mask_high = cond >= 100
        df.loc[mask_high, cond_col] = cond[mask_high].apply(round_sig3)

    # --- TDS = Conductivity * 0.65 ---
    if cond_col and tds_col:
        cond = pd.to_numeric(df[cond_col], errors="coerce")
        tds_calc = cond * 0.65
        df["TDS_Calc (mg/L)"] = tds_calc.round(1)
        tds = pd.to_numeric(df[tds_col], errors="coerce")
        tds = tds.fillna(tds_calc)
        df[tds_col] = tds.round(1)

    return df


# -----------------------------------------------------------------------------
# 5. E. COLI CLEANING
# -----------------------------------------------------------------------------

def clean_ecoli(df):
    """Apply E. coli rules (0-values, colony count, incubation, etc.)."""
    df = df.copy()

    ecoli_avg_col = find_col(df, COLUMN_MAP["ecoli_avg"])
    cfu1_col = find_col(df, COLUMN_MAP["ecoli_cfu1"])
    cfu2_col = find_col(df, COLUMN_MAP["ecoli_cfu2"])
    col1_col = find_col(df, COLUMN_MAP["ecoli_colonies1"])
    col2_col = find_col(df, COLUMN_MAP["ecoli_colonies2"])
    temp_col = find_col(df, COLUMN_MAP["ecoli_temp"])
    hold_col = find_col(df, COLUMN_MAP["ecoli_hold"])
    blank_qc_col = find_col(df, COLUMN_MAP["ecoli_blank_qc"])
    optimal_col = find_col(df, COLUMN_MAP["ecoli_optimal_colony"])

    # Remove reported 0 – should be <1 (in CSV we treat 0 as missing)
    if ecoli_avg_col:
        ecoli_avg = pd.to_numeric(df[ecoli_avg_col], errors="coerce")
        ecoli_avg = ecoli_avg.mask(ecoli_avg == 0, np.nan)
        # Step 1: round to nearest whole
        ecoli_avg = ecoli_avg.round(0)

        # Step 2: round that whole number to 2 significant figures
        def round_sig2_int(x):
            if pd.isna(x) or x == 0:
                return x
            return float(f"{float(x):.2g}")

        ecoli_avg = ecoli_avg.apply(round_sig2_int)
        df[ecoli_avg_col] = ecoli_avg

    for col in [cfu1_col, cfu2_col]:
        if not col:
            continue
        cfu = pd.to_numeric(df[col], errors="coerce")
        cfu = cfu.mask(cfu == 0, np.nan)
        df[col] = cfu

    # Colonies counted < 200
    for col in [col1_col, col2_col]:
        if not col:
            continue
        colonies = pd.to_numeric(df[col], errors="coerce")
        bad = colonies >= 200
        df.loc[bad, col] = np.nan
        if ecoli_avg_col:
            df.loc[bad, ecoli_avg_col] = np.nan

    # Incubation temperature 30–36 °C
    if temp_col:
        temp = pd.to_numeric(df[temp_col], errors="coerce")
        df["QC_Ecoli_Temp_30_36"] = (temp >= 30) & (temp <= 36)

    # Incubation period 28–31 hours (if in hours)
    if hold_col:
        hold = pd.to_numeric(df[hold_col], errors="coerce")
        df["QC_Ecoli_Hold_28_31h"] = (hold >= 28) & (hold <= 31)

    # Field blank OK
    if blank_qc_col:
        blank = df[blank_qc_col].astype(str).str.strip().str.lower()
        df["QC_Ecoli_Blank_OK"] = blank.isin(
            ["yes", "true", "ok", "no growth", "none"]
        )

    # Optional “optimal colony number” QC (if present)
    if optimal_col:
        df["QC_Ecoli_OptimalColonyFlag"] = df[optimal_col]

    return df


# -----------------------------------------------------------------------------
# 6. ADVANCED CLEANING
# -----------------------------------------------------------------------------

def clean_advanced(df):
    """Apply ADVANCED rules (turbidity, discharge)."""
    df = df.copy()
    turb_col = find_col(df, COLUMN_MAP["turbidity"])
    discharge_col = find_col(df, COLUMN_MAP["discharge"])

    # Turbidity: remove negative values
    if turb_col:
        turb = pd.to_numeric(df[turb_col], errors="coerce")
        turb = turb.mask(turb < 0, np.nan)
        df[turb_col] = turb

    # Discharge rules
    if discharge_col:
        q = pd.to_numeric(df[discharge_col], errors="coerce")
        q = q.mask(q < 0, np.nan)
        mask_low = q < 10
        df.loc[mask_low, discharge_col] = q[mask_low].round(1)
        mask_high = q >= 10
        df.loc[mask_high, discharge_col] = q[mask_high].round(0)

    return df


# -----------------------------------------------------------------------------
# 7. RIPARIAN CLEANING / QC
# -----------------------------------------------------------------------------

def clean_riparian(df):
    """Apply minimal RIPARIAN checks (bank evaluated, image submitted)."""
    df = df.copy()
    bank_col = find_col(df, COLUMN_MAP["bank_evaluated"])
    img_col = find_col(df, COLUMN_MAP["riparian_image"])

    if bank_col:
        bank = df[bank_col].astype(str).str.strip().str.lower()
        df["QC_Riparian_BankCompleted"] = bank.isin(
            ["yes", "completed", "done", "true"]
        )

    if img_col:
        img = df[img_col].astype(str).str.strip().str.lower()
        df["QC_Riparian_ImageSubmitted"] = img.isin(
            ["yes", "submitted", "true"]
        )

    return df


# -----------------------------------------------------------------------------
# 8. DSR QUANTITY CHECKS
# -----------------------------------------------------------------------------

def dsr_quantity_summary(df, category_cols):
    """
    Compute:
      - number of sites per watershed
      - number of events per site per parameter (for selected columns)
    """
    site_col = find_col(df, COLUMN_MAP["site"])
    watershed_col = find_col(df, COLUMN_MAP["watershed"])
    param_cols = category_cols[:]

    summary = {}

    # 1) watershed -> # sites
    if site_col and watershed_col:
        ws_counts = (
            df.groupby(watershed_col)[site_col]
            .nunique()
            .reset_index(name="n_sites")
        )
    elif site_col:
        ws_counts = pd.DataFrame(
            {
                "Watershed": ["(file_total)"],
                "n_sites": [df[site_col].nunique()],
            }
        )
    else:
        ws_counts = pd.DataFrame(columns=["Watershed", "n_sites"])

    summary["watershed_site_counts"] = ws_counts

    # 2) #events per site per parameter
    if site_col and param_cols:
        records = []
        for p in param_cols:
            if p not in df.columns:
                continue
            counts = (
                df.groupby(site_col)[p]
                .apply(lambda x: x.notna().sum())
                .reset_index(name="n_events")
            )
            counts["parameter"] = p
            records.append(counts)
        if records:
            param_counts = pd.concat(records, ignore_index=True)
        else:
            param_counts = pd.DataFrame(
                columns=[site_col, "n_events", "parameter"]
            )
    else:
        param_counts = pd.DataFrame(
            columns=[site_col if site_col else "Site", "n_events", "parameter"]
        )

    summary["site_param_counts"] = param_counts
    return summary


def filter_dsr_ready(df, category_cols):
    """
    Filter to site/parameter combos that meet:
      - >=10 valid events per parameter per site
      - watershed has >=3 sites (if watershed column exists)
    """
    df = df.copy()
    site_col = find_col(df, COLUMN_MAP["site"])
    watershed_col = find_col(df, COLUMN_MAP["watershed"])

    if not site_col:
        return df

    summary = dsr_quantity_summary(df, category_cols)
    param_counts = summary["site_param_counts"]
    if param_counts.empty:
        return df

    good_pairs = param_counts[param_counts["n_events"] >= 10][[site_col, "parameter"]]

    keep_mask = pd.Series(False, index=df.index)
    for _, row in good_pairs.iterrows():
        s = row[site_col]
        p = row["parameter"]
        if p not in df.columns:
            continue
        mask = (df[site_col] == s) & df[p].notna()
        keep_mask = keep_mask | mask

    df_filtered = df[keep_mask].copy()

    if watershed_col:
        ws_counts = (
            df_filtered.groupby(watershed_col)[site_col]
            .nunique()
            .reset_index(name="n_sites")
        )
        good_ws = ws_counts[ws_counts["n_sites"] >= 3][watershed_col]
        df_filtered = df_filtered[df_filtered[watershed_col].isin(good_ws)]

    return df_filtered.reset_index(drop=True)


# -----------------------------------------------------------------------------
# 9. OUTLIER CLEANER (IQR)
# -----------------------------------------------------------------------------

def iqr_outlier_cleaner(df, cols, k=1.5):
    """
    Remove outliers using IQR rule for selected columns.
    Returns filtered_df, mask_removed.
    """
    df = df.copy()
    mask_keep = pd.Series(True, index=df.index)

    for c in cols:
        if c not in df.columns:
            continue
        x = pd.to_numeric(df[c], errors="coerce")
        q1 = x.quantile(0.25)
        q3 = x.quantile(0.75)
        iqr = q3 - q1
        if pd.isna(iqr) or iqr == 0:
            continue
        lower = q1 - k * iqr
        upper = q3 + k * iqr
        mask_keep &= ((x >= lower) & (x <= upper)) | x.isna()

    filtered_df = df[mask_keep].copy()
    mask_removed = ~mask_keep
    return filtered_df, mask_removed


# -----------------------------------------------------------------------------
# 10. Helper: compute all cleaned dfs
# -----------------------------------------------------------------------------

def get_clean_dfs(raw_df):
    """Run the full cleaning pipeline & categorization."""
    cats = categorize_columns(raw_df)
    gen_df = general_cleaning(raw_df)
    core_df = clean_core(gen_df)
    ecoli_df = clean_ecoli(core_df)
    adv_df = clean_advanced(ecoli_df)
    rip_df = clean_riparian(adv_df)
    all_param_cols = cats["core"] + cats["ecoli"] + cats["advanced"]
    return {
        "categories": cats,
        "general_df": gen_df,
        "clean_df": rip_df,
        "all_param_cols": all_param_cols,
    }

# -----------------------------------------------------------------------------
# 11. UI – TABS
# -----------------------------------------------------------------------------

tabs = st.tabs(
    [
        "Upload File",
        "Site ID Description Check",   # NEW TAB
        "GENERAL Validation",
        "CORE Validation",
        "ECOLI Validation",
        "ADVANCED Validation",
        "RIPARIAN Validation",
        "Run All & Exports",
        "Outlier Cleaner (IQR)",
        "Cleaning Guide",
    ]
)

# --- Tab 1: Upload File ------------------------------------------------------
with tabs[0]:
    st.subheader("Upload File")
    uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

    if uploaded_file is not None:
        raw_bytes = uploaded_file.read()
        raw_df = pd.read_csv(io.BytesIO(raw_bytes))
        st.session_state["raw_df"] = raw_df
        st.success(
            f"File loaded with {raw_df.shape[0]} rows and {raw_df.shape[1]} columns."
        )
        st.dataframe(raw_df.head(30))
    else:
        st.info("Please upload a CSV file here, then move to the other tabs.")

has_data = "raw_df" in st.session_state

clean_context = None
if has_data:
    clean_context = get_clean_dfs(st.session_state["raw_df"])
    categories = clean_context["categories"]
    general_df = clean_context["general_df"]
    clean_df = clean_context["clean_df"]
    all_param_cols = clean_context["all_param_cols"]


# --- Tab 2: Site ID Description Check ---------------------------------------
with tabs[1]:
    st.subheader(" Site ID – Description Consistency Check")

    if not has_data:
        st.warning("Please upload a CSV file first in the 'Upload File' tab.")
    else:
        raw_df = st.session_state["raw_df"].copy()

        # Detect Site ID column
        site_col = find_col(raw_df, COLUMN_MAP["site"])

        # Detect explicit description column
        desc_col = None
        for c in raw_df.columns:
            if "Description" in c or "Site ID: Description" in c:
                desc_col = c
                break

        # CASE 1 — Two separate columns exist (your data)
        if site_col and desc_col:
            st.success("Detected separate Site ID and Description columns.")

            df_tmp = raw_df.copy()
            df_tmp["_SiteID"] = df_tmp[site_col].astype(str).str.strip()
            df_tmp["_Description"] = df_tmp[desc_col].astype(str).str.strip()

            # Count number of unique descriptions per Site ID
            desc_check = (
                df_tmp.groupby("_SiteID")["_Description"]
                .nunique(dropna=True)
                .reset_index(name="n_descriptions")
            )

            problem_sites = desc_check[desc_check["n_descriptions"] > 1]

            if problem_sites.empty:
                st.success(" All Site IDs have a single unique description. Safe to continue!")
            else:
                st.error(" Problem detected – multiple descriptions found for the same Site ID!")

                st.markdown("###  Sites with inconsistent descriptions:")
                st.dataframe(problem_sites)

                st.markdown(
                    """
                    <div style='color:red; font-size:20px; font-weight:bold;'>
                     It is recommended to stop here — inconsistent Site Descriptions were found.<br>
                    A single Site ID cannot have multiple different descriptions.
                    </div>
                    """,
                    unsafe_allow_html=True
                )

                # Show all description variants
                st.markdown("###  Description details for problematic Site IDs")
                detail_table = (
                    df_tmp[df_tmp["_SiteID"].isin(problem_sites["_SiteID"])]
                    .loc[:, ["_SiteID", "_Description"]]
                    .drop_duplicates()
                    .sort_values(["_SiteID", "_Description"])
                )
                st.dataframe(detail_table)

        # CASE 2 — No description column, check for embedded pattern (ID: Desc)
        else:
            st.info(
                "No explicit description column detected. "
                "Attempting embedded pattern 'SiteID: Description' instead."
            )

            def extract_site_id(x):
                if pd.isna(x):
                    return None
                x = str(x)
                if ":" in x:
                    return x.split(":", 1)[0].strip()
                return x.strip()

            def extract_desc(x):
                if pd.isna(x):
                    return None
                x = str(x)
                if ":" in x:
                    return x.split(":", 1)[1].strip()
                return None

            df_desc = raw_df.copy()
            df_desc["_SiteID"] = df_desc[site_col].apply(extract_site_id)
            df_desc["_Description"] = df_desc[site_col].apply(extract_desc)

            if df_desc["_Description"].notna().sum() == 0:
                st.info(
                    "No descriptions detected in embedded format (no 'SiteID: Description')."
                )
            else:
                desc_check = (
                    df_desc.groupby("_SiteID")["_Description"]
                    .nunique(dropna=True)
                    .reset_index(name="n_descriptions")
                )

                problem_sites = desc_check[desc_check["n_descriptions"] > 1]

                if problem_sites.empty:
                    st.success("All Site IDs have consistent descriptions.")
                else:
                    st.error(
                        "Multiple descriptions detected for at least one Site ID."
                    )
                    st.dataframe(problem_sites)

# --- Tab 3: GENERAL Validation ----------------------------------------------
with tabs[2]:
    st.subheader("GENERAL Validation")

    if not has_data:
        st.warning("Please upload a CSV file first.")
    else:
        st.markdown("### Raw data (sample)")
        st.dataframe(st.session_state["raw_df"].head(20))

        st.markdown("### After GENERAL cleaning (sample)")
        st.dataframe(general_df.head(20))

        st.markdown("### GENERAL QC flags")
        qc_cols = [c for c in general_df.columns if c.startswith("QC_")]
        if qc_cols:
            st.write("QC columns created at this stage:")
            st.write(qc_cols)
            st.dataframe(general_df[qc_cols].head(30))
        else:
            st.info("No GENERAL QC columns were generated for this file.")

# --- Tab 4: CORE Validation --------------------------------------------------
with tabs[3]:
    st.subheader("CORE Validation")

    if not has_data:
        st.warning("Please upload a CSV file first.")
    else:
        core_cols = categories["core"]
        if core_cols:
            st.write("Detected CORE columns:")
            st.write(core_cols)
            qc_cols = [
                c for c in clean_df.columns
                if c.startswith("QC_")
                and "Ecoli" not in c
                and "Riparian" not in c
            ]
            view_cols = core_cols + qc_cols
            st.dataframe(clean_df[view_cols].head(50))
        else:
            st.warning("No CORE columns found.")

# --- Tab 5: ECOLI Validation -------------------------------------------------
with tabs[4]:
    st.subheader("ECOLI Validation")

    if not has_data:
        st.warning("Please upload a CSV file first.")
    else:
        ecoli_cols = categories["ecoli"]
        if ecoli_cols:
            st.write("Detected ECOLI columns:")
            st.write(ecoli_cols)
            view_cols = ecoli_cols + [
                c for c in clean_df.columns if c.startswith("QC_Ecoli")
            ]
            st.dataframe(clean_df[view_cols].head(50))
        else:
            st.warning("No ECOLI columns found.")

# --- Tab 6: ADVANCED Validation ---------------------------------------------
with tabs[5]:
    st.subheader("ADVANCED Validation")

    if not has_data:
        st.warning("Please upload a CSV file first.")
    else:
        adv_cols = categories["advanced"]
        if adv_cols:
            st.write("Detected ADVANCED columns:")
            st.write(adv_cols)
            st.dataframe(clean_df[adv_cols].head(50))
        else:
            st.warning("No ADVANCED columns found.")

# --- Tab 7: RIPARIAN Validation ---------------------------------------------
with tabs[6]:
    st.subheader("RIPARIAN Validation")

    if not has_data:
        st.warning("Please upload a CSV file first.")
    else:
        rip_cols = categories["riparian"]
        if rip_cols:
            st.write("Detected RIPARIAN columns:")
            st.write(rip_cols)
            view_cols = rip_cols + [
                c for c in clean_df.columns if c.startswith("QC_Riparian")
            ]
            st.dataframe(clean_df[view_cols].head(50))
        else:
            st.warning("No RIPARIAN columns found.")

# --- Tab 8: Run All & Exports -----------------------------------------------
with tabs[7]:
    st.subheader("Run All & Exports")

    if not has_data:
        st.warning("Please upload a CSV file first.")
    else:
        st.markdown("### DSR Quantity Summary")
        summary = dsr_quantity_summary(clean_df, all_param_cols)

        st.markdown("**Number of sites per watershed**")
        st.dataframe(summary["watershed_site_counts"])

        st.markdown("**Number of events per parameter per site**")
        st.dataframe(summary["site_param_counts"])

        apply_dsr_filter = st.checkbox(
            "Apply DSR filter (≥3 sites per watershed AND ≥10 events per parameter per site)",
            value=False
        )

        if apply_dsr_filter:
            dsr_ready_df = filter_dsr_ready(clean_df, all_param_cols)
            st.success(
                f"Number of DSR-ready rows: {dsr_ready_df.shape[0]} "
                f"(out of {clean_df.shape[0]} cleaned rows)."
            )
        else:
            dsr_ready_df = clean_df.copy()
            st.info("DSR filter is OFF. All cleaned data are included.")

        st.markdown("### Preview of fully cleaned data")
        st.dataframe(clean_df.head(50))

        st.markdown("### Download outputs")

        # Cleaned CSV
        buf_clean = io.BytesIO()
        clean_df.to_csv(buf_clean, index=False)
        st.download_button(
            label="Before download check Site ID Description Check  tab- Download Cleaned CSV",
            data=buf_clean.getvalue(),
            file_name="Before download check Site ID Description Check tab - cleaned_data.csv",
            mime="text/csv",
            key="download_clean"
        )

        # DSR-ready CSV
        buf_dsr = io.BytesIO()
        dsr_ready_df.to_csv(buf_dsr, index=False)
        st.download_button(
            label="Before download check Site ID Description Check  tab- Download DSR-ready CSV",
            data=buf_dsr.getvalue(),
            file_name="Before download check Site ID Description Check tab- cleaned_data_DSR_ready.csv",
            mime="text/csv",
            key="download_dsr"
        )

# --- Tab 9: Outlier Cleaner (IQR) -------------------------------------------
with tabs[8]:
    st.subheader("Outlier Cleaner (IQR)")

    if not has_data:
        st.warning("Please upload a CSV file first.")
    else:
        st.write(
            "Use this section to remove outliers from the cleaned dataset using the IQR rule "
            "for selected numeric columns."
        )

        numeric_cols = clean_df.select_dtypes(include=[np.number]).columns.tolist()
        if not numeric_cols:
            st.info("No numeric columns found for IQR-based outlier cleaning.")
        else:
            selected_cols = st.multiselect(
                "Select numeric columns for IQR outlier cleaning:",
                numeric_cols,
                default=[]
            )
            k = st.slider(
                "IQR multiplier (typical value is 1.5):",
                min_value=0.5,
                max_value=3.0,
                value=1.5,
                step=0.1
            )

            if selected_cols:
                filtered_df, mask_removed = iqr_outlier_cleaner(
                    clean_df, selected_cols, k=k
                )
                n_removed = mask_removed.sum()
                st.write(
                    f"Number of rows removed as outliers: {n_removed} "
                    f"(out of {clean_df.shape[0]} cleaned rows)."
                )
                st.dataframe(filtered_df.head(50))

                buf_iqr = io.BytesIO()
                filtered_df.to_csv(buf_iqr, index=False)
                st.download_button(
                    label="Download IQR-filtered CSV",
                    data=buf_iqr.getvalue(),
                    file_name="Before download check Site ID Description Check  tab- cleaned_data_IQR_filtered.csv",
                    mime="text/csv",
                    key="download_iqr"
                )
            else:
                st.info("Select at least one numeric column to perform outlier cleaning.")

# --- Tab 10: Cleaning Guide --------------------------------------------------
with tabs[9]:
    st.subheader("Cleaning Guide")

    st.markdown(
        """
This tab summarizes the cleaning rules implemented by this app.

### GENERAL
- Remove duplicate entries.
- Replace “valid/invalid” with blanks.
- Remove physically impossible values.
- Ensure consistent sampling time of day.
- Minimum of 3 sites per watershed.
- Minimum of 10 events per site per parameter.
- Sort data by site and date after cleaning.

### CORE
- Sample depth must be 0.3 m or half of total depth.
- Total depth = 0 only allowed when Flow Severity indicates no water.
- DO duplicates must be within 0.5 mg/L.
- Secchi ≤ Total Depth; report to two significant figures.
- Transparency Tube ≤ 1.2 m; >1.2m reported as “>1.2m”.
- TDS = Conductivity × 0.65.
- pH, temperature, TDS, DO cleaned and range-checked.

### E. COLI
- Incubation Temperature: 30–36°C.
- Incubation Period: 28–31 hours.
- Colony Count < 200.
- Field Blank must show no growth.
- Zero values treated as <1 and removed.
- Average: round to whole number → then two significant figures.

### ADVANCED
- Turbidity: negative removed.
- Streamflow: <10 → 1 decimal; ≥10 → whole number.

### RIPARIAN
- Bank Evaluated flag checked.
- Image Submitted flag checked.

This app applies all rules that can be inferred directly from the CSV.
"""
    )

st.caption("Built to support standardized cleaning of water quality data for DSR/WSR workflows ")

