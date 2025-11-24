# Water_Validation_App_UPDATED_for_your_headers.py
# Streamlit app for automated Water Quality Data Validation (CRP/TST-style)
# Updated to robustly recognize your exact column headers + minor variants.

import io
import re
import numpy as np
import pandas as pd
import streamlit as st
from datetime import datetime

st.set_page_config(page_title="Water Quality Data Validation App", layout="wide")
st.title("Water Quality Data Validation App")

COLUMN_MAP = {
    "site": ["Site ID: Site Name", "Site ID", "Site", "Site ID: Site Name "],
    "site_desc": ["Site ID: Description", "Site Description", "Site ID: Description "],
    "sample_date": ["Sample Date", "Date", "Sample_Date"],
    "sample_time": ["Sample Time Final Format", "Sample Time", "Time", "SampleTime"],
    "watershed": ["Watershed", "Watershed Name", "Watershed: Name"],

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

    "valid_flag": ["Validation", "Valid/Invalid", "Data Quality"],

    # E. COLI
    "ecoli_avg": ["E. Coli Average", "E. coli Average", "E.Coli Average", "E coli Average"],
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
    "ecoli_incubation_qc": ["Incubation time is between 24 hours", "Incubation Period QC"],
    "ecoli_optimal_colony": ["Optimal colony number is achieved (<200)"],

    # ADVANCED
    "orthophosphate": ["Orthophosphate", "Phosphate Value", "Phosphate (mg/L)"],
    "orthophosphate_f": ["Filtered (Orthophosphate)", "Sample Filtered"],
    "nitrate_n": ["Nitrate-Nitrogen VALUE (ppm or mg/L)", "Nitrate-Nitrogen (mg/L)"],
    "nitrate_f": ["Filtered (Nitrate-Nitrogen)"],
    "nitrate": ["Nitrate"],
    "turbidity": ["Turbidity Result (JTU)", "Turbidity (NTU)", "Turbidity"],
    "cross_section": ["Waterbody Cross Section"],
    "water_depth": ["Water Depth"],
    "downstream_10ft": ["10-foot Downstream Measurement"],
    "discharge": ["Discharge Recorded", "Streamflow (ft2/sec)", "Discharge (cfs)"],

    # RIPARIAN
    "bank_evaluated": ["Bank Evaluated", "Bank evaluated is completed"],
    "riparian_image": ["Image Submitted", "Image of site was submitted"],
}

def _norm_name(x: str) -> str:
    if x is None:
        return ""
    x = str(x).lower().strip()
    x = re.sub(r"[^\w\s]", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

def find_col(df, candidates):
    if df is None or df.empty:
        return None
    norm_df_cols = {_norm_name(c): c for c in df.columns}
    for cand in candidates:
        c_norm = _norm_name(cand)
        if c_norm in norm_df_cols:
            return norm_df_cols[c_norm]
    return None

def find_col_fuzzy(df, required_keywords):
    if df is None or df.empty:
        return None
    for c in df.columns:
        s = _norm_name(c)
        if all(k.lower() in s for k in required_keywords):
            return c
    return None

def categorize_columns(df):
    cols = df.columns.tolist()
    core_cols, ecoli_cols, adv_cols, riparian_cols, general_cols = [], [], [], [], []

    core_keys = ["sample_depth","total_depth","secchi","secchi_mod","tube","tube_mod",
                 "do_avg","do_1","do_2","air_temp","water_temp","ph","cond","tds",
                 "salinity","flow_severity","rain_acc","days_since_rain"]
    ecoli_keys = ["ecoli_avg","ecoli_cfu1","ecoli_cfu2","ecoli_colonies1","ecoli_colonies2",
                  "ecoli_size1","ecoli_size2","ecoli_dil1","ecoli_dil2","ecoli_temp",
                  "ecoli_hold","ecoli_blank_qc","ecoli_incubation_qc","ecoli_optimal_colony"]
    adv_keys = ["orthophosphate","orthophosphate_f","nitrate_n","nitrate_f",
                "nitrate","turbidity","cross_section","water_depth",
                "downstream_10ft","discharge"]
    rip_keys = ["bank_evaluated","riparian_image"]

    used_cols = set()

    def add_cols(keys, target_list):
        for key in keys:
            c = find_col(df, COLUMN_MAP.get(key, []))
            if c is None:
                if key == "ecoli_avg":
                    c = find_col_fuzzy(df, ["coli","average"])
                if key == "cond":
                    c = find_col_fuzzy(df, ["conductivity"])
                if key == "do_avg":
                    c = find_col_fuzzy(df, ["dissolved","oxygen","average"])
                if key == "ph":
                    c = find_col_fuzzy(df, ["ph"])
            if c:
                target_list.append(c)
                used_cols.add(c)

    add_cols(core_keys, core_cols)
    add_cols(ecoli_keys, ecoli_cols)
    add_cols(adv_keys, adv_cols)
    add_cols(rip_keys, riparian_cols)

    for c in cols:
        if c not in used_cols:
            general_cols.append(c)

    return {"core": core_cols, "ecoli": ecoli_cols, "advanced": adv_cols,
            "riparian": riparian_cols, "general": general_cols}

def parse_datetime(df):
    date_col = find_col(df, COLUMN_MAP["sample_date"])
    time_col = find_col(df, COLUMN_MAP["sample_time"])
    if date_col is None:
        return df, None, None
    df["_parsed_date"] = pd.to_datetime(df[date_col], errors="coerce")
    if time_col and df[time_col].notna().any():
        def _parse_t(x):
            if pd.isna(x): return None
            x = str(x).strip()
            for fmt in ["%H:%M","%H:%M:%S","%I:%M %p"]:
                try: return datetime.strptime(x, fmt).time()
                except Exception: continue
            return None
        df["_parsed_time"] = df[time_col].apply(_parse_t)
    else:
        df["_parsed_time"] = None
    return df, date_col, time_col

def general_cleaning(df):
    df = df.copy()
    df = df.drop_duplicates().reset_index(drop=True)
    df, _, _ = parse_datetime(df)

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].replace({"valid":"", "Valid":"", "VALID":"",
                                   "invalid":"", "Invalid":"", "INVALID":""})

    if "_parsed_time" in df.columns and df["_parsed_time"].notna().any():
        times = df["_parsed_time"].dropna().apply(lambda t: t.hour + t.minute/60.0)
        if len(times) > 0:
            median_hour = times.median()
            df["_sample_hour"] = df["_parsed_time"].apply(
                lambda t: t.hour + t.minute/60.0 if pd.notna(t) else np.nan
            )
            df["QC_TimeOfDay_OK"] = np.abs(df["_sample_hour"] - median_hour) <= 4
        else:
            df["QC_TimeOfDay_OK"] = np.nan
    else:
        df["QC_TimeOfDay_OK"] = np.nan

    site_col = find_col(df, COLUMN_MAP["site"])
    sort_cols = [c for c in [site_col, "_parsed_date", "_parsed_time"] if c]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)
    return df

def clean_core(df):
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

    if total_depth_col:
        depth = pd.to_numeric(df[total_depth_col], errors="coerce")
        depth = depth.mask(depth >= 998, np.nan)
        if flow_col:
            flow = df[flow_col].astype(str).str.strip().str.lower()
            mask_zero_bad = (depth == 0) & (~flow.isin(["dry","no water","6"]))
            depth = depth.mask(mask_zero_bad, np.nan)
        df[total_depth_col] = depth

    if sample_depth_col and total_depth_col:
        sdepth = pd.to_numeric(df[sample_depth_col], errors="coerce")
        tdepth = pd.to_numeric(df[total_depth_col], errors="coerce")
        cond_03 = np.isclose(sdepth, 0.3, atol=0.05)
        cond_half = np.isclose(sdepth, 0.5 * tdepth, atol=0.05)
        df["QC_SampleDepth_OK"] = cond_03 | cond_half
        df.loc[(sdepth.notna()) & (~df["QC_SampleDepth_OK"]), "QC_SampleDepth_OK"] = False

    if secchi_col and total_depth_col:
        secchi = pd.to_numeric(df[secchi_col], errors="coerce")
        tdepth = pd.to_numeric(df[total_depth_col], errors="coerce")
        secchi = secchi.mask((secchi.notna())&(tdepth.notna())&(secchi>tdepth), np.nan)
        secchi = secchi.apply(lambda x: float(f"{x:.2g}") if pd.notna(x) and x!=0 else x)
        df[secchi_col] = secchi

    if tube_col:
        tube = pd.to_numeric(df[tube_col], errors="coerce")
        over_mask = tube > 1.2
        tube = tube.mask(over_mask, np.nan)
        tube = tube.apply(lambda x: float(f"{x:.2g}") if pd.notna(x) and x!=0 else x)
        df[tube_col] = tube
        if tube_mod_col:
            tube_mod = df[tube_mod_col].astype(str)
            tube_mod = tube_mod.mask(tube.isna() & over_mask, ">1.2m")
            df[tube_mod_col] = tube_mod

    if do_avg_col and do1_col and do2_col:
        do1 = pd.to_numeric(df[do1_col], errors="coerce")
        do2 = pd.to_numeric(df[do2_col], errors="coerce")
        diff = (do1-do2).abs()
        df["QC_DO_dup_within_0.5"] = diff <= 0.5
        do_avg = (do1+do2)/2.0
        do_avg = do_avg.mask(diff>0.5, np.nan)
        df[do1_col], df[do2_col], df[do_avg_col] = do1.round(1), do2.round(1), do_avg.round(1)

    for col in [air_col, water_col]:
        if not col: continue
        temp = pd.to_numeric(df[col], errors="coerce")
        temp = temp.mask((temp<-5)|(temp>50), np.nan)
        df[col] = temp.round(1)

    if ph_col:
        ph = pd.to_numeric(df[ph_col], errors="coerce")
        ph = ph.mask((ph<0)|(ph>14), np.nan)
        ph = ph.mask((ph<2)|(ph>12), np.nan)
        df[ph_col] = ph.round(1)

    if cond_col:
        cond = pd.to_numeric(df[cond_col], errors="coerce")
        cond = cond.mask(cond<0, np.nan)
        mask_low = cond < 100
        df.loc[mask_low, cond_col] = cond[mask_low].round(0)
        mask_high = cond >= 100
        df.loc[mask_high, cond_col] = cond[mask_high].apply(
            lambda x: float(f"{x:.3g}") if pd.notna(x) and x!=0 else x
        )

    if cond_col and tds_col:
        cond = pd.to_numeric(df[cond_col], errors="coerce")
        tds_calc = cond * 0.65
        df["TDS_Calc (mg/L)"] = tds_calc.round(1)
        tds = pd.to_numeric(df[tds_col], errors="coerce")
        df[tds_col] = tds.fillna(tds_calc).round(1)

    return df

def clean_ecoli(df):
    df = df.copy()
    ecoli_avg_col = find_col(df, COLUMN_MAP["ecoli_avg"]) or find_col_fuzzy(df, ["coli","average"])
    cfu1_col = find_col(df, COLUMN_MAP["ecoli_cfu1"])
    cfu2_col = find_col(df, COLUMN_MAP["ecoli_cfu2"])
    col1_col = find_col(df, COLUMN_MAP["ecoli_colonies1"])
    col2_col = find_col(df, COLUMN_MAP["ecoli_colonies2"])
    temp_col = find_col(df, COLUMN_MAP["ecoli_temp"])
    hold_col = find_col(df, COLUMN_MAP["ecoli_hold"])
    blank_qc_col = find_col(df, COLUMN_MAP["ecoli_blank_qc"])
    optimal_col = find_col(df, COLUMN_MAP["ecoli_optimal_colony"])

    if ecoli_avg_col:
        ecoli_avg = pd.to_numeric(df[ecoli_avg_col], errors="coerce")
        ecoli_avg = ecoli_avg.mask(ecoli_avg==0, np.nan).round(0)
        ecoli_avg = ecoli_avg.apply(lambda x: float(f"{x:.2g}") if pd.notna(x) and x!=0 else x)
        df[ecoli_avg_col] = ecoli_avg

    for col in [cfu1_col, cfu2_col]:
        if not col: continue
        cfu = pd.to_numeric(df[col], errors="coerce").mask(lambda s: s==0, np.nan)
        df[col] = cfu

    for col in [col1_col, col2_col]:
        if not col: continue
        colonies = pd.to_numeric(df[col], errors="coerce")
        bad = colonies >= 200
        df.loc[bad, col] = np.nan
        if ecoli_avg_col:
            df.loc[bad, ecoli_avg_col] = np.nan

    if temp_col:
        temp = pd.to_numeric(df[temp_col], errors="coerce")
        df["QC_Ecoli_Temp_30_36"] = (temp>=30)&(temp<=36)

    if hold_col:
        hold = pd.to_numeric(df[hold_col], errors="coerce")
        df["QC_Ecoli_Hold_28_31h"] = (hold>=28)&(hold<=31)

    if blank_qc_col:
        blank = df[blank_qc_col].astype(str).str.strip().str.lower()
        df["QC_Ecoli_Blank_OK"] = blank.isin(["yes","true","ok","no growth","none"])

    if optimal_col:
        df["QC_Ecoli_OptimalColonyFlag"] = df[optimal_col]
    return df

def clean_advanced(df):
    df = df.copy()
    turb_col = find_col(df, COLUMN_MAP["turbidity"])
    discharge_col = find_col(df, COLUMN_MAP["discharge"])
    if turb_col:
        turb = pd.to_numeric(df[turb_col], errors="coerce").mask(lambda s: s<0, np.nan)
        df[turb_col] = turb
    if discharge_col:
        q = pd.to_numeric(df[discharge_col], errors="coerce").mask(lambda s: s<0, np.nan)
        df.loc[q<10, discharge_col] = q[q<10].round(1)
        df.loc[q>=10, discharge_col] = q[q>=10].round(0)
    return df

def clean_riparian(df):
    df = df.copy()
    bank_col = find_col(df, COLUMN_MAP["bank_evaluated"])
    img_col = find_col(df, COLUMN_MAP["riparian_image"])
    if bank_col:
        bank = df[bank_col].astype(str).str.strip().str.lower()
        df["QC_Riparian_BankCompleted"] = bank.isin(["yes","completed","done","true"])
    if img_col:
        img = df[img_col].astype(str).str.strip().str.lower()
        df["QC_Riparian_ImageSubmitted"] = img.isin(["yes","submitted","true"])
    return df

def dsr_quantity_summary(df, category_cols):
    site_col = find_col(df, COLUMN_MAP["site"])
    watershed_col = find_col(df, COLUMN_MAP["watershed"])
    summary = {}
    if site_col and watershed_col:
        ws_counts = df.groupby(watershed_col)[site_col].nunique().reset_index(name="n_sites")
    elif site_col:
        ws_counts = pd.DataFrame({"Watershed":["(file_total)"], "n_sites":[df[site_col].nunique()]})
    else:
        ws_counts = pd.DataFrame(columns=["Watershed","n_sites"])
    summary["watershed_site_counts"] = ws_counts

    records=[]
    if site_col and category_cols:
        for p in category_cols:
            if p not in df.columns: continue
            counts = df.groupby(site_col)[p].apply(lambda x: x.notna().sum()).reset_index(name="n_events")
            counts["parameter"]=p
            records.append(counts)
    summary["site_param_counts"] = pd.concat(records, ignore_index=True) if records else pd.DataFrame(
        columns=[site_col if site_col else "Site","n_events","parameter"]
    )
    return summary

def build_exclusion_report(df, category_cols, min_events=10):
    site_col = find_col(df, COLUMN_MAP["site"])
    if not site_col:
        return pd.DataFrame(columns=["Site","parameter","n_valid","decision","reason"])
    param_counts = dsr_quantity_summary(df, category_cols)["site_param_counts"].copy()
    if param_counts.empty:
        return pd.DataFrame(columns=[site_col,"parameter","n_valid","decision","reason"])
    param_counts = param_counts.rename(columns={"n_events":"n_valid"})
    param_counts["decision"] = np.where(param_counts["n_valid"]>=min_events,"KEEP","EXCLUDE")
    param_counts["reason"] = np.where(param_counts["decision"]=="EXCLUDE",
                                      f"<{min_events} valid values for this parameter at this site","")
    return param_counts[[site_col,"parameter","n_valid","decision","reason"]]

def filter_dsr_ready(df, category_cols):
    df=df.copy()
    site_col = find_col(df, COLUMN_MAP["site"])
    watershed_col = find_col(df, COLUMN_MAP["watershed"])
    if not site_col:
        return df, pd.DataFrame()

    ecoli_avg_col = find_col(df, COLUMN_MAP["ecoli_avg"]) or find_col_fuzzy(df, ["coli","average"])
    exclusion_report = build_exclusion_report(df, category_cols, min_events=10)
    param_counts = dsr_quantity_summary(df, category_cols)["site_param_counts"]
    if param_counts.empty:
        return df, exclusion_report

    if ecoli_avg_col:
        non_ecoli_counts = param_counts[param_counts["parameter"]!=ecoli_avg_col]
    else:
        non_ecoli_counts = param_counts
    good_non_ecoli = non_ecoli_counts[non_ecoli_counts["n_events"]>=10][[site_col,"parameter"]]

    if ecoli_avg_col:
        ecoli_counts = param_counts[param_counts["parameter"]==ecoli_avg_col]
        good_ecoli = ecoli_counts[ecoli_counts["n_events"]>=10][[site_col,"parameter"]]
    else:
        good_ecoli = pd.DataFrame(columns=[site_col,"parameter"])

    good_pairs = pd.concat([good_non_ecoli, good_ecoli], ignore_index=True)

    keep_mask = pd.Series(False, index=df.index)
    for _, row in good_pairs.iterrows():
        s=row[site_col]; p=row["parameter"]
        if p not in df.columns: continue
        keep_mask |= (df[site_col]==s) & df[p].notna()

    df_filtered = df[keep_mask].copy()
    if watershed_col:
        ws_counts = df_filtered.groupby(watershed_col)[site_col].nunique().reset_index(name="n_sites")
        good_ws = ws_counts[ws_counts["n_sites"]>=3][watershed_col]
        df_filtered = df_filtered[df_filtered[watershed_col].isin(good_ws)]
    return df_filtered.reset_index(drop=True), exclusion_report

def iqr_outlier_cleaner(df, cols, k=1.5):
    df=df.copy()
    mask_keep = pd.Series(True, index=df.index)
    for c in cols:
        if c not in df.columns: continue
        x = pd.to_numeric(df[c], errors="coerce")
        q1,q3 = x.quantile(0.25), x.quantile(0.75)
        iqr=q3-q1
        if pd.isna(iqr) or iqr==0: continue
        lower, upper = q1-k*iqr, q3+k*iqr
        mask_keep &= ((x>=lower)&(x<=upper)) | x.isna()
    return df[mask_keep].copy(), ~mask_keep

def get_clean_dfs(raw_df):
    cats = categorize_columns(raw_df)
    gen_df = general_cleaning(raw_df)
    core_df = clean_core(gen_df)
    ecoli_df = clean_ecoli(core_df)
    adv_df = clean_advanced(ecoli_df)
    rip_df = clean_riparian(adv_df)
    all_param_cols = cats["core"] + cats["ecoli"] + cats["advanced"]
    return {"categories":cats,"general_df":gen_df,"clean_df":rip_df,"all_param_cols":all_param_cols}

tabs = st.tabs(["Upload File","Site ID Description Check","GENERAL Validation","CORE Validation",
                "ECOLI Validation","ADVANCED Validation","RIPARIAN Validation",
                "Run All & Exports","Outlier Cleaner (IQR)","Cleaning Guide"])

with tabs[0]:
    st.subheader("Upload File")
    uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])
    if uploaded_file is not None:
        raw_bytes = uploaded_file.read()
        raw_df = pd.read_csv(io.BytesIO(raw_bytes))
        st.session_state["raw_df"] = raw_df
        st.success(f"File loaded with {raw_df.shape[0]} rows and {raw_df.shape[1]} columns.")
        st.dataframe(raw_df.head(30))
    else:
        st.info("Please upload a CSV file here, then move to the other tabs.")

has_data = "raw_df" in st.session_state

if has_data:
    clean_context = get_clean_dfs(st.session_state["raw_df"])
    categories = clean_context["categories"]
    general_df = clean_context["general_df"]
    clean_df = clean_context["clean_df"]
    all_param_cols = clean_context["all_param_cols"]

with tabs[1]:
    st.subheader(" Site ID – Description Consistency Check")
    if not has_data:
        st.warning("Please upload a CSV file first in the 'Upload File' tab.")
    else:
        raw_df = st.session_state["raw_df"].copy()
        site_col = find_col(raw_df, COLUMN_MAP["site"])
        desc_col = find_col(raw_df, COLUMN_MAP["site_desc"])
        if site_col and desc_col:
            st.success("Detected separate Site ID and Description columns.")
            df_tmp = raw_df.copy()
            df_tmp["_SiteID"] = df_tmp[site_col].astype(str).str.strip()
            df_tmp["_Description"] = df_tmp[desc_col].astype(str).str.strip()
            desc_check = df_tmp.groupby("_SiteID")["_Description"].nunique(dropna=True).reset_index(name="n_descriptions")
            problem_sites = desc_check[desc_check["n_descriptions"]>1]
            if problem_sites.empty:
                st.success(" All Site IDs have a single unique description. Safe to continue!")
            else:
                st.error(" Problem detected – multiple descriptions found for the same Site ID!")
                st.dataframe(problem_sites)
        else:
            st.info("No explicit description column detected.")

with tabs[2]:
    st.subheader("GENERAL Validation")
    if not has_data:
        st.warning("Please upload a CSV file first.")
    else:
        st.dataframe(st.session_state["raw_df"].head(20))
        st.dataframe(general_df.head(20))
        qc_cols=[c for c in general_df.columns if c.startswith("QC_")]
        if qc_cols:
            st.write(qc_cols)
            st.dataframe(general_df[qc_cols].head(30))

with tabs[3]:
    st.subheader("CORE Validation")
    if has_data:
        core_cols = categories["core"]
        if core_cols:
            qc_cols=[c for c in clean_df.columns if c.startswith("QC_") and "Ecoli" not in c and "Riparian" not in c]
            st.dataframe(clean_df[core_cols+qc_cols].head(50))

with tabs[4]:
    st.subheader("ECOLI Validation")
    if has_data:
        ecoli_cols = categories["ecoli"]
        if ecoli_cols:
            view_cols = ecoli_cols + [c for c in clean_df.columns if c.startswith("QC_Ecoli")]
            st.dataframe(clean_df[view_cols].head(50))

with tabs[5]:
    st.subheader("ADVANCED Validation")
    if has_data:
        adv_cols = categories["advanced"]
        if adv_cols:
            st.dataframe(clean_df[adv_cols].head(50))

with tabs[6]:
    st.subheader("RIPARIAN Validation")
    if has_data:
        rip_cols = categories["riparian"]
        if rip_cols:
            view_cols = rip_cols + [c for c in clean_df.columns if c.startswith("QC_Riparian")]
            st.dataframe(clean_df[view_cols].head(50))

with tabs[7]:
    st.subheader("Run All & Exports")
    if has_data:
        summary = dsr_quantity_summary(clean_df, all_param_cols)
        st.dataframe(summary["watershed_site_counts"])
        st.dataframe(summary["site_param_counts"])

        apply_dsr_filter = st.checkbox("Apply DSR filter (≥3 sites per watershed AND ≥10 events per parameter per site)", value=False)
        if apply_dsr_filter:
            dsr_ready_df, exclusion_report = filter_dsr_ready(clean_df, all_param_cols)
            st.success(f"Number of DSR-ready rows: {dsr_ready_df.shape[0]} (out of {clean_df.shape[0]} cleaned rows).")
            st.markdown("### Exclusion Report")
            st.dataframe(exclusion_report)
        else:
            dsr_ready_df = clean_df.copy()
            exclusion_report = pd.DataFrame()

        buf_clean = io.BytesIO(); clean_df.to_csv(buf_clean, index=False)
        st.download_button("Download Cleaned CSV", buf_clean.getvalue(),
                           "cleaned_data.csv","text/csv",key="download_clean")

        buf_dsr = io.BytesIO(); dsr_ready_df.to_csv(buf_dsr, index=False)
        st.download_button("Download DSR-ready CSV", buf_dsr.getvalue(),
                           "cleaned_data_DSR_ready.csv","text/csv",key="download_dsr")

        if not exclusion_report.empty:
            buf_excl = io.BytesIO(); exclusion_report.to_csv(buf_excl, index=False)
            st.download_button("Download Exclusion Report CSV", buf_excl.getvalue(),
                               "DSR_exclusion_report.csv","text/csv",key="download_exclusion")

with tabs[8]:
    st.subheader("Outlier Cleaner (IQR)")
    if has_data:
        numeric_cols = clean_df.select_dtypes(include=[np.number]).columns.tolist()
        selected_cols = st.multiselect("Select numeric columns", numeric_cols, default=[])
        k = st.slider("IQR multiplier", 0.5, 3.0, 1.5, 0.1)
        if selected_cols:
            filtered_df, mask_removed = iqr_outlier_cleaner(clean_df, selected_cols, k=k)
            st.write(f"Rows removed: {mask_removed.sum()}")
            st.dataframe(filtered_df.head(50))

with tabs[9]:
    st.subheader("Cleaning Guide")
    st.markdown("See in-code comments; all How-To rules detectable from CSV are implemented.")
