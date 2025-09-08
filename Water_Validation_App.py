# Water_Validation_App.py
# Streamlit app for Water Quality Data Validation (GENERAL → CORE → ECOLI → ADVANCED → RIPARIAN)
# Includes: duplicates removal, flagged-row removal, midday warning, conductivity aliases,
# calibration time (≤24h), two-step E. coli rounding, stricter unit checks, riparian completeness,
# Final_Combined.xlsx (notes merged).   <-- Final_Repaired (3×IQR) REMOVED

import streamlit as st
import pandas as pd
import numpy as np
import tempfile, zipfile, io, os, re
from datetime import datetime
from typing import Optional, Tuple

# Ensure openpyxl is available for Excel I/O
from openpyxl import load_workbook  # noqa: F401

# -------------------- Page setup --------------------
st.set_page_config(layout="wide", page_title="🧪 Water Quality Data Validation App")
st.title("🧪 Water Quality Data Validation App")

# -------------------- Helpers --------------------
COND_CANDIDATES = ["Conductivity (µS/cm)", "Conductivity (?S/cm)"]

def save_excel(df: pd.DataFrame, path: str):
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
        "df_general_clean","df_general_annot",
        "df_core_clean","df_core_annot",
        "df_ecoli_clean","df_ecoli_annot",
        "df_adv_clean","df_adv_annot",
        "df_rip_clean","df_rip_annot",
        "df_final_combined","p_final_combined",
    ]:
        st.session_state.setdefault(k, None)
init_state()

def first_available(*keys, require_nonempty: bool = False):
    for k in keys:
        df = st.session_state.get(k)
        if isinstance(df, pd.DataFrame):
            if (not require_nonempty) or (not df.empty):
                return df
    return None

def get_cond_col(df: pd.DataFrame) -> Optional[str]:
    return next((c for c in COND_CANDIDATES if c in df.columns), None)

def parse_hour_from_time_string(t) -> Optional[int]:
    try:
        s = str(t).strip()
        m = re.match(r"^(\d{1,2})[:\.]?(\d{2})?", s)  # HH:MM / H:MM / HHMM
        if not m:
            return None
        h = int(m.group(1))
        return h if 0 <= h <= 23 else None
    except:
        return None

def try_parse_datetime(date_val, time_val=None) -> Optional[datetime]:
    # date part
    try:
        if pd.isna(date_val):
            return None
        if isinstance(date_val, (pd.Timestamp, datetime)):
            dt = pd.to_datetime(date_val).to_pydatetime()
        else:
            dt = pd.to_datetime(date_val, errors="coerce")
            if pd.isna(dt):
                return None
            dt = dt.to_pydatetime()
    except:
        return None
    # time part
    if time_val is not None:
        try:
            s = str(time_val)
            m = re.match(r"^(\d{1,2}):(\d{2})", s)
            if m:
                h, minute = int(m.group(1)), int(m.group(2))
                if 0 <= h <= 23 and 0 <= minute <= 59:
                    dt = dt.replace(hour=h, minute=minute, second=0, microsecond=0)
                else:
                    dt = dt.replace(hour=12, minute=0, second=0, microsecond=0)
            else:
                h = parse_hour_from_time_string(s)
                if h is not None:
                    dt = dt.replace(hour=h, minute=0, second=0, microsecond=0)
                else:
                    dt = dt.replace(hour=12, minute=0, second=0, microsecond=0)
        except:
            dt = dt.replace(hour=12, minute=0, second=0, microsecond=0)
    return dt

def is_truthy_flag(val) -> bool:
    if pd.isna(val):
        return False
    s = str(val).strip().lower()
    return s in {"y","yes","true","1","flag","flagged","invalid","bad","exclude","remove"}

# -------------------- Key builder for merges --------------------
def make_key(df: pd.DataFrame) -> pd.Series:
    cols = []
    if "Group or Affiliation" in df.columns:
        cols.append(df["Group or Affiliation"].astype(str))
    if "Site ID: Site Name" in df.columns:
        cols.append(df["Site ID: Site Name"].astype(str))
    if "Sample Date" in df.columns:
        cols.append(pd.to_datetime(df["Sample Date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna(""))
    if "Sample Time Final Format" in df.columns:
        cols.append(df["Sample Time Final Format"].astype(str))
    if not cols:
        return df.index.astype(str)
    out = cols[0].fillna("")
    for c in cols[1:]:
        out = out.str.cat(c.fillna(""), sep="|")
    return out

# -------------------- Final combiner --------------------
def build_final_combined(base_df: pd.DataFrame,
                         g_annot: Optional[pd.DataFrame],
                         c_annot: Optional[pd.DataFrame],
                         e_annot: Optional[pd.DataFrame],
                         a_annot: Optional[pd.DataFrame],
                         r_annot: Optional[pd.DataFrame]) -> pd.DataFrame:
    final = base_df.copy()
    final["_key_"] = make_key(final)

    def pick(df, cols):
        if df is None:
            return None
        use = [c for c in cols if c in df.columns]
        if not use:
            return None
        tmp = df[use].copy()
        tmp["_key_"] = make_key(df)
        return tmp

    blocks = [
        ("GENERAL_Notes",       pick(g_annot, ["ValidationNotes"])),
        ("GENERAL_Changes",     pick(g_annot, ["TransformNotes"])),
        ("CORE_Notes",          pick(c_annot, ["CORE_Notes"])),
        ("CORE_Changes",        pick(c_annot, ["CORE_ChangeNotes"])),
        ("ECOLI_Notes",         pick(e_annot, ["ECOLI_ValidationNotes"])),
        ("ECOLI_Changes",       pick(e_annot, ["ECOLI_ChangeNotes"])),
        ("ADVANCED_Notes",      pick(a_annot, ["ADVANCED_ValidationNotes"])),
        ("ADVANCED_Changes",    pick(a_annot, ["ADVANCED_ChangeNotes"])),
        ("RIPARIAN_Notes",      pick(r_annot, ["RIPARIAN_ValidationNotes"])),
        ("RIPARIAN_Changes",    pick(r_annot, ["RIPARIAN_ChangeNotes"])),
    ]

    for label, blk in blocks:
        if blk is None:
            final[label] = ""
            continue
        val_cols = [c for c in blk.columns if c != "_key_"]
        if not val_cols:
            final[label] = ""
            continue
        value_col = val_cols[0]
        blk_ren = blk[["_key_", value_col]].rename(columns={value_col: label})
        final = final.merge(blk_ren, on="_key_", how="left")
        final[label] = final[label].fillna("")

    def cat_cols(row, cols):
        vals = [str(row[c]).strip() for c in cols if c in row.index and str(row[c]).strip() != ""]
        return " | ".join(vals)

    note_cols = ["GENERAL_Notes","CORE_Notes","ECOLI_Notes","ADVANCED_Notes","RIPARIAN_Notes"]
    chg_cols  = ["GENERAL_Changes","CORE_Changes","ECOLI_Changes","ADVANCED_Changes","RIPARIAN_Changes"]
    final["All_Notes"] = final.apply(lambda r: cat_cols(r, note_cols), axis=1)
    final["All_ChangeNotes"] = final.apply(lambda r: cat_cols(r, chg_cols), axis=1)

    ordered = [c for c in final.columns if c not in ["_key_", "All_Notes", "All_ChangeNotes"]]
    final = final[ordered + ["All_Notes", "All_ChangeNotes"]]
    final.drop(columns=["_key_"], inplace=True, errors="ignore")
    return final

# -------------------- Repaired dataset (REMOVED) --------------------
#  (به درخواست شما: هیچ حذف/ماسک outlier انجام نمی‌شود)

# -------------------- GENERAL --------------------
def run_general(df0: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = df0.copy()
    df["ValidationNotes"] = ""
    df["ValidationColorKey"] = ""
    df["TransformNotes"] = ""

    cond_col = get_cond_col(df)

    # Duplicates based on identity
    key_cols = [c for c in ["Group or Affiliation","Site ID: Site Name","Sample Date","Sample Time Final Format"] if c in df.columns]
    if len(key_cols) >= 2:
        dup_mask = df.duplicated(subset=key_cols, keep="first")
        df.loc[dup_mask, "ValidationNotes"] += "Duplicate row (same site/date/time); "
        row_delete_indices = set(df[dup_mask].index.tolist())
    else:
        row_delete_indices = set()

    # Flagged rows (any column name that contains 'flag')
    flag_cols = [c for c in df.columns if "flag" in c.lower()]
    if flag_cols:
        fl_mask = df[flag_cols].applymap(is_truthy_flag).any(axis=1)
        df.loc[fl_mask, "ValidationNotes"] += "Row flagged by data flag column; "
        row_delete_indices.update(df[fl_mask].index.tolist())

    # Watershed site count
    if "Group or Affiliation" in df.columns and "Site ID: Site Name" in df.columns:
        site_counts = df.groupby("Group or Affiliation")["Site ID: Site Name"].nunique()
        invalid_ws = site_counts[site_counts < 3].index
        mask = df["Group or Affiliation"].isin(invalid_ws)
        df.loc[mask, "ValidationNotes"] += "Less than 3 sites in watershed; "
        df.loc[mask, "ValidationColorKey"] += "watershed_or_events;"
        row_delete_indices.update(df[mask].index.tolist())

    # Site event count
    if "Site ID: Site Name" in df.columns and "Sample Date" in df.columns:
        df["Sample Date"] = pd.to_datetime(df["Sample Date"], errors="coerce")
        event_counts = df.groupby("Site ID: Site Name")["Sample Date"].nunique()
        low_event_sites = event_counts[event_counts < 10].index
        mask = df["Site ID: Site Name"].isin(low_event_sites)
        df.loc[mask, "ValidationNotes"] += "Fewer than 10 events; "
        df.loc[mask, "ValidationColorKey"] += "watershed_or_events;"
        row_delete_indices.update(df[mask].index.tolist())

    # Invalid Sample Date
    if "Sample Date" in df.columns:
        mask = df["Sample Date"].isna()
        df.loc[mask, "ValidationNotes"] += "Missing or invalid Sample Date; "
        df.loc[mask, "ValidationColorKey"] += "time;"
        row_delete_indices.update(df[mask].index.tolist())

    # Sample Time parsing + midday note
    if "Sample Time Final Format" in df.columns:
        mask_bad = df["Sample Time Final Format"].apply(lambda t: parse_hour_from_time_string(t) is None)
        df.loc[mask_bad, "ValidationNotes"] += "Unparsable Sample Time; "
        df.loc[mask_bad, "ValidationColorKey"] += "time;"
        row_delete_indices.update(df[mask_bad].index.tolist())
        hours = df["Sample Time Final Format"].apply(parse_hour_from_time_string)
        mask_mid = hours.apply(lambda h: (h is not None) and (12 <= h < 16))
        df.loc[mask_mid, "ValidationNotes"] += "Sample time in 12:00–16:00 window (verify consistency); "

    # Missing all core params
    core_params = [
        "pH (standard units)",
        "Dissolved Oxygen (mg/L) Average",
        "Water Temperature (° C)",
        cond_col if cond_col else "Conductivity (µS/cm)",
        "Salinity (ppt)",
    ]
    for idx, row in df.iterrows():
        vals = []
        for p in core_params:
            if p in df.columns:
                vals.append(row.get(p))
        if vals and all((pd.isna(v) or v == 0) for v in vals):
            df.at[idx, "ValidationNotes"] += "All core parameters missing or invalid; "
            df.at[idx, "ValidationColorKey"] += "range;"
            row_delete_indices.add(idx)

    # Standard ranges & texts (these remain validation rules; they still clear invalid values)
    standard_ranges = {}
    note_texts = {}
    if cond_col:
        standard_ranges[cond_col] = (50, 1500)
        note_texts[cond_col] = "Conductivity out of range [50–1500]; "
    extra = {
        "pH (standard units)": ((6.5, 9.0), "pH out of range [6.5–9.0]; "),
        "Dissolved Oxygen (mg/L) Average": ((5.0, 14.0), "DO out of range [5.0–14.0]; "),
        "Salinity (ppt)": ((0, 35), "Salinity out of range [0–35]; "),
        "Water Temperature (° C)": ((0, 35), "Temp out of range [0–35]; "),
        "Air Temperature (° C)": ((-10, 50), "Air Temp out of range [-10–50]; "),
        "Turbidity": ((0, 1000), "Turbidity out of range [0–1000]; "),
        "E. Coli Average": ((1, 235), "E. Coli out of range [1–235]; "),
        "Secchi Disk Transparency - Average": ((0.2, 5), "Secchi out of range [0.2–5]; "),
        "Nitrate-Nitrogen VALUE (ppm or mg/L)": ((0, 10), "Nitrate out of range [0–10]; "),
        "Orthophosphate": ((0, 0.5), "Orthophosphate out of range [0–0.5]; "),
        "DO (%)": ((80, 120), "DO % out of range [80–120]; "),
        "Total Phosphorus (mg/L)": ((0, 0.05), "TP out of range [0–0.05]; "),
    }
    for k,(rng,txt) in extra.items():
        standard_ranges[k] = rng
        note_texts[k] = txt

    for col, (mn, mx) in standard_ranges.items():
        if col in df.columns:
            mask = (df[col] < mn) | (df[col] > mx)
            df.loc[mask, "ValidationNotes"] += note_texts[col]
            df.loc[mask, "ValidationColorKey"] += "range;"
            df.loc[mask, col] = np.nan  # اعتبارسنجی محدوده (حذف outlier نیست؛ قانون کیفیت داده است)

    # Contextual outliers per site (ONLY TAG, DO NOT CLEAR VALUES)
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
                # ⚠️ مقدار را دست‌نخورده می‌گذاریم (هیچ پاک‌سازی سلولی انجام نمی‌شود)

    # Expired reagents
    if "Chemical Reagents Used" in df.columns:
        mask = df["Chemical Reagents Used"].astype(str).str.contains("expired", case=False, na=False)
        df.loc[mask, "ValidationNotes"] += "Expired reagents used; "
        df.loc[mask, "ValidationColorKey"] += "expired;"
        df.loc[mask, "Chemical Reagents Used"] = np.nan

    # Comments required if flagged
    if "Comments" in df.columns:
        empty = df["Comments"].isna() | (df["Comments"].astype(str).str.strip() == "")
        flagged = df["ValidationNotes"] != ""
        mask = flagged & empty
        df.loc[mask, "ValidationNotes"] += "No explanation in Comments; "
        df.loc[mask, "ValidationColorKey"] += "comments;"

    # Remove 'valid/invalid' everywhere
    replaced = df.replace(to_replace=r'(?i)\b(valid|invalid)\b', value='', regex=True)
    changed = replaced != df
    df.update(replaced)
    df.loc[changed.any(axis=1), "TransformNotes"] += "Removed 'valid/invalid'; "

    # Sort
    if "Site ID: Site Name" in df.columns and "Sample Date" in df.columns:
        df.sort_values(by=["Site ID: Site Name", "Sample Date"], inplace=True)

    # Cleaned
    df_clean = df.drop(index=list(row_delete_indices))
    return df_clean, df

# -------------------- CORE --------------------
def run_core(df0: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = df0.copy()
    df["CORE_Notes"] = ""
    df["CORE_ChangeNotes"] = ""
    row_delete_indices = set()

    cond_col = get_cond_col(df)

    # Sample depth rule
    if "Sample Depth (meters)" in df.columns and "Total Depth (meters)" in df.columns:
        for idx, row in df.iterrows():
            sample = row["Sample Depth (meters)"]
            total = row["Total Depth (meters)"]
            try:
                if not (np.isclose(sample, 0.3, atol=0.05) or np.isclose(sample, total / 2, atol=0.05)):
                    df.at[idx, "CORE_Notes"] += "Sample Depth not 0.3m or mid-depth; "
            except Exception:
                pass

    # Depth=0 requires Flow=6
    if "Flow Severity" in df.columns and "Total Depth (meters)" in df.columns:
        mask = (df["Total Depth (meters)"] == 0) & (df["Flow Severity"] != 6)
        df.loc[mask, "CORE_Notes"] += "Zero Depth with non-dry flow; "
        row_delete_indices.update(df[mask].index.tolist())

    # DO titration presence + difference
    do1, do2 = "Dissolved Oxygen (mg/L) 1st titration", "Dissolved Oxygen (mg/L) 2nd titration"
    has1, has2 = (do1 in df.columns), (do2 in df.columns)
    if has1 ^ has2:
        missing = do1 if not has1 else do2
        df["CORE_Notes"] += f"Missing DO titration column: {missing}; "
    if has1 and has2:
        diff = (df[do1] - df[do2]).abs()
        mask = diff > 0.5
        df.loc[mask, "CORE_Notes"] += "DO Difference > 0.5; "
        df["DO1 Rounded"] = pd.to_numeric(df[do1], errors="coerce").round(1)
        df["DO2 Rounded"] = pd.to_numeric(df[do2], errors="coerce").round(1)
        df["CORE_ChangeNotes"] += "Rounded DO to 0.1; "

    # Secchi rules
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

    # Calibration ±20% of standard (post-test)
    if "Post-Test Calibration Conductivity" in df.columns and "Standard Value" in df.columns:
        post_cal = pd.to_numeric(df["Post-Test Calibration Conductivity"], errors="coerce")
        std_val = pd.to_numeric(df["Standard Value"], errors="coerce")
        valid_cal = (post_cal >= 0.8 * std_val) & (post_cal <= 1.2 * std_val)
        df.loc[~valid_cal, "CORE_Notes"] += "Post-Test Calibration outside ±20% of standard; "

    # Calibration time within 24h (pre/post) vs sample datetime
    pre_time_cols  = [c for c in df.columns if ("pre" in c.lower() and "calibration" in c.lower() and "time" in c.lower())]
    post_time_cols = [c for c in df.columns if ("post" in c.lower() and "calibration" in c.lower() and "time" in c.lower())]
    if "Sample Date" in df.columns:
        for idx, row in df.iterrows():
            samp_dt = try_parse_datetime(row.get("Sample Date"), row.get("Sample Time Final Format"))
            if samp_dt is None:
                continue
            # Pre
            for c in pre_time_cols:
                pre_dt = try_parse_datetime(row.get(c)) or try_parse_datetime(row.get("Sample Date"), row.get(c))
                if pre_dt is not None and abs((samp_dt - pre_dt).total_seconds()) > 24*3600:
                    df.at[idx, "CORE_Notes"] += f"Pre-calibration time >24h from sample ({c}); "
            # Post
            for c in post_time_cols:
                post_dt = try_parse_datetime(row.get(c)) or try_parse_datetime(row.get("Sample Date"), row.get(c))
                if post_dt is not None and abs((post_dt - samp_dt).total_seconds()) > 24*3600:
                    df.at[idx, "CORE_Notes"] += f"Post-calibration time >24h from sample ({c}); "

    # Rounding pH & Temp to 0.1
    if "pH (standard units)" in df.columns:
        df["pH Rounded"] = pd.to_numeric(df["pH (standard units)"], errors="coerce").round(1)
        df["CORE_ChangeNotes"] += "Rounded pH to 0.1; "
    if "Water Temperature (° C)" in df.columns:
        df["Water Temp Rounded"] = pd.to_numeric(df["Water Temperature (° C)"], errors="coerce").round(1)
        df["CORE_ChangeNotes"] += "Rounded Water Temp to 0.1; "

    # Conductivity formatting
    if cond_col:
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

    # Salinity display
    if "Salinity (ppt)" in df.columns:
        def fmt_sal(val):
            try:
                if pd.isna(val): return val
                v = float(val)
                return "< 2.0" if v < 2.0 else round(v, 1)
            except:
                return val
        df["Salinity Formatted"] = df["Salinity (ppt)"].apply(fmt_sal)
        df["CORE_ChangeNotes"] += "Formatted Salinity display; "

    # Numeric formats
    for col in ["Time Spent Sampling/Traveling", "Roundtrip Distance Traveled"]:
        if col in df.columns:
            mask = ~df[col].apply(lambda x: isinstance(x, (int, float, np.integer, np.floating)))
            df.loc[mask, "CORE_Notes"] += f"{col} format not numeric; "

    df_clean = df.drop(index=row_delete_indices)
    return df_clean, df

# -------------------- ECOLI --------------------
def run_ecoli(df0: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = df0.copy()
    df["ECOLI_ValidationNotes"] = ""
    df["ECOLI_ChangeNotes"] = ""

    all_zero_cols = [col for col in df.columns
                     if pd.api.types.is_numeric_dtype(df[col]) and (df[col].fillna(0) == 0).all()]

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

    # E. Coli average
    col_ecoli = "E. Coli Average"
    if col_ecoli in df.columns and col_ecoli not in all_zero_cols:
        df[col_ecoli] = pd.to_numeric(df[col_ecoli], errors="coerce")
        mask = df[col_ecoli] == 0
        df.loc[mask, "ECOLI_ValidationNotes"] += "E. coli = 0; "
        df.loc[mask, col_ecoli] = np.nan

        # Two-step rounding: nearest integer → 2 significant figures
        def round_to_2sf_after_int(n):
            if pd.isna(n):
                return n
            try:
                n_int = int(round(float(n)))
                if n_int == 0:
                    return 0
                k = int(np.floor(np.log10(abs(n_int))))
                return int(round(n_int, -k + 1))
            except:
                return n

        df["E. Coli Rounded (int→2SF)"] = df[col_ecoli].apply(round_to_2sf_after_int)
        df["ECOLI_ChangeNotes"] += "Rounded E. coli: nearest int then to 2 significant figures; "

    # CFU formula validation
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
            valid = df.apply(lambda row: check_dilution(row, prefix), axis=1)
            df.loc[~valid, "ECOLI_ValidationNotes"] += f"{prefix} CFU formula mismatch; "

    df_clean = df[df["ECOLI_ValidationNotes"].str.strip() == ""]
    return df_clean, df

# -------------------- ADVANCED --------------------
def run_adv(df0: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = df0.copy()
    df["ADVANCED_ValidationNotes"] = ""
    df["ADVANCED_ChangeNotes"] = ""

    all_zero_cols = [col for col in df.columns
                     if pd.api.types.is_numeric_dtype(df[col]) and (df[col].fillna(0) == 0).all()]
    for col in all_zero_cols:
        df["ADVANCED_ChangeNotes"] += f"Skipped checks for unmeasured parameter: {col}; "

    def log_issue(idx, text):
        df.at[idx, "ADVANCED_ValidationNotes"] += text + "; "

    # Column name checks (labels)
    phosphate_cols = [c for c in df.columns if "phosphate" in c.lower() and "value" in c.lower() and c not in all_zero_cols]
    for c in phosphate_cols:
        if ("mg/l" not in c.lower()) and ("ppm" not in c.lower()):
            for idx in df.index: log_issue(idx, f"{c} not labeled in mg/L or ppm")

    nitrate_cols = [c for c in df.columns if "nitrate-nitrogen" in c.lower() and "value" in c.lower() and c not in all_zero_cols]
    for c in nitrate_cols:
        if ("mg/l" not in c.lower()) and ("ppm" not in c.lower()):
            for idx in df.index: log_issue(idx, f"{c} not labeled in mg/L or ppm")

    turbidity_cols = [c for c in df.columns if "turbidity" in c.lower() and "result" in c.lower() and c not in all_zero_cols]
    for c in turbidity_cols:
        if ("ntu" not in c.lower()) and ("jtu" in c.lower()):
            for idx in df.index: log_issue(idx, f"{c} appears to be in JTU not NTU")

    # Record-level unit checks via CharacteristicName + ResultMeasure/MeasureUnitCode
    unit_col = "ResultMeasure/MeasureUnitCode"
    param_col = "CharacteristicName"
    if unit_col in df.columns and param_col in df.columns:
        for idx in df.index:
            p = str(df.at[idx, param_col]).lower()
            u = str(df.at[idx, unit_col]).lower()
            if "phosphate" in p and u not in ["mg/l", "ppm"]:
                log_issue(idx, f"Phosphate unit invalid: {u}")
            elif "nitrate" in p and u not in ["mg/l", "ppm"]:
                log_issue(idx, f"Nitrate-Nitrogen unit invalid: {u}")
            elif "turbidity" in p and u != "ntu":
                log_issue(idx, f"Turbidity unit should be NTU, found: {u}")
            elif "streamflow" in p and u != "ft2/sec":
                log_issue(idx, f"Streamflow unit should be ft2/sec, found: {u}")
            elif "discharge" in p and u != "ft2/sec":
                log_issue(idx, f"Discharge unit should be ft2/sec, found: {u}")

    # Discharge format rules
    col_discharge = "Discharge Recorded"
    if col_discharge in df.columns and col_discharge not in all_zero_cols:
        def fix_discharge(val):
            try:
                v = float(val)
                if v < 10:
                    new_v = round(v, 1)
                    return new_v, None if abs(v - new_v) < 0.05 else f"{v} → {new_v} (1 decimal)"
                else:
                    new_v = round(v)
                    return new_v, None if float(v).is_integer() else f"{v} → {new_v} (integer)"
            except:
                return val, "Invalid or non-numeric discharge value"

        for idx in df.index:
            val = df.at[idx, col_discharge]
            fixed, issue = fix_discharge(val)
            if issue: log_issue(idx, f"Discharge format issue: {issue}")
            if (fixed is not None) and (fixed != val):
                df.at[idx, col_discharge] = fixed
                df.at[idx, "ADVANCED_ChangeNotes"] += f"Discharge corrected {val} → {fixed}; "

    df_clean = df[df["ADVANCED_ValidationNotes"].str.strip() == ""]
    return df_clean, df

# -------------------- RIPARIAN --------------------
def run_rip(df0: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = df0.copy()
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

    # Skip all-zero columns
    zeroed_columns = []
    for c in available_cols:
        numeric_col = pd.to_numeric(df[c], errors="coerce").fillna(0)
        if numeric_col.eq(0).all():
            zeroed_columns.append(c)
    for c in zeroed_columns:
        df["RIPARIAN_ChangeNotes"] += f"Skipped checks for unmeasured parameter: {c}; "

    # Bank evaluated present
    if "Bank Evaluated" in df.columns:
        for idx, val in df["Bank Evaluated"].items():
            if pd.isna(val) or str(val).strip() == "":
                log_issue(idx, "Bank evaluation missing")

    # Per-indicator presence, require comments if missing
    for idx, row in df.iterrows():
        missing_count = 0
        for c in available_cols:
            if c in zeroed_columns:
                continue
            val = row.get(c)
            if pd.isna(val) or str(val).strip() == "":
                comments = str(row.get("Comments", "")).strip().lower()
                if comments in ["", "n/a", "na", "none"]:
                    log_issue(idx, f"{c} missing without explanation")
                else:
                    df.at[idx, c] = np.nan  # keep row; clear the cell
                missing_count += 1
        if missing_count > 0:
            comments = str(row.get("Comments", "")).strip().lower()
            if comments in ["", "n/a", "na", "none"]:
                log_issue(idx, f"Riparian indicators incomplete: {missing_count} missing")

    # Image submission standardization
    image_col = "Image of site was submitted"
    if image_col in df.columns:
        for idx, val in df[image_col].items():
            raw = str(val).strip().lower()
            if raw in ["no", "false", "n", "", "nan"]:
                log_issue(idx, "Site image not submitted")
            elif raw in ["yes", "true", "y"]:
                if str(val).strip() != "Yes":
                    log_change(idx, f"Image value standardized: '{val}' → 'Yes'")
                    df.at[idx, image_col] = "Yes"

    df_clean = df[df["RIPARIAN_ValidationNotes"].str.strip() == ""]
    return df_clean, df

# ==== Tabs ====
tabs = st.tabs([
    "📁 Upload File",
    "1️⃣ GENERAL Validation",
    "2️⃣ CORE Validation",
    "3️⃣ ECOLI Validation",
    "4️⃣ ADVANCED Validation",
    "5️⃣ RIPARIAN Validation",
    "🚀 Run All & Exports",
    "📘Cleaning Guide",
])

# ------------------------ 1) UPLOAD ------------------------
with tabs[0]:
    st.header("📁 Upload Your Excel File (once)")
    uploaded = st.file_uploader("Upload a .xlsx file", type=["xlsx"])
    if uploaded:
        st.session_state.input_basename = os.path.basename(uploaded.name)
        bytes_data = uploaded.read()
        df0 = pd.read_excel(io.BytesIO(bytes_data), engine="openpyxl")
        st.session_state.df_original = df0.copy()
        mark_success("File loaded. You can proceed to the next tabs or use 'Run All'.")
        st.write("Rows:", len(df0), " | Columns:", len(df0.columns))
        with st.expander("Preview first 20 rows"):
            st.dataframe(df0.head(20))

# ------------------------ 2) GENERAL ------------------------
with tabs[1]:
    st.header("1️⃣ GENERAL Validation")
    if not isinstance(st.session_state.df_original, pd.DataFrame):
        st.info("Upload a file in the first tab to enable this step.")
    else:
        if st.button("Run GENERAL Validation"):
            g_clean, g_annot = run_general(st.session_state.df_original)
            base = st.session_state.input_basename or "input.xlsx"
            p_clean = path_with_suffix(base, "cleaned_GENERAL")
            p_annot = path_with_suffix(base, "annotated_GENERAL")
            save_excel(g_clean, p_clean)
            save_excel(g_annot, p_annot)
            st.session_state.df_general_clean = g_clean
            st.session_state.df_general_annot = g_annot
            mark_success("GENERAL validation complete.")
            c1, c2 = st.columns(2)
            with c1:
                st.download_button("📥 Download cleaned_GENERAL.xlsx", data=open(p_clean, "rb").read(),
                                   file_name="cleaned_GENERAL.xlsx")
            with c2:
                st.download_button("📥 Download annotated_GENERAL.xlsx", data=open(p_annot, "rb").read(),
                                   file_name="annotated_GENERAL.xlsx")

# ------------------------ 3) CORE ------------------------
with tabs[2]:
    st.header("2️⃣ CORE Validation")
    src_core = first_available("df_general_clean")
    if src_core is None:
        st.info("Run GENERAL first (or use Run All).")
    else:
        if st.button("Run CORE Validation"):
            c_clean, c_annot = run_core(src_core)
            base = st.session_state.input_basename or "input.xlsx"
            p_clean = path_with_suffix(base, "cleaned_CORE")
            p_annot = path_with_suffix(base, "annotated_CORE")
            save_excel(c_clean, p_clean)
            save_excel(c_annot, p_annot)
            st.session_state.df_core_clean = c_clean
            st.session_state.df_core_annot = c_annot
            mark_success("CORE validation files generated.")
            c1, c2 = st.columns(2)
            with c1:
                st.download_button("📥 Download cleaned_CORE.xlsx", data=open(p_clean, "rb").read(),
                                   file_name="cleaned_CORE.xlsx")
            with c2:
                st.download_button("📥 Download annotated_CORE.xlsx", data=open(p_annot, "rb").read(),
                                   file_name="annotated_CORE.xlsx")

# ------------------------ 4) ECOLI ------------------------
with tabs[3]:
    st.header("3️⃣ ECOLI Validation")
    src_ecoli = first_available("df_general_clean")
    if src_ecoli is None:
        st.info("Run GENERAL first (or use Run All).")
    else:
        if st.button("Run ECOLI Validation"):
            e_clean, e_annot = run_ecoli(src_ecoli)
            base = st.session_state.input_basename or "input.xlsx"
            p_clean = path_with_suffix(base, "cleaned_ECOLI")
            p_annot = path_with_suffix(base, "annotated_ECOLI")
            save_excel(e_clean, p_clean)
            save_excel(e_annot, p_annot)
            st.session_state.df_ecoli_clean = e_clean
            st.session_state.df_ecoli_annot = e_annot
            mark_success("ECOLI validation files generated.")
            c1, c2 = st.columns(2)
            with c1:
                st.download_button("📥 Download cleaned_ECOLI.xlsx", data=open(p_clean, "rb").read(),
                                   file_name="cleaned_ECOLI.xlsx")
            with c2:
                st.download_button("📥 Download annotated_ECOLI.xlsx", data=open(p_annot, "rb").read(),
                                   file_name="annotated_ECOLI.xlsx")

# ------------------------ 5) ADVANCED ------------------------
with tabs[4]:
    st.header("4️⃣ ADVANCED Validation")
    src_adv = first_available("df_ecoli_clean", "df_general_clean", require_nonempty=False)
    if src_adv is None:
        st.info("Run GENERAL (and optionally ECOLI) first, or use Run All.")
    else:
        if st.button("Run ADVANCED Validation"):
            a_clean, a_annot = run_adv(src_adv)
            base = st.session_state.input_basename or "input.xlsx"
            p_clean = path_with_suffix(base, "cleaned_ADVANCED")
            p_annot = path_with_suffix(base, "annotated_ADVANCED")
            save_excel(a_clean, p_clean)
            save_excel(a_annot, p_annot)
            st.session_state.df_adv_clean = a_clean
            st.session_state.df_adv_annot = a_annot
            mark_success("ADVANCED validation files generated.")
            c1, c2 = st.columns(2)
            with c1:
                st.download_button("📥 Download cleaned_ADVANCED.xlsx", data=open(p_clean, "rb").read(),
                                   file_name="cleaned_ADVANCED.xlsx")
            with c2:
                st.download_button("📥 Download annotated_ADVANCED.xlsx", data=open(p_annot, "rb").read(),
                                   file_name="annotated_ADVANCED.xlsx")

# ------------------------ 6) RIPARIAN ------------------------
with tabs[5]:
    st.header("5️⃣ RIPARIAN Validation")
    src_rip = first_available("df_adv_clean", "df_general_clean", require_nonempty=False)
    if src_rip is None:
        st.info("Run prior steps (or use Run All).")
    else:
        if st.button("Run RIPARIAN Validation"):
            r_clean, r_annot = run_rip(src_rip)
            base = st.session_state.input_basename or "input.xlsx"
            p_clean = path_with_suffix(base, "cleaned_RIPARIAN")
            p_annot = path_with_suffix(base, "annotated_RIPARIAN")
            save_excel(r_clean, p_clean)
            save_excel(r_annot, p_annot)
            st.session_state.df_rip_clean = r_clean
            st.session_state.df_rip_annot = r_annot
            mark_success("RIPARIAN validation files generated.")
            c1, c2 = st.columns(2)
            with c1:
                st.download_button("📥 Download cleaned_RIPARIAN.xlsx", data=open(p_clean, "rb").read(),
                                   file_name="cleaned_RIPARIAN.xlsx")
            with c2:
                st.download_button("📥 Download annotated_RIPARIAN.xlsx", data=open(p_annot, "rb").read(),
                                   file_name="annotated_RIPARIAN.xlsx")

# ------------------------ 7) RUN ALL + FINAL COMBINED ------------------------
with tabs[6]:
    st.header("🚀 Run All (GENERAL → CORE → ECOLI → ADVANCED → RIPARIAN)")
    st.caption("Final_Combined is generated. ")

    if not isinstance(st.session_state.df_original, pd.DataFrame):
        st.info("Upload a file in the first tab.")
    else:
        if st.button("Run All Steps"):
            base = st.session_state.input_basename or "input.xlsx"

            # 1) GENERAL
            g_clean, g_annot = run_general(st.session_state.df_original)
            st.session_state.df_general_clean, st.session_state.df_general_annot = g_clean, g_annot
            p_g_clean = path_with_suffix(base, "cleaned_GENERAL")
            p_g_annot = path_with_suffix(base, "annotated_GENERAL")
            save_excel(g_clean, p_g_clean); save_excel(g_annot, p_g_annot)

            # 2) CORE (on GENERAL-clean)
            c_clean, c_annot = run_core(g_clean)
            st.session_state.df_core_clean, st.session_state.df_core_annot = c_clean, c_annot
            p_c_clean = path_with_suffix(base, "cleaned_CORE")
            p_c_annot = path_with_suffix(base, "annotated_CORE")
            save_excel(c_clean, p_c_clean); save_excel(c_annot, p_c_annot)

            # 3) ECOLI (on GENERAL-clean)
            e_clean, e_annot = run_ecoli(g_clean)
            st.session_state.df_ecoli_clean, st.session_state.df_ecoli_annot = e_clean, e_annot
            p_e_clean = path_with_suffix(base, "cleaned_ECOLI")
            p_e_annot = path_with_suffix(base, "annotated_ECOLI")
            save_excel(e_clean, p_e_clean); save_excel(e_annot, p_e_annot)

            # 4) ADVANCED (on ECOLI-clean if not empty, else GENERAL-clean)
            a_source = e_clean if not e_clean.empty else g_clean
            a_clean, a_annot = run_adv(a_source)
            st.session_state.df_adv_clean, st.session_state.df_adv_annot = a_clean, a_annot
            p_a_clean = path_with_suffix(base, "cleaned_ADVANCED")
            p_a_annot = path_with_suffix(base, "annotated_ADVANCED")
            save_excel(a_clean, p_a_clean); save_excel(a_annot, p_a_annot)

            # 5) RIPARIAN (on ADVANCED-clean if not empty, else GENERAL-clean)
            r_source = a_clean if not a_clean.empty else g_clean
            r_clean, r_annot = run_rip(r_source)
            st.session_state.df_rip_clean, st.session_state.df_rip_annot = r_clean, r_annot
            p_r_clean = path_with_suffix(base, "cleaned_RIPARIAN")
            p_r_annot = path_with_suffix(base, "annotated_RIPARIAN")
            save_excel(r_clean, p_r_clean); save_excel(r_annot, p_r_annot)

            # 6) Final_Combined (merge notes from all annotated DataFrames)
            final_base = r_clean if not r_clean.empty else (a_clean if not a_clean.empty else g_clean)
            df_final = build_final_combined(
                base_df=final_base,
                g_annot=g_annot,
                c_annot=c_annot,
                e_annot=e_annot,
                a_annot=a_annot,
                r_annot=r_annot
            )
            p_final = path_with_suffix(base, "Final_Combined")
            save_excel(df_final, p_final)

            # Keep in session
            st.session_state.df_final_combined = df_final.copy()
            st.session_state.p_final_combined = p_final

            st.success("✅ All steps completed. Final_Combined is ready.")
            c1, c2 = st.columns(2)
            with c1:
                st.download_button(
                    "📥 Download Final_Combined.xlsx",
                    data=open(p_final, "rb").read(),
                    file_name="Final_Combined.xlsx"
                )
            with c2:
                # ZIP all step outputs + Final_Combined (no Final_Repaired anymore)
                mem_zip = io.BytesIO()
                with zipfile.ZipFile(mem_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                    for path in [
                        p_g_clean, p_g_annot, p_c_clean, p_c_annot, p_e_clean, p_e_annot,
                        p_a_clean, p_a_annot, p_r_clean, p_r_annot, p_final
                    ]:
                        if os.path.exists(path):
                            zf.write(path, arcname=os.path.basename(path))
                mem_zip.seek(0)
                st.download_button(
                    "📦 Download ALL outputs (ZIP incl. Final_Combined)",
                    data=mem_zip.getvalue(),
                    file_name=f"Validation_Outputs_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
                    mime="application/zip",
                )

# ------------------------ 8) GUIDE ------------------------
with tabs[7]:
    st.header("📘 Download Data Cleaning Guide")
    st.markdown("Download the official data cleaning and validation guide.")

    guide_filename_on_disk = "Validation_Rules_for_Parameters.pdf"
    if os.path.exists(guide_filename_on_disk):
        with open(guide_filename_on_disk, "rb") as f:
            st.download_button(
                label="📄 Download Validation Guide (PDF)",
                data=f.read(),
                file_name="Validation_Rules_for_Parameters.pdf",
                mime="application/pdf"
            )
    else:
        st.info("Place 'Validation_Rules_for_Parameters.pdf' next to the app to enable this download.")
