"""
cycle_counter_email_single.py — run once daily at 09:00

New process:
• One designated person handles ALL businesses.
• Each day: choose 3 distinct zids (uniform random) from ALL_ZIDS.
• For each chosen zid: select 3 items (top-N, quarter exclusion, value-weighted).
• Persist (zid, itemcode) in per-zid JSON logs scoped to the current quarter.
• Send a single HTML email (no attachments) listing Department, zid, itemcode, itemname.
"""

import json
import os
import random
import sys
from datetime import date

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# === Load Environment Variables ===
load_dotenv()

# === Add root to Python path ===
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# === Import shared modules ===
from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL

# === Suppress warnings ===
import warnings
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
pd.set_option('display.float_format', '{:.2f}'.format)

# ────────────────────────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────────────────────────
LOG_DIR  = "count_logs"         # JSON history root
POOL_N   = 200                  # top-N by stockvalue before sampling
ITEMS_PER_ZID = 3
ZIDS_PER_DAY  = 1
SKIP_DAYS = {"Friday"}          # set() to include Fridays

# One person for all businesses, every day
COUNTER_NAME = "Inventory Controller"

# Recipients from environment variable COUNTER_EMAILS=ithmbrbd@gmail.com,asad@gmail.com
COUNTER_EMAILS = os.environ.get('COUNTER_EMAILS').split(',')
print("📧 Counter emails:", COUNTER_EMAILS)

# Global list of all departments (zids)
ALL_ZIDS = [100002]

DEPT = {
    100002: "Central",

}

# ────────────────────────────────────────────────────────────────────
# QUARTER HELPERS / JSON LOGS
# ────────────────────────────────────────────────────────────────────
def quarter_start(d: date) -> str:
    m = (d.month - 1) // 3 * 3 + 1
    return date(d.year, m, 1).isoformat()

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def log_path(zid: int, qroot: str) -> str:
    return os.path.join(qroot, f"{zid}.json")

def load_counted(zid: int, qroot: str) -> set[str]:
    fp = log_path(zid, qroot)
    return set(json.load(open(fp))) if os.path.exists(fp) else set()

def append_counted(zid: int, itemcodes: list[str], qroot: str):
    fp = log_path(zid, qroot)
    counted = load_counted(zid, qroot)
    counted.update(itemcodes)
    with open(fp, "w") as f:
        json.dump(sorted(counted), f)

# ────────────────────────────────────────────────────────────────────
# DATA PULL
# ────────────────────────────────────────────────────────────────────
SQL = """
SELECT imtrn.zid,
       imtrn.xitem          AS itemcode,
       caitem.xdesc         AS itemname,
       caitem.xgitem        AS itemgroup,
       imtrn.xwh            AS warehouse,
       SUM(imtrn.xqty * imtrn.xsign) AS stockqty,
       SUM(imtrn.xval * imtrn.xsign) AS stockvalue
FROM   imtrn
JOIN   caitem ON imtrn.xitem = caitem.xitem AND imtrn.zid = caitem.zid
WHERE  imtrn.zid = :zid
GROUP  BY imtrn.zid, imtrn.xitem, caitem.xdesc, caitem.xgitem, imtrn.xwh
"""

def pull_inventory_for_all(zids: list[int], engine) -> dict[int, pd.DataFrame]:
    out = {}
    with engine.begin() as conn:
        for zid in zids:
            out[zid] = pd.read_sql(text(SQL), conn, params={"zid": zid})
    return out

# ────────────────────────────────────────────────────────────────────
# SELECTION LOGIC
# ────────────────────────────────────────────────────────────────────
def remaining_pool(df: pd.DataFrame, counted: set[str]) -> pd.DataFrame:
    """Filter out already-counted and keep top-N by stockvalue."""
    if df.empty:
        return df
    rem = df[~df["itemcode"].isin(counted)]
    if rem.empty:
        return rem
    rem = rem.sort_values("stockvalue", ascending=False).head(POOL_N).copy()
    return rem

def choose_zids_uniform(eligible_zids: list[int]) -> list[int]:
    """Uniform random choice of up to ZIDS_PER_DAY distinct zids."""
    if not eligible_zids:
        return []
    k = min(ZIDS_PER_DAY, len(eligible_zids))
    return random.sample(eligible_zids, k=k)

def choose_items_value_weighted(pool_df: pd.DataFrame, n: int) -> pd.DataFrame:
    """Weighted by stockvalue (clip <=0 to 0; fallback to uniform if all zero/NaN)."""
    if pool_df.empty:
        return pool_df
    weights = pool_df["stockvalue"].clip(lower=0)
    # If all weights are 0 or NaN, fall back to uniform sampling
    if not (weights.fillna(0).sum() > 0):
        return pool_df.sample(n=min(n, len(pool_df)))
    return pool_df.sample(n=min(n, len(pool_df)), weights=weights)

# ────────────────────────────────────────────────────────────────────
# EMAIL — MODIFIED TO RETURN (df, heading) TUPLES
# ────────────────────────────────────────────────────────────────────
def build_html(counter_name: str, today: str, rows: list[dict]) -> list:
    """
    Returns list of tuples: (DataFrame, heading) for use with send_mail's html_body.
    """
    sections = []

    if not rows:
        # Show placeholder message
        placeholder_df = pd.DataFrame([{
            "Message": "No fresh items remain for any selected department today."
        }])
        sections.append((placeholder_df, "Cycle Count Items"))
    else:
        df = pd.DataFrame(rows)
        df.insert(0, "Department", df["zid"].map(DEPT))
        df = df[["Department", "zid", "itemcode", "itemname"]]
        sections.append((df, "Cycle Count Items"))

    return sections

# ────────────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────────────
def main():
    today = date.today()
    weekday = today.strftime("%A")
    if weekday in SKIP_DAYS:
        print(f"🚫 Skipping {weekday}")
        return

    # Quarter dir
    q_root = os.path.join(LOG_DIR, quarter_start(today))
    ensure_dir(q_root)

    # Load history & pull inventory
    engine = create_engine(DATABASE_URL)
    counted_by_zid = {z: load_counted(z, q_root) for z in ALL_ZIDS}
    inv_by_zid = pull_inventory_for_all(ALL_ZIDS, engine)

    # Build remaining pools per zid and filter to those with >=1 fresh item
    pools = {}
    eligible_zids = []
    for zid in ALL_ZIDS:
        pool = remaining_pool(inv_by_zid[zid], counted_by_zid[zid])
        pools[zid] = pool
        if not pool.empty:
            eligible_zids.append(zid)

    # Choose up to ZIDS_PER_DAY zids uniformly at random
    chosen_zids = choose_zids_uniform(eligible_zids)
    all_rows = []

    # For each chosen zid, pick items (value-weighted), log them
    for zid in chosen_zids:
        picks_df = choose_items_value_weighted(pools[zid], ITEMS_PER_ZID)
        if picks_df.empty:
            continue
        append_counted(zid, picks_df["itemcode"].tolist(), q_root)
        all_rows.extend(picks_df.to_dict("records"))

    # Build email content

    try:
        # Extract report name from filename
        report_name = os.path.splitext(os.path.basename(__file__))[0]
        recipients = get_email_recipients(report_name)
        #recipients = ["ithmbrbd@gmail.com"] 
        print(f"📬 Recipients: {recipients}")
    except Exception as e:
        print(f"⚠️ Failed to fetch recipients: {e}")
        recipients = ["ithmbrbd@gmail.com"]  # Fallback

    intro_text = f"Dear {COUNTER_NAME},\nPlease perform a blind count of the following items today ({today.isoformat()})."
    html_sections = build_html(COUNTER_NAME, today.isoformat(), all_rows)
    subject = f"Fixit-10 Random Cycle Count – {today.isoformat()}"

    # Send using shared mail module,
    print(f"📧 Sending email to: {COUNTER_EMAILS}")
    print(f"📊 Items to count: {len(all_rows)} across {len(chosen_zids)} departments.")

    send_mail(
        subject=subject,
        bodyText=intro_text,
        attachment=[],
        recipient=recipients,
        html_body=html_sections
    )

    print(f"✅ {today}: Email sent with {len(all_rows)} items across {len(chosen_zids)} departments.")

# ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ ERROR:", e, file=sys.stderr)
        sys.exit(1)