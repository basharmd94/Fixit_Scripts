"""
🚀 F_04_Woocom_Delivery_Stats.py – Woocommerce Delivery Status Dashboard

📌 PURPOSE:
    - Fetch Woocommerce order stats across statuses
    - Normalize, aggregate, and email a dashboard

🔧 DATA SOURCES:
    - Woocommerce REST API (wc/v3)

📅 SCHEDULE:
    Runs daily or on-demand

📧 EMAIL:
    - Recipients: get_email_recipients("F_04_Woocom_Delivery_Stats")
    - Fallback: ithmbrbd@gmail.com

🔐 SECRETS:
    - API_KEY and API_SECRET from environment (.env)

💡 NOTE:
    - Fixes pagination return bug
    - Uses shared mail utility and passes DataFrames for HTML rendering
"""

import os
import sys
import pandas as pd
import numpy as np
from urllib.error import HTTPError
from woocommerce import API
from dateutil import parser
from datetime import datetime, timedelta

# === 1. Add project root to Python path ===
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# === 2. Shared mail ===
from mail import send_mail, get_email_recipients

# === 3. Load env keys ===
print("\n==[ ENV ]===============================================")
API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')
if not API_KEY or not API_SECRET:
    raise RuntimeError('!! Missing API_KEY or API_SECRET in environment')
print("[OK] Loaded Woocommerce credentials from environment")

# === 4. Woocommerce Connection ===
print("\n==[ WOOCOMMERCE ]======================================")
wcapi = API(
  url="https://fixit.com.bd",
  consumer_key=API_KEY,
  consumer_secret=API_SECRET,
  version="wc/v3",
  timeout=2000
)
print("[OK] WC API client initialized -> https://fixit.com.bd (wc/v3)")

# === 5. Helpers ===
def flatListAllStats(status: str, parsing_days: int = 6000):
    """Fetch paginated orders for a status since now - parsing_days."""
    now_dt = datetime.now()
    start_dt = now_dt - timedelta(days=parsing_days)
    after_iso = parser.parse(str(start_dt)).isoformat()
    all_pages = []
    page = 1
    print(f"-> Fetching status '{status}' since {after_iso} ...")
    try:
        while True:
            resp = wcapi.get('orders', params={'per_page': 100, 'page': page, 'status': status, 'after': after_iso}).json()
            if not resp:
                break
            all_pages.append(resp)
            print(f"   .. page {page:>2} ✓  ({len(resp)} records)")
            page += 1
        print(f"[OK] Collected {sum(len(p) for p in all_pages)} records for '{status}'\n")
        return all_pages
    except HTTPError as e:
        print(f"!! HTTPError while fetching '{status}': {e}")
        return []


def df_flatListAllStats(status: str) -> pd.DataFrame:
    parsing_days = 5 if status in ('order-cancelled', 'completed') else 6000
    pages = flatListAllStats(status, parsing_days)
    flat_list_orders = [x for page in pages for x in page]
    filter_keys = ['id', 'date_created', 'date_modified', 'status', 'date_completed', 'total', 'customer_note']
    flat_list = [{k: v for k, v in rec.items() if k in filter_keys} for rec in flat_list_orders]
    if not flat_list:
        return pd.DataFrame(columns=['id', 'status', 'date_created', 'date_completed', 'customer_note', 'total'])
    df = pd.DataFrame(flat_list, columns=['id', 'status', 'date_created', 'date_completed', 'customer_note', 'total']).sort_values(by=['status'])
    return df

# === 6. Fetch data for each status ===
print("\n==[ FETCH STATUSES ]====================================")
statuses = [
    'processing', 'order-confirmed', 'will-ecourier', 'will-sundarban',
    'consundorban', 'on-the-way-to-del', 'order-cancelled', 'completed'
]

status_to_df = {}
for st in statuses:
    try:
        df = df_flatListAllStats(st)
        if df.empty and st == 'will-sundarban':
            # Fallback row if no sundarban orders
            date_iso = datetime.now().isoformat()
            df = pd.DataFrame([{
                'id': 'no-sundarban-courier',
                'status': 'no-sundarban-courier',
                'date_created': date_iso,
                'date_completed': None,
                'customer_note': 'no-sundarban-courier',
                'total': 'no-sundarban-courier'
            }])
        status_to_df[st] = df
        print(f"[OK] Status '{st}' -> {len(df)} rows")
    except Exception as ex:
        # Specific fallback for on-the-way-to-del
        if st == 'on-the-way-to-del':
            date_iso = datetime.now().isoformat()
            status_to_df[st] = pd.DataFrame([{
                'id': 'NO-DELIVERY-TODAY',
                'status': 'on-the-way-to-del',
                'date_created': date_iso,
                'date_completed': None,
                'customer_note': 'NO-DELIVERY-TODAY',
                'total': 'NO-DELIVERY-TODAY'
            }])
            print(f"[WARN] '{st}' fallback used (no records)")
        else:
            status_to_df[st] = pd.DataFrame()
            print(f"[WARN] '{st}' -> exception handled: {ex}")

# === 7. Combine and transform ===
print("\n==[ TRANSFORM ]==========================================")
df_all_order_stats = pd.concat([status_to_df[s] for s in statuses if not status_to_df[s].empty], ignore_index=True)
print(f"[OK] Combined rows: {len(df_all_order_stats)}")

# Convert datetime strings
for col in ['date_created', 'date_completed']:
    df_all_order_stats[col] = pd.to_datetime(df_all_order_stats[col], errors='coerce')

# To date only
df_all_order_stats['date_completed'] = pd.to_datetime(df_all_order_stats['date_completed']).dt.date
df_all_order_stats['date_created'] = pd.to_datetime(df_all_order_stats['date_created']).dt.date

# Derived columns
df_all_order_stats['completed_in'] = (pd.to_datetime(df_all_order_stats['date_completed']) - pd.to_datetime(df_all_order_stats['date_created']))
df_all_order_stats['today_date'] = pd.to_datetime('today').normalize().date()

def _age_row(row):
    if row.get('status') == 'completed' and row.get('date_completed'):
        return row['date_completed'] - row['date_created']
    return row['today_date'] - row['date_created']

df_all_order_stats['days_for_order'] = df_all_order_stats.apply(_age_row, axis=1).astype(str)
df_all_order_stats['days_pass_order'] = df_all_order_stats.apply(
    lambda x: f"completed within {x['days_for_order']}" if x['status'] == 'completed' else f"{x['days_for_order']}  pass after created",
    axis=1
)

# Drop unused
df_all_order_stats.drop(columns=[c for c in ['date_modified', 'completed_in', 'today_date'] if c in df_all_order_stats.columns], inplace=True, errors='ignore')

# Status rename
rename_map = {
    'will-ecourier': 'Redx Done',
    'will-sundarban': 'Sundarban order confirmed',
    'consundorban': 'Sundarban courier done',
    'on-the-way-to-del': 'Out for delivery in Dhaka',
    'order-cancelled': 'Cancelled by Customer',
    'completed': 'Order Completed'
}

df_all_order_stats['now_status'] = df_all_order_stats['status'].map(rename_map).fillna(df_all_order_stats['status'])

# Final columns
df_woocommerce_data = df_all_order_stats.loc[:, ['id', 'now_status', 'date_created', 'date_completed', 'customer_note', 'total']]

# Split per status for email sections
print("[OK] Preparing email sections ...")
split_statuses = [
    'processing', 'order-confirmed', 'Redx Done', 'Sundarban order confirmed',
    'Sundarban courier done', 'Out for delivery in Dhaka', 'Cancelled by Customer', 'Order Completed'
]

section_tables = []
for label in split_statuses:
    section_df = df_woocommerce_data[df_woocommerce_data['now_status'] == label].sort_values(by='date_created', ascending=True).reset_index(drop=True)
    section_tables.append((section_df, label))
    print(f"   [+] Section '{label}' -> {len(section_df)} rows")

# === 8. Email ===
print("\n==[ EMAIL ]==============================================")
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"[OK] Recipients loaded: {recipients}")
except Exception as e:
    print(f"[WARN] Failed to get recipients: {e}. Using fallback.")
    recipients = ["ithmbrbd@gmail.com"]

subject = f"Fixit-04 Woocommerce Delivery Stats – {datetime.now().strftime('%Y-%m-%d')}"
body_text = "Please find today's Woocommerce delivery status breakdown below."

send_mail(
    subject=subject,
    bodyText=body_text,
    html_body=section_tables,
    attachment=[],
    recipient=recipients
)

print("[DONE] Email sent successfully.\n")
