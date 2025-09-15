"""
🚀 F_05_Last_Purchase_Supplier.py – Last Purchase & Supplier Info from Recent Woo Orders

📌 PURPOSE:
    - Compute inventory values per warehouse
    - Fetch last day's Woocommerce orders and extract item SKUs
    - Join with ERP purchase data to get last supplier and price
    - Export to Excel and email a concise dashboard

🔧 DATA SOURCES:
    - ERP: imtrn, poord, poodt, casup
    - Woocommerce REST API (wc/v3)
    - Database: PostgreSQL via DATABASE_URL in project_config.py

📅 SCHEDULE:
    Runs daily (last 1-day Woo orders)

📧 EMAIL:
    - Recipients: get_email_recipients("F_05_Last_Purchase_Supplier")
    - Fallback: ithmbrbd@gmail.com

🔐 SECRETS:
    - API_KEY and API_SECRET from environment (.env)

💡 NOTE:
    - Parameterized queries (safe IN clause construction)
    - .xlsx export using openpyxl
    - Emoji/ASCII prints for progress tracking
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine
from dateutil import parser
from woocommerce import API

# === 1. Add project root to Python path ===
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# === 2. Shared modules ===
from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL

# === 3. Create shared DB engine ===
print("\n==[ DB ]==============================================")
engine = create_engine(DATABASE_URL)
print("[OK] Database engine created")

# === 4. Load env for Woo ===
print("\n==[ ENV ]=============================================")
API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')
if not API_KEY or not API_SECRET:
    raise RuntimeError('!! Missing API_KEY or API_SECRET in environment')
print("[OK] Loaded Woo credentials from environment")

# === 5. Woo API client ===
print("\n==[ WOOCOMMERCE ]=====================================")
wcapi = API(
    url="https://fixit.com.bd",
    consumer_key=API_KEY,
    consumer_secret=API_SECRET,
    version="wc/v3",
    timeout=2000
)
print("[OK] WC API client initialized -> https://fixit.com.bd (wc/v3)")

TODAY_STR = date.today().strftime('%Y-%m-%d')

# === 6. Inventory value helper ===
def get_inventory_value_by_wh(zid: int, last_date: str) -> pd.DataFrame:
    query = """
        SELECT SUM(imtrn.xval * imtrn.xsign) AS sum, imtrn.xwh
        FROM imtrn
        WHERE imtrn.zid = %(zid)s AND imtrn.xdate <= %(last_date)s
        GROUP BY imtrn.xwh
        ORDER BY imtrn.xwh
    """
    return pd.read_sql(query, con=engine, params={'zid': zid, 'last_date': last_date})

print("\n==[ INVENTORY ]=======================================")
zids = [100001, 100002, 100003]  # Gulshan, Central, Ecommerce

inv_gul = get_inventory_value_by_wh(zids[0], TODAY_STR)
inv_cen = get_inventory_value_by_wh(zids[1], TODAY_STR)
inv_ecom = get_inventory_value_by_wh(zids[2], TODAY_STR)

# Filter by warehouse names
inv_gul = round(inv_gul[inv_gul["xwh"].str.contains("Fixit", na=False)]['sum'].sum(), 2)
inv_cen = round(inv_cen[inv_cen["xwh"].str.contains("Fixit Central", na=False)]['sum'].sum(), 2)
inv_ecom = round(inv_ecom[inv_ecom["xwh"].str.contains("Ecommerce", na=False)]['sum'].sum(), 2)

inv_df = pd.DataFrame({
    'gulshan_inv_value': [inv_gul],
    'central_inv_value': [inv_cen],
    'ecommerce_inv_value': [inv_ecom]
})
print(f"[OK] Inventory values -> Gulshan={inv_gul} | Central={inv_cen} | Ecommerce={inv_ecom}")

# === 7. Pull Woo orders (last 1 day) and extract SKUs ===
print("\n==[ WOO ORDERS ]======================================")
now_dt = datetime.now()
from_dt = now_dt - timedelta(days=1)
after_iso = parser.parse(from_dt.strftime("%Y-%m-%d, %H:%M:%S")).isoformat()

all_pages = []
page = 1
while True:
    r = wcapi.get('orders', params={'per_page': 100, 'page': page, 'after': after_iso})
    if getattr(r, 'status_code', 200) != 200:
        try:
            snippet = r.text[:200]
        except Exception:
            snippet = '<no-body>'
        print(f"[WARN] Woo API non-200 ({getattr(r, 'status_code', 'NA')}), stopping. Body: {snippet}")
        break
    try:
        resp = r.json()
    except Exception as ex:
        try:
            snippet = r.text[:200]
        except Exception:
            snippet = '<no-body>'
        print(f"[WARN] Woo API JSON decode failed on page {page}: {ex}. Body: {snippet}")
        break
    if not resp:
        break
    all_pages.append(resp)
    print(f"   .. page {page:>2} ✓  ({len(resp)} records)")
    page += 1

orders = [row for pg in all_pages for row in pg]
print(f"[OK] Collected {len(orders)} orders since {after_iso}")

# Extract line_items -> SKUs
def _strip_order(record: dict) -> dict:
    # Keep minimal keys and line_items
    keep = ['id', 'status', 'date_created', 'line_items']
    return {k: record.get(k) for k in keep}

minimal_orders = [_strip_order(o) for o in orders]

# Remove unneeded keys within line_items
line_item_drop = {'product_id', 'variation_id', 'quantity', 'tax_class', 'subtotal', 'subtotal_tax',
                  'total', 'total_tax', 'taxes', 'meta_data', 'price', 'image', 'parent_name'}
for o in minimal_orders:
    for li in o.get('line_items', []):
        for k in list(li.keys()):
            if k in line_item_drop:
                del li[k]

# Rename id -> orders_id and flatten
for o in minimal_orders:
    o['orders_id'] = o.pop('id', None)

# Build Woo dataframe safely
expected_cols = ['product_id', 'xitem', 'orders_id', 'status', 'date_created']
if minimal_orders and any(o.get('line_items') for o in minimal_orders):
    try:
        woo_df = pd.json_normalize(minimal_orders, ['line_items'], ['orders_id', 'status', 'date_created']).rename(
            columns={'id': 'product_id', 'sku': 'xitem'}
        )
    except Exception as ex:
        print(f"[WARN] Failed to normalize line_items: {ex}")
        woo_df = pd.DataFrame(columns=expected_cols)
else:
    print("[WARN] No Woo orders or line_items to normalize")
    woo_df = pd.DataFrame(columns=expected_cols)

# Ensure expected columns exist
for col in expected_cols:
    if col not in woo_df.columns:
        woo_df[col] = pd.Series(dtype=object)

sku_tuple = tuple(woo_df.get('xitem', pd.Series(dtype=object)).dropna().astype(str).tolist())
print(f"[OK] Extracted {len(sku_tuple)} SKUs from last-day orders")

# === 8. Purchase info for SKUs ===
print("\n==[ ERP PURCHASE ]====================================")
if sku_tuple:
    # Safe IN clause composition
    in_list = '(' + ','.join(["%s"] * len(sku_tuple)) + ')'
    query = f"""
        SELECT poord.xdate, poord.xpornum, poord.xsup, casup.xshort, casup.xphone,
               poodt.xitem, poodt.xpornum, poodt.xrate
        FROM poord
        INNER JOIN poodt ON poord.xpornum = poodt.xpornum AND poord.zid = poodt.zid
        JOIN casup ON poord.xsup = casup.xsup AND casup.zid = poord.zid
        WHERE poord.zid = %s
          AND poodt.xitem IN {in_list}
    """
    # Pass positional parameters: first zid, then all SKU values
    df_sup_item = pd.read_sql(query, con=engine, params=(100002, *sku_tuple))
    print(f"[OK] Retrieved {len(df_sup_item)} purchase rows from ERP")
else:
    df_sup_item = pd.DataFrame(columns=['xdate','xpornum','xsup','xshort','xphone','xitem','xrate'])
    print("[WARN] No SKUs found; purchase dataset empty")

# Last purchase per item
if not df_sup_item.empty:
    df_sup_item = df_sup_item.sort_values(['xitem', 'xdate']).groupby('xitem', as_index=False).last()

# Join Woo SKUs with last purchase info
final_df = pd.merge(
    woo_df,
    df_sup_item,
    left_on='xitem', right_on='xitem', how='left'
).rename(columns={
    'xitem': 'item_code',
    'orders_id': 'woo_order_id',
    'xdate': 'last_purchase_date',
    'xpornum': 'last_po_number',
    'xsup': 'supplier_code',
    'xshort': 'supplier_name',
    'xphone': 'supplier_mobile',
    'xrate': 'last_purchase_price',
    'date_created': 'woo_order_create_date',
    'name': 'item_desc'
})

# Select and fill
keep_cols = ['item_code', 'woo_order_id', 'woo_order_create_date', 'status', 'supplier_code',
             'supplier_name', 'supplier_mobile', 'last_purchase_date', 'last_po_number', 'last_purchase_price']
for col in keep_cols:
    if col not in final_df.columns:
        final_df[col] = np.nan
final_df = final_df.loc[:, keep_cols].fillna('no-order-yet')

# === 9. Export and Email ===
print("\n==[ EXPORT ]===========================================")
excel_file = 'F_05_Supplier_and_Inv_Info.xlsx'
with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
    inv_df.to_excel(writer, sheet_name='inv_value_fixit', index=False)
    final_df.to_excel(writer, sheet_name='supplier_and_order_info', index=False)
print(f"[OK] Excel written -> {excel_file}")

print("\n==[ EMAIL ]============================================")
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"[OK] Recipients: {recipients}")
except Exception as e:
    print(f"[WARN] Recipient lookup failed: {e} -> using fallback")
    recipients = ['ithmbrbd@gmail.com']

subject = f"Fixit-05 Last Purchase & Supplier – {TODAY_STR}"
body_text = "Attached: Inventory values and last purchase info for items found in last day's Woo orders."

send_mail(
    subject=subject,
    bodyText=body_text,
    attachment=[excel_file],
    recipient=recipients,
    html_body=[(inv_df, 'Fixit Inventory Value'), (final_df, 'Supplier & Order Info ')]
)
print("[DONE] Email sent successfully.\n")

# === 10. Cleanup ===
engine.dispose()
print("✅ Script completed and DB engine disposed.")


