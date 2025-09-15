"""
🚀 F_06_Update_Price_Ecom_by_Fixit_ERP.py – Sync Woo Prices with ERP

📌 PURPOSE:
    - Fetch Woo products (id, sku, regular_price)
    - Compare against ERP Gulshan and Ecommerce prices
    - Update ERP Ecommerce prices when Woo < ERP
    - Update Woo when Woo < Gulshan (batch API)
    - Notify via email with attachments if needed

🔧 DATA SOURCES:
    - WooCommerce API (wc/v3)
    - ERP via DATABASE_URL in project_config.py

📧 EMAIL:
    - Recipients: get_email_recipients("F_06_Update_Price_Ecom_by_Fixit_ERP")
    - Fallback: ithmbrbd@gmail.com
"""

import os
import sys
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from woocommerce import API

# === 1. Add project root to Python path ===
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# === 2. Import shared modules (consistent with F_01) ===
from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL

print("\n==[ START ]============================================")
print("[OK] Loading environment and database config ...")

# === DB Engine ===
engine = create_engine(DATABASE_URL)
print("[OK] Database engine initialized")

# === Woo API from environment ===
API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')
if not API_KEY or not API_SECRET:
    raise RuntimeError('Missing API_KEY or API_SECRET in environment')
wcapi = API(
    url="https://fixit.com.bd",
    consumer_key=API_KEY,
    consumer_secret=API_SECRET,
    version="wc/v3",
    timeout=2000
)
print("[OK] Woo API client initialized -> https://fixit.com.bd (wc/v3)")

# === 1) Fetch Woo products ===
print("\n==[ WOO PRODUCTS ]=====================================")
products = []
page = 1
while True:
    r = wcapi.get('products', params={'per_page': 100, 'page': page})
    if getattr(r, 'status_code', 200) != 200:
        try:
            snippet = r.text[:200]
        except Exception:
            snippet = '<no-body>'
        print(f"[WARN] Woo non-200 on products page {page}: {getattr(r, 'status_code', 'NA')} Body: {snippet}")
        break
    try:
        prods = r.json()
    except Exception as ex:
        print(f"[WARN] JSON decode failed on products page {page}: {ex}")
        break
    if not prods:
        break
    products.append(prods)
    print(f"   .. page {page:>2} ✓  ({len(prods)} items)")
    page += 1

flat = [x for l in products for x in l]
print(f"[OK] Total products fetched: {len(flat)}")

# DataFrame construction
cols = ['id','sku','regular_price']
df_woocommerce_data = pd.DataFrame(flat, columns=cols).rename(columns={'sku': 'xitem'})

# === 2) ERP items ===
def caitem(zid):
    return pd.read_sql("select xitem, xstdprice from caitem where zid={}". format(zid) , con = engine)

print("\n==[ ERP PRICES ]=======================================")
try:
    FIXIT_ID = 100001
    ECOMMERCE_ID = 100003
    gulshan_price = caitem(FIXIT_ID)
    ecommerce_price = caitem(ECOMMERCE_ID)
    print(f"[OK] ERP prices loaded: Gulshan={len(gulshan_price)} | Ecommerce={len(ecommerce_price)}")
except Exception as ex:
    print(f"[ERR] Failed to load ERP prices: {ex}")
    gulshan_price = pd.DataFrame(columns=['xitem','xstdprice'])
    ecommerce_price = pd.DataFrame(columns=['xitem','xstdprice'])

# === 3) Compare Woo vs ERP: Ecommerce ===
print("\n==[ COMPARE: WOO vs ECOMMERCE ]========================")
df_we = pd.merge(df_woocommerce_data, ecommerce_price, on='xitem', how='left').dropna().sort_values(by='xitem')
df_we = df_we[df_we['xitem'] != ''].rename(columns={'regular_price': 'website_price', 'xstdprice': 'ecommerce_price'}).reset_index(drop=True)
df_we['website_price'] = pd.to_numeric(df_we['website_price'], errors='coerce')

if not df_we.empty:
    df_we['is_website_price_less_than_ecommerceErp'] = np.where(df_we['website_price'] < df_we['ecommerce_price'], True, False)
    df_to_update_erp = df_we[df_we['is_website_price_less_than_ecommerceErp'] == True].dropna().rename(columns={'ecommerce_price': 'xstdprice'})
    df_to_update_erp['xstdprice'] = df_to_update_erp['xstdprice'].astype(int)
    df_update_price_to_ecommerceErp = df_to_update_erp[['xitem','xstdprice']]
    update_ecommerce_erp_tuple_list = tuple(list(df_update_price_to_ecommerceErp.to_records(index=False)))
    print(f"[OK] ERP updates to apply: {len(update_ecommerce_erp_tuple_list)}")
else:
    print("[OK] No ecommerce comparison data available")

# === 4) Apply ERP Ecommerce updates ===
if 'update_ecommerce_erp_tuple_list' in locals() and update_ecommerce_erp_tuple_list:
    update_query = """UPDATE caitem AS d
                set xstdprice = s.a
                FROM (VALUES %s) AS s(xitem, a)
                WHERE d.xitem = s.xitem and d.zid=100003;"""
    try:
        raw_conn = engine.raw_connection()
        with raw_conn:
            with raw_conn.cursor() as curs:
                psycopg2.extras.execute_values(curs, update_query, update_ecommerce_erp_tuple_list, template=None, page_size=1000000)
                print("[OK] ERP Ecommerce prices updated")
    except Exception as e:
        print(f"[ERR] ERP price update failed: {e}")
else:
    send_mail(
        "Website price compare with ecommerce",
        "No dataframe to update"
    )

# === 5) Compare Woo vs ERP: Gulshan ===
print("\n==[ COMPARE: WOO vs GULSHAN ]==========================")
df_wg = pd.merge(df_woocommerce_data, gulshan_price, on='xitem', how='left').dropna().sort_values(by='xitem')
df_wg = df_wg[df_wg['xitem'] != ''].rename(columns={'regular_price': 'website_price', 'xstdprice': 'gulshan_price'}).reset_index(drop=True)
df_wg['website_price'] = pd.to_numeric(df_wg['website_price'], errors='coerce')

if not df_wg.empty:
    df_wg['is_website_price_less_than_gulshan'] = np.where(df_wg['website_price'] < df_wg['gulshan_price'], True, False)
    df_update_woo = df_wg[df_wg['is_website_price_less_than_gulshan'] == True].dropna().rename(columns={'gulshan_price': 'regular_price'})
    df_update_woo = df_update_woo[['id','regular_price']].astype(int)
    print(f"[OK] Woo items to raise price: {len(df_update_woo)}")
else:
    print("[OK] No gulshan comparison data available")

# === 6) Update Woo prices in batch ===
if 'df_update_woo' in locals() and not df_update_woo.empty:
    import requests
    try:
        jsonData = df_update_woo.to_dict('records')
        entry_no = 99
        total_item = []
        for i in range(0, len(jsonData), entry_no):
            sub = jsonData[i:i+entry_no]
            data = {'update': sub}
            wcapi.post("products/batch", data).json()
            total_item.append(len(sub))
        print(f"[OK] Woo prices updated in {len(total_item)} batches")
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)
else:
    send_mail("website price update", "nothing found any dataframe to update website data")

# === 7) Notify where Gulshan is cheaper than Woo ===
print("\n==[ REPORT: GULSHAN < WOO ]===========================")
if not df_wg.empty:
    df_wg['is_website_price_greater_than_gulshan'] = np.where(df_wg['website_price'] > df_wg['gulshan_price'], True, False)
    df_website_price_is_greater_than_gulshan = df_wg[df_wg['is_website_price_greater_than_gulshan'] == True].dropna()
    df_website_price_is_greater_than_gulshan.to_excel('need_to_change_gulshan_price.xlsx')
    excel_file_list = ["need_to_change_gulshan_price.xlsx"]
    try:
        recipients = get_email_recipients("F_06_Update_Price_Ecom_by_Fixit_ERP")
    except Exception as e:
        print(f"[WARN] Recipient lookup failed: {e}; using fallback")
        recipients = ['ithmbrbd@gmail.com']
    send_mail(
        "Fixit Price is Lower Than Website",
        "Need to change Price of Gulshan which price is less than Website Price",
        excel_file_list,
        recipients
    )
else:
    send_mail(
        "Fixit Price is Lower Than Website",
        "Need to change Price of Gulshan which price is less than Website Price",
    )

print("\n[DONE] Price sync finished.\n")


