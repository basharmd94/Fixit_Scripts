import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine

# === 1. Add project root to Python path ===
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# === 2. Import shared modules ===
from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL

# === 3. Configuration ===
ZID_FIXIT = 100001  # Fixit (Gulshan) warehouse ID
TODAY_DATE = date.today().strftime("%Y-%m-%d")

print(f"📌 Processing for: Fixit Warehouse (ZID={ZID_FIXIT})")
print(f"📅 Report Date: {TODAY_DATE}")

# === 4. Create database engine ===
engine = create_engine(DATABASE_URL)

# === 5. Suppress warnings ===
import warnings
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
pd.set_option('display.float_format', '{:.2f}'.format)

# === 6. SQL Query Functions (Parameterized) ===
def make_product_tuple(zid: int, from_date: str):
    """Return tuple of items purchased since from_date."""
    query = (
        "SELECT DISTINCT imtrn.xitem FROM imtrn "
        "WHERE imtrn.zid = %s AND imtrn.xdocnum LIKE %s AND imtrn.xdate > %s "
        "GROUP BY imtrn.xitem"
    )
    df_item = pd.read_sql(query, con=engine, params=[zid, 'GRN-%', from_date])
    return tuple(df_item['xitem'])


def build_analysis(zid: int, from_date: str) -> pd.DataFrame:
    """Build purchase/stock/sales analysis for a given zid and start date."""
    item_tuple = make_product_tuple(zid, from_date)
    if len(item_tuple) == 0:
        return pd.DataFrame(columns=[
            'Business_Id','Item Code','Item Name','Item Group','Purchase Qty','Purchase Rate',
            'Purchase_Value','Stock','Sales Qty','Sales Rate','Sales Value','purchase_total','sales_total'
        ])

    purchase_query = (
        "SELECT imtrn.zid, imtrn.xitem, caitem.xdesc, caitem.xgitem, imtrn.xdate, "
        "SUM(imtrn.xqty*imtrn.xsign) AS purchase, AVG(imtrn.xval/imtrn.xqty) AS rate, caitem.xstdprice "
        "FROM imtrn JOIN caitem ON imtrn.xitem = caitem.xitem "
        "WHERE imtrn.zid = %s AND caitem.zid = %s AND imtrn.xdocnum LIKE %s AND imtrn.xdate > %s "
        "GROUP BY imtrn.zid, imtrn.xitem, caitem.xdesc, caitem.xgitem, imtrn.xdate, caitem.xstdprice"
    )
    df_purchase = pd.read_sql(purchase_query, con=engine, params=[zid, zid, 'GRN-%', from_date])

    stock_query = (
        "SELECT imtrn.xitem, SUM(imtrn.xqty*imtrn.xsign) AS inventory "
        "FROM imtrn WHERE imtrn.zid = %s AND imtrn.xitem IN %s GROUP BY imtrn.xitem"
    )
    df_stock = pd.read_sql(stock_query, con=engine, params=[zid, item_tuple])

    sales_query = (
        "SELECT imtrn.xitem, SUM(imtrn.xqty*imtrn.xsign) AS sales "
        "FROM imtrn WHERE imtrn.zid = %s AND imtrn.xitem IN %s AND imtrn.xdocnum LIKE %s AND imtrn.xdate > %s "
        "GROUP BY imtrn.xitem"
    )
    df_sales = pd.read_sql(sales_query, con=engine, params=[zid, item_tuple, 'CO-%', from_date])

    df_main = df_purchase.merge(df_stock, on='xitem', how='left')
    df_main = df_main.merge(df_stock, on='xitem', how='left')
    df_main = df_main.merge(df_sales, on='xitem', how='left')
    df_main = df_main.drop(['inventory_x'], axis=1)
    df_main = df_main.rename(columns={
        'zid': 'Business_Id', 'xitem': 'Item Code', 'xdesc': 'Item Name', 'xgitem': 'Item Group',
        'purchase': 'Purchase Qty', 'rate': 'Purchase Rate', 'xstdprice': 'Sales Rate',
        'inventory_y': 'Stock', 'sales': 'Sales Qty'
    })
    df_main = df_main.fillna(0)
    df_main['Purchase_Value'] = df_main['Purchase Qty'] * df_main['Purchase Rate']
    df_main['purchase_total'] = df_main['Purchase_Value'].sum()
    df_main['Sales Value'] = df_main['Sales Qty'] * df_main['Sales Rate'] * -1
    df_main['sales_total'] = df_main['Sales Value'].sum()
    df_main = df_main[[
        'Business_Id','Item Code','Item Name','Item Group','Purchase Qty','Purchase Rate',
        'Purchase_Value','Stock','Sales Qty','Sales Rate','Sales Value','purchase_total','sales_total'
    ]]
    return df_main


# === 7. Main Data Processing ===
DATE_FROM = (datetime.now() - timedelta(days=30)).date().strftime('%Y-%m-%d')
print(f"🔄 Building analysis since: {DATE_FROM}")

df = build_analysis(ZID_FIXIT, DATE_FROM)
df_for_totals = df.copy()
if not df_for_totals.empty:
    df_for_totals = df_for_totals.drop(columns=['purchase_total','sales_total'])

df_html = build_analysis(ZID_FIXIT, DATE_FROM)
if df_html.empty:
    df_purcharse_history = 0
    df_purcharse_history_1 = 0
else:
    df_purcharse_history = df_html['purchase_total'][0]
    df_purcharse_history_1 = df_html['sales_total'][0]

df_profit_percentage = 0 if df_purcharse_history == 0 else (df_purcharse_history_1 / df_purcharse_history) * (-100)
summary_dict = {
    'Total purchase Value': [df_purcharse_history],
    'total Sales Value': [df_purcharse_history_1],
    'Sales of purchase analysis': [df_profit_percentage]
}

df_total_sale_purchase = pd.DataFrame(data=summary_dict)

# Prepare detail HTML frame (reduced columns)
if not df_html.empty:
    df_html = df_html.drop(columns=[
        'Business_Id','Item Group','Purchase Qty','Purchase Rate','Purchase_Value','Stock','Sales Qty','Sales Rate','Sales Value','purchase_total','sales_total'
    ])

# === 8. Export to Excel ===
excel_file = 'salesOfPurchaseAnalysis.xlsx'
print(f"📊 Generating Excel report: {excel_file}")
with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
    df_for_totals.to_excel(writer, 'fixit', index=False)
    df_total_sale_purchase.to_excel(writer, 'summary', index=False)

print("✅ Report generated successfully.")

# === 9. Send Email ===
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Failed to get recipients: {e}. Using fallback.")
    recipients = ["ithmbrbd@gmail.com", "asaddat87@gmail.com", "fixitc.central@gmail.com"]

subject = f"Fixit-11Sales Of Purchase Analysis – {TODAY_DATE}"
body_text = "Please find the Sales Of Purchase Analysis attached."

send_mail(
    subject=subject,
    bodyText=body_text,
    html_body=[(df_total_sale_purchase, "Sales/Purchase Summary")],
    attachment=[excel_file],
    recipient=recipients
)

print("📧 Email sent successfully.")

# === 10. Cleanup ===
engine.dispose()
print("✅ Script completed successfully.")
