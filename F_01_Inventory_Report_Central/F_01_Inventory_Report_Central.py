"""
🚀 F_01_Inventory_Report_Central.py – Daily Inventory Report for Central Warehouse

📌 PURPOSE:
    - Generate inventory report for central warehouse (zid: 100002)
    - Merge with latest purchase data from Fixit (zid: 100001)
    - Calculate item rates and create Excel report
    - Send automated email with attachments

🔧 DATA SOURCES:
    - Inventory: imtrn, caitem tables
    - Database: PostgreSQL via DATABASE_URL in project_config.py

📅 SCHEDULE:
    Runs daily for inventory tracking

🏢 INPUT:
    - ZID_CENTRAL: 100002 (Central warehouse)
    - ZID_FIXIT: 100001 (Fixit warehouse)

📧 EMAIL:
    - Recipients: get_email_recipients("F_01_Inventory_Report_Central")
    - Fallback: ithmbrbd@gmail.com

💡 NOTE:
    - Uses parameterized queries for security
    - Merges inventory with purchase data for rate calculation
    - Exports to Excel and sends via email
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, date
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
ZID_CENTRAL = 100002  # Central warehouse ID
ZID_FIXIT = 100001    # Fixit warehouse ID
TODAY_DATE = date.today().strftime("%Y-%m-%d")

print(f"📌 Processing for: Central Warehouse (ZID={ZID_CENTRAL})")
print(f"📅 Report Date: {TODAY_DATE}")

# === 4. Create database engine ===
engine = create_engine(DATABASE_URL)

# === 5. Suppress warnings ===
import warnings
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
pd.set_option('display.float_format', '{:.2f}'.format)

# === 6. SQL Query Functions (Parameterized) ===
def get_inventory_data(zid):
    """Fetch inventory data for specified warehouse."""
    query = """
    SELECT imtrn.zid, imtrn.xitem, caitem.xdesc, 
           sum(imtrn.xqty*imtrn.xsign) as inventory,
           max(imtrn.xdate) as max_date
    FROM imtrn
    JOIN caitem ON imtrn.xitem = caitem.xitem
    WHERE imtrn.zid = %(zid)s AND caitem.zid = %(zid)s
    GROUP BY imtrn.zid, imtrn.xitem, caitem.xdesc
    """
    return pd.read_sql(query, con=engine, params={'zid': zid})

def get_purchase_data(zid):
    """Fetch latest purchase data for rate calculation."""
    query = """
    SELECT * FROM (
        SELECT zid, xitem, xdate, xval, xqty, 
               ROW_NUMBER() OVER(PARTITION BY xitem ORDER BY xdate DESC) as rn
        FROM imtrn
        WHERE zid = %(zid)s
    ) t
    WHERE t.rn = 1
    """
    return pd.read_sql(query, con=engine, params={'zid': zid})

# === 7. Fetch Data ===
print("📥 Fetching inventory data...")
df_inventory = get_inventory_data(ZID_CENTRAL)

print("📥 Fetching purchase data...")
df_purchase = get_purchase_data(ZID_FIXIT)

# === 8. Process Data ===
print("🔄 Merging datasets...")
df_main = df_inventory.merge(df_purchase, on='xitem', how='left')

# Calculate item rates
df_main['xrate'] = df_main['xval'] / df_main['xqty']

# Clean up unnecessary columns
df_main = df_main.drop(['zid_y', 'xdate'], axis=1)

# === 9. Export to Excel ===
excel_file = 'F_01_Inventory_Report_Central.xlsx'
print(f"📊 Generating Excel report: {excel_file}")
with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
    df_main.to_excel(writer, 'central_stock', index=False)

print(f"✅ Report generated successfully:")
print(f"   📊 Excel: {excel_file}")
print(f"   📈 Total items: {len(df_main)}")

# === 10. Send Email ===
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Failed to get recipients: {e}. Using fallback.")
    recipients = ["ithmbrbd@gmail.com"]

subject = f"Fixit-01 Daily Inventory Report Central – {TODAY_DATE}"
body_text = "Please find today's inventory report below."

send_mail(
    subject=subject,
    bodyText=body_text,
    html_body=[(df_main, "Central Inventory Report")],
    attachment=[excel_file],
    recipient=recipients
)

print("📧 Email sent successfully.")

# === 11. Cleanup ===
engine.dispose()
print("✅ Script completed successfully.")