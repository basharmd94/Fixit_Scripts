
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
def get_data(zid):
    """Fetch inventory data for specified warehouse."""
    query = """

    """
    return pd.read_sql(query, con=engine, params={'zid': zid})
    
    
df = get_data(ZID_FIXIT)

# === 7. Export to Excel ===
excel_file = 'F_xx_related_name.xlsx'
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

subject = f"Fixit-xx Related Subject goes here – {TODAY_DATE}"
body_text = "Please find today's related  "

send_mail(
    subject=subject,
    bodyText=body_text,
    html_body=[(df, "related report")],
    attachment=[excel_file],
    recipient=recipients
)

print("📧 Email sent successfully.")

# === 11. Cleanup ===
engine.dispose()
print("✅ Script completed successfully.")