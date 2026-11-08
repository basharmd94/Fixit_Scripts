from datetime import datetime, timedelta, date
from typing import Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text

import os
import sys
import warnings


# Add project root for shared modules (mail, config)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import shared utilities
from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL

# Create DB engine
engine = create_engine(DATABASE_URL)

# Pandas display + warning config
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
pd.set_option('display.float_format', '{:.2f}'.format)


# ================= BUSINESS INPUT ================= #

ZID_FIXIT = 100001

TODAY = datetime.today().date()
# TODAY_STR = TODAY.strftime("%Y-%m-%d")
TODAY_STR = TODAY.strftime("%Y-%m-%d")

print (TODAY_STR)

# ================= FETCH FUNCTIONS ================= #

# ---------------- PURCHASE (GRN) ----------------
def fetch_purchase(zid, today):
    return pd.read_sql(text("""
        SELECT 
            pogrn.xdate,
            pogrn.xgrnnum,
            pogdt.xitem,
            caitem.xdesc,
            SUM(pogdt.xqty) AS today_grn_qty
        FROM pogrn
        JOIN pogdt 
            ON pogrn.xgrnnum = pogdt.xgrnnum
        JOIN caitem 
            ON caitem.xitem = pogdt.xitem
        WHERE pogrn.zid = :zid
          AND pogdt.zid = :zid
          AND caitem.zid = :zid
          AND pogrn.xdate = :today
        GROUP BY pogrn.xdate, pogrn.xgrnnum, pogdt.xitem, caitem.xdesc
    """), engine, params={"zid": zid, "today": today})


# ---------------- SALES (ONLY opord/opodt) ----------------
def fetch_sales(zid, today):
    return pd.read_sql(text("""
        SELECT 
            opord.xdate,
            opodt.xitem,
            SUM(opodt.xqtyord) AS today_sales_qty
        FROM opord
        JOIN opodt 
            ON opord.xordernum = opodt.xordernum
        WHERE opord.zid = :zid
          AND opodt.zid = :zid
          AND opord.xdate = :today
        GROUP BY opord.xdate, opodt.xitem
    """), engine, params={"zid": zid, "today": today})


# ---------------- STOCK (IMTRN) ----------------
def fetch_stock(zid, today):
    return pd.read_sql(text("""
        SELECT 
            xitem,
            SUM(CASE WHEN xdate < :today THEN xqty * xsign ELSE 0 END) AS yesterday_stock,
            SUM(xqty * xsign) AS today_stock
        FROM imtrn
        WHERE zid = :zid
        GROUP BY xitem
    """), engine, params={"zid": zid, "today": today})


# ---------------- INVENTORY POSTING CHECK ----------------
def fetch_inventory_insert(zid, today):
    return pd.read_sql(text("""
        SELECT 
            xitem,
            SUM(xqty) AS item_inserted_inv
        FROM imtrn
        WHERE zid = :zid
          AND xdate = :today
          AND xsign = 1
        GROUP BY xitem
    """), engine, params={"zid": zid, "today": today})


# ================= LOAD DATA ================= #

df_purchase = fetch_purchase(ZID_FIXIT, TODAY_STR)
df_sales = fetch_sales(ZID_FIXIT, TODAY_STR)
df_stock = fetch_stock(ZID_FIXIT, TODAY_STR)
df_inv = fetch_inventory_insert(ZID_FIXIT, TODAY_STR)

# ================= MERGE ================= #

df = df_purchase.merge(
    df_inv,
    on="xitem",
    how="left"
).merge(
    df_sales,
    on="xitem",
    how="left"
).merge(
    df_stock,
    on="xitem",
    how="left"
)

# Fill nulls
df["item_inserted_inv"] = df["item_inserted_inv"].fillna(0)
df["today_sales_qty"] = df["today_sales_qty"].fillna(0)
df["yesterday_stock"] = df["yesterday_stock"].fillna(0)
df["today_stock"] = df["today_stock"].fillna(0)

# ================= FINAL CALC ================= #

df["today_stock"] = df["yesterday_stock"] + df["today_grn_qty"] - df["today_sales_qty"]

# ================= FORMAT OUTPUT ================= #

df["xdate"] = TODAY_STR

df = df[[
    "xdate",
    "xgrnnum",
    "xitem",
    "xdesc",
    "today_grn_qty",
    "item_inserted_inv",
    "today_sales_qty",
    "yesterday_stock",
    "today_stock"
]]

# Rename like your Excel style
df.rename(columns={
    "yesterday_stock": f"yesterday_stock_closing-{TODAY - timedelta(days=1):%m-%d}",
    "today_stock": f"today_stock_closing-{TODAY:%m-%d}"
}, inplace=True)

# ================= EXPORT ================= #

file_name = "purchase_check_inventory.xlsx"
df.to_excel(file_name, index=False)

# ================= EMAIL ================= #

try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
except:
    recipients = ['ithmbrbd@gmail.com']

subject = f"Fixit-13 Purchase Check With Inventory – {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
body_text = "Please find the report attached."

html_body = [
    (df.head(20), "Top 20 Purchase Check Items")
]

send_mail(
    subject=subject,
    bodyText=body_text,
    html_body=html_body,
    attachment=[file_name],
    recipient=recipients
)

print("📧 Email sent successfully.")
engine.dispose()