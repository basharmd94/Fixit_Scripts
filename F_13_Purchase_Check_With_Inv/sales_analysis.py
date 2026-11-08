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

# Fixit (Gulshan sales)
ZID_FIXIT = 100001

# Central (warehouse purchase)
ZID_CENTRAL = 100002

# Report date logic:
# Run on June → take May (previous month)
TODAY = datetime.today()
FIRST_DAY_THIS_MONTH = TODAY.replace(day=1)
LAST_MONTH_END = FIRST_DAY_THIS_MONTH - timedelta(days=1)
LAST_MONTH_START = LAST_MONTH_END.replace(day=1)

NOW_STR = TODAY.strftime("%Y-%m-%d")


# ================= FETCH FUNCTIONS ================= #

def fetch_sales(zid, start_date, end_date):
    # Fetch sales for Gulshan for "current month base" (previous month data)
    return pd.read_sql(text("""
        SELECT opord.xdate, opodt.xitem, opodt.xdesc, opodt.xqtyord, opodt.xlineamt
        FROM opord
        JOIN opodt ON opord.xordernum = opodt.xordernum
        WHERE opord.zid = :zid
        AND opodt.zid = :zid
        AND opord.xdate BETWEEN :start AND :end
    """), engine, params={"zid": zid, "start": start_date, "end": end_date})


def fetch_purchase(zid):
    # Fetch ALL purchase history for Central (needed to calculate current + previous PO)
    return pd.read_sql(text("""
        SELECT pogrn.xdate, pogdt.xitem, pogdt.xqty, pogdt.xlineamt, caitem.xdesc
        FROM pogrn
        JOIN pogdt ON pogrn.xgrnnum = pogdt.xgrnnum
        JOIN caitem ON caitem.xitem = pogdt.xitem
        WHERE pogrn.zid = :zid
        AND pogdt.zid = :zid
        AND caitem.zid = :zid
        AND pogrn.xref LIKE '%SINV%'
    """), engine, params={"zid": zid})


def fetch_stock(zid):
    # Calculate stock per item using imtrn (SUM(qty * sign))
    return pd.read_sql(text("""
        SELECT xitem, SUM(xqty * xsign) AS stock
        FROM imtrn
        WHERE zid = :zid
        GROUP BY xitem
    """), engine, params={"zid": zid})


# ================= LOAD DATA ================= #

# Sales for previous month → base dataset
sales_fixit = fetch_sales(
    ZID_FIXIT,
    LAST_MONTH_START.strftime("%Y-%m-%d"),
    LAST_MONTH_END.strftime("%Y-%m-%d")
)

# All purchase history → needed to derive current & previous purchase
purchase_central = fetch_purchase(ZID_CENTRAL)

# Stock snapshot
stock_central = fetch_stock(ZID_CENTRAL)
stock_fixit = fetch_stock(ZID_FIXIT)


# ================= PURCHASE LOGIC ================= #

# Sort by item + date → REQUIRED for correct "previous purchase" logic
purchase_central = purchase_central.sort_values(['xitem', 'xdate'])

# Calculate purchase rate → (line amount / qty)
purchase_central['rate'] = purchase_central.apply(
    lambda r: r['xlineamt'] / r['xqty'] if r['xqty'] else 0,
    axis=1
)

# ---------------- CURRENT PO ---------------- #
# current_po_central_date = latest purchase date
# current_po_qty = qty of latest purchase
# current_po_rate = rate of latest purchase
current_po = purchase_central.groupby('xitem').tail(1).copy()
current_po.rename(columns={
    'xdate': 'current_po_central_date',
    'xqty': 'current_po_qty',
    'rate': 'current_po_rate'
}, inplace=True)

# ---------------- PREVIOUS PO ---------------- #
# prev_po_central_date = purchase BEFORE latest purchase
# prev_qty = qty of that previous purchase
# prev_rate = rate of that previous purchase
purchase_central['prev_date'] = purchase_central.groupby('xitem')['xdate'].shift(1)
purchase_central['prev_qty'] = purchase_central.groupby('xitem')['xqty'].shift(1)
purchase_central['prev_rate'] = purchase_central.groupby('xitem')['rate'].shift(1)

prev_po = purchase_central.groupby('xitem').tail(1)[
    ['xitem', 'prev_date', 'prev_qty', 'prev_rate']
].rename(columns={
    'prev_date': 'prev_po_central_date',
    'prev_qty': 'prev_po_qty_central',
    'prev_rate': 'prev_po_rate_central'
})


# ================= SALES LOGIC ================= #

# Calculate sales rate
sales_fixit['rate'] = sales_fixit.apply(
    lambda r: r['xlineamt'] / r['xqtyord'] if r['xqtyord'] else 0,
    axis=1
)

# Sort to ensure "last" = latest sale
sales_fixit = sales_fixit.sort_values(['xitem', 'xdate'])

# Build required columns:
# current_month_total_sale_qty_gulshan = SUM(qty)
# current_sales_rate_gulshan = latest rate
# item_name = product name
sales_summary = sales_fixit.groupby('xitem').agg({
    'xqtyord': 'sum',
    'rate': 'last',
    'xdate': 'last',
    'xdesc': 'first'
}).reset_index()

sales_summary.rename(columns={
    'xqtyord': 'current_month_total_sale_qty_gulshan',
    'rate': 'current_sales_rate_gulshan',
    'xdate': 'current_sales_date'
}, inplace=True)


# ================= PREVIOUS SALES ================= #

# previous_sales_rate_gulshan:
# last sales BEFORE current reporting month
prev_sales = pd.read_sql(text("""
    SELECT opord.xdate, opodt.xitem, opodt.xqtyord, opodt.xlineamt
    FROM opord
    JOIN opodt ON opord.xordernum = opodt.xordernum
    WHERE opord.zid = :zid
    AND opodt.zid = :zid
    AND opord.xdate < :cutoff
"""), engine, params={
    "zid": ZID_FIXIT,
    "cutoff": LAST_MONTH_START.strftime("%Y-%m-%d")
})

# Calculate rate
prev_sales['rate'] = prev_sales.apply(
    lambda r: r['xlineamt'] / r['xqtyord'] if r['xqtyord'] else 0,
    axis=1
)

# Sort to get latest previous sale
prev_sales = prev_sales.sort_values(['xitem', 'xdate'])

# Extract:
# previous_sales_rate_gulshan
# previous_sales_date_gulshan
prev_sales_last = prev_sales.groupby('xitem').tail(1).rename(columns={
    'rate': 'previous_sales_rate_gulshan',
    'xdate': 'previous_sales_date_gulshan'
})


# ================= FINAL MERGE ================= #

# Base = sales items (only items sold in month)
df = sales_summary.copy()

# Add purchase columns:
# prev_po_central_date, prev_qty, prev_rate
# current_po_central_date, current_po_qty, current_po_rate
df = df.merge(
    current_po[['xitem', 'current_po_central_date', 'current_po_qty', 'current_po_rate']],
    on='xitem', how='left'
)

df = df.merge(prev_po, on='xitem', how='left')

# Add stock columns:
# central_stock, gulshan_stock
df = df.merge(
    stock_central.rename(columns={'stock': 'central_stock'}),
    on='xitem', how='left'
)

df = df.merge(
    stock_fixit.rename(columns={'stock': 'gulshan_stock'}),
    on='xitem', how='left'
)

# Add previous sales columns
df = df.merge(
    prev_sales_last[['xitem', 'previous_sales_rate_gulshan', 'previous_sales_date_gulshan']],
    on='xitem', how='left'
)


# ================= CLEANING ================= #

# Fill numeric nulls (important for Excel accuracy)
df.fillna({
    'prev_po_qty_central': 0,
    'prev_po_rate_central': 0,
    'previous_sales_rate_gulshan': 0,
    'central_stock': 0,
    'gulshan_stock': 0
}, inplace=True)

# Rename final output columns
df.rename(columns={
    'xitem': 'gulshan_item_code',
    'xdesc': 'item_name'
}, inplace=True)


# ================= FINAL COLUMN ORDER ================= #

df = df[[
    "gulshan_item_code",
    "item_name",

    "current_month_total_sale_qty_gulshan",
    "current_sales_rate_gulshan",
    "current_sales_date",

    "previous_sales_rate_gulshan",
    "previous_sales_date_gulshan",

    "current_po_central_date",
    "current_po_qty",
    "current_po_rate",

    "prev_po_central_date",
    "prev_po_qty_central",
    "prev_po_rate_central",

    "central_stock",
    "gulshan_stock"
]]


# ================= EXPORT ================= #

file_name = 'result_df.xlsx'
df.to_excel(file_name, index=False)


# ================= EMAIL ================= #

try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
except:
    recipients = ['ithmbrbd@gmail.com']

subject = f"Fixit-13 Purchase Check With Inventory – {NOW_STR}"
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