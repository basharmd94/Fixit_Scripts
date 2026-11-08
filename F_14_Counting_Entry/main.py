
"""
Inventory Check Against Purchase Report Generator.

This module provides functionality to check and compare inventory movements
against purchase orders, including GRN (Goods Received Note) verification.
"""

from datetime import datetime, timedelta, date
from typing import Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine

import os
import sys
import warnings


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# === 2. Import shared modules ===
from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL

# === 3. Configuration ===

engine = create_engine(DATABASE_URL)

# === 5. Suppress warnings ===

warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
pd.set_option('display.float_format', '{:.2f}'.format)

# Date constants
NOW = datetime.now() 
NOW_STR = NOW.strftime('%Y-%m-%d')

# Example of date constants
# NOW = datetime(2025, 9, 21)
# NOW_STR = '2025-09-21'
YESTERDAY = NOW - timedelta(1)
YESTERDAY_STR = YESTERDAY.strftime('%Y-%m-%d')


def format_tuple_for_sql(items: tuple) -> str:
    """Convert Python tuple to SQL-compatible string format."""
    return str(tuple(items)).replace(",)", ")")

def get_grn_details(zid: int, today_date: str) -> pd.DataFrame:
    """
    Fetch GRN (Goods Received Note) details for a specific date.

    Args:
        zid: Zone ID for the query
        today_date: Date to fetch GRN details for

    Returns:
        DataFrame containing GRN details
    """
    query = """
        SELECT 
            pogrn.xdate,
            pogrn.xgrnnum,
            pogdt.xitem,
            caitem.xdesc,
            pogdt.xqty as today_grn_qty
        FROM pogrn
        JOIN pogdt ON pogrn.xgrnnum = pogdt.xgrnnum
        JOIN caitem ON pogdt.xitem = caitem.xitem
        WHERE pogrn.zid = %s
            AND pogdt.zid = %s
            AND caitem.zid = %s
            AND pogrn.xdate = '%s'
    """
    return pd.read_sql(query % (zid, zid, zid, today_date), con=engine)





def get_inventory_stock(
    zid: int,
    execute_date: str,
    item_list: Optional[Tuple[str, ...]] = None
) -> pd.DataFrame:
    """
    Get inventory stock levels for specified items up to a given date.

    Args:
        zid: Zone ID for the query
        execute_date: Date up to which to check stock
        item_list: Tuple of item codes to check

    Returns:
        DataFrame containing stock levels
    """
    items_sql = format_tuple_for_sql(item_list)
    query = """
        SELECT
            xitem,
            sum(xqty * xsign) as stock
        FROM imtrn
        WHERE zid = %s
            AND xdate <= '%s'
            AND xitem in %s
        GROUP BY xitem
    """
    return pd.read_sql(query % (zid, execute_date, items_sql), con=engine)



def get_today_sales(
    zid: int,
    today_date: str,
    item_list: Optional[Tuple[str, ...]] = None
) -> pd.DataFrame:
    """
    Get sales details for specified items for a given date.

    Args:
        zid: Zone ID for the query
        today_date: Date to fetch sales for
        item_list: Tuple of item codes to check

    Returns:
        DataFrame containing sales details
    """
    items_sql = format_tuple_for_sql(item_list)
    query = """
        SELECT
            opodt.xitem,
            SUM(opodt.xqtyord) as today_sales_qty
        FROM opord
        JOIN opodt ON opord.xordernum = opodt.xordernum
        WHERE opord.zid = %s
            AND opodt.zid = %s
            AND opord.xdate = '%s'
            AND opodt.xitem in %s
        GROUP BY opodt.xitem
    """
    return pd.read_sql(query % (zid, zid, today_date, items_sql), con=engine)

def get_inventory_check_grn(
    zid: int,
    grn_numbers: Optional[Tuple[str, ...]] = None
) -> pd.DataFrame:
    """
    Check if GRN items are reflected in inventory.

    Args:
        zid: Zone ID for the query
        grn_numbers: Tuple of GRN numbers to check

    Returns:
        DataFrame containing inventory entries for GRNs
    """
    grn_list = format_tuple_for_sql(grn_numbers)
    query = """
        SELECT
            xdocnum,
            xitem,
            xqty as item_inserted_inv
        FROM imtrn
        WHERE zid = %s
            AND xdocnum in %s
    """
    return pd.read_sql(query % (zid, grn_list), con=engine)



# Main execution
ZID = 100001
CHECK_DATE = NOW_STR  # Current date, e.g., '2025-09-21'

# Get purchase/GRN details
df_purchase = get_grn_details(ZID, CHECK_DATE)

# If no purchases, exit
if df_purchase.empty:
    subject = f"Fixit-13 Purchase Check With Inventory – {NOW_STR}"
    body_text = "No purchases found for the given date."
    send_mail(
        subject=subject,
        bodyText=body_text,
        html_body=[],
        attachment=[],
        recipient=['ithmbrbd@gmail.com','fixitc.central@gmail.com','hmbrfixit@gmail.com']
    )
    print("No purchases found for the given date.")
    
    engine.dispose()
    print("✅ Script completed successfully.")

    sys.exit()

# Get list of items from purchases
grn_items_list = tuple(set(df_purchase['xitem'].to_list()))

# Get yesterday's and today's stock
prev_date = (datetime.strptime(CHECK_DATE, '%Y-%m-%d') - timedelta(1)).strftime('%Y-%m-%d')

df_stock_yesterday = get_inventory_stock(ZID, prev_date, grn_items_list)
df_stock_yesterday = df_stock_yesterday.rename(
    columns={'stock': f'yesterday_stock_closing-{prev_date[-5:]}'}
)

df_stock_today = get_inventory_stock(ZID, CHECK_DATE, grn_items_list)
df_stock_today = df_stock_today.rename(
    columns={'stock': f'today_stock_closing-{CHECK_DATE[-5:]}'}
)

# Merge stock data
df_balance = pd.merge(
    df_stock_yesterday,
    df_stock_today,
    on=['xitem'],
    how='outer'
).fillna(0)

# Get sales data
df_sales = get_today_sales(ZID, CHECK_DATE, grn_items_list)

# Merge purchase and sales data
df_purchase_and_sales = pd.merge(
    df_purchase,
    df_sales,
    on=['xitem'],
    how='left'
).fillna(0)

# Merge with stock balance
df_purchase_check_stock_balance = pd.merge(
    df_purchase_and_sales,
    df_balance,
    on=['xitem'],
    how='left'
)

# Check GRN entries in inventory
grn_numbers = tuple(df_purchase_check_stock_balance['xgrnnum'].dropna().unique())
check_grn_on_imtrn = get_inventory_check_grn(ZID, grn_numbers)
check_grn_on_imtrn = check_grn_on_imtrn.rename(columns={'xdocnum': 'xgrnnum'})

# Final merge and column organization
result_df = pd.merge(
    df_purchase_check_stock_balance,
    check_grn_on_imtrn,
    on=['xgrnnum', 'xitem'],
    how='left'
)

# Organize columns in logical order
result_df = result_df[[
    'xdate',
    'xgrnnum',
    'xitem',
    'xdesc',
    'today_grn_qty',
    'item_inserted_inv',
    'today_sales_qty',
    f'yesterday_stock_closing-{prev_date[-5:]}',
    f'today_stock_closing-{CHECK_DATE[-5:]}'
]]


# Email embeed dataframe grnnum, xitem, xdesc, today_grn_qty and item_inserted_inv and today_sales_qty

df_email = result_df[['xgrnnum', 'xitem', 'xdesc', 'today_grn_qty', 'item_inserted_inv', 'today_sales_qty']]

html_body = [(df_email, 'Purchase Check With Inventory')]

# export result_df to excel
result_df.to_excel('result_df.xlsx', index=False)

print("✅ Report generated successfully.")

print("\n==[ EMAIL ]============================================")
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"[OK] Recipients: {recipients}")
except Exception as e:
    print(f"[WARN] Recipient lookup failed: {e} -> using fallback")
    recipients = ['ithmbrbd@gmail.com']

subject = f"Fixit-13 Purchase Check With Inventory – {NOW_STR}"
body_text = "Please find today's Purchase Check With Inventory report below."


send_mail(
    subject=subject,
    bodyText=body_text,
    html_body=html_body,
    attachment=['result_df.xlsx'],
    recipient=recipients
)

print("📧 Email sent successfully.")
engine.dispose()
