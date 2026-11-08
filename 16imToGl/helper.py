from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import sys

# Add the root directory (where dcon.py is located) to sys.path
sys.path.append(r"D:\1Pyreports")

from dcon import GROCERY_ID, LOCAL_CONNECTION_PSYCOP, HMBR_CONNECTION_PSYCOP, get_data


# Settings
ZID = GROCERY_ID
GLOBAL_ENVIRONMENT = "global"
LOCAL_ENVIRONMENT = "local"

# ? Set the environment Important
ENVIRONMENT = GLOBAL_ENVIRONMENT  # change only this to swithc local or gloabal

local_global = ""
# Database connection based on the environment
if ENVIRONMENT == GLOBAL_ENVIRONMENT:
    engine = create_engine("postgresql://postgres:postgres@69.162.102.58:5432/da")
    psycop_connection = HMBR_CONNECTION_PSYCOP  # Ensure this is defined
    local_global = GLOBAL_ENVIRONMENT
else:
    engine = create_engine("postgresql://postgres:postgres@localhost:5432/da")
    psycop_connection = LOCAL_CONNECTION_PSYCOP  # Ensure this is defined
    local_global = LOCAL_ENVIRONMENT


# def get_data (query , fetchOneOrAll = 'all' , df = True, conn = "local"):


# Get last PO and next PO number
last_po_number_query = f"""
                            SELECT xpornum from poord where zid = {ZID} order by xpornum desc limit 1
                        """
get_last_po_number = get_data(
    last_po_number_query, fetchOneOrAll="one", df=False, conn=local_global
)
get_last_po_number = get_last_po_number[0]
prefix, po_number = get_last_po_number.split("--")
next_po_number = f"{int(po_number) + 1:06d}"
next_po_number = f"{prefix}--{next_po_number}"


# Get last grn and next grn number
last_grn_number_query = f"""
                            SELECT xgrnnum from pogrn where zid = {ZID} order by xgrnnum desc limit 1
                        """

get_last_grn_number = get_data(
    last_grn_number_query, fetchOneOrAll="one", df=False, conn=local_global
)
get_last_grn_number = get_last_grn_number[0]
prefix, grn_number = get_last_grn_number.split("-")
next_grn_number = f"{int(grn_number) + 1:06d}"
next_grn_number = f"{prefix}-{next_grn_number}"
print(next_grn_number)


# Gen Last and next Imtrn ximtrnnum  RE--
last_imtrn_re_number = f"""
                            SELECT ximtrnnum from imtrn where zid = {ZID} and ximtrnnum like '%%RE--%%' order by ximtrnnum desc limit 1
                        """
get_last_re_number = get_data(
    last_imtrn_re_number, fetchOneOrAll="one", df=False, conn=local_global
)
get_last_re_number = get_last_re_number[0]
prefix, re_number = get_last_re_number.split("--")
next_re_number = f"{int(re_number) + 1:06d}"
next_re_number = f"{prefix}--{next_re_number}"

# Gen Last and next xsinnum from pogrn table--
last_xsinnum_query = f"""
                            SELECT xsinnum from pogrn where zid = {ZID}   order by xgrnnum desc limit 1
                        """
last_xsinnum = get_data(
    last_xsinnum_query, fetchOneOrAll="one", df=False, conn=local_global
)
last_xsinnum = str(last_xsinnum[0])
next_xsinnum = f"{int(last_xsinnum) + 1:06d}"


# Generate Last SINV NUMBER. At first we need to get the month and year from the current date like 09 month and 24 year.
# Then concat this like 0924 with SINV-
# After that search in database table glheader
#       (query will be- select xvoucher from glheader where zid = 100006 order by xvoucher desc limit 1)

#  Lets Say it found if found the xvoucher SINV0924-000025
# then we need to add 1 to it get the last SINV number and it will be SINV0924-000026

# if SINV0924 like not found in xvoucher then create the SINV Number SINV0924-000001


def generate_next_voucher_number(zid):
    # Step 1: Get current month and year in MMYY format
    now = datetime.now()
    month = now.strftime("%m")  # Get month (e.g., '10' for October)
    year = now.strftime("%y")  # Get last two digits of year (e.g., '24' for 2024)

    # Step 2: Create voucher prefix
    voucher_prefix = f"SINV{month}{year}-"  # e.g., 'SINV1024-'

    # Step 3: Query the database to get the last voucher number with this prefix
    last_invoice_number_query = f"""
        SELECT xvoucher 
        FROM glheader 
        WHERE zid = {zid} 
        AND xvoucher LIKE '{voucher_prefix}%' 
        ORDER BY xvoucher DESC 
        LIMIT 1
    """

    # Execute query (assuming get_data is a function that interacts with your database)
    last_voucher = get_data(
        last_invoice_number_query, fetchOneOrAll="one", df=False, conn=local_global
    )

    # Step 4: Generate the next voucher number
    if last_voucher:
        # If last_voucher is a tuple, extract the first element
        last_voucher_str = (
            last_voucher[0] if isinstance(last_voucher, tuple) else last_voucher
        )

        # Extract the numeric part of the last voucher (e.g., '000024' from 'SINV1024-000024')
        last_voucher_number = int(
            last_voucher_str.split("-")[-1]
        )  # Split to get the numeric part

        # Increment the numeric part
        next_voucher_number = last_voucher_number + 1

        # Format the next voucher with leading zeros (e.g., '000025')
        next_voucher = f"{voucher_prefix}{next_voucher_number:06d}"
    else:
        # If no voucher found, start from 000001
        next_voucher = f"{voucher_prefix}000001"

    return next_voucher


next_voucher = generate_next_voucher_number(ZID)
print(next_voucher)

print(next_xsinnum)
