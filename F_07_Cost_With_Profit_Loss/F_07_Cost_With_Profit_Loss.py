"""
🚀 F_07_Cost_With_Profit_Loss.py – Item-wise Profit & Loss (Gulshan, Ecommerce, Central)

📌 PURPOSE:
    - Compute sales, returns, COGS by item per business
    - Summarize gross profit and GL income/expenditure
    - Export Excel with detail sheets and email summary

🔧 DATA SOURCES:
    - ERP: imtrn, opodt/opord (sales), imtdt/imtor (central), imtemptdt/imtemptrn (returns), glmst/gldetail/glheader
    - Database: PostgreSQL via DATABASE_URL in project_config.py

📧 EMAIL:
    - Recipients: get_email_recipients("F_07_Cost_With_Profit_Loss")
    - Fallback: ithmbrbd@gmail.com
"""

import os
import sys
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
import openpyxl
from sqlalchemy import create_engine

# === 1. Add project root to Python path ===
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# === 2. Import shared modules ===
from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL

print("\n==[ START ]============================================")
engine = create_engine(DATABASE_URL)
print("[OK] Database engine initialized")

# === 3. Inlined DB functions (from profit_func.py) ===

def get_sales_COGS(zid, start_date, end_date):
    query = """
        SELECT caitem.zid, caitem.xitem, caitem.xdesc, caitem.xgitem,
               (imtrn.xqty*imtrn.xsign) as qty, (imtrn.xval*imtrn.xsign) as totalvalue,
               opodt.xqtyreq, opodt.xrate, opodt.xlineamt,
               (opodt.xdtwotax - opodt.xdtdisc) as xdtwotax
        FROM caitem
        JOIN imtrn ON caitem.xitem = imtrn.xitem
        JOIN opodt ON imtrn.xdocnum = opodt.xordernum AND imtrn.xitem = opodt.xitem
        JOIN opord ON imtrn.xdocnum = opord.xordernum
        WHERE caitem.zid = %(zid)s AND imtrn.zid = %(zid)s AND opodt.zid = %(zid)s AND opord.zid = %(zid)s
          AND imtrn.xdocnum like %(doc)s AND imtrn.xdate >= %(start)s AND imtrn.xdate <= %(end)s
    """
    return pd.read_sql(query, con=engine, params={'zid': zid, 'doc': '%CO--%', 'start': start_date, 'end': end_date})


def get_sales_COGS_central(zid, start_date, end_date):
    query = """
        SELECT caitem.zid, caitem.xitem, caitem.xdesc, caitem.xgitem,
               (imtrn.xqty*imtrn.xsign) as qty, (imtrn.xval*imtrn.xsign) as totalvalue,
               imtdt.xqtyord, imtdt.xrate, imtdt.xdtwotax
        FROM caitem
        JOIN imtrn ON caitem.xitem = imtrn.xitem
        JOIN imtdt ON imtrn.xdocnum = imtdt.ximtor AND imtrn.xitem = imtdt.xitem
        JOIN imtor ON imtrn.xdocnum = imtor.ximtor
        WHERE caitem.zid = %(zid)s AND imtrn.zid = %(zid)s AND imtdt.zid = %(zid)s AND imtor.zid = %(zid)s
          AND imtrn.xdocnum like %(doc)s AND imtrn.xdate >= %(start)s AND imtrn.xdate <= %(end)s
    """
    return pd.read_sql(query, con=engine, params={'zid': zid, 'doc': '%TO--%', 'start': start_date, 'end': end_date})


def get_return(zid, start_date, end_date):
    query = """
        SELECT imtrn.xitem, imtrn.xdocnum, imtrn.xqty, imtemptdt.xrate,
               (imtemptdt.xrate*imtrn.xqty) as totamt
        FROM imtrn
        JOIN imtemptdt ON imtrn.xdocnum = imtemptdt.ximtrnnum AND imtrn.xitem = imtemptdt.xitem
        JOIN imtemptrn ON imtrn.xdocnum = imtemptrn.ximtmptrn
        WHERE imtrn.zid = %(zid)s AND imtemptdt.zid = %(zid)s AND imtemptrn.zid = %(zid)s
          AND imtrn.xdocnum like %(doc)s AND imtrn.xdate >= %(start)s AND imtrn.xdate <= %(end)s
    """
    return pd.read_sql(query, con=engine, params={'zid': zid, 'doc': '%SRE-%', 'start': start_date, 'end': end_date})


def get_gl_details(zid, COGS, start_date, end_date):
    query = """
        SELECT glmst.xacctype, SUM(gldetail.xprime)
        FROM glmst
        JOIN gldetail ON glmst.xacc = gldetail.xacc
        JOIN glheader ON gldetail.xvoucher = glheader.xvoucher
        WHERE glmst.zid = %(zid)s AND gldetail.zid = %(zid)s AND glheader.zid = %(zid)s
          AND (glmst.xacctype = 'Income' OR glmst.xacctype = 'Expenditure')
          AND glmst.xacc != %(cogs)s AND glheader.xdate >= %(start)s AND glheader.xdate <= %(end)s
        GROUP BY glmst.xacctype
    """
    return pd.read_sql(query, con=engine, params={'zid': zid, 'cogs': COGS, 'start': start_date, 'end': end_date})

# === 4. Inputs and containers ===
zid_list = [100001, 100003, 100002]
COGS = ['04010020', '4010020', '4010020']
business = ['fixit', 'ecommerce', 'central']
end_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=33)).strftime('%Y-%m-%d')
main_data_dict = {}
df_all_business = []

print("\n==[ COMPUTE ]==========================================")
for i in range(len(zid_list)):
    zid = zid_list[i]
    if zid == 100002:
        df_sales_all = get_sales_COGS_central(100002, start_date, end_date)
        df_sales_all['xlineamt'] = df_sales_all['xdtwotax'] * 1
    else:
        df_sales_all = get_sales_COGS(zid, start_date, end_date)
    df_sales_all = df_sales_all.groupby(['xitem', 'xdesc', 'xgitem'])[['totalvalue', 'xlineamt']].sum().reset_index().round(1)

    df_return_all = get_return(zid, start_date, end_date)
    df_return_all = df_return_all.groupby(['xitem'])['totamt'].sum().reset_index().round(1)

    df_final_all = df_sales_all.merge(df_return_all[['xitem', 'totamt']], on=['xitem'], how='left').fillna(0)
    df_final_all['final_sales'] = df_final_all['xlineamt'] - df_final_all['totamt']
    df_final_all = df_final_all.drop(['xlineamt', 'totamt'], axis=1)
    df_final_all['Gross_Profit'] = df_final_all['final_sales'] + df_final_all['totalvalue']
    df_final_all = df_final_all.sort_values(by=['Gross_Profit']).reset_index(drop=True)
    df_final_all.loc[len(df_final_all.index), :] = df_final_all.sum(axis=0, numeric_only=True)
    df_final_all.at[len(df_final_all.index) - 1, 'xdesc'] = 'Total_Item_Profit'

    df_pl_all = get_gl_details(zid, COGS[i], start_date, end_date)
    summary_all = df_final_all.tail(1).drop('xitem', axis=1)
    summary_all = summary_all.to_dict('records')
    df_pl_all_list = df_pl_all.to_dict('records')

    exception_dicts = [{'income': 0, 'sum': 0}, {'expenditure': 0, 'sum': 0}]
    df_pl_all_list = exception_dicts if len(df_pl_all_list) <= 0 else df_pl_all_list

    summary_all[0]['Income_gl'] = df_pl_all_list[0].get('sum', 0)
    try:
        summary_all[0]['Expenditure_gl'] = df_pl_all_list[1].get('sum', 0)
    except Exception:
        summary_all[0]['Expenditure_gl'] = 0

    main_data_dict[zid] = summary_all[0]
    main_data_dict[zid]['Net'] = main_data_dict[zid]['Gross_Profit'] - main_data_dict[zid]['Expenditure_gl']
    df_pl_all_df = pd.DataFrame(df_pl_all_list)
    df_final_all = pd.concat([df_final_all, df_pl_all_df], axis=1)

    main_data_dict[business[i]] = main_data_dict.pop(zid)
    df_all_business.append(df_final_all)

print("[OK] Computation finished for all businesses")

# Assign business dfs
gulshan_final_df = df_all_business[0]
ecom_final_df = df_all_business[1]
central_final_df = df_all_business[2]

# Cleanup keys
for key, value in list(main_data_dict.items()):
     main_data_dict[key].pop('xdesc', None)

# Profit ratio per item group
item_group_sale = []
for i in range(len(df_all_business)):
    df_all_business[i] = df_all_business[i].iloc[1:-1, : -2]
    df_all_business[i] = df_all_business[i].groupby(['xgitem']).sum().reset_index().sort_values(by=['Gross_Profit']).reset_index(drop=True)
    df_all_business[i]['profit_ratio'] = df_all_business[i]['Gross_Profit'] / df_all_business[i]['totalvalue'] * 100 * (-1)
    df_all_business[i]['profit_ratio'] = df_all_business[i]['profit_ratio'].round(2)
    item_group_sale.append(df_all_business[i])

# Overall summary
df_overall = pd.DataFrame.from_dict(main_data_dict).reset_index().round(1)

# === 5. Export Excel ===
print("\n==[ EXPORT ]===========================================")
excel_file = 'F_07_ItemWiseProfit_fixit.xlsx'
with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
    gulshan_final_df.to_excel(writer, 'Gulshan', index=False)
    item_group_sale[0].to_excel(writer, 'Gulshan_sale_xgitem', index=False)
    ecom_final_df.to_excel(writer, 'Ecommerce', index=False)
    item_group_sale[1].to_excel(writer, 'Ecomm_sale_xgitem', index=False)
    central_final_df.to_excel(writer, 'Central', index=False)
    item_group_sale[2].to_excel(writer, 'Central_sale_xgitem', index=False)

wb = openpyxl.load_workbook(excel_file)
for sheet in wb:
    if sheet.title in ('Gulshan_sale_xgitem', 'Ecomm_sale_xgitem', 'Central_sale_xgitem'):
        sheet.column_dimensions['B'].width = 35
    else:
        sheet.column_dimensions['C'].width = 35
        sheet.column_dimensions['D'].width = 35
wb.save(excel_file)
print(f"[OK] Excel written -> {excel_file}")

# === 6. Email ===
print("\n==[ EMAIL ]============================================")
try:
    recipients = get_email_recipients("F_07_Cost_With_Profit_Loss")
except Exception as e:
    print(f"[WARN] Recipient lookup failed: {e} -> using fallback")
    recipients = ['ithmbrbd@gmail.com']

body_text = f"Fixit-07 Fixit Item Wise Profit and Loss from [ {start_date} to {end_date} ]"
send_mail(
    subject=f"Fixit-07 Fixit Item Wise Profit and Loss from [ {start_date} to {end_date} ] ",
    bodyText=body_text,
    attachment=[excel_file],
    recipient=recipients,
    html_body=[(df_overall, 'All Business Overall Sales and Accounts'),
              (item_group_sale[0], 'Gulshan Profit Ratio Item Group Wise'),
              (item_group_sale[1], 'Ecommerce Profit Ratio Item Group Wise'),
              (item_group_sale[2], 'Central Profit Ratio Item Group Wise')]
)

engine.dispose()
print("✅ Script completed successfully.")