"""
🚀 F_03_Fixit_Cash_Flow.py – Monthly Cash Flow, P&L, Payables, Receivables, Inventory

📌 PURPOSE:
    - Generate monthly Cash Flow, Profit/Loss, Accounts Payable, Accounts Receivable, and Inventory summaries
    - Export to Excel workbooks
    - Email results to configured recipients

🔧 DATA SOURCES:
    - GL: glmst, gldetail, glheader
    - Master: cacus, casup, caitem
    - Inventory: imtrn
    - Database: PostgreSQL via DATABASE_URL in project_config.py

📅 SCHEDULE:
    Runs daily or on-demand

🏢 INPUT:
    - Projects and ZIDs defined in script mapping

📧 EMAIL:
    - Recipients: get_email_recipients("F_03_Fixit_Cash_Flow")
    - Fallback: ithmbrbd@gmail.com

💡 NOTE:
    - Uses parameterized queries (except IN clause composed safely)
    - Exports .xlsx with openpyxl engine
    - Single shared DB engine
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine

# === 1. Add project root to Python path ===
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# === 2. Import shared modules ===
from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL

# === 3. Create database engine ===
engine = create_engine(DATABASE_URL)

# === 4. Pandas settings ===
pd.options.mode.chained_assignment = None
pd.set_option('display.float_format', '{:.2f}'.format)

# === 5. Date ranges ===
TODAY_DATE = date.today().strftime('%Y-%m-%d')
now = datetime.now()
last_month_ref = now - timedelta(days=3)
end_date = last_month_ref.strftime('%Y-%m-%d')
start_date = (last_month_ref - timedelta(days=720)).strftime('%Y-%m-%d')

# === 6. Project/ZID mapping ===
project_zid = {
    'Fix iT Central': [100002, '1020001,1010001,10010002'],
    'HMBR FIXIT GULSHAN': [100001, '01020001, 01010001'],
    'Fix iT Ecommerce.': [100003, '1020001,1010001'],
}

# === 7. SQL helpers (parameterized; IN clause composed safely) ===
def get_cashflow_details(zid: int, project: str, acc_csv: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    acc_tuple = tuple([a.strip() for a in acc_csv.split(',') if a.strip()])
    acc_in = '(' + ','.join([f"'{a}'" for a in acc_tuple]) + ')'
    query = f"""
        SELECT glmst.zid, glmst.xacc, glheader.xper, glheader.xyear, SUM(gldetail.xprime)
        FROM glmst
        JOIN gldetail ON glmst.xacc = gldetail.xacc
        JOIN glheader ON gldetail.xvoucher = glheader.xvoucher
        WHERE glmst.zid = %(zid)s
          AND gldetail.zid = %(zid)s
          AND glheader.zid = %(zid)s
          AND gldetail.xvoucher IN (
              SELECT gldetail.xvoucher
              FROM gldetail
              JOIN glheader ON glheader.xvoucher = gldetail.xvoucher
              JOIN glmst   ON glmst.xacc     = gldetail.xacc
              WHERE gldetail.zid = %(zid)s
                AND glheader.zid = %(zid)s
                AND glmst.zid    = %(zid)s
                AND gldetail.xproj = %(project)s
                AND gldetail.xacc IN {acc_in}
                AND glheader.xper <> 0
                AND glheader.xdate >= %(start)s
                AND glheader.xdate <= %(end)s
          )
          AND gldetail.xproj = %(project)s
          AND glheader.xper <> 0
          AND glheader.xdate >= %(start)s
          AND glheader.xdate <= %(end)s
        GROUP BY glmst.zid, glmst.xacc, glheader.xper, glheader.xyear
    """
    return pd.read_sql(query, con=engine, params={'zid': zid, 'project': project, 'start': start_date_str, 'end': end_date_str})


def get_gl_details_project(zid: int, project: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    query = """
        SELECT glmst.zid, glmst.xacc, glmst.xacctype, glheader.xyear, glheader.xper, SUM(gldetail.xprime)
        FROM glmst
        JOIN gldetail ON glmst.xacc = gldetail.xacc
        JOIN glheader ON gldetail.xvoucher = glheader.xvoucher
        WHERE glmst.zid = %(zid)s
          AND gldetail.zid = %(zid)s
          AND glheader.zid = %(zid)s
          AND gldetail.xproj = %(project)s
          AND (glmst.xacctype = 'Income' OR glmst.xacctype = 'Expenditure')
          AND glheader.xdate >= %(start)s
          AND glheader.xdate <= %(end)s
        GROUP BY glmst.zid, glmst.xacc, glheader.xyear, glheader.xper
        ORDER BY glheader.xper ASC , glmst.xacctype
    """
    return pd.read_sql(query, con=engine, params={'zid': zid, 'project': project, 'start': start_date_str, 'end': end_date_str})


def get_acc_payable(zid: int, project: str, end_date_str: str) -> pd.DataFrame:
    query = """
        SELECT gldetail.xsub, glheader.xyear, glheader.xper, casup.xshort, SUM(gldetail.xprime) AS ap
        FROM glheader
        JOIN gldetail ON glheader.xvoucher = gldetail.xvoucher
        JOIN casup    ON gldetail.xsub     = casup.xsup
        WHERE glheader.zid = %(zid)s
          AND gldetail.zid = %(zid)s
          AND casup.zid    = %(zid)s
          AND gldetail.xproj = %(project)s
          AND gldetail.xvoucher NOT LIKE 'OB--%%'
          AND glheader.xdate <= %(end)s
        GROUP BY gldetail.xsub, glheader.xyear, glheader.xper, casup.xshort
    """
    return pd.read_sql(query, con=engine, params={'zid': zid, 'project': project, 'end': end_date_str})


def get_acc_receivable(zid: int, project: str, end_date_str: str) -> pd.DataFrame:
    query = """
        SELECT gldetail.xsub, cacus.xshort, cacus.xadd2, cacus.xcity, cacus.xstate,
               glheader.xyear, glheader.xper, SUM(gldetail.xprime) AS ar
        FROM glheader
        JOIN gldetail ON glheader.xvoucher = gldetail.xvoucher
        JOIN cacus    ON gldetail.xsub     = cacus.xcus
        WHERE glheader.zid = %(zid)s
          AND gldetail.zid = %(zid)s
          AND cacus.zid    = %(zid)s
          AND gldetail.xproj = %(project)s
          AND gldetail.xvoucher NOT LIKE 'OB--%%'
          AND glheader.xdate <= %(end)s
        GROUP BY gldetail.xsub, cacus.xshort, cacus.xadd2, cacus.xcity, cacus.xstate,
                 glheader.xyear, glheader.xper
    """
    return pd.read_sql(query, con=engine, params={'zid': zid, 'project': project, 'end': end_date_str})


def get_imtrn_value(zid: int, end_date_str: str) -> pd.DataFrame:
    query = """
        SELECT caitem.zid, caitem.xitem, caitem.xdesc, caitem.xgitem,
               imtrn.xyear, imtrn.xper,
               (imtrn.xqty * imtrn.xsign) AS qty,
               (imtrn.xval * imtrn.xsign) AS totalvalue
        FROM caitem
        JOIN imtrn ON caitem.xitem = imtrn.xitem
        WHERE caitem.zid = %(zid)s
          AND imtrn.zid  = %(zid)s
          AND imtrn.xdate <= %(end)s
    """
    return pd.read_sql(query, con=engine, params={'zid': zid, 'end': end_date_str})


def get_gl_master(zid: int) -> pd.DataFrame:
    query = "SELECT zid, xacc, xdesc, xhrc3, xacctype FROM glmst WHERE glmst.zid = %(zid)s"
    return pd.read_sql(query, con=engine, params={'zid': zid})

# === 8. Build reports ===
main_data_dict_pl: dict[int, pd.DataFrame] = {}
main_data_dict_cf: dict[int, pd.DataFrame] = {}
main_data_dict_pay: dict[int, pd.DataFrame] = {}
main_data_dict_inv: dict[int, pd.DataFrame] = {}
main_data_dict_rcv: dict[int, pd.DataFrame] = {}

for project_name, (zid, acc_csv) in project_zid.items():
    # Master
    df_master = get_gl_master(zid)

    # P&L
    df_pl = get_gl_details_project(zid, project_name, start_date, end_date)
    df_pl['time_line'] = df_pl['xyear'].astype(str) + '/' + df_pl['xper'].astype(str)
    df_pl_pvt = pd.pivot_table(df_pl, values='sum', index=['zid', 'xacc'], columns=['time_line'], aggfunc=np.sum).reset_index().fillna(0)
    df_pl_pvt.loc[len(df_pl_pvt.index), :] = df_pl_pvt.sum(axis=0, numeric_only=True)
    df_pl_pvt.at[len(df_pl_pvt.index) - 1, 'xacc'] = 'Profit/Loss'
    df_pl_pvt = df_master.merge(df_pl_pvt, on=['xacc'], how='right').fillna(0)
    df_pl_pvt = df_pl_pvt[(df_pl_pvt['xacctype'] != 'Asset') & (df_pl_pvt['xacctype'] != 'Liability')]
    df_pl_pvt = df_pl_pvt.drop(columns=['zid_x', 'zid_y'], errors='ignore')
    main_data_dict_pl[zid] = df_pl_pvt

    # Cash Flow
    df_cf = get_cashflow_details(zid, project_name, acc_csv, start_date, end_date)
    df_cf['time_line'] = df_cf['xyear'].astype(str) + '/' + df_cf['xper'].astype(str)
    df_cf_pvt = pd.pivot_table(df_cf, values='sum', index=['zid', 'xacc'], columns=['time_line'], aggfunc=np.sum).reset_index().fillna(0)
    try:
        df_cf_pvt = df_master.merge(df_cf_pvt, on=['xacc'], how='right').fillna(0).sort_values('xacctype')
    except Exception:
        continue
    df_cf_pvt = df_cf_pvt.drop(columns=['zid_x', 'zid_y'], errors='ignore')
    df_cf_pvt.at[df_cf_pvt['xdesc'] == 'Net Cash Flow', 'xhrc3'] = 'Total'
    df_cf_pvt = df_cf_pvt.sort_values('xhrc3')
    # Rollups
    df_cf_pvt.loc[len(df_cf_pvt.index), :] = df_cf_pvt.loc[(df_cf_pvt['xhrc3'] == 'Operating') | (df_cf_pvt['xhrc3'] == 'Operating Investment')].sum(axis=0, numeric_only=True)
    df_cf_pvt.at[len(df_cf_pvt.index) - 1, 'xhrc3'] = 'Operating Cash Flow'
    df_cf_pvt.loc[len(df_cf_pvt.index), :] = df_cf_pvt.loc[(df_cf_pvt['xhrc3'] == 'Investing')].sum(axis=0, numeric_only=True)
    df_cf_pvt.at[len(df_cf_pvt.index) - 1, 'xhrc3'] = 'Investing Cash Flow'
    df_cf_pvt.loc[len(df_cf_pvt.index), :] = df_cf_pvt.loc[(df_cf_pvt['xhrc3'] == 'Financing')].sum(axis=0, numeric_only=True)
    df_cf_pvt.at[len(df_cf_pvt.index) - 1, 'xhrc3'] = 'Financing Cash Flow'
    df_cf_pvt.loc[len(df_cf_pvt.index), :] = df_cf_pvt.loc[(df_cf_pvt['xhrc3'] == 'Operating Cash Flow') | (df_cf_pvt['xhrc3'] == 'Investing Cash Flow')].sum(axis=0, numeric_only=True)
    df_cf_pvt.at[len(df_cf_pvt.index) - 1, 'xhrc3'] = 'Free Cash Flow'
    # Reorder columns
    cols = df_cf_pvt.columns.to_list()
    fixed = [c for c in cols if '/' not in c]
    monthly = sorted([c for c in cols if '/' in c], key=len)
    fixed.extend(monthly)
    df_cf_pvt = df_cf_pvt[fixed]
    main_data_dict_cf[zid] = df_cf_pvt

    # Payables
    df_ap = get_acc_payable(zid, project_name, end_date)
    df_ap['time_line'] = df_ap['xyear'].astype(str) + '/' + df_ap['xper'].astype(str)
    df_ap_pvt = pd.pivot_table(df_ap, values='ap', index=['xsub', 'xshort'], columns=['time_line'], aggfunc=np.sum).reset_index().fillna(0)
    df_ap_pvt.loc[len(df_ap_pvt.index), :] = df_ap_pvt.sum(axis=0, numeric_only=True)
    df_ap_pvt.at[len(df_ap_pvt.index) - 1, 'xshort'] = 'Monthly Payable'
    main_data_dict_pay[zid] = df_ap_pvt

    # Inventory value
    df_inv = get_imtrn_value(zid, end_date)
    df_inv['time_line'] = df_inv['xyear'].astype(str) + '/' + df_inv['xper'].astype(str)
    df_inv_pvt = pd.pivot_table(df_inv, values='totalvalue', index=['xgitem'], columns=['time_line'], aggfunc=np.sum).reset_index().fillna(0)
    df_inv_pvt.loc[len(df_inv_pvt.index), :] = df_inv_pvt.sum(axis=0, numeric_only=True)
    df_inv_pvt.at[len(df_inv_pvt.index) - 1, 'xgitem'] = 'Monthly Inventory'
    main_data_dict_inv[zid] = df_inv_pvt

    # Receivables
    df_ar = get_acc_receivable(zid, project_name, end_date)
    df_ar['time_line'] = df_ar['xyear'].astype(str) + '/' + df_ar['xper'].astype(str)
    df_ar_pvt = pd.pivot_table(df_ar, values='ar', index=['xsub', 'xshort', 'xadd2', 'xcity', 'xstate'], columns=['time_line'], aggfunc=np.sum).reset_index().fillna(0)
    df_ar_pvt.loc[len(df_ar_pvt.index), :] = df_ar_pvt.sum(axis=0, numeric_only=True)
    df_ar_pvt.at[len(df_ar_pvt.index) - 1, 'xstate'] = 'Monthly Receivable'
    main_data_dict_rcv[zid] = df_ar_pvt

# === 9. Export to Excel (.xlsx) ===
cf_file = 'F_03_Fixit_Cash_Flow_CF.xlsx'
pl_file = 'F_03_Fixit_Cash_Flow_PL.xlsx'

with pd.ExcelWriter(cf_file, engine='openpyxl') as writer:
    for zid, df in main_data_dict_cf.items():
        sheet = f'cf_{zid}'
        df.to_excel(writer, sheet_name=sheet, index=False)

with pd.ExcelWriter(pl_file, engine='openpyxl') as writer:
    for zid, df in main_data_dict_pl.items():
        sheet = f'pl_{zid}'
        df.to_excel(writer, sheet_name=sheet, index=False)

print('✅ Reports generated:')
print(f'   📊 CF: {cf_file}')
print(f'   📊 PL: {pl_file}')

# === 10. Email ===
try:
    report_name = os.path.splitext(os.path.basename(__file__))[0]
    recipients = get_email_recipients(report_name)
    print(f'📬 Recipients: {recipients}')
except Exception as e:
    print(f'⚠️ Failed to get recipients: {e}. Using fallback.')
    recipients = ['ithmbrbd@gmail.com']

subject = f'Fixit-03 Cash Flow – {TODAY_DATE}'
body_text = 'Please find attached the latest Cash Flow and P&L reports.'

# Include a small summary table in email (optional): count of sheets
summary_df = pd.DataFrame({
    'Report': ['Cash Flow Sheets', 'P&L Sheets'],
    'Count': [len(main_data_dict_cf), len(main_data_dict_pl)]
})

send_mail(
    subject=subject,
    bodyText=body_text,
    html_body=[(summary_df, 'F_03 Summary')],
    attachment=[cf_file, pl_file],
    recipient=recipients
)

print('📧 Email sent successfully.')

# === 11. Cleanup ===
engine.dispose()
print('✅ Script completed successfully.')

