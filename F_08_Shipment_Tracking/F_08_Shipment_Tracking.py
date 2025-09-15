from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from openpyxl import load_workbook
from datetime import date,datetime,timedelta
import psycopg2
import time
import datetime
import os

# === 1. Add project root to Python path ===
import sys
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# === 2. Import shared modules ===
from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL

print("\n==[ START F_08 Shipment Tracking ]=====================")
engine = create_engine(DATABASE_URL)
print("[OK] Database engine initialized")

# Helpers to build IN clause placeholders
_def_empty = pd.DataFrame()

def _in_clause(values):
    if not isinstance(values, (list, tuple)) or len(values) == 0:
        return "()", []
    placeholders = ','.join(["%s"] * len(values))
    return f"({placeholders})", list(values)

# Parameterized query functions using shared engine

def get_caitem(zid):
    query = """
        SELECT xitem, xdesc, xgitem, xstdprice
        FROM caitem
        WHERE zid = %s
    """
    return pd.read_sql(query, con=engine, params=[zid])


def get_item_stock(zid, end_date, items):
    in_sql, in_params = _in_clause(items)
    if not in_params:
        return _def_empty
    query = f"""
        SELECT imtrn.xitem, SUM(imtrn.xqty*imtrn.xsign) as stock
        FROM imtrn
        WHERE imtrn.zid = %s
          AND imtrn.xdate <= %s
          AND imtrn.xitem IN {in_sql}
        GROUP BY imtrn.xitem
    """
    return pd.read_sql(query, con=engine, params=[zid, end_date, *in_params])


def get_purchase(zid, start_date):
    query = """
        SELECT poodt.xitem, poodt.xqtyord, poodt.xrate, pogrn.xgrnnum, pogrn.xdate
        FROM poord
        JOIN poodt ON poord.xpornum = poodt.xpornum
        JOIN pogrn ON poord.xpornum = pogrn.xpornum
        WHERE poord.zid = %s
          AND poodt.zid = %s
          AND pogrn.zid = %s
          AND poord.xpornum LIKE %s
          AND poord.xstatuspor = %s
          AND poord.xdate >= %s
    """
    return pd.read_sql(query, con=engine, params=[zid, zid, zid, 'IP--%', '5-Received', start_date])


def get_purchase_item(zid, items, start_date):
    in_sql, in_params = _in_clause(items)
    if not in_params:
        return _def_empty
    query = f"""
        SELECT poodt.xitem, SUM(poodt.xqtyord) as sum, AVG(poodt.xrate) as avg
        FROM poord
        JOIN poodt ON poord.xpornum = poodt.xpornum
        JOIN pogrn ON poord.xpornum = pogrn.xpornum
        WHERE poord.zid = %s
          AND poodt.zid = %s
          AND pogrn.zid = %s
          AND poodt.xitem IN {in_sql}
          AND poord.xdate >= %s
        GROUP BY poodt.xitem
    """
    return pd.read_sql(query, con=engine, params=[zid, zid, zid, *in_params, start_date])


def get_sales(zid, start_date, items):
    in_sql, in_params = _in_clause(items)
    if not in_params:
        return _def_empty
    query = f"""
        SELECT imtrn.xitem, sum(imtrn.xqty*imtrn.xsign) as sales
        FROM imtrn
        WHERE imtrn.zid = %s
          AND imtrn.xdocnum LIKE %s
          AND imtrn.xdate >= %s
          AND imtrn.xitem IN {in_sql}
        GROUP BY imtrn.xitem
    """
    return pd.read_sql(query, con=engine, params=[zid, 'CO--%', start_date, *in_params])


def get_transfer_product(zid, items, start_date):
    in_sql, in_params = _in_clause(items)
    if not in_params:
        return _def_empty
    query = f"""
        SELECT xitem, SUM(xqty*xsign) as transfer
        FROM imtrn
        WHERE zid = %s
          AND xitem IN {in_sql}
          AND xdate >= %s
          AND xdocnum LIKE %s
        GROUP BY xitem
    """
    return pd.read_sql(query, con=engine, params=[zid, *in_params, start_date, 'TO--%'])


def get_return_product(zid, items, start_date):
    in_sql, in_params = _in_clause(items)
    if not in_params:
        return _def_empty
    query = f"""
        SELECT xitem, SUM(xqty*xsign) as return
        FROM imtrn
        WHERE zid = %s
          AND xitem IN {in_sql}
          AND xdate >= %s
          AND xdocnum LIKE %s
        GROUP BY xitem
    """
    return pd.read_sql(query, con=engine, params=[zid, *in_params, start_date, 'SRE-%'])


def get_purchase_return(zid, items, start_date):
    in_sql, in_params = _in_clause(items)
    if not in_params:
        return _def_empty
    query = f"""
        SELECT xitem, SUM(xqty*xsign) as preturn
        FROM imtrn
        WHERE zid = %s
          AND xitem IN {in_sql}
          AND xdate >= %s
          AND xdocnum LIKE %s
        GROUP BY xitem
    """
    return pd.read_sql(query, con=engine, params=[zid, *in_params, start_date, 'PRE-%'])


def get_igrn(zid, start_date):
    query = """
        SELECT pogrn.xgrnnum, pogrn.xdate, poodt.xitem
        FROM poord
        JOIN poodt ON poord.xpornum = poodt.xpornum
        JOIN pogrn ON poord.xpornum = pogrn.xpornum
        WHERE poord.zid = %s
          AND poodt.zid = %s
          AND pogrn.zid = %s
          AND poord.xpornum LIKE %s
          AND poord.xstatuspor = %s
          AND poord.xdate > %s
        GROUP BY pogrn.xgrnnum, pogrn.xdate, poodt.xitem
    """
    return pd.read_sql(query, con=engine, params=[zid, zid, zid, 'IP--%', '5-Received', start_date])

# Main Script
start_date = datetime.datetime.now() - timedelta(days = 365)
start_date = start_date.strftime("%Y-%m-%d")
today_date = datetime.datetime.now().strftime("%Y-%m-%d")

# make zid variables
zid_central = 100002
zid_gulshan = 100001
zid_ecommerce = 100003

print(f"[INFO] Start date: {start_date}")

# get list of igrn within given datecd
df_igrn = get_igrn(zid_central,start_date)
print(f"[OK] IGRN rows: {len(df_igrn)}")

item_dict = df_igrn.groupby('xgrnnum')['xitem'].apply(lambda x: (x.to_list())).to_dict()
date_dict = df_igrn.groupby('xgrnnum')['xdate'].apply(lambda x: (x.to_list())[0]).to_dict()

shipment_dict = {}
master_dict = {}
stock_0_gulshan = {}
ge_price_error = {}
cs_error = {}
gs_error = {}
es_error = {}
t_error_dict = {}

for (ik,iv), (dk,dv) in zip(item_dict.items(), date_dict.items()):
    dv = dv.strftime("%Y-%m-%d")
    # Get purchase information for central, gulshan and ecommerce for these products
    df_cpurchase = get_purchase(zid_central,start_date)
    df_gpurchase = get_purchase_item(zid_gulshan,tuple(iv),dv)
    df_gpreturn = get_purchase_return(zid_gulshan,tuple(iv),dv)
    df_epurchase = get_purchase_item(zid_ecommerce,tuple(iv),dv)
    df_epreturn = get_purchase_return(zid_ecommerce,tuple(iv),dv)

    # merge totals
    df_ctransfer = get_transfer_product(zid_central,tuple(iv),dv)
    df_ctransfer['transfer'] = df_ctransfer['transfer']*-1
    df_creturn = get_return_product(zid_central,tuple(iv),dv)
    df_greturn = get_return_product(zid_gulshan,tuple(iv),dv)
    df_ereturn = get_return_product(zid_ecommerce,tuple(iv),dv)

    # current stock
    df_cstock = get_item_stock(zid_central,today_date,tuple(iv))
    df_gstock = get_item_stock(zid_gulshan,today_date,tuple(iv))
    df_estock = get_item_stock(zid_ecommerce,today_date,tuple(iv))

    # sales
    df_gsales = get_sales(zid_gulshan,dv,tuple(iv))
    df_esales = get_sales(zid_ecommerce,dv,tuple(iv))

    df_ccaitem = get_caitem(zid_central)
    df_gcaitem = get_caitem(zid_gulshan)
    df_ecaitem = get_caitem(zid_ecommerce)

    # assemble
    df_main = df_cpurchase.merge(df_ccaitem[['xitem','xdesc', 'xgitem','xstdprice']],on=['xitem'],how='left')\
                        .merge(df_gcaitem[['xitem','xstdprice']],on=['xitem'],how='left')\
                        .merge(df_ecaitem[['xitem','xstdprice']],on=['xitem'],how='left')\
                        .merge(df_gpurchase[['xitem','sum']],on=['xitem'],how='left')\
                        .merge(df_epurchase[['xitem','sum']],on=['xitem'],how='left')\
                        .merge(df_gpreturn[['xitem','preturn']],on=['xitem'],how='left')\
                        .merge(df_epreturn[['xitem','preturn']],on=['xitem'],how='left')\
                        .merge(df_ctransfer[['xitem','transfer']],on=['xitem'],how='left')\
                        .merge(df_creturn[['xitem','return']],on=['xitem'],how='left')\
                        .merge(df_greturn[['xitem','return']],on=['xitem'],how='left')\
                        .merge(df_ereturn[['xitem','return']],on=['xitem'],how='left')\
                        .merge(df_cstock[['xitem','stock']],on=['xitem'],how='left')\
                        .merge(df_gstock[['xitem','stock']],on=['xitem'],how='left')\
                        .merge(df_estock[['xitem','stock']],on=['xitem'],how='left')\
                        .merge(df_gsales[['xitem','sales']],on=['xitem'],how='left')\
                        .merge(df_esales[['xitem','sales']],on=['xitem'],how='left')\
                        .fillna(0).rename(columns={'xitem':'code',
                                                   'xqtyord':'cpqty',
                                                   'xdate':'cpdate',
                                                   'xrate':'cprate',
                                                   'xgrnnum':'cgrnnum',
                                                   'sum_x':'gpurchase',
                                                   'sum_y':'epurchase',
                                                   'stock_x':'cstock',
                                                   'stock_y':'gstock',
                                                   'stock':'estock',
                                                   'return_x':'creturn',
                                                   'return_y':'greturn',
                                                   'return':'ereturn',
                                                   'preturn_x':'gpreturn',
                                                   'preturn_y':'epreturn',
                                                   'sales_x':'gsales',
                                                   'sales_y':'esales',
                                                  'xstdprice_x':'cprice',
                                                  'xstdprice_y':'gprice',
                                                  'xstdprice':'eprice'})
    if 'gsales' in df_main.columns:
        df_main['gsales'] = df_main['gsales']*-1
    if 'esales' in df_main.columns:
        df_main['esales'] = df_main['esales']*-1
    if 'gpreturn' in df_main.columns:
        df_main['gpreturn'] = df_main['gpreturn']*-1

    df_main['crprcheck'] = (df_main['gprice']-df_main['cprice'])*100/df_main['cprate']
    df_main['ceprcheck'] = df_main['gprice']-df_main['eprice']
    df_main['csCheck'] = df_main['cpqty']-df_main['transfer']-df_main['cstock']+df_main['creturn']
    df_main['gsCheck'] = df_main['gpurchase']-df_main['gpreturn']-df_main['gsales']+df_main['greturn']-df_main['gstock']
    df_main['esCheck'] = df_main['epurchase']-df_main['epreturn']-df_main['esales']+df_main['ereturn']-df_main['estock']
    df_main['Totalpgp'] = (((df_main['cprice']-df_main['cprate'])*df_main['cpqty'])+((df_main['gprice']-df_main['cprice'])*df_main['cpqty']))
    df_main['Totalcpgp'] = (df_main['cprice'] - df_main['cprate'])*df_main['cpqty']
    df_main['Totalcgp'] = (df_main['cprice'] - df_main['cprate'])*df_main['transfer']
    df_main['Totalrgp'] = (df_main['gprice'] - df_main['cprice'])*df_main['cpqty']
    df_main['Totalggp'] = (df_main['gprice'] - df_main['cprice'])*df_main['gsales']
    df_main['Totalegp'] = (df_main['eprice'] - df_main['cprice'])*df_main['esales']
    df_main['Totalgp'] = df_main['Totalcgp'] + df_main['Totalggp'] + df_main['Totalegp']
    df_main['%gp'] = (df_main['Totalgp']/df_main['Totalpgp'])*100

    df_main = df_main.sort_values('%gp',ascending=False)
    df_main = df_main[['code','xdesc','xgitem','cgrnnum','cpdate','cpqty','cprate','cprice','transfer','creturn','cstock','Totalcpgp','Totalcgp','csCheck', 
                       'gpurchase','gpreturn','gsales','gprice','greturn','gstock','Totalggp','gsCheck',
                     'epurchase','epreturn', 'esales','eprice','ereturn','estock','Totalegp','esCheck',
                      'crprcheck', 'ceprcheck','Totalpgp','Totalgp','Totalrgp','%gp']]

    ge_price_error[dk] = df_main.loc[df_main['ceprcheck']!=0,['code','xdesc','gprice','eprice','ceprcheck']]
    cs_error[dk] = df_main[df_main['csCheck']!=0]
    gs_error[dk] = df_main[df_main['gsCheck']!=0]
    es_error[dk] = df_main[df_main['esCheck']!=0]
    main_dict = {'Total Possible Gross Profit':df_main['Totalpgp'].sum().round(2),
                'Running Gross Profit':df_main['Totalgp'].sum().round(2),
                '% of Gross Profit':(df_main['Totalgp'].sum()/df_main['Totalpgp'].sum()).round(4)*100 if df_main['Totalpgp'].sum()!=0 else 0,
                'Total Possible Gross Profit Central': df_main['Totalcpgp'].sum().round(2),
                'Central Running Gross Profit':df_main['Totalcgp'].sum().round(2),
                'Total Possible Retail Gross Profit':df_main['Totalrgp'].sum().round(2),
                'Gulshan Running Gross Profit':df_main['Totalggp'].sum().round(2),
                'E-commerce Running Gross Profit':df_main['Totalegp'].sum().round(2),
                'Retail Running Gross Profit': (df_main['Totalggp'].sum() + df_main['Totalegp'].sum()).round(2)}
    master_dict[dk] = pd.DataFrame.from_dict(main_dict.items()).rename(columns={0:'Subject',1:'Results'})

    stock_0_gulshan[dk] = df_main.loc[df_main['gstock']<=0,['code','xdesc','cgrnnum','gstock']]

    shipment_dict[dk] = df_main

# Write Excel outputs
writer = pd.ExcelWriter('shipment_main.xlsx', engine='openpyxl')
for k,v in shipment_dict.items():
    v.to_excel(writer,k, index=False)
writer.save()
writer.close()

writer2 = pd.ExcelWriter('error_main.xlsx', engine='openpyxl')
for k,v in gs_error.items():
    name = k + 'gs_error'
    v.to_excel(writer2,name, index=False)
for k,v in cs_error.items():
    name = k + 'cs_error'
    v.to_excel(writer2,name, index=False)
for k,v in es_error.items():
    name = k + 'es_error'
    v.to_excel(writer2,name, index=False)
writer2.save()
writer2.close()

# Prepare email body sections
html_sections = []
for head, table in master_dict.items():
    html_sections.append((table, f"{head} Master Dict"))
for head, table in stock_0_gulshan.items():
    html_sections.append((table, f"{head} Gulshan Stock 0 Error Check"))
for head, table in ge_price_error.items():
    html_sections.append((table, f"{head} Gulshan & Ecommerce pricing Error Check"))

# Send email via shared mail module
try:
    recipients = get_email_recipients("F_08_Shipment_Tracking")
except Exception as e:
    print(f"[WARN] Recipient lookup failed: {e}; using fallback")
    recipients = ["ithmbrbd@gmail.com"]

send_mail(
    subject = "Fixit-08 Fixit Shipment tracking",
    bodyText = "Please find attached shipment tracking reports.",
    attachment  = ['shipment_main.xlsx','error_main.xlsx'],
    recipient = recipients,
    html_body = html_sections
)

engine.dispose()
print("✅ Script completed successfully.")
