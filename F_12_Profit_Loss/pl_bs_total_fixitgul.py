from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import date,datetime,timedelta
import psycopg2
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import openpyxl

####################################

def get_gl_details(zid, year, smonth, emonth):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    query = """
        SELECT glmst.zid, glmst.xacc, glmst.xdesc, glmst.xhrc1, glmst.xhrc2, glmst.xhrc3, glmst.xhrc4, glmst.xhrc5, glheader.xyear, glheader.xper, SUM(gldetail.xprime)
        FROM glmst
        JOIN gldetail ON glmst.xacc = gldetail.xacc
        JOIN glheader ON gldetail.xvoucher = glheader.xvoucher
        WHERE glmst.zid = %s
        AND gldetail.zid = %s
        AND glheader.zid = %s
        AND (glmst.xacctype = 'Income' OR glmst.xacctype = 'Expenditure')
        AND glheader.xyear = %s
        AND glheader.xper >= %s
        AND glheader.xper <= %s
        GROUP BY glmst.zid, glmst.xacc, glmst.xdesc, glmst.xacctype, glmst.xhrc1, glmst.xhrc2, glmst.xhrc3, glmst.xhrc4, glmst.xhrc5, glheader.xyear, glheader.xper
        ORDER BY glheader.xper ASC, glmst.xacctype
    """
    params = (zid, zid, zid, year, smonth, emonth)
    df = pd.read_sql_query(query, con=engine, params=params)
    return df

def get_gl_details_bs(zid, year, smonth, emonth):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    query = f"""
        SELECT glmst.zid, glmst.xacc, glmst.xdesc, glmst.xhrc1, glmst.xhrc2, glmst.xhrc3, glmst.xhrc4, glmst.xhrc5, glheader.xyear, glheader.xper, SUM(gldetail.xprime)
        FROM glmst
        JOIN gldetail ON glmst.xacc = gldetail.xacc
        JOIN glheader ON gldetail.xvoucher = glheader.xvoucher
        WHERE glmst.zid = {zid}
        AND gldetail.zid = {zid}
        AND glheader.zid = {zid}
        AND (glmst.xacctype = 'Asset' OR glmst.xacctype = 'Liability')
        AND glheader.xyear = '{year}'
        AND glheader.xper <= '{emonth}'
        GROUP BY glmst.zid, glmst.xacc, glmst.xdesc, glmst.xacctype, glmst.xhrc1, glmst.xhrc2, glmst.xhrc3, glmst.xhrc4, glmst.xhrc5, glheader.xyear, glheader.xper
    """
    df = pd.read_sql_query(query, con=engine)
    return df





def get_gl_master(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df = pd.read_sql("""SELECT xacc, xacctype, xdesc, xhrc1, xhrc2, glmst.xhrc3, glmst.xhrc4,  xhrc5 FROM glmst WHERE glmst.zid = %s"""%(zid),con=engine)
    return df

income_statement_label = {'04-Cost of Goods Sold':'02-1-Cost of Revenue',
'0401-DIRECT EXPENSES':'07-1-Other Operating Expenses, Total',
'0401-PURCHASE':'07-1-Other Operating Expenses, Total',
'0501-OTHERS DIRECT EXPENSES':'07-1-Other Operating Expenses, Total',
'0601-OTHERS DIRECT EXPENSES':'07-1-Other Operating Expenses, Total',
'0631- Development Expenses':'07-1-Other Operating Expenses, Total',
'06-Office & Administrative Expenses':'03-1-Office & Administrative Expenses',
'0625-Property Tax & Others':'09-1-Income Tax & VAT',
'0629- HMBR VAT & Tax Expenses':'09-1-Income Tax & VAT',
'0629-VAT & Tax Expenses':'09-1-Income Tax & VAT',
'0630- Bank Interest & Charges':'08-1-Interest Expense',
'0630-Bank Interest & Charges':'08-1-Interest Expense',
'0631-Other Expenses':'07-1-Other Operating Expenses, Total',
'0633-Interest-Loan':'08-1-Interest Expense',
'0636-Depreciation':'05-1-Depreciation/Amortization',
'07-Sales & Distribution Expenses':'04-1-Sales & Distribution Expenses',
'SALES & DISTRIBUTION EXPENSES':'04-1-Sales & Distribution Expenses',
'0701-MRP-Discount' : '04-2-MRP Discount',
'0702-Discount-Expense' : '04-3-Discount Expense',
'08-Revenue':'01-1-Revenue',
'14-Purchase Return':'06-1-Unusual Expenses (Income)',
'15-Sales Return':'06-1-Unusual Expenses (Income)',
'':'06-1-Unusual Expenses (Income)',
'Profit/Loss':'10-1-Net Income'}

income_label = pd.DataFrame(income_statement_label.items(),columns = ['xhrc4','Income Statement'])
# balance sheet label
### balance sheet
balance_sheet_label = {
'0101-CASH & CASH EQUIVALENT':'01-3-Cash',
'0102-BANK BALANCE':'01-3-Cash',
'0103-ACCOUNTS RECEIVABLE':'02-1-Accounts Receivable',
'ACCOUNTS RECEIVABLE':'02-1-Accounts Receivable',
'0104-PREPAID EXPENSES':'04-1-Prepaid Expenses',
'0105-ADVANCE ACCOUNTS':'04-1-Prepaid Expenses',
'0106-STOCK IN HAND':'03-1-Inventories',
'02-OTHER ASSET':'05-1-Other Assets',
'0201-DEFFERED CAPITAL EXPENDITURE':'05-1-Other Assets',
'0203-LOAN TO OTHERS CONCERN':'05-1-Other Assets',
'0204-SECURITY DEPOSIT':'05-1-Other Assets',
'0205-LOAN TO OTHERS CONCERN':'05-1-Other Assets',
'0206-Other Investment':'05-1-Other Assets',
'0301-Lab Equipment':'06-1-Property, Plant & Equipment',
'0301-Office Equipment':'06-1-Property, Plant & Equipment',
'0302-Corporate Office Equipments':'06-1-Property, Plant & Equipment',
'0303-Furniture & Fixture':'06-1-Property, Plant & Equipment',
'0303-Lab Decoration':'06-1-Property, Plant & Equipment',
'0304-Trading Vehicles':'06-1-Property, Plant & Equipment',
'0305-Private Vehicles':'06-1-Property, Plant & Equipment',
'0305-Plants & Machinery':'06-1-Property, Plant & Equipment',
'0306- Plants & Machinery':'06-1-Property, Plant & Equipment',
'0307-Intangible Asset':'07-1-Goodwill & Intangible Asset',
'0308-Land & Building':'06-1-Property, Plant & Equipment',
'0901-Accrued Expenses':'09-1-Accrued Liabilities',
'0902-Income Tax Payable':'09-1-Accrued Liabilities',
'0903-Accounts Payable':'08-1-Accounts Payable',
'0904-Money Agent Liability':'10-1-Other Short Term Liabilities',
'0904-Reconciliation Liability':'10-1-Other Short Term Liabilities',
'0905-C & F Liability':'10-1-Other Short Term Liabilities',
'0906-Others Liability':'10-1-Other Short Term Liabilities',
'INTERNATIONAL PURCHASE TAX & COMMISSION':'10-1-Other Short Term Liabilities',
'1001-Short Term Bank Loan':'11-1-Debt',
'1002-Short Term Loan':'11-1-Debt',
'11-Reserve & Fund':'12-1-Other Long Term Liabilities',
'1202-Long Term Bank Loan':'11-1-Debt',
'13-Owners Equity':'13-1-Total Shareholders Equity'}

balance_label = pd.DataFrame(balance_sheet_label.items(),columns = ['xhrc4','Balance Sheet'])


# zid list
zid_list_fixitceg = [100001,100002,100003]

start_year = int (input ("input year eg:2022________   "))

start_month = int (input ("input start month eg: 1________   "))
end_month = int (input ("input end of  month eg: 1________   "))
### define business Id and date time year list for comparison (separate if project)


### make a 3 year list
year_list = []
new_year = 0
for i in range(5):
    new_year = start_year - i
    year_list.append(new_year)
year_list.reverse()
    
#create master dataframe

# in order for a proper debug we are going to do sum tests on each part of the project algorithm loop to find our why the merge is not working
#that is exactly what is not working becuase the data behaves until then.

main_data_dict_pl = {}
for i in zid_list_fixitceg:
    df_master = get_gl_master(i)
    df_master = df_master[(df_master['xacctype']!='Asset') & (df_master['xacctype']!='Liability')]
    for item,idx in enumerate(year_list):
        df = get_gl_details(i,idx,start_month,end_month)
        df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
        if item == 0:
            df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
        else:
            df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)

    main_data_dict_pl[i] = df_new.merge(income_label[['xhrc4','Income Statement']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)


main_data_dict_bs = {}
for i in zid_list_fixitceg:
    df_master = get_gl_master(i)
    df_master = df_master[(df_master['xacctype']!='Income') & (df_master['xacctype']!='Expenditure')]
    for item,idx in enumerate(year_list):
        df = get_gl_details_bs(i,idx,start_month,end_month)
        df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
        if item == 0:
            df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').fillna(0).rename(columns={'sum':idx})
        else:
            df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').fillna(0).rename(columns={'sum':idx})

    main_data_dict_bs[i] = df_new.merge(balance_label[['xhrc4','Balance Sheet']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)
    

level_1_dict = {}
for key in main_data_dict_pl:
    level_1_dict[key] = main_data_dict_pl[key].groupby(['xacctype'])[[i for i in year_list]].sum().reset_index().round(1)
    level_1_dict[key].loc[len(level_1_dict[key].index),:]=level_1_dict[key].sum(axis=0,numeric_only = True)
    level_1_dict[key].at[len(level_1_dict[key].index)-1,'xacctype'] = 'Profit/Loss'
    
level_1_dict_bs = {}
for key in main_data_dict_bs:
    level_1_dict_bs[key] = main_data_dict_bs[key].groupby(['xacctype'])[[i for i in year_list]].sum().reset_index().round(1)
    level_1_dict_bs[key].loc[len(level_1_dict_bs[key].index),:]=level_1_dict_bs[key].sum(axis=0,numeric_only = True)
    level_1_dict_bs[key].at[len(level_1_dict_bs[key].index)-1,'xacctype'] = 'Profit/Loss'
    ## we can add new ratios right here!
    
level_2_dict = {}
for key in main_data_dict_pl:
    level_2_dict[key] = main_data_dict_pl[key].groupby(['xhrc1'])[[i for i in year_list]].sum().reset_index().round(1)
    level_2_dict[key].loc[len(level_2_dict[key].index),:]=level_2_dict[key].sum(axis=0,numeric_only = True)
    level_2_dict[key].at[len(level_2_dict[key].index)-1,'xhrc1'] = 'Profit/Loss'
    
level_3_dict = {}
for key in main_data_dict_pl:
    level_3_dict[key] = main_data_dict_pl[key].groupby(['xhrc2'])[[i for i in year_list]].sum().reset_index().round(1)
    level_3_dict[key].loc[len(level_3_dict[key].index),:]=level_3_dict[key].sum(axis=0,numeric_only = True)
    level_3_dict[key].at[len(level_3_dict[key].index)-1,'xhrc2'] = 'Profit/Loss'
##########################


level_4_dict = {}
income_s_dict = {}
for key in main_data_dict_pl:
    print(key)
    level_4_dict[key] = main_data_dict_pl[key].groupby(['xhrc4'])[[i for i in year_list]].sum().reset_index().round(1)
    level_4_dict[key].loc[len(level_4_dict[key].index),:]=level_4_dict[key].sum(axis=0,numeric_only = True)
    level_4_dict[key].at[len(level_4_dict[key].index)-1,'xhrc4'] = 'Profit/Loss'
    df_i = level_4_dict[key].merge(income_label[['xhrc4','Income Statement']],on=['xhrc4'],how='left').sort_values('Income Statement').set_index('Income Statement').reset_index()
    df_i = df_i.groupby(['Income Statement']).sum().reset_index()    
    if ~df_i['Income Statement'].isin(['06-1-Unusual Expenses (Income)']).any():
        df_i.loc[len(df_i.index)] = ['06-1-Unusual Expenses (Income)',0,0,0,0,0]
    df_i.loc[len(df_i.index)] = ['02-2-Gross Profit','-','-','-','-','-']
    df_i.loc[len(df_i.index)] = ['07-2-EBIT','-','-','-','-','-']
    df_i.loc[len(df_i.index)] = ['08-2-EBT','-','-','-','-','-']
    df_i = df_i.set_index('Income Statement')
    df_i.loc['02-2-Gross Profit'] = df_i.loc['01-1-Revenue']+df_i.loc['02-1-Cost of Revenue']
    df_i.loc['07-2-EBIT'] = df_i.loc['02-2-Gross Profit'] + df_i.loc['03-1-Office & Administrative Expenses'] + df_i.loc['04-1-Sales & Distribution Expenses'] + df_i.loc['05-1-Depreciation/Amortization'] + df_i.loc['06-1-Unusual Expenses (Income)'] + df_i.loc['07-1-Other Operating Expenses, Total']
    df_i.loc['08-2-EBT'] = df_i.loc['07-2-EBIT'] + df_i.loc['08-1-Interest Expense']
    df_i = df_i.sort_index().reset_index()
    income_s_dict[key] = df_i



### balance sheet

level_4_dict_bs = {}
balance_s_dict = {}

for key in main_data_dict_bs:
    level_4_dict_bs[key] = main_data_dict_bs[key].groupby(['xhrc4'])[[i for i in year_list]].sum().reset_index().round(1)
    level_4_dict_bs[key].loc[len(level_4_dict_bs[key].index),:]=level_4_dict_bs[key].sum(axis=0,numeric_only = True)
    level_4_dict_bs[key].at[len(level_4_dict_bs[key].index)-1,'xhrc4'] = 'Balance'
    df_b = level_4_dict_bs[key].merge(balance_label[['xhrc4','Balance Sheet']],on=['xhrc4'],how='left').sort_values('Balance Sheet').set_index('Balance Sheet').reset_index().drop(['xhrc4'],axis=1)
    df_b = df_b.groupby(['Balance Sheet']).sum().reset_index()
#     df1 = ap_final_dict[key][ap_final_dict[key]['AP_TYPE']=='EXTERNAL'].rename(columns={'AP_TYPE':'Balance Sheet'})
#     df_b = df_b.append(df1).reset_index().drop(['index'],axis=1)
    df_b.loc[len(df_b.index)] = ['01-1-Assets','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['01-2-Current Assets','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['04-2-Total Current Assets','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['04-3-Non-Current Assets','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['07-2-Total Non-Current Assets','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['07-3-Current Liabilities','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['11-2-Total Current Liabilities','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['11-4-Non-Current Liabilities','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['12-2-Total Non-Current Liabilities','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['13-2-Retained Earnings','-','-','-','-','-']
    df_b.loc[len(df_b.index)] = ['13-3-Balance Check','-','-','-','-','-']
    df_b = df_b.set_index('Balance Sheet')
    try:
        df_b.loc['04-2-Total Current Assets'] = df_b.loc['01-3-Cash']+df_b.loc['02-1-Accounts Receivable']+df_b.loc['03-1-Inventories']+df_b.loc['04-1-Prepaid Expenses']
        df_b.loc['07-2-Total Non-Current Assets'] = df_b.loc['05-1-Other Assets']+df_b.loc['06-1-Property, Plant & Equipment']+df_b.loc['07-1-Goodwill & Intangible Asset']
        df_b.loc['11-2-Total Current Liabilities'] = df_b.loc['08-1-Accounts Payable']+df_b.loc['09-1-Accrued Liabilities']+df_b.loc['10-1-Other Short Term Liabilities']+df_b.loc['11-1-Debt']
        df_b.loc['12-2-Total Non-Current Liabilities'] = df_b.loc['12-1-Other Long Term Liabilities']
        df1 = income_s_dict[key].set_index('Income Statement')
        df_b.loc['13-2-Retained Earnings'] = df1.loc['10-1-Net Income']
        df_b.loc['13-3-Balance Check'] = df_b.loc['04-2-Total Current Assets'] + df_b.loc['07-2-Total Non-Current Assets'] + df_b.loc['11-2-Total Current Liabilities'] + df_b.loc['12-2-Total Non-Current Liabilities'] + df_b.loc['13-1-Total Shareholders Equity'] + df_b.loc['13-2-Retained Earnings']
    except Exception as e:
        print(e)
        pass
    df_b = df_b.sort_index().reset_index().round(0)
    balance_s_dict[key] = df_b


#cash flow statement
cashflow_s_dict = {}
for key in income_s_dict:
    print(key)
    df_i2= income_s_dict[key].set_index('Income Statement').replace('-',0)
    df_b2 = balance_s_dict[key].set_index('Balance Sheet').replace('-',0)
    df_b22 = df_b2
    #create a temporary dataframe which caluclates the difference between the 2 years
    df_b2 = df_b2.diff(axis=1).fillna(0)
    
    df2 = pd.DataFrame(columns=balance_s_dict[key].columns).rename(columns={'Balance Sheet':'Description'})
    ##operating cashflow
    df2.loc[len(df2.index)] = ['01-Operating Activities','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['02-Net Income','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['03-Depreciation and amortization','-','-','-','-','-']
#     df2.loc[len(df2.index)] = ['04-Increase/Decrease in Current Asset','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['04-1-Accounts Receivable','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['04-2-Inventories','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['04-3-Prepaid Expenses','-','-','-','-','-']

#     df2.loc[len(df2.index)] = ['05-Increase/Decrease in Current Liabilities','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['05-1-Accounts Payable','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['05-2-Accrued Liabilities','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['05-3-Other Short Term Liabilities','-','-','-','-','-']
    
    df2.loc[len(df2.index)] = ['06-Other operating cash flow adjustments',0,0,0,0,0]
    df2.loc[len(df2.index)] = ['07-Total Operating Cashflow','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['08','-','-','-','-','-']
    
    #investing cashflow
    df2.loc[len(df2.index)] = ['09-Investing Cashflow','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['10-Capital asset acquisitions/disposal','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['11-Other investing cash flows',0,0,0,0,0]
    df2.loc[len(df2.index)] = ['12-Total Investing Cashflow','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['13','-','-','-','-','-']

    #financing cashflow
    df2.loc[len(df2.index)] = ['14-Financing Cashflow','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['15-Increase/Decrease in Debt','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['16-Increase/Decrease in Equity','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['16-1-Increase/Decrease in Retained Earning','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['17-Other financing cash flows','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['18-Total Financing Cashflow','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['19','-','-','-','-','-']
    
    ##change in cash calculations
    df2.loc[len(df2.index)] = ['20-Year Opening Cash','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['21-Change in Cash','-','-','-','-','-']
    df2.loc[len(df2.index)] = ['22-Year Ending Cash','-','-','-','-','-']
    df2 = df2.set_index('Description')
    
    try:
        #operating cashflow calculations
        df2.loc['02-Net Income'] = df_i2.loc['10-1-Net Income']
        df2.loc['03-Depreciation and amortization'] = df_i2.loc['05-1-Depreciation/Amortization']

        df2.loc['04-1-Accounts Receivable'] = df_b2.loc['02-1-Accounts Receivable']
        df2.loc['04-2-Inventories'] = df_b2.loc['03-1-Inventories']
        df2.loc['04-3-Prepaid Expenses'] = df_b2.loc['04-1-Prepaid Expenses']

        df2.loc['05-1-Accounts Payable'] = df_b2.loc['08-1-Accounts Payable']
        df2.loc['05-2-Accrued Liabilities'] = df_b2.loc['09-1-Accrued Liabilities']
        df2.loc['05-3-Other Short Term Liabilities'] = df_b2.loc['10-1-Other Short Term Liabilities']
    
        df2.loc['07-Total Operating Cashflow'] = df2.loc['02-Net Income'] + df2.loc['03-Depreciation and amortization'] + df2.loc['04-1-Accounts Receivable'] + df2.loc['04-2-Inventories'] + df2.loc['04-3-Prepaid Expenses'] + df2.loc['05-1-Accounts Payable'] + df2.loc['05-2-Accrued Liabilities'] + df2.loc['05-3-Other Short Term Liabilities']
    except Exception as e:
        print(e)
        pass
    
    #investing cashflow calculations
    df2.loc['10-Capital asset acquisitions/disposal'] = df_b2.loc['07-2-Total Non-Current Assets']
    df2.loc['12-Total Investing Cashflow'] = df2.loc['10-Capital asset acquisitions/disposal'] + df2.loc['11-Other investing cash flows']
    
    #financing cashflow calculations
    df2.loc['15-Increase/Decrease in Debt'] = df_b2.loc['11-1-Debt']
    df2.loc['16-Increase/Decrease in Equity'] = df_b2.loc['13-1-Total Shareholders Equity']
    df2.loc['16-1-Increase/Decrease in Retained Earning'] = df_b2.loc['13-2-Retained Earnings']
    df2.loc['17-Other financing cash flows'] = df_b2.loc['12-2-Total Non-Current Liabilities']
    df2.loc['18-Total Financing Cashflow'] = df2.loc['15-Increase/Decrease in Debt'] + df2.loc['16-Increase/Decrease in Equity'] + df2.loc['16-1-Increase/Decrease in Retained Earning'] + df2.loc['17-Other financing cash flows']
    
    ##change in cash calculations
    try:
        df2.loc['20-Year Opening Cash'] = df_b22.loc['01-3-Cash'].shift(periods=1,axis=0)
        df2.loc['21-Change in Cash'] = -(df2.loc['07-Total Operating Cashflow'] + df2.loc['12-Total Investing Cashflow'] + df2.loc['18-Total Financing Cashflow'] - df2.loc['02-Net Income'] - df2.loc['03-Depreciation and amortization'])
        df2.loc['22-Year Ending Cash'] = df2.loc['20-Year Opening Cash'] + df2.loc['21-Change in Cash']
    except Exception as e:
        print(e)
        pass
    
    cashflow_s_dict[key] = df2.sort_index().reset_index().fillna(0)


statement_3_dict = {}
for key in income_s_dict:
    print(key)
    df_i3 = income_s_dict[key].rename(columns={'Income Statement':'Description'})
    df_b3 = balance_s_dict[key].rename(columns={'Balance Sheet':'Description'})
    df_c = cashflow_s_dict[key]
    
    df12 = pd.concat([df_i3,df_b3,df_c]).reset_index(drop=True)
    daysinyear = 365
    #ratios
    df12.loc[len(df12.index)] = ['Ratios','-','-','-','-','-']
    df12.loc[len(df12.index)] = ['COGS Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Gross Profit Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Operating Profit','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Net Profit Ratio','-','-','-','-','-']  

    ##coverages
    df12.loc[len(df12.index)] = ['Tax Coverage','-','-','-','-','-']
    df12.loc[len(df12.index)] = ['Interest Coverage','-','-','-','-','-'] 

    #expense ratios
    df12.loc[len(df12.index)] = ['OAE Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['S&D Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Deprication Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Unusual Expense Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Other Operating Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Interest Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Tax Ratio','-','-','-','-','-'] 

    #efficiency ratios
    df12.loc[len(df12.index)] = ['Quick Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Quick Ratio Adjusted','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Current Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Current Ratio Adjusted','-','-','-','-','-'] 

    #asset ratios
    df12.loc[len(df12.index)] = ['Total Asset Turnover Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Net Asset Turnover Ratio','-','-','-','-','-'] 

    #working capital days
    df12.loc[len(df12.index)] = ['Inventory Turnover','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Inventory Days','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Accounts Receivable Turnover','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Accounts Receivable Days','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Accounts Payable Turnover','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Accounts Payable Turnover*','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Accounts Payable Days','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Accounts Payable Days*','-','-','-','-','-'] 

    #other ratios
    df12.loc[len(df12.index)] = ['PP&E Ratio','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Working Capital Turnover','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Working Capital Turnover*','-','-','-','-','-'] 

    #debt ratios
    df12.loc[len(df12.index)] = ['Cash Turnover','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Debt/Equity','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Debt/Capital','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Debt/TNW','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Total Liabilities/Equity','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Total Liabilities/Equity*','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Total Assets to Equity','-','-','-','-','-'] 


    df12.loc[len(df12.index)] = ['Debt/EBITDA','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Capital Structure Impact','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Acid Test','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['Acid Test*','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['ROE','-','-','-','-','-'] 
    df12.loc[len(df12.index)] = ['ROA','-','-','-','-','-'] 

    df12 = df12.set_index('Description').replace(0,np.nan)
    #ratio calculation
    try:
        ##profitability ratios
        df12.loc['COGS Ratio'] = df12.loc['02-1-Cost of Revenue']*100/df12.loc['01-1-Revenue']
        df12.loc['Gross Profit Ratio'] = df12.loc['02-2-Gross Profit']*100/df12.loc['01-1-Revenue']
        df12.loc['Operating Profit'] = df12.loc['07-2-EBIT']*100/df12.loc['01-1-Revenue']
        df12.loc['Net Profit Ratio'] = df12.loc['10-1-Net Income']*100/df12.loc['01-1-Revenue']

        ##coverages
        df12.loc['Tax Coverage'] = df12.loc['09-1-Income Tax & VAT']*100/df12.loc['08-2-EBT']
        df12.loc['Interest Coverage'] = df12.loc['08-1-Interest Expense']*100/df12.loc['07-2-EBIT']

        #expense ratios
        df12.loc['OAE Ratio'] = df12.loc['03-1-Office & Administrative Expenses']*100/df12.loc['01-1-Revenue']
        df12.loc['S&D Ratio'] = df12.loc['04-1-Sales & Distribution Expenses']*100/df12.loc['01-1-Revenue']
        df12.loc['Deprication Ratio'] = df12.loc['05-1-Depreciation/Amortization']*100/df12.loc['01-1-Revenue']
        df12.loc['Unusual Expense Ratio'] = df12.loc['06-1-Unusual Expenses (Income)']*100/df12.loc['01-1-Revenue']
        df12.loc['Other Operating Ratio'] = df12.loc['07-1-Other Operating Expenses, Total']*100/df12.loc['01-1-Revenue']
        df12.loc['Interest Ratio'] = df12.loc['08-1-Interest Expense']*100/df12.loc['01-1-Revenue']
        df12.loc['Tax Ratio'] = df12.loc['09-1-Income Tax & VAT']*100/df12.loc['01-1-Revenue']

        #efficiency ratios
        df12.loc['Quick Ratio'] = df12.loc['04-2-Total Current Assets']/df12.loc['11-2-Total Current Liabilities']
#         df12.loc['Quick Ratio Adjusted'] = df12.loc['04-2-Total Current Assets']/(df12.loc['11-2-Total Current Liabilities']-df12.loc['EXTERNAL'])
        df12.loc['Current Ratio'] = df12.loc['04-2-Total Current Assets']/df12.loc['11-2-Total Current Liabilities']
#         df12.loc['Current Ratio Adjusted'] = df12.loc['04-2-Total Current Assets']/(df12.loc['11-2-Total Current Liabilities']-df12.loc['EXTERNAL'])

        #asset ratios
        df12.loc['Total Asset Turnover Ratio'] = df12.loc['01-1-Revenue']/(df12.loc['04-2-Total Current Assets']+df12.loc['07-2-Total Non-Current Assets'])
        df12.loc['Net Asset Turnover Ratio'] = df12.loc['01-1-Revenue']/(df12.loc['04-2-Total Current Assets']+df12.loc['07-2-Total Non-Current Assets']+df12.loc['11-2-Total Current Liabilities']+df12.loc['12-2-Total Non-Current Liabilities'])

        #working capital days
        df12.loc['Inventory Turnover'] = df12.loc['02-1-Cost of Revenue']/df12.loc['03-1-Inventories']
        df12.loc['Inventory Days'] = df12.loc['03-1-Inventories']*daysinyear/df12.loc['02-1-Cost of Revenue']
        df12.loc['Accounts Receivable Turnover'] = df12.loc['01-1-Revenue']/df12.loc['02-1-Accounts Receivable']
        df12.loc['Accounts Receivable Days'] = df12.loc['02-1-Accounts Receivable']*daysinyear/df12.loc['01-1-Revenue']
        df12.loc['Accounts Payable Turnover'] = df12.loc['02-1-Cost of Revenue']/df12.loc['08-1-Accounts Payable']
#         df12.loc['Accounts Payable Turnover*'] = df12.loc['02-1-Cost of Revenue']/df12.loc['EXTERNAL']
        df12.loc['Accounts Payable Days'] = df12.loc['08-1-Accounts Payable']*daysinyear/df12.loc['02-1-Cost of Revenue']
#         df12.loc['Accounts Payable Days*'] = df12.loc['EXTERNAL']*daysinyear/df12.loc['02-1-Cost of Revenue']

        #other ratios
        df12.loc['PP&E Ratio'] = df12.loc['06-1-Property, Plant & Equipment']/df12.loc['01-1-Revenue']
        df12.loc['Working Capital Turnover'] = df12.loc['01-1-Revenue']/(df12.loc['02-1-Accounts Receivable']+df12.loc['03-1-Inventories']+df12.loc['08-1-Accounts Payable'])
#         df12.loc['Working Capital Turnover*'] = df12.loc['01-1-Revenue']/(df12.loc['02-1-Accounts Receivable']+df12.loc['03-1-Inventories']+df12.loc['EXTERNAL'])

        total_debt = df12.loc['11-1-Debt'] + df12.loc['10-1-Other Short Term Liabilities'] + df12.loc['12-1-Other Long Term Liabilities']
        #debt ratios
        df12.loc['Cash Turnover'] = df12.loc['01-1-Revenue']/df12.loc['01-3-Cash']
        df12.loc['Debt/Equity'] = total_debt/(df12.loc['13-1-Total Shareholders Equity'])
        df12.loc['Debt/Capital'] = total_debt/(df12.loc['13-1-Total Shareholders Equity']-total_debt)
        df12.loc['Debt/TNW'] = total_debt/(df12.loc['07-2-Total Non-Current Assets']-df12.loc['07-1-Goodwill & Intangible Asset'])
        df12.loc['Total Liabilities/Equity'] = (df12.loc['11-2-Total Current Liabilities']+df12.loc['12-2-Total Non-Current Liabilities'])/(df12.loc['13-1-Total Shareholders Equity']-total_debt)
#         df12.loc['Total Liabilities/Equity*'] = (df12.loc['11-2-Total Current Liabilities']-df12.loc['EXTERNAL']+df12.loc['12-2-Total Non-Current Liabilities'])/(df12.loc['13-1-Total Shareholders Equity']-total_debt)
        df12.loc['Total Assets to Equity'] = (df12.loc['04-2-Total Current Assets']+df12.loc['07-2-Total Non-Current Assets'])/df12.loc['13-1-Total Shareholders Equity']


        df12.loc['Debt/EBITDA'] = total_debt/(df12.loc['07-2-EBIT']+df12.loc['05-1-Depreciation/Amortization'])
        df12.loc['Capital Structure Impact'] = df12.loc['08-2-EBT']/df12.loc['07-2-EBIT']
        df12.loc['Acid Test'] = (df12.loc['04-2-Total Current Assets']-df12.loc['03-1-Inventories'])/df12.loc['11-2-Total Current Liabilities']
#         df12.loc['Acid Test*'] =(df12.loc['04-2-Total Current Assets']-df12.loc['03-1-Inventories'])/(df12.loc['11-2-Total Current Liabilities']-df12.loc['EXTERNAL'])
        df12.loc['ROE'] = df12.loc['10-1-Net Income']/df12.loc['13-1-Total Shareholders Equity']
        df12.loc['ROA'] = df12.loc['10-1-Net Income']/(df12.loc['04-2-Total Current Assets']+df12.loc['07-2-Total Non-Current Assets'])
    except:
        pass
    
    statement_3_dict[key] = (df12*-1).round(3).reset_index().fillna(0)

#######################
zid_dict = {100001:'Gulshan',100002:'Central',100003:'E-commerce'}

pl_data_income = main_data_dict_pl
income_dict = {}
for key in pl_data_income:
    df = pl_data_income[key]
    for i in year_list:
        income_dict[key] = [df[df['xacctype'] == 'Income'].sum()[i] for i in year_list]
income_df = pd.DataFrame.from_dict(income_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
income_df['Name'] = income_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
income_df = income_df[new_cols]
income_df.loc[len(income_df.index),:] = income_df.sum(axis=0,numeric_only=True)
income_df.at[len(income_df.index)-1,'Name'] = 'Total'

pl_data_COGS = main_data_dict_pl
COGS_dict = {}
for key in pl_data_COGS:
    df = pl_data_COGS[key]
    for i in year_list:
        if key == 100001:
            COGS_dict[key] = [df[df['xacc'] == '04010020'][i][df.loc[df['xacc']=='04010020'].index[0]] for i in year_list]
        else:
            COGS_dict[key] = [df[df['xacc'] == '4010020'][i][df.loc[df['xacc']=='4010020'].index[0]] for i in year_list]
COGS_df = pd.DataFrame.from_dict(COGS_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
COGS_df['Name'] = COGS_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
COGS_df = COGS_df[new_cols]
COGS_df.loc[len(COGS_df.index),:] = COGS_df.sum(axis=0,numeric_only=True)
COGS_df.at[len(COGS_df.index)-1,'Name'] = 'Total'


pl_data_expense = main_data_dict_pl
expense_dict = {}
for key in pl_data_expense:
    df = pl_data_expense[key]
    for i in year_list:
        expense_dict[key] = [df[(df['xacc'] != '04010020') & (df['xacctype'] == 'Expenditure')].sum()[i] for i in year_list]
expense_df = pd.DataFrame.from_dict(expense_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
expense_df['Name'] = expense_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
expense_df = expense_df[new_cols]
expense_df.loc[len(expense_df.index),:] = expense_df.sum(axis=0,numeric_only=True)
expense_df.at[len(expense_df.index)-1,'Name'] = 'Total'

pl_data_profit = main_data_dict_pl
profit_dict = {}
for key in pl_data_profit:
    df = pl_data_profit[key]
    for i in year_list:
        profit_dict[key] = [df.sum()[i] for i in year_list]
profit_df = pd.DataFrame.from_dict(profit_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
profit_df['Name'] = profit_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
profit_df = profit_df[new_cols]
profit_df.loc[len(profit_df.index),:] = profit_df.sum(axis=0,numeric_only=True)
profit_df.at[len(profit_df.index)-1,'Name'] = 'Total'

## taxes should be separated according to VAT and income tax. Also I think now the structure is even more different
pl_data_EBITDA = level_3_dict
EBITDA_dict = {}
for key in pl_data_EBITDA:
    df = pl_data_EBITDA[key]
    for i in year_list:
        EBITDA_dict[key] = [df[(df['xhrc2']!='0625-Property Tax & Others') & (df['xhrc2']!='0604-City Corporation Tax') & (df['xhrc2']!='0629- HMBR VAT & Tax Expenses') & (df['xhrc2']!='0629-VAT & Tax Expenses') & (df['xhrc2']!='0630- Bank Interest & Charges') & (df['xhrc2']!='0630-Bank Interest & Charges') & (df['xhrc2']!='0633-Interest-Loan') & (df['xhrc2']!='0633-Interest on Loan') & (df['xhrc2']!='0636-Depreciation') & (df['xhrc2']!='Profit/Loss')].sum()[i] for i in year_list]
EBITDA_df = pd.DataFrame.from_dict(EBITDA_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
EBITDA_df['Name'] = EBITDA_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
EBITDA_df = EBITDA_df[new_cols]
EBITDA_df.loc[len(EBITDA_df.index),:] = EBITDA_df.sum(axis=0,numeric_only=True)
EBITDA_df.at[len(EBITDA_df.index)-1,'Name'] = 'Total'

pl_data_tax = level_3_dict
tax_dict = {}
for key in pl_data_tax:
    df = pl_data_tax[key]
    for i in year_list:
        tax_dict[key] = [df[(df['xhrc2']=='0625-Property Tax & Others') | (df['xhrc2']=='0604-City Corporation Tax') | (df['xhrc2']=='0629- HMBR VAT & Tax Expenses') | (df['xhrc2']=='0629-VAT & Tax Expenses')].sum()[i] for i in year_list]
tax_df = pd.DataFrame.from_dict(tax_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
tax_df['Name'] = tax_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
tax_df = tax_df[new_cols]
tax_df.loc[len(tax_df.index),:] = tax_df.sum(axis=0,numeric_only=True)
tax_df.at[len(tax_df.index)-1,'Name'] = 'Total'

pl_data_interest = level_3_dict
interest_dict = {}
for key in pl_data_interest:
    df = pl_data_interest[key]
    for i in year_list:
        interest_dict[key] = [df[(df['xhrc2']=='0630- Bank Interest & Charges') | (df['xhrc2']!='0630-Bank Interest & Charges') | (df['xhrc2']=='0633-Interest-Loan') | (df['xhrc2']=='0633-Interest on Loan')].sum()[i] for i in year_list] ### here 
interest_df = pd.DataFrame.from_dict(interest_dict,orient = 'index', columns = [i for i in year_list]).reset_index()
interest_df['Name'] = interest_df['index'].map(zid_dict)
new_cols = ['index','Name']+[i for i in year_list] 
interest_df = interest_df[new_cols]
interest_df.loc[len(interest_df.index),:] = interest_df.sum(axis=0,numeric_only=True)
interest_df.at[len(interest_df.index)-1,'Name'] = 'Total'

##New code addition by director on 19112022 regarding ap ar and inv
# pl_data_apari = main_data_dict_bs
# apari_dict = {}
# for key in pl_data_apari:
#     if key != 100000:
#         df = pl_data_apari[key]
#         apari_dict[key] = df[(df['xacc'] == '09030001')|(df['xacc'] == '01030001')|(df['xacc'] == '01060003')|(df['xacc'] == '01060003')]
#         apari_dict[key]['Business'] = key
#     apari_df = pd.concat([apari_dict[key] for key in apari_dict],axis=0)
#     apari_df['Name'] = apari_df['Business'].map(zid_dict)

gulshan_pl = main_data_dict_pl[100001]
central_pl = main_data_dict_pl[100002]
ecommerce_pl = main_data_dict_pl[100003]

### Blance Sheet
gulshan_bs = main_data_dict_bs[100001]
central_bs = main_data_dict_bs[100002]
ecommerce_bs = main_data_dict_bs[100003]

### Summery Details
gulshan_summery = level_1_dict[100001]
central_summery = level_1_dict[100002]
ecommerce_summery = level_1_dict[100003]

gulshan_summery_lvl_4 = level_4_dict[100001]
central_summery_lvl_4 = level_4_dict[100002]
ecommerce_summery_lvl_4 = level_4_dict[100003]

gulshan_summery_lvl_4_bs = level_4_dict_bs[100001]
central_summery_lvl_4_bs = level_4_dict_bs[100002]
ecommerce_summery_lvl_4_bs = level_4_dict_bs[100003]

gulshan_summery_statements = statement_3_dict[100001]
central_summery_statements = statement_3_dict[100002]
ecommerce_summery_statements = statement_3_dict[100003]

###Excel File Generate
profit_excel = f'p&l{start_year}_{start_month}_{end_month}.xlsx'
balance_excel = f'b&l{start_year}_{start_month}_{end_month}.xlsx'
details_excel = f'profitLossDetail{start_year}_{start_month}_{end_month}.xlsx'
lvl_4_details_excel = f'level_4{start_year}_{start_month}_{end_month}.xlsx'
lvl_4_bs_details_excel = f'level_4_bs{start_year}_{start_month}_{end_month}.xlsx'
statement_3_dict_excel = f'statement_3_dict{start_year}_{start_month}_{end_month}.xlsx'

with pd.ExcelWriter(profit_excel) as writer:  
    gulshan_pl.to_excel(writer, sheet_name='100001')
    central_pl.to_excel(writer, sheet_name='100002')
    ecommerce_pl.to_excel(writer, sheet_name='100003')

with pd.ExcelWriter(balance_excel) as writer:  
    gulshan_bs.to_excel(writer, sheet_name='100001')
    central_bs.to_excel(writer, sheet_name='100002')
    ecommerce_bs.to_excel(writer, sheet_name='100003')

with pd.ExcelWriter(details_excel) as writer:  
    gulshan_summery.to_excel(writer, sheet_name='100001')
    central_summery.to_excel(writer, sheet_name='100002')
    ecommerce_summery.to_excel(writer, sheet_name='100003')

with pd.ExcelWriter(lvl_4_details_excel) as writer:  
    gulshan_summery_lvl_4.to_excel(writer, sheet_name='100001')
    central_summery_lvl_4.to_excel(writer, sheet_name='100002')
    ecommerce_summery_lvl_4.to_excel(writer, sheet_name='100003')

with pd.ExcelWriter(lvl_4_bs_details_excel) as writer:  
    gulshan_summery_lvl_4_bs.to_excel(writer, sheet_name='100001')
    central_summery_lvl_4_bs.to_excel(writer, sheet_name='100002')
    ecommerce_summery_lvl_4_bs.to_excel(writer, sheet_name='100003')

with pd.ExcelWriter(statement_3_dict_excel) as writer:  
    gulshan_summery_statements.to_excel(writer, sheet_name='100001')
    central_summery_statements.to_excel(writer, sheet_name='100002')
    ecommerce_summery_statements .to_excel(writer, sheet_name='100003')

###Email    
me = "pythonhmbr12@gmail.com"
you = [ "asaddat87@gmail.com","ithmbrbd@gmail.com", "motiurhmbr@gmail.com", "hmbr12@gmail.com", "fixitc.central@gmail.com"]
#you = [ "ithmbrbd@gmail.com"]

msg = MIMEMultipart('alternative')
msg['Subject'] = f"Yearly profit & loss Fixit from {start_year} - {start_month} to  {end_month}"
msg['From'] = me
msg['To'] = ", ".join(you)

HEADER = '''
<html>
    <head>
    </head>
    <body>
'''
FOOTER = '''
    </body>
</html>
'''
with open('hello.html','w') as f:
    f.write(HEADER)
    f.write('Gulshan Details')
    f.write(gulshan_summery.to_html(classes='df_summery'))
    f.write('Central Details')
    f.write(central_summery.to_html(classes='df_summery1'))
    f.write('Ecommerce Details')
    f.write(ecommerce_summery.to_html(classes='df_summery2'))
    f.write('Cost of good sold details')
    f.write(COGS_df.to_html(classes='df_summery10'))
    f.write('Income Details')
    f.write(income_df.to_html(classes='df_summery11'))
    f.write('Expense details')
    f.write(expense_df.to_html(classes='df_summery12'))
    f.write('Profit Details')
    f.write(profit_df.to_html(classes='df_summery13'))
    f.write(FOOTER)

filename = "hello.html"
f = open(filename)
attachment = MIMEText(f.read(),'html')
msg.attach(attachment)

part1 = MIMEBase('application', "octet-stream")
part1.set_payload(open(profit_excel, "rb").read())
encoders.encode_base64(part1)
part1.add_header('Content-Disposition', 'attachment; filename="p&l_.xlsx"')
msg.attach(part1)

part2 = MIMEBase('application', "octet-stream")
part2.set_payload(open(balance_excel, "rb").read())
encoders.encode_base64(part2)
part2.add_header('Content-Disposition', 'attachment; filename="b&l_.xlsx"')
msg.attach(part2)

part3 = MIMEBase('application', "octet-stream")
part3.set_payload(open(details_excel, "rb").read())
encoders.encode_base64(part3)
part3.add_header('Content-Disposition', 'attachment; filename="profitLossDetail_.xlsx"')
msg.attach(part3)

part4 = MIMEBase('application', "octet-stream")
part4.set_payload(open(lvl_4_details_excel, "rb").read())
encoders.encode_base64(part4)
part4.add_header('Content-Disposition', 'attachment; filename="level_4_.xlsx"')
msg.attach(part4)

part5 = MIMEBase('application', "octet-stream")
part5.set_payload(open(lvl_4_bs_details_excel, "rb").read())
encoders.encode_base64(part5)
part5.add_header('Content-Disposition', 'attachment; filename="level_4_bs.xlsx"')
msg.attach(part5)

part6 = MIMEBase('application', "octet-stream")
part6.set_payload(open(statement_3_dict_excel, "rb").read())
encoders.encode_base64(part6)
part6.add_header('Content-Disposition', 'attachment; filename="statement_3_dict_.xlsx"')
msg.attach(part6)

username = 'pythonhmbr12'
password = 'vksikttussvnbqef'

s = smtplib.SMTP('smtp.gmail.com:587')
s.starttls()
s.login(username, password)
s.sendmail(me,you,msg.as_string())
s.quit()

