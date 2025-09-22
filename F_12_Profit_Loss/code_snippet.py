main_data_dict_pl = {}
for i in zid_list_hmbr:
    df_master = get_gl_master(i)
    df_master = df_master[(df_master['xacctype']!='Asset') & (df_master['xacctype']!='Liability')]
#     df_main = get_gl_details(i,start_year,start_month,end_month)
#     df_main = df_main.groupby(['xacc'])['sum'].sum().reset_index().round(1).rename(columns={'sum':start_year})
    for item,idx in enumerate(year_list):
        df = get_gl_details(i,idx,start_month,end_month)
        df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
        if item == 0:
#             df_new = df_main.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
            df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
        else:
            df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    main_data_dict_pl[i] = df_new.merge(income_label[['xhrc4','Income Statement']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)

# df_trade = get_gl_details_project(zid_trade,project_trade,start_year,start_month,end_month)
# df_trade = df_trade.groupby(['xacc'])['sum'].sum().reset_index().round(1).rename(columns={'sum':start_year})
df_master = get_gl_master(zid_trade)
df_master = df_master[(df_master['xacctype']!='Asset') & (df_master['xacctype']!='Liability')]
for item,idx in enumerate(year_list):
    df = get_gl_details_project(zid_trade,project_trade,idx,start_month,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
main_data_dict_pl[zid_trade] = df_new.merge(income_label[['xhrc4','Income Statement']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)

# df_plastic = get_gl_details_project(zid_plastic,project_plastic,start_year,start_month,end_month)
# df_plastic = df_plastic.groupby(['xacc'])['sum'].sum().reset_index().round(1).rename(columns={'sum':start_year})
df_master = get_gl_master(zid_plastic)
df_master = df_master[(df_master['xacctype']!='Asset') & (df_master['xacctype']!='Liability')]
for item,idx in enumerate(year_list):
    df = get_gl_details_project(zid_plastic,project_plastic,idx,start_month,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
main_data_dict_pl[zid_plastic] = df_new.merge(income_label[['xhrc4','Income Statement']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)

df_master = get_gl_master(zid_karigor)
df_master = df_master[(df_master['xacctype']!='Asset') & (df_master['xacctype']!='Liability')]
for item,idx in enumerate(year_list):
    df = get_gl_details_project(zid_karigor,project_karigor,idx,start_month,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    print(df['sum'].sum(),'profit & loss')
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    print('kargor work is done')
main_data_dict_pl[zid_karigor] = df_new.merge(income_label[['xhrc4','Income Statement']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)

#create master dataframe



main_data_dict_bs = {}
for i in zid_list_hmbr:
    df_master = get_gl_master(i)
    df_master = df_master[(df_master['xacctype']!='Income') & (df_master['xacctype']!='Expenditure')]
    for item,idx in enumerate(year_list):
        df = get_gl_details_bs(i,idx,end_month)
        df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
        if item == 0:
            df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').fillna(0).rename(columns={'sum':idx})
        else:
            df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').fillna(0).rename(columns={'sum':idx})
        main_data_dict_bs[i] = df_new.merge(balance_label[['xhrc4','Balance Sheet']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)

# df_trade_bs = get_gl_details_bs_project(zid_trade,project_trade,start_year,start_month,end_month)
# df_trade_bs = df_trade_bs.groupby(['xacc','xdesc'])['sum'].sum().reset_index().round(1).rename(columns={'sum':start_year})
df_master = get_gl_master(zid_trade)
df_master = df_master[(df_master['xacctype']!='Income') & (df_master['xacctype']!='Expenditure')]
for item,idx in enumerate(year_list):
    df = get_gl_details_bs_project(zid_trade,project_trade,idx,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
# df_mst = get_gl_master(i)
# df_new = df_new.merge(df_mst,on='xacc',how='left')
main_data_dict_bs[zid_trade] = df_new.merge(balance_label[['xhrc4','Balance Sheet']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)

# df_plastic_bs = get_gl_details_bs_project(zid_plastic,project_plastic,start_year,start_month,end_month)
# df_plastic_bs = df_plastic_bs.groupby(['xacc','xdesc'])['sum'].sum().reset_index().round(1).rename(columns={'sum':start_year})
df_master = get_gl_master(zid_plastic)
df_master = df_master[(df_master['xacctype']!='Income') & (df_master['xacctype']!='Expenditure')]
for item,idx in enumerate(year_list):
    df = get_gl_details_bs_project(zid_plastic,project_plastic,idx,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
# df_mst = get_gl_master(i)
# df_new = df_new.merge(df_mst,on='xacc',how='left')
main_data_dict_bs[zid_plastic] = df_new.merge(balance_label[['xhrc4','Balance Sheet']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)

df_master = get_gl_master(zid_karigor)
df_master = df_master[(df_master['xacctype']!='Income') & (df_master['xacctype']!='Expenditure')]
for item,idx in enumerate(year_list):
    df = get_gl_details_bs_project(zid_karigor,project_karigor,idx,end_month)
    df = df.groupby(['xacc'])['sum'].sum().reset_index().round(1)
    if item == 0:
        df_new = df_master.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
    else:
        df_new = df_new.merge(df[['xacc','sum']],on=['xacc'],how='left').rename(columns={'sum':idx}).fillna(0)
# df_mst = get_gl_master(i)
# df_new = df_new.merge(df_mst,on='xacc',how='left')
main_data_dict_bs[zid_karigor] = df_new.merge(balance_label[['xhrc4','Balance Sheet']],on=['xhrc4'],how='left').sort_values(['xacctype'],ascending=True)




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
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(exc_type, exc_tb.tb_lineno)
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
