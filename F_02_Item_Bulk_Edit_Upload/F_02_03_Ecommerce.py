from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from openpyxl import load_workbook
from datetime import date,datetime,timedelta
import psycopg2
import time
import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from sqlalchemy import exc

#Read Excel File
data = pd.ExcelFile("central.xlsx",engine='openpyxl')
df_input_file_by_user = pd.read_excel(r'central.xlsx', engine='openpyxl')
df_input_file_by_user = df_input_file_by_user.rename(columns={'Business ID': 'zid', 'Item Code':'xitem', 'Description':'xdesc', 'Long Description':'xlong', 'Standard Cost':'xstdcost', 'Standard Price':'xstdprice', 'Selling Unit':'xunitsel', 'Stocking Unit':'xunitstk', 'Alternative Unit':'xunitalt', 'Issue Unit':'xunitiss', 'Packing Unit':'xunitpck', 'Statistical Unit':'xunitsta', 'Item Group':'xgitem', 'Local/Import':'xduty', 'Country of Origin':'xorigin', 'Supplier Number':'xsup', 'Weight':'xwtunit', 'Weight Unit':'xunitwt', 'Power':'xitemnew', 'Voltage':'xitemold', 'Measurement Unit 1':'xeccrange', 'Measurement Unit 2':'xrandom', 'Measurement Unit 3':'xnameonly', 'Design':'xdrawing', 'RPM':'xeccnum', 'Measurement-1':'xscode', 'Measurement-2':'xresponse', 'Measurement-3':'xslot', 'Unit of Length':'xunitlen', 'Length':'xl', 'Width':'xw', 'Height':'xh', 'Brand':'xbrand', 'Model':'xalias', 'Color':'xcolor', 'Material':'xmaterial', 'Remarks':'xremark'})
df_need_to_check_values = pd.DataFrame.from_records(df_input_file_by_user, columns =['zid','xeccrange','xrandom','xnameonly', 'xbrand', 'xcolor', 'xmaterial']).rename(columns={'zid': 'zid','xeccrange':'Measurement Unit 1','xrandom':'Measurement Unit 2','xnameonly':'Measurement Unit 3', 'xbrand': 'Brand', 'xcolor': 'Color', 'xmaterial': 'Material'})
df_need_to_check_values_m = df_need_to_check_values.melt(id_vars=["zid"],var_name="xtype",value_name="xcode")
df_value_convert_to_tuple = df_need_to_check_values_m.to_records(index=False)
df_value_converted_to_tuple = list(df_value_convert_to_tuple)
remove_duplicates_for_tuple = []
for i in df_value_converted_to_tuple:
    if i not in remove_duplicates_for_tuple:
        remove_duplicates_for_tuple.append(i)


to_tuple_list = ', '.join(map(str, remove_duplicates_for_tuple))

#Get xcodes data From ERP
def get_all_xcodes_from_erp(list):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df_items_character = pd.read_sql("""SELECT * FROM xcodes WHERE (zid, xtype, xcode) IN (VALUES %s)"""%(list),con = engine)
    return df_items_character

already_exising_value =get_all_xcodes_from_erp(to_tuple_list)
value_already_existing = already_exising_value[['zid','xtype', 'xcode']]
df_value_already_existing = value_already_existing.to_records(index=False)
df_value_already_existing = list(df_value_already_existing)

#Compare xcodes data between ERP and Excel File
need_to_insert_value_in_xcodes =  [sub for sub in remove_duplicates_for_tuple if sub not in df_value_already_existing]
#print(need_to_insert_value_in_xcodes)
to_tuple_list_for_insert = ', '.join(map(str, need_to_insert_value_in_xcodes))

#inserted the compared file to xcodes table
def insert_into_xcodes(values):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    try:
        df_items_character_insertation = pd.read_sql("""INSERT INTO xcodes (zid, xtype, xcode) VALUES %s"""%(values),con = engine)
    except exc.SQLAlchemyError as e:
        print (e)
        
    return "Data has been inserted successfully"

insert_into_xcodes(to_tuple_list_for_insert)

#Get All item from caitem
def get_all_item_from_erp(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df_items = pd.read_sql("""SELECT distinct caitem.zid, caitem.xitem, caitem.xdesc, caitem.xgitem, caitem.xstdprice\n
        FROM caitem\n
        where caitem.zid='%s'\n
        ORDER BY caitem.xitem ASC"""%(zid),con = engine)
    return df_items

zid_fixit = 100003

df_all_item_from_erp = get_all_item_from_erp(zid_fixit)
df_new_comparison_data_file = pd.merge(df_input_file_by_user,df_all_item_from_erp,on='xitem')

#Compared those item with user file is it there or not
df_not_available_in_erp = df_input_file_by_user[~df_input_file_by_user['xitem'].isin(df_new_comparison_data_file['xitem'])]
df_not_available_in_erp = df_not_available_in_erp.sort_values(by='xitem', ascending=True)

#joininging the constant column in the excel file
df_not_available_in_erp['ztime'] = datetime.datetime.now()
df_not_available_in_erp['xcatful'] = 'Auto'
df_not_available_in_erp['xtypestk'] = 'Stock-N-Sell'
df_not_available_in_erp['xwh'] = 'Ecommerce Warehouse'
df_not_available_in_erp['xcfiss'] = 1
df_not_available_in_erp['xcfpck'] = 1
df_not_available_in_erp['xcfsta'] = 1

#Creating tuple value for insert
df_value_need_to_insert = df_not_available_in_erp.to_records(index=False)
df_value_need_to_insert = list(df_value_need_to_insert)
#print(df_value_need_to_insert)
to_tuple_list_for_product_insertation = ', '.join(map(str, df_value_need_to_insert))
#print(to_tuple_list_for_product_insertation)

#Query About Bulk Insert data into postgres
def insert_into_caitem_table(tablevalues):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    try:
        df_new_items_insertation = pd.read_sql("""INSERT INTO caitem (zid, xitem, xdesc,xlong,xstdcost,xstdprice,xunitsel,xunitstk,xunitalt,xunitiss,xunitpck,xunitsta,xgitem,xduty,xorigin,xsup,xwtunit,xunitwt, xitemnew, xitemold, xscode,xeccrange, xresponse, xrandom, xslot, xnameonly, xdrawing, xeccnum, xunitlen,xl,xw,xh,xbrand,xalias,xcolor,xmaterial,xremark,ztime,xcatful,xtypestk,xwh,xcfiss,xcfpck,xcfsta) VALUES %s"""%(tablevalues),con = engine)
    except exc.SQLAlchemyError as e:
        print (e)
        
    return "Data has been inserted successfully"

#Insert Data into postgres
insert_into_caitem_table(to_tuple_list_for_product_insertation)

dr_update_table_1 = df_new_comparison_data_file.rename(columns={'zid_x': 'zid','xdesc_x':'xdesc','xstdprice_x':'xstdprice','xgitem_x':'xgitem'})
dr_update_table_1.drop(['xgitem_y', 'xdesc_y', 'zid_y', 'xstdprice_y'], axis=1, inplace=True)
df_all_rows = pd.concat([dr_update_table_1, df_not_available_in_erp])


#Query About Data Update
def update_value_into_caitem_table(xlong,zid, xitem, xdesc, xstdcost, xstdprice, xduty, xorigin, xsup, xwtunit, xunitwt, xitemnew, xitemold, xdrawing, xeccnum, xunitlen, xl, xw, xh, xbrand, xalias, xcolor, xmaterial,xeccrange, xscode,xrandom, xresponse,xnameonly, xslot):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    try:
        df_existing_items_update = pd.read_sql("""UPDATE caitem SET
                xlong = '%s',
                xdesc = '%s',
                xstdcost = %s,
                xstdprice = %s,
                xduty = '%s',
                xorigin = '%s',
                xsup = '%s',
                xwtunit = %s,
                xunitwt = '%s',
                xitemnew = '%s',
                xitemold = '%s',
                xdrawing = '%s',
                xeccnum = '%s',
                xunitlen = '%s',
                xl = %s,
                xw = %s,
                xh = %s,
                xbrand = '%s',
                xalias = '%s',
                xcolor = '%s',
                xmaterial = '%s',
                xeccrange = '%s',
                xscode = '%s',
                xrandom = '%s', 
                xresponse = '%s',
                xnameonly = '%s',
                xslot = '%s'
                WHERE zid = %s
                AND xitem='%s'"""%(xlong, zid, xitem,xdesc, xstdcost, xstdprice, xduty, xorigin, xsup, xwtunit, xunitwt, xitemnew, xitemold, xdrawing, xeccnum, xunitlen, xl, xw, xh, xbrand, xalias, xcolor, xmaterial, xeccrange, xscode, xrandom, xresponse, xnameonly, xslot),con = engine)
    except exc.SQLAlchemyError as e:
        print (e)
    return "Data has been inserted successfully"

#Update data by item code and zid
update_xitem_list = df_new_comparison_data_file['xitem'].to_list()
print(update_xitem_list)

for idx,item in enumerate(update_xitem_list):
    xlong = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xlong'][idx]
    zid = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['zid_x'][idx]
    xdesc = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xdesc_x'][idx]
    xstdcost = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xstdcost'][idx]
    xstdprice = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xstdprice_x'][idx]
    xduty = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xduty'][idx]
    xorigin = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xorigin'][idx]
    xsup = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xsup'][idx]
    xwtunit = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xwtunit'][idx]
    xunitwt =df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xunitwt'][idx]
    xitemnew = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xitemnew'][idx]
    xitemold = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xitemold'][idx]
    xdrawing = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xdrawing'][idx]
    xeccnum = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xeccnum'][idx]
    xunitlen = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xstdprice_x'][idx]
    xl = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xl'][idx]
    xw = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xw'][idx]
    xh = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xh'][idx]
    xbrand = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xbrand'][idx]
    xalias = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xalias'][idx]
    xcolor = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xcolor'][idx]
    xmaterial = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xmaterial'][idx] 
    xeccrange = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xeccrange'][idx]
    xscode = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xscode'][idx]
    xrandom = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xrandom'][idx]
    xresponse = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xresponse'][idx]
    xnameonly = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xnameonly'][idx]
    xslot = df_new_comparison_data_file[df_new_comparison_data_file['xitem']==item]['xslot'][idx]
    print(xlong, xdesc, xstdcost, xstdprice, xbrand, xalias, xcolor, xmaterial,xeccrange, xscode,xrandom, xresponse,xnameonly, xslot, zid, item)
    update_value_into_caitem_table(xlong, xdesc, xstdcost, xstdprice, xduty, xorigin, xsup, xwtunit, xunitwt, xitemnew, xitemold, xdrawing, xeccnum, xunitlen, xl, xw, xh, xbrand, xalias, xcolor, xmaterial,xeccrange, xscode,xrandom, xresponse,xnameonly, xslot, zid, item)

	
df_all_rows=df_all_rows.set_index('xitem')
df_all_rows.to_excel("new_products.xlsx",sheet_name='Sheet_name_1')
me = "pythonhmbr12@gmail.com"
#you = ["ithmbrbd@gmail.com"]
you = ["ithmbrbd@gmail.com","asaddat87@gmail.com","ecommercefixit@gmail.com","fixitc.central@gmail.com"]

msg = MIMEMultipart('alternative')
msg['Subject'] = "New Products Ecommerce"
msg['From'] = me
msg['To'] = ", ".join(you)


part1 = MIMEBase('application', "octet-stream")
part1.set_payload(open("new_products.xlsx", "rb").read())
encoders.encode_base64(part1)
part1.add_header('Content-Disposition', 'attachment; filename="new_products.xlsx"')
msg.attach(part1)

username = 'pythonhmbr12'
password = 'vksikttussvnbqef'

s = smtplib.SMTP('smtp.gmail.com:587')
s.starttls()
s.login(username, password)
s.sendmail(me,you,msg.as_string())
s.quit()
