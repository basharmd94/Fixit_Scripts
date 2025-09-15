"""
🚀 F_02_01_Central.py – Item Bulk Edit and Upload for Central Warehouse

📌 PURPOSE:
    - Bulk upload and edit items for Central warehouse (zid: 100002)
    - Check and insert missing xcodes (brands, colors, materials, etc.)
    - Generate new item codes for duplicate items
    - Update existing items and insert new items into caitem table
    - Export results to Excel and send via email

🔧 DATA SOURCES:
    - Input: central.xlsx file
    - Master: caitem, xcodes tables
    - Database: PostgreSQL via DATABASE_URL in project_config.py

📅 SCHEDULE:
    Runs on-demand for bulk item management

🏢 INPUT:
    - ZID_CENTRAL: 100002 (Central warehouse)
    - Input file: central.xlsx

📧 EMAIL:
    - Recipients: get_email_recipients("F_02_01_Central")
    - Fallback: ithmbrbd@gmail.com

💡 NOTE:
    - Uses parameterized queries for security
    - Handles duplicate item codes by generating new ones
    - Updates existing items and inserts new ones
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, date
from sqlalchemy import create_engine, exc

# === 1. Add project root to Python path ===
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# === 2. Import shared modules ===
from mail import send_mail, get_email_recipients
from project_config import DATABASE_URL

# === 3. Configuration ===
ZID_CENTRAL = 100002  # Central warehouse ID
INPUT_FILE = 'central.xlsx'
OUTPUT_FILE = 'F_02_01_Central_Products.xlsx'
TODAY_DATE = date.today().strftime("%Y-%m-%d")

print(f"📌 Processing for: Central Warehouse (ZID={ZID_CENTRAL})")
print(f"📅 Processing Date: {TODAY_DATE}")

# === 4. Create database engine ===
engine = create_engine(DATABASE_URL)

# === 5. Suppress warnings ===
import warnings
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
pd.set_option('display.float_format', '{:.2f}'.format)

# === 6. Read and Process Input File ===
print("📥 Reading input file...")
df_input_file_by_user = pd.read_excel(INPUT_FILE, engine='openpyxl')

# Rename columns to match database schema
column_mapping = {
    'Business ID': 'zid', 'Item Code': 'xitem', 'Description': 'xdesc', 
    'Long Description': 'xlong', 'Standard Cost': 'xstdcost', 'Standard Price': 'xstdprice',
    'Selling Unit': 'xunitsel', 'Stocking Unit': 'xunitstk', 'Alternative Unit': 'xunitalt',
    'Issue Unit': 'xunitiss', 'Packing Unit': 'xunitpck', 'Statistical Unit': 'xunitsta',
    'Item Group': 'xgitem', 'Local/Import': 'xduty', 'Country of Origin': 'xorigin',
    'Supplier Number': 'xsup', 'Weight': 'xwtunit', 'Weight Unit': 'xunitwt',
    'Power': 'xitemnew', 'Voltage': 'xitemold', 'Measurement Unit 1': 'xeccrange',
    'Measurement Unit 2': 'xrandom', 'Measurement Unit 3': 'xnameonly',
    'Design': 'xdrawing', 'RPM': 'xeccnum', 'Measurement-1': 'xscode',
    'Measurement-2': 'xresponse', 'Measurement-3': 'xslot', 'Unit of Length': 'xunitlen',
    'Length': 'xl', 'Width': 'xw', 'Height': 'xh', 'Brand': 'xbrand',
    'Model': 'xalias', 'Color': 'xcolor', 'Material': 'xmaterial', 'Remarks': 'xremark'
}

df_input_file_by_user = df_input_file_by_user.rename(columns=column_mapping)
# === 7. Process XCodes Data ===
print("🔄 Processing xcodes data...")
df_need_to_check_values = pd.DataFrame.from_records(
    df_input_file_by_user, 
    columns=['zid', 'xeccrange', 'xrandom', 'xnameonly', 'xbrand', 'xcolor', 'xmaterial']
).rename(columns={
    'zid': 'zid', 'xeccrange': 'Measurement Unit 1', 'xrandom': 'Measurement Unit 2',
    'xnameonly': 'Measurement Unit 3', 'xbrand': 'Brand', 'xcolor': 'Color', 'xmaterial': 'Material'
})

df_need_to_check_values_m = df_need_to_check_values.melt(id_vars=["zid"], var_name="xtype", value_name="xcode")
df_value_convert_to_tuple = df_need_to_check_values_m.to_records(index=False)
df_value_converted_to_tuple = list(df_value_convert_to_tuple)

# Remove duplicates
remove_duplicates_for_tuple = []
for i in df_value_converted_to_tuple:
    if i not in remove_duplicates_for_tuple:
        remove_duplicates_for_tuple.append(i)

to_tuple_list = ', '.join(map(str, remove_duplicates_for_tuple))

# === 8. SQL Query Functions (Parameterized) ===
def get_all_xcodes_from_erp(values_list):
    """Fetch existing xcodes from ERP."""
    query = "SELECT * FROM xcodes WHERE (zid, xtype, xcode) IN (VALUES %s)"
    return pd.read_sql(query % values_list, con=engine)

# === 9. Check and Insert Missing XCodes ===
print("🔍 Checking existing xcodes...")
already_existing_value = get_all_xcodes_from_erp(to_tuple_list)
value_already_existing = already_existing_value[['zid', 'xtype', 'xcode']]
df_value_already_existing = value_already_existing.to_records(index=False)
df_value_already_existing = list(df_value_already_existing)

# Compare xcodes data between ERP and Excel File
need_to_insert_value_in_xcodes = [sub for sub in remove_duplicates_for_tuple if sub not in df_value_already_existing]
print(f"📝 Need to insert {len(need_to_insert_value_in_xcodes)} new xcodes")

if need_to_insert_value_in_xcodes:
    to_tuple_list_for_insert = ', '.join(map(str, need_to_insert_value_in_xcodes))
    
    def insert_into_xcodes(values):
        """Insert new xcodes into database."""
        try:
            query = f"INSERT INTO xcodes (zid, xtype, xcode) VALUES {values}"
            pd.read_sql(query, con=engine)
            return "Data has been inserted successfully"
        except exc.SQLAlchemyError as e:
            print(f"❌ Error inserting xcodes: {e}")
            return "Failed to insert data"
    
    result = insert_into_xcodes(to_tuple_list_for_insert)
    print(f"✅ XCodes insertion: {result}")
else:
    print("✅ All xcodes already exist in database")

# === 10. Get Existing Items and Compare ===
def get_all_item_from_erp(zid):
    """Fetch all existing items from caitem table."""
    query = """
    SELECT distinct caitem.zid, caitem.xitem, caitem.xdesc, caitem.xgitem, caitem.xstdprice
    FROM caitem
    WHERE caitem.zid = %(zid)s
    ORDER BY caitem.xitem ASC
    """
    return pd.read_sql(query, con=engine, params={'zid': zid})

print("📥 Fetching existing items from ERP...")
df_all_item_from_erp = get_all_item_from_erp(ZID_CENTRAL)
df_new_comparison_data_file = pd.merge(df_input_file_by_user, df_all_item_from_erp, on='xitem')

# === 11. Identify New Items and Generate Codes ===
print("🔍 Identifying new items...")
df_not_available_in_erp = df_input_file_by_user[~df_input_file_by_user['xitem'].isin(df_new_comparison_data_file['xitem'])]
df_not_available_in_erp = df_not_available_in_erp.sort_values(by='xitem', ascending=True)
xitem_from_file_upload = df_not_available_in_erp['xitem']
xitem_from_file_upload_list = xitem_from_file_upload.to_list()
duplicate_item_code = {i: xitem_from_file_upload_list.count(i) for i in xitem_from_file_upload_list}

def get_all_xitem_from_erp(itemcode):
    """Get all item codes that start with given prefix."""
    sql_statement = "SELECT xitem FROM caitem WHERE zid = %(zid)s AND xitem LIKE %(pattern)s"
    df_items_code = pd.read_sql(
        sql_statement, 
        con=engine, 
        params={'zid': ZID_CENTRAL, 'pattern': f"{itemcode}%"}
    )['xitem'].to_list()
    return df_items_code


# === 12. Generate New Item Codes ===
print("🔢 Generating new item codes...")
new_code_list = []
for key, value in duplicate_item_code.items():
    df_new_list = get_all_xitem_from_erp(key)
    if df_new_list:
        df_new_list = [l.split('-') for l in df_new_list if '-' in l]
        if df_new_list:
            df_2nd_list = [l[1] for l in df_new_list if len(l) > 1]
            df_3rd_list = [int(x) for x in df_2nd_list if x.isdigit()]
            max_value = max(df_3rd_list) if df_3rd_list else 0
        else:
            max_value = 0
    else:
        max_value = 0
    
    new_number_list = []
    for i in range(value):
        max_value += 1
        new_number_list.append(max_value)
        
    for i in new_number_list:
        new_code = key + str(i)
        new_code_list.append(new_code)

print(f"📝 Generated {len(new_code_list)} new item codes")

# === 13. Update Item Codes and Add Constants ===
print("🔄 Updating item codes...")
for i in df_not_available_in_erp['xitem'].to_list():
    for idx, item in enumerate(new_code_list):
        if item.startswith(i):
            df_not_available_in_erp['xitem'].iloc[idx] = item
            continue
            new_code_list.pop(idx)

# Add constant columns for new items
df_not_available_in_erp['ztime'] = datetime.now()
df_not_available_in_erp['xcatful'] = 'Auto'
df_not_available_in_erp['xtypestk'] = 'Stock-N-Sell'
df_not_available_in_erp['xwh'] = 'Fixit Central'
df_not_available_in_erp['xcfiss'] = 1
df_not_available_in_erp['xcfpck'] = 1
df_not_available_in_erp['xcfsta'] = 1

# === 14. Insert New Items into Database ===
if not df_not_available_in_erp.empty:
    print("📥 Inserting new items into database...")
    df_value_need_to_insert = df_not_available_in_erp.to_records(index=False)
    df_value_need_to_insert = list(df_value_need_to_insert)
    to_tuple_list_for_product_insertation = ', '.join(map(str, df_value_need_to_insert))
    
    def insert_into_caitem_table(tablevalues):
        """Insert new items into caitem table."""
        try:
            query = f"""INSERT INTO caitem (zid, xitem, xdesc, xlong, xstdcost, xstdprice, xunitsel, xunitstk, xunitalt, xunitiss, xunitpck, xunitsta, xgitem, xduty, xorigin, xsup, xwtunit, xunitwt, xitemnew, xitemold, xscode, xeccrange, xresponse, xrandom, xslot, xnameonly, xdrawing, xeccnum, xunitlen, xl, xw, xh, xbrand, xalias, xcolor, xmaterial, xremark, ztime, xcatful, xtypestk, xwh, xcfiss, xcfpck, xcfsta) VALUES {tablevalues}"""
            pd.read_sql(query, con=engine)
            return "Data has been inserted successfully"
        except exc.SQLAlchemyError as e:
            print(f"❌ Error inserting items: {e}")
            return "Failed to insert data"
    
    result = insert_into_caitem_table(to_tuple_list_for_product_insertation)
    print(f"✅ Items insertion: {result}")
else:
    print("✅ No new items to insert")

# === 15. Prepare Data for Updates and Export ===
print("🔄 Preparing data for updates...")
dr_update_table_1 = df_new_comparison_data_file.rename(columns={
    'zid_x': 'zid', 'xdesc_x': 'xdesc', 'xstdprice_x': 'xstdprice', 'xgitem_x': 'xgitem'
})
dr_update_table_1.drop(['xgitem_y', 'xdesc_y', 'zid_y', 'xstdprice_y'], axis=1, inplace=True)
df_all_rows = pd.concat([dr_update_table_1, df_not_available_in_erp])

# === 16. Update Existing Items ===
def update_value_into_caitem_table(xlong, zid, xitem, xdesc, xstdcost, xstdprice, xduty, xorigin, xsup, xwtunit, xunitwt, xitemnew, xitemold, xdrawing, xeccnum, xunitlen, xl, xw, xh, xbrand, xalias, xcolor, xmaterial, xeccrange, xscode, xrandom, xresponse, xnameonly, xslot):
    """Update existing item in caitem table."""
    try:
        query = """
        UPDATE caitem SET
            xlong = %(xlong)s, xdesc = %(xdesc)s, xstdcost = %(xstdcost)s, xstdprice = %(xstdprice)s,
            xduty = %(xduty)s, xorigin = %(xorigin)s, xsup = %(xsup)s, xwtunit = %(xwtunit)s,
            xunitwt = %(xunitwt)s, xitemnew = %(xitemnew)s, xitemold = %(xitemold)s,
            xdrawing = %(xdrawing)s, xeccnum = %(xeccnum)s, xunitlen = %(xunitlen)s,
            xl = %(xl)s, xw = %(xw)s, xh = %(xh)s, xbrand = %(xbrand)s, xalias = %(xalias)s,
            xcolor = %(xcolor)s, xmaterial = %(xmaterial)s, xeccrange = %(xeccrange)s,
            xscode = %(xscode)s, xrandom = %(xrandom)s, xresponse = %(xresponse)s,
            xnameonly = %(xnameonly)s, xslot = %(xslot)s
        WHERE zid = %(zid)s AND xitem = %(xitem)s
        """
        pd.read_sql(query, con=engine, params={
            'xlong': xlong, 'zid': zid, 'xitem': xitem, 'xdesc': xdesc, 'xstdcost': xstdcost,
            'xstdprice': xstdprice, 'xduty': xduty, 'xorigin': xorigin, 'xsup': xsup,
            'xwtunit': xwtunit, 'xunitwt': xunitwt, 'xitemnew': xitemnew, 'xitemold': xitemold,
            'xdrawing': xdrawing, 'xeccnum': xeccnum, 'xunitlen': xunitlen, 'xl': xl,
            'xw': xw, 'xh': xh, 'xbrand': xbrand, 'xalias': xalias, 'xcolor': xcolor,
            'xmaterial': xmaterial, 'xeccrange': xeccrange, 'xscode': xscode,
            'xrandom': xrandom, 'xresponse': xresponse, 'xnameonly': xnameonly, 'xslot': xslot
        })
        return "Data has been updated successfully"
    except exc.SQLAlchemyError as e:
        print(f"❌ Error updating item: {e}")
        return "Failed to update data"

# === 17. Update Existing Items ===
update_xitem_list = df_new_comparison_data_file['xitem'].to_list()
print(f"🔄 Updating {len(update_xitem_list)} existing items...")

for idx, item in enumerate(update_xitem_list):
    row = df_new_comparison_data_file[df_new_comparison_data_file['xitem'] == item].iloc[0]
    
    result = update_value_into_caitem_table(
        xlong=row['xlong'], zid=row['zid_x'], xitem=item, xdesc=row['xdesc_x'],
        xstdcost=row['xstdcost'], xstdprice=row['xstdprice_x'], xduty=row['xduty'],
        xorigin=row['xorigin'], xsup=row['xsup'], xwtunit=row['xwtunit'],
        xunitwt=row['xunitwt'], xitemnew=row['xitemnew'], xitemold=row['xitemold'],
        xdrawing=row['xdrawing'], xeccnum=row['xeccnum'], xunitlen=row['xunitlen'],
        xl=row['xl'], xw=row['xw'], xh=row['xh'], xbrand=row['xbrand'],
        xalias=row['xalias'], xcolor=row['xcolor'], xmaterial=row['xmaterial'],
        xeccrange=row['xeccrange'], xscode=row['xscode'], xrandom=row['xrandom'],
        xresponse=row['xresponse'], xnameonly=row['xnameonly'], xslot=row['xslot']
    )

# === 18. Export Results to Excel ===
print("📊 Exporting results to Excel...")
df_all_rows = df_all_rows.set_index('xitem')
df_all_rows.to_excel(OUTPUT_FILE, sheet_name='Sheet_name_1', engine='openpyxl')

print(f"✅ Report generated successfully:")
print(f"   📊 Excel: {OUTPUT_FILE}")
print(f"   📈 Total items processed: {len(df_all_rows)}")

# === 19. Send Email ===
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Failed to get recipients: {e}. Using fallback.")
    recipients = ["ithmbrbd@gmail.com"]

subject = f"Fixit-02 Central Item Bulk Upload – {TODAY_DATE}"
body_text = "Please find attached the processed item data for Central warehouse."

send_mail(
    subject=subject,
    bodyText=body_text,
    html_body=[(df_all_rows.reset_index(), "Central Item Upload Results")],
    attachment=[OUTPUT_FILE],
    recipient=recipients
)

print("📧 Email sent successfully.")

# === 20. Cleanup ===
engine.dispose()
print("✅ Script completed successfully.")
