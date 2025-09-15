import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine

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
ZID_FIXIT = 100001    # Fixit warehouse ID
TODAY_DATE = date.today().strftime("%Y-%m-%d")

CREDIT_SUPPLIER = ['SUP-000002','SUP-000005','SUP-000006','SUP-000007','SUP-000009','SUP-000010',
                   'SUP-000013','SUP-000014','SUP-000015','SUP-000018','SUP-000020','SUP-000022',
                   'SUP-000024','SUP-000025','SUP-000026','SUP-000027','SUP-000030','SUP-000031',
                   'SUP-000033','SUP-000036','SUP-000037','SUP-000038','SUP-000040','SUP-000043',
                   'SUP-000050','SUP-000051','SUP-000053','SUP-000055','SUP-000056','SUP-000058',
                    'SUP-000060','SUP-000062','SUP-000063','SUP-000066','SUP-000068','SUP-000069',
                    'SUP-000070','SUP-000153','SUP-000207','SUP-000277','SUP-000291','SUP-000299',
                    'SUP-000327','SUP-000356', 'SUP-000464', 'SUP-000538', 'SUP-000612','SUP-000656', 'SUP-000673','SUP-000462'
                    ]

print(f"📌 Processing for: Central Warehouse (ZID={ZID_CENTRAL})")
print(f"📅 Report Date: {TODAY_DATE}")

# === 4. Create database engine ===
engine = create_engine(DATABASE_URL)

# === 5. Suppress warnings ===
import warnings
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
pd.set_option('display.float_format', '{:.2f}'.format)


# === 6. SQL Query Functions (Parameterized) ===
def last_purchase_date_sup(supplier_list, zid):
    """Get last purchase date for each supplier."""
    dict_of_date = {}
    
    for supplier in supplier_list:
        key_name = str(supplier)
        query = "SELECT MAX(xdate) FROM pogrn WHERE xsup = %s AND zid = %s"
        max_date = pd.read_sql(query, con=engine, params=[supplier, zid])
        try:
            dict_of_date[key_name] = (max_date.at[0,'max']).strftime('%Y-%m-%d')
        except AttributeError as e:
            print(f"⚠️ {supplier}: {e}")
    
    return dict_of_date

def create_rest_supplier_list(zid, credit_supplier):
    """Create list of suppliers not in credit supplier list."""
    query = "SELECT DISTINCT pogrn.xsup FROM pogrn WHERE pogrn.zid = %s"
    df_supplier = pd.read_sql(query, con=engine, params=[zid])
    total_supplier_list = df_supplier['xsup'].tolist()
    rest_supplier_list = [i for i in total_supplier_list if i not in credit_supplier]
    return rest_supplier_list

def central_stock_purchase(zid):
    """Get central stock and purchase data for specified warehouse."""
    # Get all items
    item_query = "SELECT caitem.xitem FROM caitem WHERE caitem.zid = %s"
    df_item = pd.read_sql(item_query, con=engine, params=[zid])
    
    # Get stock data
    stock_query = """
        SELECT imtrn.zid, imtrn.xitem, caitem.xdesc, caitem.xgitem, 
               sum(imtrn.xqty*imtrn.xsign) as inventory
        FROM imtrn
        JOIN caitem ON imtrn.xitem = caitem.xitem
        WHERE imtrn.zid = %s AND caitem.zid = %s
        GROUP BY imtrn.zid, imtrn.xitem, caitem.xdesc, caitem.xgitem
    """
    df_stock = pd.read_sql(stock_query, con=engine, params=[zid, zid])
    
    # Get latest purchase data
    purchase_query = """
        SELECT * FROM (
            SELECT imtrn.zid, imtrn.xitem, imtrn.xdate, imtrn.xval, imtrn.xqty,
                   ROW_NUMBER() OVER(PARTITION BY xitem ORDER BY xdate DESC) AS rn
            FROM imtrn
            WHERE imtrn.zid = %s AND imtrn.xdocnum LIKE %s
        ) t
        WHERE t.rn = 1
    """
    df_purchase = pd.read_sql(purchase_query, con=engine, params=[zid, 'GRN-%'])
    
    # Get supplier data
    supplier_query = """
        SELECT pogdt.xitem, pogrn.xsup
        FROM pogdt
        JOIN pogrn ON pogdt.xgrnnum = pogrn.xgrnnum
        WHERE pogdt.zid = %s AND pogrn.zid = %s
    """
    df_supplier = pd.read_sql(supplier_query, con=engine, params=[zid, zid])
    
    # Merge data
    df_main = df_item.merge(df_stock, on='xitem', how='left')
    df_main = df_main.merge(df_purchase, on='xitem', how='left')
    df_main = df_main.merge(df_supplier, on='xitem', how='left')
    df_main = df_main.drop_duplicates()
    
    # Calculate purchase rate
    df_main['Purchase_Rate'] = df_main['xval'] / df_main['xqty']
    
    # Rename columns
    col_dict = {
        'xitem': 'Code', 'xdesc': 'Name', 'xgitem': 'Group', 
        'inventory': 'Central_Stock', 'xdate': 'Last_Purchase_Date', 
        'xsup': 'Supplier_Code', 'xqty': 'Last_Purchase_Qty'
    }
    df_main = df_main.rename(columns=col_dict)
    df_main = df_main.sort_values('Code').drop(['zid_x', 'zid_y', 'xval', 'rn'], axis=1)
    
    # Order columns
    df_main = df_main[['Code', 'Name', 'Group', 'Supplier_Code', 'Central_Stock', 
                      'Last_Purchase_Date', 'Last_Purchase_Qty', 'Purchase_Rate']].fillna(0)
    return df_main

def create_main(supplier_list, zid_central, zid_gulshan):
    """Create main analysis data for credit suppliers."""
    dict_df_main = {}
    
    df_main = central_stock_purchase(zid_central)
    pur_date_dict = last_purchase_date_sup(CREDIT_SUPPLIER, zid_central)
    
    for supplier in supplier_list:
        key_name = str(supplier)
        df_central = df_main[df_main['Supplier_Code'] == key_name]
        product_tuple = tuple(df_central['Code'])
        
        if len(product_tuple) == 0:
            continue
            
        # Get stock data for Gulshan warehouse
        stock_query = """
            SELECT imtrn.zid, imtrn.xitem, caitem.xdesc, 
                   sum(imtrn.xqty*imtrn.xsign) as inventory, caitem.xstdprice
            FROM imtrn
            JOIN caitem ON imtrn.xitem = caitem.xitem
            WHERE imtrn.zid = %s AND caitem.zid = %s AND imtrn.xitem IN %s
            GROUP BY imtrn.zid, imtrn.xitem, caitem.xdesc, caitem.xstdprice
        """
        df_stock = pd.read_sql(stock_query, con=engine, params=[zid_gulshan, zid_gulshan, product_tuple])
        
        # Get sales data for last 60 days
        date_since_sales = (datetime.now() - timedelta(days=60)).date().strftime('%Y-%m-%d')
        print(f"📊 Sales data from: {date_since_sales}")
        
        sales_query = """
            SELECT imtrn.zid, imtrn.xitem, caitem.xdesc, 
                   sum(imtrn.xqty*imtrn.xsign) as inventory
            FROM imtrn
            JOIN caitem ON imtrn.xitem = caitem.xitem
            WHERE imtrn.zid = %s AND caitem.zid = %s AND imtrn.xitem IN %s
                  AND imtrn.xdocnum LIKE %s AND imtrn.xdate > %s
            GROUP BY imtrn.zid, imtrn.xitem, caitem.xdesc
        """
        df_sales = pd.read_sql(sales_query, con=engine, params=[zid_gulshan, zid_gulshan, product_tuple, 'CO-%', date_since_sales])
        
        # Rename columns
        df_stock = df_stock.rename(columns={'xitem': 'Code'})
        df_sales = df_sales.rename(columns={'xitem': 'Code'})
        
        # Merge data
        df_main_final = df_central.merge(df_stock, on='Code', how='left')
        df_main_final = df_main_final.merge(df_sales, on='Code', how='left')
        df_main_final = df_main_final.sort_values('Code').drop(['zid_x', 'xdesc_x', 'zid_y', 'xdesc_y'], axis=1)
        
        # Rename columns
        col_dict = {'inventory_x': 'Gulshan_Stock', 'inventory_y': 'Gulshan_Sale', 'xstdprice': 'Sales_Price'}
        df_main_final = df_main_final.rename(columns=col_dict)
        df_main_final['Last_Supplier_Date'] = pur_date_dict[supplier]
        df_main_final = df_main_final.fillna(0)
        df_main_final['Total_Stock'] = df_main_final['Central_Stock'] + df_main_final['Gulshan_Stock']
        dict_df_main[key_name] = df_main_final
        
    return dict_df_main
        
def create_rest(supplier_list, zid_central, zid_gulshan):
    """Create analysis data for non-credit suppliers."""
    df_main = central_stock_purchase(zid_central)
    
    rest_supplier_list = create_rest_supplier_list(zid_central, supplier_list)
    df_main_final = df_main[df_main.Supplier_Code.isin(rest_supplier_list)]
    product_tuple = tuple(df_main_final['Code'])
    
    # Get stock data for Gulshan warehouse
    stock_query = """
        SELECT imtrn.zid, imtrn.xitem, caitem.xdesc, 
               sum(imtrn.xqty*imtrn.xsign) as inventory, caitem.xstdprice
        FROM imtrn
        JOIN caitem ON imtrn.xitem = caitem.xitem
        WHERE imtrn.zid = %s AND caitem.zid = %s AND imtrn.xitem IN %s
        GROUP BY imtrn.zid, imtrn.xitem, caitem.xdesc, caitem.xstdprice
    """
    df_stock = pd.read_sql(stock_query, con=engine, params=[zid_gulshan, zid_gulshan, product_tuple])
    
    # Get sales data for last 30 days
    date_since_sales = (datetime.now() - timedelta(days=30)).date().strftime('%Y-%m-%d')
    sales_query = """
        SELECT imtrn.zid, imtrn.xitem, caitem.xdesc, 
               sum(imtrn.xqty*imtrn.xsign) as inventory
        FROM imtrn
        JOIN caitem ON imtrn.xitem = caitem.xitem
        WHERE imtrn.zid = %s AND caitem.zid = %s AND imtrn.xitem IN %s
              AND imtrn.xdocnum LIKE %s AND imtrn.xdate > %s
        GROUP BY imtrn.zid, imtrn.xitem, caitem.xdesc
    """
    df_sales = pd.read_sql(sales_query, con=engine, params=[zid_gulshan, zid_gulshan, product_tuple, 'CO-%', date_since_sales])
    
    # Rename columns
    df_stock = df_stock.rename(columns={'xitem': 'Code'})
    df_sales = df_sales.rename(columns={'xitem': 'Code'})
    
    # Merge data
    df_main_final = df_main_final.merge(df_stock, on='Code', how='left')
    df_main_final = df_main_final.merge(df_sales, on='Code', how='left')
    df_main_final = df_main_final.sort_values('Code').drop(['zid_x', 'xdesc_x', 'zid_y', 'xdesc_y'], axis=1)
    
    # Rename columns
    col_dict = {'inventory_x': 'Gulshan_Stock', 'inventory_y': 'Gulshan_Sale', 'xstdprice': 'Sales_Price'}
    df_main_final = df_main_final.rename(columns=col_dict)
    df_main_final['Sales_Date'] = date_since_sales
    df_main_final['Total_Stock'] = df_main_final['Central_Stock'] + df_main_final['Gulshan_Stock']
    df_main_final = df_main_final.fillna(0)
    return df_main_final
    
def create_financial_analysis(supplier_list, zid_central, zid_gulshan):
    """Create financial analysis for both credit and non-credit suppliers."""
    df_main = create_main(supplier_list, zid_central, zid_gulshan)
    df_main_rest = create_rest(supplier_list, zid_central, zid_gulshan)
    
    financial_list = []
    
    for supplier in CREDIT_SUPPLIER:
        try:
            row_dict = {}
            row_dict['Supplier_Code'] = supplier
            df_main[supplier]['Stock_Value'] = df_main[supplier]['Total_Stock'] * df_main[supplier]['Purchase_Rate']
            purchase_value = df_main[supplier]['Stock_Value'].sum()
            row_dict['Inventory_Purchase_Value'] = purchase_value
            
            df_main[supplier]['Stock_Value'] = df_main[supplier]['Total_Stock'] * df_main[supplier]['Sales_Price']
            inv_sale_value = df_main[supplier]['Stock_Value'].sum()
            row_dict['Inventory_Sale_Value'] = inv_sale_value
            
            df_main[supplier]['Stock_Value'] = -1 * df_main[supplier]['Gulshan_Sale'] * df_main[supplier]['Sales_Price']
            sale_value = df_main[supplier]['Stock_Value'].sum()
            row_dict['Total_Sales'] = sale_value
            
            purchase_date = df_main[supplier]['Last_Supplier_Date'].iloc[0]
            row_dict['Purchase_Date'] = purchase_date
            financial_list.append(row_dict)
        except Exception as e:
            print(f"⚠️ Error processing supplier {supplier}: {e}")
        
    df_financial_main = pd.DataFrame(financial_list).round(2)
    df_financial_main['Difference'] = df_financial_main['Total_Sales'] - df_financial_main['Inventory_Sale_Value']
    df_financial_main = df_financial_main.sort_values('Difference')
    
    # Process rest suppliers
    df_main_rest['Purchase_Value'] = df_main_rest['Total_Stock'] * df_main_rest['Purchase_Rate']
    df_main_rest['Inventory_Sale_Value'] = df_main_rest['Total_Stock'] * df_main_rest['Sales_Price']
    df_main_rest['Sales_Value'] = -1 * df_main_rest['Gulshan_Sale'] * df_main_rest['Sales_Price']
    df_financial_rest = df_main_rest.groupby('Supplier_Code').sum()
    df_financial_rest = df_financial_rest[['Purchase_Value', 'Inventory_Sale_Value', 'Sales_Value']].round(2)
    df_financial_rest['Difference'] = df_financial_rest['Sales_Value'] - df_financial_rest['Inventory_Sale_Value']
    df_financial_rest = df_financial_rest.sort_values('Difference')
    df_financial_rest = df_financial_rest.reindex()
    
    return df_financial_main, df_financial_rest
    
# === 7. Main Data Processing ===
print("🔄 Processing main analysis data...")
main_dict = create_main(CREDIT_SUPPLIER, ZID_CENTRAL, ZID_FIXIT)

print("🔄 Processing rest suppliers data...")
df_rest = create_rest(CREDIT_SUPPLIER, ZID_CENTRAL, ZID_FIXIT)

print("🔄 Creating financial analysis...")
df_financial_main, df_financial_rest = create_financial_analysis(CREDIT_SUPPLIER, ZID_CENTRAL, ZID_FIXIT)

# === 8. Export to Excel ===
excel_file = 'FixitPurchaseAnalysis.xlsx'
print(f"📊 Generating Excel report: {excel_file}")

with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
    for i in CREDIT_SUPPLIER:
        if i in main_dict:
            main_dict[i].to_excel(writer, i, index=False)
    
    df_rest.to_excel(writer, 'Rest', index=False)
    df_financial_main.to_excel(writer, 'main_financial', index=False)
    df_financial_rest.to_excel(writer, 'rest_financial', index=False)

print(f"✅ Report generated successfully:")
print(f"   📊 Excel: {excel_file}")
print(f"   📈 Total credit suppliers: {len(main_dict)}")
print(f"   📈 Rest suppliers: {len(df_rest)}")

# === 9. Send Email ===
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"📬 Recipients: {recipients}")
except Exception as e:
    print(f"⚠️ Failed to get recipients: {e}. Using fallback.")
    recipients = ["asaddat87@gmail.com", "fixitbdonline@gmail.com", "fixitc.central@gmail.com"]

subject = f"Fixit Purchase Analysis – {TODAY_DATE}"
body_text = "Please find today's Fixit Purchase Analysis report with detailed supplier analysis and financial summaries."

# Prepare HTML content for email

send_mail(
    subject=subject,
    bodyText=body_text,
    html_body=[(df_financial_main, "Credit Suppliers Analysis"), (df_financial_rest, "Other Suppliers Analysis")],
    attachment=[excel_file],
    recipient=recipients
)

print("📧 Email sent successfully.")

# === 10. Cleanup ===
engine.dispose()
print("✅ Script completed successfully.")
