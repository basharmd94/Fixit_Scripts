####### in this version fetch all order stats from start to  
# end without order-cancel and order-completed. order complete
#  and order cancel limit is 5 days
import os
import sys
from urllib.error import HTTPError
import pandas as pd
import numpy as np
import time
from woocommerce import API
from dateutil import parser
from datetime import datetime, timedelta



# === 1. Add project root to Python path ===
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# === 2. Shared mail ===
from mail import send_mail, get_email_recipients

# === 3. Load env keys ===
print("\n==[ ENV ]===============================================")
API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')
if not API_KEY or not API_SECRET:
    raise RuntimeError('!! Missing API_KEY or API_SECRET in environment')
print("[OK] Loaded Woocommerce credentials from environment")

# === 4. Woocommerce Connection ===
print("\n==[ WOOCOMMERCE ]======================================")
wcapi = API(
  url="https://fixit.com.bd",
  consumer_key=API_KEY,
  consumer_secret=API_SECRET,
  version="wc/v3",
  timeout=2000
)
print("[OK] WC API client initialized -> https://fixit.com.bd (wc/v3)")

############ GET WOOCOMMERCE PRODUCT DATA for all status so we need to put all stats in a function to avoid repetetive  api call
def flatListAllStats(status ,parsingDate = 6000 ):
    NowDate = datetime.now()
    fiveDaysBefore = NowDate - timedelta(days = parsingDate)
    strDate = str(fiveDaysBefore)
    parsingDate = parser.parse(strDate).isoformat()
    statusList=[]
    page = 1
    
    print(f"\n-> Fetching status '{status}' since {parsingDate} ...")
    print(f"   Parsing days: {parsingDate}")
    
    try:
        while True:
            print(f"   .. Fetching page {page} ...")
            woocommerce_order_stats = wcapi.get('orders',params={'per_page':100,'page':page,'status':status, 'after' : parsingDate}).json()
            
            print(f"   .. Page {page} response: {len(woocommerce_order_stats) if woocommerce_order_stats else 0} records")
            
            if not woocommerce_order_stats:
                print(f"   .. No more records found, breaking loop")
                break
                
            statusList.append(woocommerce_order_stats)
            print(f"   .. Page {page} ✓  ({len(woocommerce_order_stats)} records)")
            page += 1
            
        total_records = sum(len(page) for page in statusList)
        print(f"[OK] Collected {total_records} total records for '{status}'\n")
        return statusList

    except HTTPError as e:
        print(f"!! HTTPError while fetching '{status}': {e}")
        return []
    except Exception as e:
        print(f"!! Unexpected error while fetching '{status}': {e}")
        return []

####### create flat list and then list to dataframe for each status. note here stats parameter need stats argument such
### as df_flatListAllStats('processing') and then its call back to flatListAllStats(status) function, if stats order cancel or completed its take 5 days time delta otherwise take 
###### all of time status
def df_flatListAllStats(stats):
    print(f"\n=== Processing status: {stats} ===")
    
    if stats == 'order-cancelled' or stats == 'completed':
        print(f"   Using 5-day limit for {stats}")
        finalstats = flatListAllStats(stats,5)
    else:
        print(f"   Using full history for {stats}")
        finalstats = flatListAllStats(stats)
    
    print(f"   Raw API response pages: {len(finalstats)}")
    
    if not finalstats:
        print(f"   No data returned for {stats}")
        return pd.DataFrame(columns=['id','status', 'date_created','date_completed','customer_note' , 'total'])
    
    flat_list_orders = [x for l in finalstats for x in l]
    print(f"   Flattened orders: {len(flat_list_orders)}")
    
    filter_item = ['id','date_created','date_modified',  'status','date_completed', 'total','customer_note']
    flat_list= [{k:v for k,v in item.items() if k  in filter_item} for item in flat_list_orders]
    print(f"   Filtered records: {len(flat_list)}")
    
    df_woocommerce_data = pd.DataFrame (flat_list, columns = ['id','status', 'date_created','date_completed','customer_note' , 'total']).sort_values(by=['status'])
    print(f"   Final DataFrame: {len(df_woocommerce_data)} rows")
    print(f"   Status: {df_woocommerce_data['status'].unique() if not df_woocommerce_data.empty else 'Empty'}")
    
    return df_woocommerce_data

# === 6. Fetch data for each status ===
print("\n==[ FETCH STATUSES ]====================================")
print(f"Starting data fetch at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

#### pass the argument and get dataframe for each status
print("\n[1/8] Fetching 'processing' orders...")
df_processing = df_flatListAllStats('processing')

print("\n[2/8] Fetching 'order-confirmed' orders...")
df_order_confirm = df_flatListAllStats('order-confirmed')

print("\n[3/8] Fetching 'will-ecourier' orders...")
df_will_ecourier = df_flatListAllStats('will-ecourier')

print("\n[4/8] Fetching 'will-sundarban' orders...")
try:
    df_will_sunarban = df_flatListAllStats('will-sundarban')
except Exception as e:
    print(f"   Error fetching will-sundarban: {e}")
    print("   Using fallback data...")
    date_iso = datetime.now().isoformat()
    df_will_sunarban =[

                    {'id': "no-sundarban-courier",
                    'status': 'no-sundarban-courier',
                    'date_created': date_iso,
                    'date_completed': None,
                    'customer_note': 'no-sundarban-courier',
                    'total': 'no-sundarban-courier'
                    },

                            ]
    df_will_sunarban = pd.DataFrame.from_dict(df_will_sunarban)
    
print("\n[5/8] Fetching 'order-cancelled' orders...")
df_order_cancelled = df_flatListAllStats('order-cancelled')

print("\n[6/8] Fetching 'completed' orders...")
df_order_completed = df_flatListAllStats('completed')

print("\n[7/8] Fetching 'consundorban' orders...")
df_sent_to_sundarban = df_flatListAllStats('consundorban')

print("\n[8/8] Fetching 'on-the-way-to-del' orders...")
######################### fix bug 16-aug-22 if no delivery today then try except block
try:
    df_goes_for_delivery = df_flatListAllStats('on-the-way-to-del')
except Exception as e:
    print(f"   Error fetching on-the-way-to-del: {e}")
    print("   Using fallback data...")
    date_iso = datetime.now().isoformat()
    df_goes_for_delivery_dict =[

                    {'id': "NO-DELIVERY-TODAY",
                    'status': 'on-the-way-to-del',
                    'date_created': date_iso,
                    'date_completed': None,
                    'customer_note': 'NO-DELIVERY-TODAY',
                    'total': 'NO-DELIVERY-TODAY'
                    },

                            ]
    
    df_goes_for_delivery = pd.DataFrame.from_dict(df_goes_for_delivery_dict)

# === 7. Combine and transform ===
print("\n==[ TRANSFORM ]==========================================")
print("Combining all dataframes...")
df_all_order_stats = pd.concat([df_processing , df_order_confirm, df_will_ecourier, df_will_sunarban, df_sent_to_sundarban,   df_goes_for_delivery, df_order_cancelled,df_order_completed, ], ignore_index=True)
print(f"[OK] Combined rows: {len(df_all_order_stats)}")

# Convert datetime strings to pandas datetime objects
print("Converting datetime columns...")
for col in ['date_created', 'date_completed']:
    df_all_order_stats[col] = pd.to_datetime(df_all_order_stats[col], errors='coerce')

# To date only
df_all_order_stats['date_completed'] = pd.to_datetime(df_all_order_stats['date_completed']).dt.date
df_all_order_stats['date_created'] = pd.to_datetime(df_all_order_stats['date_created']).dt.date

# Derived columns
print("Calculating time deltas...")
df_all_order_stats['completed_in'] = (pd.to_datetime(df_all_order_stats['date_completed']) - pd.to_datetime(df_all_order_stats['date_created']))
df_all_order_stats['today_date'] = pd.to_datetime('today').normalize().date()

def _age_row(row):
    if row.get('status') == 'completed' and row.get('date_completed'):
        return row['date_completed'] - row['date_created']
    return row['today_date'] - row['date_created']

df_all_order_stats['days_for_order'] = df_all_order_stats.apply(_age_row, axis=1).astype(str)

df_all_order_stats['days_pass_order'] = df_all_order_stats.apply(lambda x: "completed within " + x['days_for_order'] if x['status']=='completed' else x['days_for_order']+"  pass after created", axis=1)

# Drop unused columns safely
print("Cleaning up columns...")
columns_to_drop = [c for c in ['date_modified', 'completed_in', 'today_date'] if c in df_all_order_stats.columns]
if columns_to_drop:
    df_all_order_stats.drop(columns=columns_to_drop, inplace=True, errors='ignore')
    print(f"   Dropped columns: {columns_to_drop}")

########## rename order status and apply to new column
def statsRename(df):
    if df['status'] == 'will-ecourier':
        df['status'] = 'Redx Done'
    if df['status'] == 'will-sundarban':
        df['status'] = 'Sundarban order confirmed'
    if df['status'] == 'consundorban':
        df['status'] = 'Sundarban courier done'
    if df['status'] == 'on-the-way-to-del':
        df['status'] = 'Out for delivery in Dhaka'
    if df['status'] == 'order-cancelled':
        df['status'] = 'Cancelled by Customer'
    if df['status'] == 'completed':
        df['status'] = 'Order Completed'
    return df['status']
df_all_order_stats ['now_status'] = df_all_order_stats.apply(statsRename, axis=1)
##### rearrange all column
df_woocommerce_data= df_all_order_stats.iloc[:,[0,6,5,4,2,3]]
# Split per status for email sections
print("[OK] Preparing email sections ...")
split_statuses = [
    'processing', 'order-confirmed', 'Redx Done', 'Sundarban order confirmed',
    'Sundarban courier done', 'Out for delivery in Dhaka', 'Cancelled by Customer', 'Order Completed'
]

section_tables = []
for label in split_statuses:
    section_df = df_woocommerce_data[df_woocommerce_data['now_status'] == label].sort_values(by='date_created', ascending=True).reset_index(drop=True)
    section_tables.append((section_df, label))
    print(f"   [+] Section '{label}' -> {len(section_df)} rows")

# === 8. Email ===
print("\n==[ EMAIL ]==============================================")
try:
    recipients = get_email_recipients(os.path.splitext(os.path.basename(__file__))[0])
    print(f"[OK] Recipients loaded: {recipients}")
except Exception as e:
    print(f"[WARN] Failed to get recipients: {e}. Using fallback.")
    recipients = ["ithmbrbd@gmail.com"]

subject = f"Fixit-04 Woocommerce Delivery Stats – {datetime.now().strftime('%Y-%m-%d')}"
body_text = "Please find today's Woocommerce delivery status breakdown below."

send_mail(
    subject=subject,
    bodyText=body_text,
    html_body=section_tables,
    attachment=[],
    recipient=recipients
)

print("[DONE] Email sent successfully.\n")

