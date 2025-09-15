# %%
####################### ALGORITHM #####################
# 1 : Get all branches erp stock data central , gulshan, ecommerce
# 2 : create a new column and sum all item 
# 3 : make a excel sheet of that dataframe as name prev_day_stock.xlsx. this is closing balance.
# 4 . take those excel sheet as [now days] openning stock as dataframe
# 5. compare two dataframe and filter out which items stock is changed
# 6. create a list of those items
# 7. call api and take json data of all products stats in a dataframe
# 8. filter above mentioned list item and those item which has no shipping class.
# 9. now merge this dataframe with all branches stock [df_merging_all_stock]
#10. create a column for shipping_class update as per stock and apply condition
#11. create dataframe to json object for post
#12. post json data to woocommerce api.

import pandas as pd
import numpy as np
import time
from woocommerce import API
from mail import send_mail
import openpyxl
from mainFuncShipping import *
import pandas as pd
import requests
from urllib.error import HTTPError
main_execution_time =time.time()

# %%

############ GET CENTRAL STOCK ########
df_stock_total_central = get_all_product_stock_from_erp(100002)
df_purchase_list_central = get_all_product_last_price_from_erp(100002)
df_item_group_central=pd.merge(df_stock_total_central, df_purchase_list_central, on='xitem')
df_item_group_central['central_unit_price'] = df_item_group_central['xval']/df_item_group_central['xqty']
df_item_group_central=df_item_group_central.drop(columns=['zid_y', 'rn']).rename(columns={'zid': 'Central BI',
'xdesc': 'Product Name Central', 'xgitem': 'Item Group Central','xstdprice':'Central Selling Price',
'inventory':'Central Inventory','xdate':'Central Last Purchase'}).sort_values(by='Item Group Central', ascending=True)
df_Negative_inventory_central = df_item_group_central.loc[(df_item_group_central['xqty'] < 0)]
#last Purchase Price
df_item_group_central = df_item_group_central.drop(columns=['zid_x', 'xbrand','xval','xqty'])
#for update shipping class
df_item_group_central = df_item_group_central.drop(columns=['Item Group Central', 'Central Selling Price','Central Last Purchase','central_unit_price']).rename(columns={'xitem':'sku'})

### GET Ecommerce STOCK
df_stock_total_ecommerce = get_all_product_stock_from_erp(100003)
df_purchase_list_ecommerce = get_all_product_last_price_from_erp(100003)
df_item_group_ecommerce =pd.merge(df_stock_total_ecommerce, df_purchase_list_ecommerce, on='xitem')
df_item_group_ecommerce['ecommerce_purchase_price'] = df_item_group_ecommerce['xval']/df_item_group_ecommerce['xqty']
df_item_group_ecommerce=df_item_group_ecommerce.drop(columns=['zid_y', 'rn']).rename(columns={'zid': 'Ecom BI', 
'xdesc': 'Product_Name_ecom_erp', 'xgitem': 'Item_Group_ecom_erp','xstdprice':'ecom_erp_Selling_Price',
'inventory':'ecom_erp_Inventory','xdate':'ecom_erp_Last_Purchase'})
df_item_group_ecommerce = df_item_group_ecommerce.sort_values(by='Item_Group_ecom_erp', ascending=True)
#last Purchase Price
df_Negative_inventory_ecommerce = df_item_group_ecommerce.loc[(df_item_group_ecommerce['xqty'] < 0)]
#for update shipping class
df_item_group_ecommerce_erp = df_item_group_ecommerce.drop(columns=['zid_x', 'xbrand','xval','xqty','Item_Group_ecom_erp','ecom_erp_Selling_Price','ecom_erp_Last_Purchase','ecommerce_purchase_price']).rename(columns={'xitem':'sku'})
df_item_group_ecommerce_erp.head(2)

# %%
### GET Gulshan STOCK
df_stock_total_gulshan = get_all_product_stock_from_gulshan(100001)
df_purchase_list_gulshan = get_all_product_last_price_from_gulshan(100001)
df_item_group_gulshan =pd.merge(df_stock_total_gulshan, df_purchase_list_gulshan, on='xitem')
df_item_group_gulshan['gulshan_purchase_price'] = df_item_group_gulshan['xval']/df_item_group_gulshan['xqty']
df_item_group_gulshan=df_item_group_gulshan.drop(columns=['zid_y', 'rn']).rename(columns={'xdesc': 'Product_Name_fixit', 
'xgitem': 'Item_Group_fixit','xstdprice':'fixit_Selling_Price','inventory':'fixit_Inventory','xdate':'fixit_Last_Purchase'})
df_item_group_gulshan = df_item_group_gulshan.sort_values(by='Item_Group_fixit', ascending=True)
df_Negative_inventory_gulshan = df_item_group_gulshan.loc[(df_item_group_gulshan['xqty'] < 0)]

df_item_group_gulshan_erp = df_item_group_gulshan.drop(columns=['zid_x', 'xbrand','xval','xqty','Item_Group_fixit','fixit_Selling_Price','fixit_Last_Purchase','gulshan_purchase_price']).rename(columns={'xitem':'sku'})


# %%
###### merging all stock
df_merging_all_stock= pd.merge (df_item_group_central,df_item_group_gulshan_erp, on = 'sku', how='left')
df_merging_all_stock= pd.merge (df_merging_all_stock,df_item_group_ecommerce_erp, on = 'sku',how='left').drop(columns=['Product_Name_fixit','Product_Name_ecom_erp']).fillna(0)
df_merging_all_stock.iloc[:,[2,3,4]] =df_merging_all_stock.iloc[:,[2,3,4]].astype(int)
####################### FINISH OF TAKING ALL BUSINESS STOCK ###########


############ sum all stock ############
df_merging_all_stock['sum_of_stock'] = df_merging_all_stock.iloc[:, 2:].sum(axis=1)


############# take previous days stock and compare. if any change only take those column ###############
df_compare_prev_day_stock = pd.read_excel('prev_day_stock.xlsx', engine = 'openpyxl')
df_compare_prev_day_stock=df_compare_prev_day_stock.drop(columns={'Unnamed: 0','Product Name Central','Central Inventory','fixit_Inventory','ecom_erp_Inventory'}).rename(columns={'sum_of_stock':'prev_day_stock'})
df_compare_prev_day_stock= pd.merge(df_merging_all_stock,df_compare_prev_day_stock, on = 'sku', how='left')
df_compare_prev_day_stock['changes'] = np.where(df_compare_prev_day_stock['prev_day_stock']== df_compare_prev_day_stock['sum_of_stock'], True, False)
############# filter out all true which is not changed.
df_compare_prev_day_stock=df_compare_prev_day_stock[df_compare_prev_day_stock['changes'] == False]

# %%
df_merging_all_stock.to_excel("total_stock.xlsx")

# %%

# Woocommerce Connection
apiTime = time.time()
wcapi = API(
    url="https://fixit.com.bd",
    consumer_key="ck_3a0d5decb7b4d4309d58de6bea6cb0d3caea5981",
    consumer_secret="cs_e20fc6b7e01ba0c5d619710854d6075f56c40e01",
    version="wc/v3",
    timeout=2000
)
apiTimeout = time.time() - apiTime
apiTimeout

# GET WOOCOMMERCE PRODUCT DATA

f = open("log.txt", "a")
get_product_start_time = time.time()
page = 1
products = []

try:
    while True:
        prods = wcapi.get('products', params={
                          'per_page': 100, 'page': page}).json()
        page += 1
        f.write(f"get item {len(prods) * page}\n\n")
        print(len(prods) * page)
        if not prods:
            break
        products.extend(prods)  # Use extend to append individual products to the list
except HTTPError as e:
    send_mail("connection error for get request")

# %%
len(products)

# %%
#################### create dataframe from woocommerce json data #####################
flat_3 = []
for l in products:
    flat_3.extend(l)
end_time = time.time()
extend_time = end_time - get_product_start_time
print(extend_time, 'extend_time')
df_woocommerce_data = pd.DataFrame (flat_3, columns = ['id','sku','shipping_class', 'categories']).to_excel("categories.xlsx")
df_woocommerce_data_for_not_seo = pd.DataFrame (flat_3, columns = ['id','sku','shipping_class','meta_data']).to_excel("seoNotComplete.xlsx")

######## create a list from erp data which is need to change api data
sku_changes_list = [x for x in df_compare_prev_day_stock['sku']]

######### check if api data's shipping class equal to null or inventory item list is in api data
df_shipping_class_need_to_change= df_woocommerce_data.loc[(df_woocommerce_data['sku'].isin([sku_changes_list])) | (df_woocommerce_data['shipping_class']=='') ]

# %%
# Check if 'sku' and 'shipping_class' are blank or null
if df_shipping_class_need_to_change.empty:
        print ("no data to be updated, PROGRAM EXITED\n")
        f.write("no data to be updated, PROGRAM EXITED\n")
else:
    df_woocommerce_data_for_post = pd.merge(df_shipping_class_need_to_change, df_merging_all_stock, on = 'sku', how='left').reset_index(drop=True).drop(columns={'shipping_class'})
    df_woocommerce_data_for_post.iloc[:, 3:6]= df_woocommerce_data_for_post.iloc[:, 3:6].fillna(0)
    df_woocommerce_data_for_post.iloc[:, 3:6]= df_woocommerce_data_for_post.iloc[:, 3:6].fillna(0)
    df_woocommerce_data_for_post['Product Name Central']= df_woocommerce_data_for_post['Product Name Central'].fillna('na')
########### create a column for updating nothouse, inhouse and nearby
    def compare (row):
        if (row['fixit_Inventory']+row['Central Inventory']+row['ecom_erp_Inventory']<=0) or (row['Product Name Central']=='na'):
            return 'nothouse'
        if row['fixit_Inventory']+row['ecom_erp_Inventory']>0: 
            return 'inhouse'
        if row['Central Inventory']>0: 
            return 'nearby'

    df_woocommerce_data_for_post.iloc[:,[3,4,5]] =df_woocommerce_data_for_post.iloc[:,[3,4,5]].astype(int)
    df_woocommerce_data_for_post['compare'] = df_woocommerce_data_for_post.apply(lambda row: compare (row), axis=1)
    df_woocommerce_data_for_post = df_woocommerce_data_for_post.drop(columns=['Product Name Central','sku','Central Inventory','fixit_Inventory','ecom_erp_Inventory','sum_of_stock']).rename(columns={'compare':'shipping_class'})
    df_woocommerce_data_for_post
############## create json data for posting to api    
    try:
        jsonData = df_woocommerce_data_for_post.to_dict('records')
        entry_no = 99
        start_time = time.time()
        total_item = []
        for i in range(0,len(jsonData),entry_no):
            sub = jsonData[i:i+entry_no]
            data = {}
            data['update'] = sub
            wcapi.post("products/batch", data).json()
            total_item.append(len(sub))
            f.write(f"{len(sub)} item has been updated successfully")
            print(f"{len(sub)} item has been updated successfully")

        
        end_time = time.time()-start_time
        sum_total_item = sum(total_item)
        end_main_execution_time = (time.time()-main_execution_time)
        send_mail(f"SUCCESSFULLY {sum_total_item} ITEM UPDATED \n Total time Elapsed \n api connection take {apiTimeout} second,\n post Method take {end_time/60} minuite \n  and get method take {extend_time/60} minuite\n and all the code runtime is {end_main_execution_time}")
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        send_mail (f"data not inserted")
        raise SystemExit(e)
f.close()
df_merging_all_stock.to_excel('prev_day_stock.xlsx')

# %%
df_merging_all_stock = pd.read_excel("total_stock.xlsx")
df_wocommerce_stock_manage_stock = pd.read_excel('woocommerce_data.xlsx')

# %%
df_wocommerce_stock_manage_stock = pd.merge( df_merging_all_stock,df_wocommerce_stock_manage_stock, on = 'sku' , how= 'left').fillna(0)
df_wocommerce_stock_manage_stock = df_wocommerce_stock_manage_stock[df_wocommerce_stock_manage_stock ['sum_of_stock'] !=0]

df_wocommerce_stock_manage_stock

# %%


# %%
df_wocommerce_stock_manage_stock = df_wocommerce_stock_manage_stock[['id','sum_of_stock']].rename(columns={'sum_of_stock' : 'stock_quantity'})
df_wocommerce_stock_manage_stock['id'] = df_wocommerce_stock_manage_stock['id'].astype(int)
df_wocommerce_stock_manage_stock['manage_stock'] = True
df_wocommerce_stock_manage_stock

# %%
jsonData = df_wocommerce_stock_manage_stock.to_dict("records")
jsonData

# %%
jsonData = list(filter(lambda x: x['id'] > 0, jsonData ))
jsonData

# %%
import time
from woocommerce import API

# WooCommerce Connection
apiTime = time.time()
wcapi = API(
    url="https://fixit.com.bd",
    consumer_key="ck_3a0d5decb7b4d4309d58de6bea6cb0d3caea5981",
    consumer_secret="cs_e20fc6b7e01ba0c5d619710854d6075f56c40e01",
    version="wc/v3",
    timeout=2000
)
apiTimeout = time.time() - apiTime
apiTimeout

# Your data with 2500+ dict records
all_records = jsonData  # This should be your list of records

# Define batch size
batch_size = 100

# Split your data into smaller batches
batches = [all_records[i:i+batch_size] for i in range(0, len(all_records), batch_size)]

# Iterate through batches and send requests
for batch in batches:
    data = {'update': batch}
    response = wcapi.post("products/batch", data)

    if response.status_code == 200:
        updated_data = response.json()
        print("Batch successfully updated.")
        print("Updated Data:")
        print(updated_data)
    else:
        print(f"Error: {response.status_code} - {response.json()}")


# %%



