# %%

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

############ GET CENTRAL STOCK ########

# %%
df_stock = pd.read_excel("total_stock.xlsx" , engine= 'openpyxl' )
try:
    df_stock.drop(columns=["Unnamed: 0", "Product Name Central", "Central Inventory", "fixit_Inventory", "ecom_erp_Inventory",] , axis=1, inplace=True)
except:
    pass

df_stock

# %%
df_cat = pd.read_excel("categories.xlsx" , engine= 'openpyxl' )
df_cat

# %%
df_final = pd.merge(df_stock, df_cat, on = 'sku' , how = 'left')
df_final

# %%
df_final = df_final.dropna(subset= ['id'] ).reset_index(drop= True)
df_final

# %%
#  update stock in ecommerce

df_update_stock = df_final.iloc[:, [2,1]]
df_update_stock.loc[:, 'id'] = df_update_stock['id'].astype(int)

df_update_stock

# %%
df_update_stock['manage_stock'] = True

# %%
df_update_stock.columns.values[1] = "stock_quantity"
jsonData = df_update_stock.to_dict("records")
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
    else:
        print(f"Error: {response.status_code} - {response.json()}")



