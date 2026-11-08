from ast import Pass
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from woocommerce import API
import psycopg2
import sys
import pandas as pd
from psycopg2 import sql
load_dotenv()


# ######### FIXIT DEVELOPMENT SERVER ######
FIXIT_LOCAL_SERVER = os.getenv('FIXIT_ENGINE_DEVELOPMENT')
FIXIT_LOCAL_SERVER = create_engine(FIXIT_LOCAL_SERVER)



############## HMBR DEVELOPMENT SERVER ###########
HMBR_LOCAL_SERVER = os.getenv('HMBR_ENGINE_DEVELOPMENT')
HMBR_LOCAL_SERVER = create_engine(HMBR_LOCAL_SERVER)
print (HMBR_LOCAL_SERVER, "server")





HMBR_ID = os.getenv('HMBR_ID')
KARIGOR_ID = os.getenv('KARIGOR_ID')
CHEMICAL_ID = os.getenv('CHEMICAL_ID')
THREADTAPE_ID = os.getenv('THREADTAPE_ID')
PLASTIC_ID = os.getenv('PLASTIC_ID')
ZEPTO_ID = os.getenv('ZEPTO_ID')
GROCERY_ID = os.getenv('GROCERY_ID')
PAINTROLLER_ID = os.getenv('PAINTROLLER_ID')
SCRUBBER_ID = os.getenv('SCRUBBER_ID')
PACKAGING_ID = os.getenv('PACKAGING_ID')

######### EMAIL ID #######
IT_MAIL = os.getenv('IT')
DIRECTOR_MAIL = os.getenv('DIRECTOR')
MOTIUR_SIR_MAIL =os.getenv('MOTIUR')
ADMIN_MAIL = os.getenv('ADMIN')
CENTRAL_MAIL = os.getenv('CENTRAL')
PYTHON_MAIL = os.getenv('PYTHON')
SHAHALAM_MAIL = os.getenv('SHAHALAM_MAIL')
COMMERCIAL_MAIL = os.getenv('COMMERCIAL_MAIL')
ANALYST_MAIL = os.getenv('ANALYST_MAIL')

############# python password ########
PYTHON_USER = os.getenv('PYTHON_USER')
PYTHON_PASS = os.getenv('PYTHON_PASS')

RELATIVE_PATH ="E:/FastApi/pyreport/backend/app"
############# Woocommerce Connection ###########
WOOCOMMERCE_API_KEY = os.getenv('API_KEY')
WOOCOMMERCE_API_SECRET = os.getenv('API_SECRET')


########### put you variable like wcapi = APICONNECT in main file
APICONNECT= API(
url="https://fixit.com.bd",
consumer_key = WOOCOMMERCE_API_KEY,
consumer_secret = WOOCOMMERCE_API_SECRET,
version="wc/v3",
    timeout = 10000
    )

########in main file variable will be--- conn = PSYCOPG2CONNECT
local_params = {
  'database': os.getenv('DB_NAME'),
  'user': os.getenv('DB_USER'),
  'password': os.getenv('DB_PASSWORD'),
  'host': os.getenv('DB_LOCAL_HOST'),
  'port': os.getenv('DB_PORT'),
 }

hmbr_params = {
  'database': os.getenv('DB_NAME'),
  'user': os.getenv('DB_USER'),
  'password': os.getenv('DB_PASSWORD'),
  'host': os.getenv('DB_HMBR_HOST'),
  'port': os.getenv('DB_PORT'),
 }

fixit_params = {
  'database': os.getenv('DB_NAME'),
  'user': os.getenv('DB_USER'),
  'password': os.getenv('DB_PASSWORD'),
  'host': os.getenv('DB_FIXIT_HOST'),
  'port': os.getenv('DB_PORT'),
 }

local_params_reports = {
  'database': os.getenv('REPORT_DB_NAME'),
  'user': os.getenv('DB_USER'),
  'password': os.getenv('DB_PASSWORD'),
  'host': os.getenv('DB_LOCAL_HOST'),
  'port': os.getenv('DB_PORT'),
 }



# HMBR_CONNECTION_PSYCOP = psycopg2.connect(**hmbr_params)
LOCAL_CONNECTION_PSYCOP = psycopg2.connect(**local_params)
REPORT_CONNECTION_PSYCOP = psycopg2.connect(**local_params_reports)


# all project
PROJ_TRADING = 'GULSHAN TRADING'
PROJ_ZEPTO = 'Zepto Chemicals'
PROJ_KARIGOR = 'Karigor Ltd.'
PROJ_CHEMICAL = 'Gulshan Chemical'
PROJ_THREAD_TAPE = 'Gulshan Thread Tape'
PROJ_PLASTIC = 'Gulshan Plastic'
PROJ_GROCERY = 'HMBR Grocery Shop'
PROJ_PAINT_ROLLER = 'HMBR Paint Roller Co.'
PROJ_SCRUBBER = 'Steel Scrubber Co.'
PROJ_PACKAGING = 'Gulshan Packaging'


# create dataframe or get one item from db tabl
def get_data (query , fetchOneOrAll = 'all' , df = True, conn = "local"):
    if conn == 'local':
        with LOCAL_CONNECTION_PSYCOP as connection:
            with connection.cursor() as cursor:
                query = query
                cursor.execute (query)
                records = cursor.fetchall() if fetchOneOrAll == 'all' else cursor.fetchone()
                if fetchOneOrAll == 'all' and df:
                    df = pd.DataFrame(records, columns=[desc[0] for desc in cursor.description])
                    return df
                else:
                    return records

    else:
        with HMBR_CONNECTION_PSYCOP as connection:
            with connection.cursor() as cursor:
                query = query
                cursor.execute (query)
                records = cursor.fetchall() if fetchOneOrAll == 'all' else cursor.fetchone()
                if fetchOneOrAll == 'all' and df:
                    df = pd.DataFrame(records, columns=[desc[0] for desc in cursor.description])
                    return df
                else:
                    return records






def insert_sql(dataframe, table):
    # now insert to database sales
    records = dataframe.to_dict(orient='records')

    # Define the target table
    table_name = table
    try:
        # Get the column names
        columns = ', '.join(records[0].keys())

        # Generate placeholders for the values
        values_template = ', '.join(['%({})s'.format(col) for col in records[0].keys()])

        # Construct the SQL query
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(columns),
            sql.SQL(values_template)
        )
        connection = REPORT_CONNECTION_PSYCOP
        # Execute the query with the data
        with connection.cursor() as cur:
            cur.executemany(query, records)
            connection.commit()
    except Exception as e:
        # Handle other exceptions
        print(f"Error: {e}")

