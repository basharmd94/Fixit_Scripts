import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import time

main_execution_time = time.time()
### Central & Ecommerce Item Stock
def get_all_product_stock_from_erp(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df_item = pd.read_sql("""SELECT caitem.xitem FROM caitem WHERE caitem.zid = %s"""%(zid),con=engine)
    df_stock = pd.read_sql("""SELECT imtrn.zid, imtrn.xitem, caitem.xdesc, caitem.xgitem,caitem.xstdprice,caitem.xbrand, sum(imtrn.xqty*imtrn.xsign) as inventory\n
                            FROM imtrn\n
                            JOIN caitem\n
                            ON imtrn.xitem = caitem.xitem\n
                            WHERE imtrn.zid = %s\n
                            AND caitem.zid = %s\n
                            GROUP BY imtrn.zid, imtrn.xitem, caitem.xdesc, caitem.xgitem, caitem.xstdprice,caitem.xbrand"""%(zid,zid), con = engine)
    return df_stock

### Central & Ecommerce Item Price
def get_all_product_last_price_from_erp(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df_purchase = pd.read_sql("""SELECT * FROM (\n
                                                SELECT imtrn.zid, imtrn.xitem, imtrn.xdate, imtrn.xval, imtrn.xqty,\n
                                                ROW_NUMBER() OVER(PARTITION BY xitem ORDER BY xdate DESC) AS rn\n
                                                FROM imtrn\n
                                                WHERE imtrn.zid = %s\n
                                                ) t\n
                                WHERE t.rn = 1"""%(zid),con = engine)
    return df_purchase

def get_all_product_stock_from_gulshan(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df_item = pd.read_sql("""SELECT caitem.xitem FROM caitem WHERE caitem.zid = %s"""%(zid),con=engine)
    df_stock_gulshan = pd.read_sql("""SELECT imtrn.zid, imtrn.xitem, caitem.xdesc, caitem.xgitem,caitem.xstdprice,caitem.xbrand, sum(imtrn.xqty*imtrn.xsign) as inventory\n
                            FROM imtrn\n
                            JOIN caitem\n
                            ON imtrn.xitem = caitem.xitem\n
                            WHERE imtrn.zid = %s\n
                            AND caitem.zid = %s\n
                            GROUP BY imtrn.zid, imtrn.xitem, caitem.xdesc, caitem.xgitem, caitem.xstdprice,caitem.xbrand"""%(zid,zid), con = engine)
    return df_stock_gulshan

### Gulshan Item Last Price
def get_all_product_last_price_from_gulshan(zid):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/da')
    df_purchase_gulshan = pd.read_sql("""SELECT * FROM (\n
                                                SELECT imtrn.zid, imtrn.xitem, imtrn.xdate, imtrn.xval, imtrn.xqty,\n
                                                ROW_NUMBER() OVER(PARTITION BY xitem ORDER BY xdate DESC) AS rn\n
                                                FROM imtrn\n
                                                WHERE imtrn.zid = %s\n
                                                ) t\n
                                WHERE t.rn = 1"""%(zid),con = engine)
    return df_purchase_gulshan
