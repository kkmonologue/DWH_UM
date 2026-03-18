import pyodbc
import pandas as pd
import numpy as np

DRIVER_PATH = '/opt/homebrew/lib/psqlodbcw.so'
DB_NAME = 'dwh'
PASSWORD = '563634851'
PORT = '1234'
SCHEMA_SOURCE = 'ingestion'
SCHEMA_TARGET = 'transformation'

dwh_conn_str = f"DRIVER={{{DRIVER_PATH}}};SERVER=localhost;DATABASE={DB_NAME};UID=postgres;PWD={PASSWORD};PORT={PORT};"

def process_and_load_data():
    conn = None
    try:
        conn = None

        # 1. build connection and read date from the ingestion
        conn = pyodbc.connect(dwh_conn_str)
        print(f"successfully connect dwh: {DB_NAME}")

        query = f"SELECT * FROM {SCHEMA_SOURCE}.crm_sales_details"
        df = pd.read_sql(query, conn)
        print(f"from{SCHEMA_SOURCE} load {len(df)} lines")



        #  2. data cleaning
        
        df['sls_order_dt'] = pd.to_datetime(df['sls_order_dt'], errors='coerce')

        df['sls_order_dt'] = df.groupby('sls_ord_num')['sls_order_dt'].transform('max')

        df['sls_order_dt'] = df['sls_order_dt'].dt.strftime('%Y-%m-%d').replace('NaT', None)

        #first check all null and <=0 price value. use sales/qtty = price
        #then check all numm and <=0 sales value.use price*qtty = sales

        df['sls_price'] = np.where(
            (df['sls_price'].isna()) | (df['sls_price'] <= 0),
            df['sls_sales'] / df['sls_quantity'].replace(0, np.nan), # 防止除以0
            df['sls_price']
        )

        df['sls_sales'] = np.where(
            (df['sls_sales'].isna()) | (df['sls_sales'] <= 0),
            df['sls_price'] * df['sls_quantity'],
            df['sls_sales']
        )

        df['sls_price'] = df['sls_price'].fillna(0).astype(int)
        df['sls_sales'] = df['sls_sales'].fillna(0).astype(int)
        df['sls_quantity'] = df['sls_quantity'].fillna(0).astype(int)


        df_cleaned = df.where(pd.notnull(df), None)

        
        print(df)
        df_cleaned = df.copy()
        df_cleaned = df_cleaned.replace({np.nan: None})

    # 3. Transformation
        conn.autocommit = False # close auto submit
        cursor = conn.cursor()

                # A. create Schema 和 table structure
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_TARGET};")
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.crm_sales_details;")
        cursor.execute(f"""
                CREATE TABLE {SCHEMA_TARGET}.crm_sales_details (
                sls_ord_num VARCHAR(50), 
                sls_prd_key VARCHAR(50), 
                sls_cust_id int,
                sls_order_dt VARCHAR(50), 
                sls_ship_dt VARCHAR(50), 
                sls_due_dt VARCHAR(50),
                sls_sales int, 
                sls_quantity int, 
                sls_price int
                );
            """)

            # B. insert
        insert_sql = f"""
            INSERT INTO {SCHEMA_TARGET}.crm_sales_details
            (   
                sls_ord_num, 
                sls_prd_key , 
                sls_cust_id ,
                sls_order_dt , 
                sls_ship_dt , 
                sls_due_dt ,
                sls_sales , 
                sls_quantity , 
                sls_price 
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ? ,?)
        """

        
        records = [
            tuple(x) for x in df_cleaned[[
                'sls_ord_num', 
                'sls_prd_key', 
                'sls_cust_id',
                'sls_order_dt', 
                'sls_ship_dt', 
                'sls_due_dt',
                'sls_sales', 
                'sls_quantity', 
                'sls_price' 
            ]].to_numpy() 
        ]

        if records:
            cursor.fast_executemany = True 
            cursor.executemany(insert_sql, records)

        conn.commit() # submit
        print(f"save successfully {SCHEMA_TARGET}.crm_sales_details， {len(records)} records")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"fail: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    process_and_load_data()
  
