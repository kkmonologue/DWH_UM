#file: erp_cust_az12
#cid remove NAS(if have NAS)
#bdate > current_date replace with null or drop them
#gen inconsistancy null/F/M/empty

import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime

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

        # 1. build connection and read date from ingestion
        conn = pyodbc.connect(dwh_conn_str)
        print(f"successfully connect dwh: {DB_NAME}")
        query = f"SELECT * FROM {SCHEMA_SOURCE}.erp_cust_az12"
        df = pd.read_sql(query, conn)
        print(f"from{SCHEMA_SOURCE} load {len(df)} lines")



        #  2. data cleaning

        df['cid'] = df['cid'].astype(str).str.replace('NAS', '', regex=False)

        df['bdate'] = pd.to_datetime(df['bdate'], errors='coerce')

        current_date = pd.Timestamp(datetime.now().date())

        df.loc[df['bdate'] > current_date, 'bdate'] = pd.NaT

        df['gen'] = df['gen'].astype(str).str.strip().str.upper()

        gender_map = {
            'F': 'Female',
            'M': 'Male',
            'FEMALE': 'Female',
            'MALE': 'Male'
        }

        df['gen'] = df['gen'].map(gender_map)

        df['gen'] = df['gen'].replace({np.nan: None})

        print(df)
        df_cleaned = df.copy()
        df_cleaned = df_cleaned.replace({np.nan: None})

    # 3. Transformation
        conn.autocommit = False # close auto submit
        cursor = conn.cursor()

                # A. create Schema 和 table structure
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_TARGET};")
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.erp_cust_az12;")
        cursor.execute(f"""
                CREATE TABLE {SCHEMA_TARGET}.erp_cust_az12 (
                CID VARCHAR(50), 
                BDATE TIMESTAMP, 
                GEN VARCHAR(50)
                );
            """)

            # B. insert
        insert_sql = f"""
            INSERT INTO {SCHEMA_TARGET}.erp_cust_az12
            (   
                CID , 
                BDATE , 
                GEN 
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ? ,?)
        """

        
        records = [
            tuple(x) for x in df_cleaned[[
                 'CID ', 
                'BDATE ', 
                'GEN '
            ]].to_numpy() 
        ]

        if records:
            cursor.fast_executemany = True 
            cursor.executemany(insert_sql, records)

        conn.commit() # submit
        print(f"save successfully {SCHEMA_TARGET}.erp_cust_az12， {len(records)} records")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"fail: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    process_and_load_data()
