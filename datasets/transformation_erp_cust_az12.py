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
        conn = pyodbc.connect(dwh_conn_str)
        print(f"successfully connect dwh: {DB_NAME}")
        
        query = f"SELECT * FROM {SCHEMA_SOURCE}.erp_cust_az12"
        df = pd.read_sql(query, conn)
        print(f"from {SCHEMA_SOURCE} load {len(df)} lines")

        
        df.columns = df.columns.str.strip().str.upper()

        df['CID'] = df['CID'].astype(str).str.replace('NAS', '', regex=False)
        df['CID'] = df['CID'].replace(['nan', 'None', ''], None)

        df['BDATE'] = pd.to_datetime(df['BDATE'], errors='coerce')
        current_date = pd.Timestamp(datetime.now().date())
        df.loc[df['BDATE'] > current_date, 'BDATE'] = pd.NaT

        df['GEN'] = df['GEN'].astype(str).str.strip().str.upper()
        gender_map = {
            'F': 'Female', 'M': 'Male',
            'FEMALE': 'Female', 'MALE': 'Male'
        }
        df['GEN'] = df['GEN'].map(gender_map)

        df_cleaned = df.where(pd.notnull(df), None)

        conn.autocommit = False 
        cursor = conn.cursor()

        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_TARGET};")
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.erp_cust_az12;")
        cursor.execute(f"""
                CREATE TABLE {SCHEMA_TARGET}.erp_cust_az12 (
                CID VARCHAR(50), 
                BDATE TIMESTAMP, 
                GEN VARCHAR(50)
                );
            """)

        insert_sql = f"INSERT INTO {SCHEMA_TARGET}.erp_cust_az12 (CID, BDATE, GEN) VALUES (?, ?, ?)"

        records = [tuple(x) for x in df_cleaned[['CID', 'BDATE', 'GEN']].to_numpy()]

        if records:
            cursor.fast_executemany = True 
            cursor.executemany(insert_sql, records)

        conn.commit() 
        print(f"save successfully {SCHEMA_TARGET}.erp_cust_az12, {len(records)} records")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"fail: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    process_and_load_data()
