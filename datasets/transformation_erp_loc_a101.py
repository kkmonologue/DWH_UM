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
        conn = pyodbc.connect(dwh_conn_str)
        print(f"successfully connect dwh: {DB_NAME}")

        query = f"SELECT * FROM {SCHEMA_SOURCE}.erp_loc_a101"
        df = pd.read_sql(query, conn)
        print(f"from {SCHEMA_SOURCE} load {len(df)} lines")

        
        df.columns = df.columns.str.strip().str.upper()

        df['CID'] = df['CID'].astype(str).str.replace('-', '', regex=False)

        df['CNTRY'] = df['CNTRY'].astype(str).str.strip() 
        df['CNTRY'] = df['CNTRY'].replace(['', 'nan', 'None', 'nan '], None)

        print(df.head()) 
        
        df_cleaned = df.replace({np.nan: None})

        # Transformation ---
        conn.autocommit = False 
        cursor = conn.cursor()

        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_TARGET};")
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.erp_loc_a101;")
        cursor.execute(f"""
                CREATE TABLE {SCHEMA_TARGET}.erp_loc_a101 (
                CID VARCHAR(50), 
                CNTRY VARCHAR(50)
                );
            """)


        insert_sql = f"INSERT INTO {SCHEMA_TARGET}.erp_loc_a101 (CID, CNTRY) VALUES (?, ?)"

        records = [
            tuple(x) for x in df_cleaned[['CID', 'CNTRY']].to_numpy() 
        ]

        if records:
            cursor.fast_executemany = True 
            cursor.executemany(insert_sql, records)

        conn.commit() 
        print(f"save successfully {SCHEMA_TARGET}.erp_loc_a101， {len(records)} records")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"fail: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    process_and_load_data()

