#location file: erp_loc_a101
#remove the minus of the cid column 
#cntry: empty to null

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

        # 1. build connection and read date from ingestion
        conn = pyodbc.connect(dwh_conn_str)
        print(f"successfully connect dwh: {DB_NAME}")

        query = f"SELECT * FROM {SCHEMA_SOURCE}.erp_loc_a101"
        df = pd.read_sql(query, conn)
        print(f"from{SCHEMA_SOURCE} load {len(df)} lines")



        #  2. data cleaning
        
        df_cleaned = df.where(pd.notnull(df), None)

        df['cid'] = df['cid'].astype(str).str.replace('-', '', regex=False)


        df['cntry'] = df['cntry'].astype(str).str.strip() 
        df['cntry'] = df['cntry'].replace(['', 'nan', 'None', 'nan '], None)

        print(df)
        df_cleaned = df.copy()
        df_cleaned = df_cleaned.replace({np.nan: None})

    # 3. Transformation
        conn.autocommit = False # close auto submit
        cursor = conn.cursor()

                # A. create Schema 和 table structure
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_TARGET};")
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.erp_loc_a101;")
        cursor.execute(f"""
                CREATE TABLE {SCHEMA_TARGET}.erp_loc_a101 (
                CID VARCHAR(50), 
                CNTRY VARCHAR(50)
                );
            """)

            # B. insert
        insert_sql = f"""
            INSERT INTO {SCHEMA_TARGET}.erp_loc_a101
            (   
               CID , 
                CNTRY
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ? ,?)
        """

        
        records = [
            tuple(x) for x in df_cleaned[[
                'CID' , 
                'CNTRY'
            ]].to_numpy() 
        ]

        if records:
            cursor.fast_executemany = True 
            cursor.executemany(insert_sql, records)

        conn.commit() # submit
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

