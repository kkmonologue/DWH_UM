import pyodbc
import pandas as pd

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

        query = f"SELECT * FROM {SCHEMA_SOURCE}.crm_prd_info"
        df = pd.read_sql(query, conn)
        print(f"from{SCHEMA_SOURCE} load {len(df)} lines")



        #  2. data cleaning

        #prd_key
        df['prd_category'] =df['prd_key'].str[6:]
        df['prd_key'] = df['prd_key'].str[:5]
        df['prd_key'] = df['prd_key'].str.replace('-','_')

        df.loc[df['prd_cost'] < 0, 'prd_cost'] = 0
        df['prd_cost'] = df['prd_cost'].fillna(0)
        df['prd_line'] = df['prd_line'].str.replace('R','Road')
        df['prd_line'] = df['prd_line'].str.replace('S','Sport')
        df['prd_line'] = df['prd_line'].str.replace('M','Modified')
        df['prd_line'] = df['prd_line'].str.replace('T','Tour')
        df['prd_line'] = df['prd_line'].fillna('NA')
        df['prd_start_dt'] = pd.to_datetime(df['prd_start_dt']).dt.date
        df.loc[df['prd_end_dt'].notnull(), 'prd_end_dt'] = df['prd_start_dt'].shift(-1) - pd.Timedelta(days=1)
        
        print(df)
        df_cleaned = df.copy()

        # 3. Transformation
        conn.autocommit = False # close auto submit
        cursor = conn.cursor()

                # A. create Schema 和 table structure
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_TARGET};")
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.crm_prd_info;")
        cursor.execute(f"""
                CREATE TABLE {SCHEMA_TARGET}.crm_prd_info (
                    prd_id int, 
                    prd_key VARCHAR(50), 
                    prd_category VARCHAR(50),
                    prd_nm VARCHAR(50), 
                    prd_cost int, 
                    prd_line VARCHAR(50), 
                    prd_start_dt DATE, 
                    prd_end_dt DATE
                );
            """)

            # B. insert
        insert_sql = f"""
            INSERT INTO {SCHEMA_TARGET}.crm_prd_info 
            (   
                prd_id, 
                prd_key, 
                prd_category,
                prd_nm, 
                prd_cost, 
                prd_line, 
                prd_start_dt, 
                prd_end_dt
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        
        records = [
            tuple(x) for x in df_cleaned[[
                'prd_id',
                'prd_key', 
                'prd_category',
                'prd_nm', 
                'prd_cost', 
                'prd_line', 
                'prd_start_dt', 
                'prd_end_dt'
            ]].to_numpy() 
        ]

        if records:
            cursor.fast_executemany = True 
            cursor.executemany(insert_sql, records)

        conn.commit() # submit
        print(f"save successfully {SCHEMA_TARGET}.crm_prd_info， {len(records)} records")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"fail: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    process_and_load_data()
