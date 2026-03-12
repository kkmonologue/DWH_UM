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
        # 1. build connection and read date from ingestion
        conn = pyodbc.connect(dwh_conn_str)
        print(f"successfully connect dwh: {DB_NAME}")
        
        query = f"SELECT * FROM {SCHEMA_SOURCE}.crm_cust_info"
        df = pd.read_sql(query, conn)
        print(f"from{SCHEMA_SOURCE} load {len(df)} lines")

        #  2. data cleaning
        # A. delete all NULL lines
        df = df.dropna()

        # B. delete space in name colums
        df['cst_firstname'] = df['cst_firstname'].str.strip()
        df['cst_lastname'] = df['cst_lastname'].str.strip()

        #replace name
        df['cst_marital_status'] = df['cst_marital_status'].str.replace('M','Married')
        df['cst_marital_status'] = df['cst_marital_status'].str.replace('S','Single')
        df['cst_gndr'] = df['cst_gndr'].str.replace('M','Male')
        df['cst_gndr'] = df['cst_gndr'].str.replace('F','Female')
        
        # C. check：AW000 + cst_id == cst_key
        # make sure cst_id is int to str，to avoid 1.0 float
        mask = ('AW000' + df['cst_id'].astype(int).astype(str)) == df['cst_key']
        df_cleaned = df[mask].copy()

        print(f"🧹 delete {len(df) - len(df_cleaned)} lines")

        # 3. Transformation
        conn.autocommit = False # close auto submit
        cursor = conn.cursor()

        # A. create Schema 和 table structure
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_TARGET};")
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.crm_cust_info;")
        cursor.execute(f"""
            CREATE TABLE {SCHEMA_TARGET}.crm_cust_info (
                cst_id int, 
                cst_key VARCHAR(50), 
                cst_firstname VARCHAR(50), 
                cst_lastname VARCHAR(50), 
                cst_marital_status VARCHAR(50), 
                cst_gndr VARCHAR(50), 
                cst_create_date TIMESTAMP
            );
        """)

        # B. insert
        insert_sql = f"""
            INSERT INTO {SCHEMA_TARGET}.crm_cust_info 
            (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        records = [
            tuple(x) for x in df_cleaned[[
                'cst_id', 'cst_key', 'cst_firstname', 'cst_lastname', 
                'cst_marital_status', 'cst_gndr', 'cst_create_date'
            ]].values
        ]

        if records:
            cursor.fast_executemany = True 
            cursor.executemany(insert_sql, records)
        
        conn.commit() # submit
        print(f"save successfully {SCHEMA_TARGET}.crm_cust_info， {len(records)} records")

    except Exception as e:
        if conn:
            conn.rollback() 
        print(f"fail: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    process_and_load_data()
