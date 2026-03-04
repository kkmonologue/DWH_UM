import pyodbc

# --- CONFIGURATION ---
DRIVER_PATH = '/opt/homebrew/lib/psqlodbcw.so'
DB_NAME = 'dwh'
SCHEMA_NAME = 'ingestion'
PASSWORD = '563634851'
PORT = '1234'

dwh_conn_str = f"DRIVER={{{DRIVER_PATH}}};SERVER=localhost;DATABASE={DB_NAME};UID=postgres;PWD={PASSWORD};PORT={PORT};"

def load_data():
    ingestion_map = {
        "crm_prd_info": "/Library/PostgreSQL/18/data/ingestion/prd_info.csv",
        "crm_cust_info": "/Library/PostgreSQL/18/data/ingestion/cust_info.csv",
        "crm_sales_details": "/Library/PostgreSQL/18/data/ingestion/sales_details.csv",
        "erp_cust_az12": "/Library/PostgreSQL/18/data/ingestion/CUST_AZ12.csv",
        "erp_loc_a101": "/Library/PostgreSQL/18/data/ingestion/LOC_A101.csv",
        "erp_px_cat_g1v2": "/Library/PostgreSQL/18/data/ingestion/PX_CAT_G1V2.csv"
    }

    conn = None
    try:
        conn = pyodbc.connect(dwh_conn_str)
        conn.autocommit = True
        cursor = conn.cursor()

        for table, file_path in ingestion_map.items():
            print(f"Loading {table}...")
            # Truncate table first to prevent duplicates if re-running
            cursor.execute(f"TRUNCATE TABLE {SCHEMA_NAME}.{table};")
            
            copy_sql = f"COPY {SCHEMA_NAME}.{table} FROM '{file_path}' DELIMITER ',' CSV HEADER;"
            cursor.execute(copy_sql)
            print(f"✅ {table} loaded.")

        print("\n🚀 All data loaded successfully!")

    except Exception as e:
        print(f"❌ Load Error: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    load_data()
