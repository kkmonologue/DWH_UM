import pyodbc

# --- CONFIGURATION ---
DRIVER_PATH = '/opt/homebrew/lib/psqlodbcw.so'
DB_NAME = 'dwh'
SCHEMA_NAME = 'ingestion'
PASSWORD = '563634851'
PORT = '1234'

# Connection strings
system_conn_str = f"DRIVER={{{DRIVER_PATH}}};SERVER=localhost;DATABASE=postgres;UID=postgres;PWD={PASSWORD};PORT={PORT};"
dwh_conn_str = f"DRIVER={{{DRIVER_PATH}}};SERVER=localhost;DATABASE={DB_NAME};UID=postgres;PWD={PASSWORD};PORT={PORT};"

def create_structure():
    try:
        # 1. Ensure DB exists
        sys_conn = pyodbc.connect(system_conn_str)
        sys_conn.autocommit = True
        sys_cursor = sys_conn.cursor()
        try:
            sys_cursor.execute(f"CREATE DATABASE {DB_NAME}")
            print(f"✅ Created database: {DB_NAME}")
        except pyodbc.Error as e:
            if '42P04' not in str(e): raise e
        sys_cursor.close(); sys_conn.close()

        # 2. Create Schema and Tables
        conn = pyodbc.connect(dwh_conn_str)
        conn.autocommit = True
        cursor = conn.cursor()

        print(f"--- Creating Schema: {SCHEMA_NAME} ---")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")

        # Table: crm_prd_info
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.crm_prd_info CASCADE;")
        cursor.execute(f"""
            CREATE TABLE {SCHEMA_NAME}.crm_prd_info (
                prd_id int, prd_key VARCHAR(50), prd_nm VARCHAR(50), 
                prd_cost int, prd_line VARCHAR(50), prd_start_dt TIMESTAMP, prd_end_dt TIMESTAMP
            );
        """)

        # Table: crm_cust_info
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.crm_cust_info CASCADE;")
        cursor.execute(f"""
            CREATE TABLE {SCHEMA_NAME}.crm_cust_info (
                cst_id int, cst_key VARCHAR(50), cst_firstname VARCHAR(50), 
                cst_lastname VARCHAR(50), cst_marital_status VARCHAR(50), 
                st_gndr VARCHAR(50), cst_create_date TIMESTAMP
            );
        """)

        # Table: crm_sales_details (Using VARCHAR for Dates to avoid "0" errors)
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.crm_sales_details CASCADE;")
        cursor.execute(f"""
            CREATE TABLE {SCHEMA_NAME}.crm_sales_details (
                sls_ord_num VARCHAR(50), sls_prd_key VARCHAR(50), sls_cust_id int,
                sls_order_dt VARCHAR(50), sls_ship_dt VARCHAR(50), sls_due_dt VARCHAR(50),
                sls_sales int, sls_quantity int, sls_price int
            );
        """)

        # ERP Tables
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.erp_cust_az12 CASCADE;")
        cursor.execute(f"CREATE TABLE {SCHEMA_NAME}.erp_cust_az12 (CID VARCHAR(50), BDATE TIMESTAMP, GEN VARCHAR(50));")

        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.erp_loc_a101 CASCADE;")
        cursor.execute(f"CREATE TABLE {SCHEMA_NAME}.erp_loc_a101 (CID VARCHAR(50), CNTRY VARCHAR(50));")

        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.erp_px_cat_g1v2 CASCADE;")
        cursor.execute(f"CREATE TABLE {SCHEMA_NAME}.erp_px_cat_g1v2 (ID VARCHAR(50), CAT VARCHAR(50), SUBCAT VARCHAR(50), MAINTENANCE VARCHAR(50));")

        print("🚀 Tables created successfully.")
        conn.close()

    except Exception as e:
        print(f"❌ DDL Error: {e}")

if __name__ == "__main__":
    create_structure()
