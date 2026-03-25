import pyodbc
import pandas as pd
import numpy as np


#CONNECT DWH
DRIVER_PATH = '/opt/homebrew/lib/psqlodbcw.so'
DB_NAME = 'dwh'
PASSWORD = '563634851'
PORT = '1234'
SCHEMA_SOURCE = 'transformation'
SCHEMA_TARGET = 'curated'

dwh_conn_str = f"DRIVER={{{DRIVER_PATH}}};SERVER=localhost;DATABASE={DB_NAME};UID=postgres;PWD={PASSWORD};PORT={PORT};"

conn = None

#build connection 
conn = pyodbc.connect(dwh_conn_str)
print(f"successfully connect dwh: {DB_NAME}")


customer_crm_df = pd.read_sql(f"SELECT * FROM {SCHEMA_SOURCE}.crm_cust_info",conn)
customer_erp_df = pd.read_sql(f"SELECT * FROM {SCHEMA_SOURCE}.erp_cust_az12",conn)
customer_erp_loc = pd.read_sql(f"SELECT * FROM {SCHEMA_SOURCE}.erp_loc_a101",conn)


# merge data

df = pd.merge(
    left = customer_crm_df,
    right = customer_erp_df,
    how = 'left',
    left_on = 'cst_key',
    right_on = 'cid'
)

df = pd.merge(
    left = df,
    right = customer_erp_loc,
    how = 'left',
    left_on = 'cst_key',
    right_on = 'cid',
    suffixes= ('','_loc') #suffix for same name

)

print(df.columns)
print(df)

conn.autocommit = False 
cursor = conn.cursor()

#change column name of dim_customers
dim_customers = pd.DataFrame({
    "customer_id": df["cst_id"],
    "customer_number":df["cst_key"],
    "first_name":df["cst_firstname"],
    "last_name":df["cst_lastname"],
    "country":df["cntry"],
    "marital_status":df["cst_marital_status"],
    "gender":df["gen"],
    "birthdate":df["bdate"],
    "create_date":df["cst_create_date"]
})

print(dim_customers.columns)
print(dim_customers)


#insert
dim_customers.insert(0,'customer_key',dim_customers.index +1)
print(dim_customers)


#write back to dwh
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_TARGET};")
cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.dim_customers;")
cursor.execute(f"""
                CREATE TABLE {SCHEMA_TARGET}.dim_customers (
                customer_key int,
                customer_id int,
                customer_number VARCHAR(50),
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                country VARCHAR(50),
                marital_status VARCHAR(50),
                gender VARCHAR(50),
                birthdate TIMESTAMP,
                create_date TIMESTAMP
                );
            """)

insert_sql = f"INSERT INTO {SCHEMA_TARGET}.dim_customers(customer_key,customer_id,customer_number,first_name,last_name,country,marital_status,gender,birthdate,create_date) VALUES (?,?,?,?,?,?,?,?,?,?)"

for row in dim_customers.itertuples(index=False):
    cursor.execute(insert_sql, tuple(row))
conn.commit()
print("dim_customers write in successfully")

#dim_product
product_prd_info= pd.read_sql(f"SELECT * FROM {SCHEMA_SOURCE}.crm_prd_info",conn)
product_prd_catgory= pd.read_sql(f"SELECT * FROM ingestion.erp_px_cat_g1v2",conn)

print(product_prd_info)
print(product_prd_catgory)

# merge data
#left prd_info 
#right erp_px_cat_g1v2

df = pd.merge(
    left = product_prd_info,
    right = product_prd_catgory,
    how = 'left',
    left_on = 'prd_key',
    right_on = 'id'
)

print(df.columns)
print(df)

dim_products = pd.DataFrame({
    "product_id": df["prd_id"],
    "product_number":df["prd_key"],
    "product_category_id":df["prd_category"],
    "product_category":df["cat"],
    "product_sub_category":df["subcat"],
    "product_name":df["prd_nm"],
    "product_cost":df["prd_cost"],
    "product_line":df["prd_line"],
    "start_dt":df["prd_start_dt"],
    "end_dt":df["prd_end_dt"],
    "maintenance":df["maintenance"]
})

print(dim_products.columns)
print(dim_products)

#insert
dim_products.insert(0,'product_key',dim_products.index +1)
print(dim_products)

#write back to dwh
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_TARGET};")
cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.dim_products;")
cursor.execute(f"""
                CREATE TABLE {SCHEMA_TARGET}.dim_products (
                product_key   INT,
                product_id int,
                product_number VARCHAR(50),
                product_category_id VARCHAR(50),
                product_category VARCHAR(50),
                product_sub_category VARCHAR(50),
                product_name VARCHAR(50),
                product_cost float,
                product_line VARCHAR(50),
                start_dt TIMESTAMP,
                end_dt TIMESTAMP,
                maintenance VARCHAR(50)
                );
            """)

insert_sql = f"INSERT INTO {SCHEMA_TARGET}.dim_products(product_key,product_id,product_number,product_category_id,product_category,product_sub_category,product_name,product_cost,product_line,start_dt,end_dt,maintenance) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"

for row in dim_products.itertuples(index=False):
    cursor.execute(insert_sql, tuple(row))
conn.commit()
print("dim_products write in successfully")

# FACT TABLE: fact_sales
sales_details = pd.read_sql(f"SELECT * FROM {SCHEMA_SOURCE}.crm_sales_details", conn)
print(sales_details.columns)
print(sales_details)

# customer_key
# product_key
fact_sales = pd.merge(
    left=sales_details,
    right=dim_customers[['customer_key', 'customer_id']],  
    how='left',
    left_on='sls_cust_id',
    right_on='customer_id'   
)

fact_sales = pd.merge(
    left=fact_sales,
    right=dim_products[['product_key', 'product_number']],
    how='left',
    left_on='sls_prd_key',
    right_on='product_number'  # product_number match prd_key
)

print(fact_sales.columns)
print(fact_sales)

fact_sales_df = pd.DataFrame({
    "order_number":   fact_sales["sls_ord_num"],
    "customer_key":   fact_sales["customer_key"],
    "product_key":    fact_sales["product_key"],
    "order_date":     fact_sales["sls_order_dt"],
    "ship_date":      fact_sales["sls_ship_dt"],
    "due_date":       fact_sales["sls_due_dt"],
    "sales_amount":   fact_sales["sls_sales"],
    "quantity":       fact_sales["sls_quantity"],
    "price":          fact_sales["sls_price"]
})

fact_sales_df = fact_sales_df.dropna(subset=["customer_key", "product_key"])
fact_sales_df["customer_key"] = fact_sales_df["customer_key"].astype(int)
fact_sales_df["product_key"]  = fact_sales_df["product_key"].astype(int)
print(fact_sales_df)

# write back to DWH
cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.fact_sales;")
cursor.execute(f"""
    CREATE TABLE {SCHEMA_TARGET}.fact_sales (
        order_number  VARCHAR(50),
        customer_key  INT,
        product_key   INT,
        order_date    TIMESTAMP,
        ship_date     TIMESTAMP,
        due_date      TIMESTAMP,
        sales_amount  FLOAT,
        quantity      INT,
        price         FLOAT
    );
""")

insert_sql = f"""
    INSERT INTO {SCHEMA_TARGET}.fact_sales
    (order_number, customer_key, product_key, order_date, ship_date, due_date, sales_amount, quantity, price)
    VALUES (?,?,?,?,?,?,?,?,?)
"""

for row in fact_sales_df.itertuples(index=False):
    cursor.execute(insert_sql, tuple(row))
conn.commit()
print("fact_sales write in successfully")

cursor.close()
conn.close()
print("connection close")
