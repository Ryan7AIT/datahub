from cassandra.cluster import Cluster
import csv

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('ssb')

# Define the function to load data
def load_data(file_path, query):
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header row
        expected_columns = len(header)
        for row in reader:
            if len(row) != expected_columns:
                print(f"Row length {len(row)} does not match expected length {expected_columns}")
                print(row)
                continue
            session.execute(query, tuple(row))
            

# Adjust the file path to your CSV file
file_path = '/Users/mac/tpch-dbgen/ssbden.csv'

# Define the CQL query for inserting data into the wide_table
query = '''
    INSERT INTO wide_table (
        L_ORDERKEY, L_LINENUMBER, L_PARTKEY, L_SUPPKEY, L_QUANTITY, 
        L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, 
        L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, 
        L_COMMENT, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, 
        O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, 
        C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, 
        C_MKTSEGMENT, C_COMMENT, P_NAME, P_MFGR, P_BRAND, 
        P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT, 
        S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, 
        S_COMMENT, N_NAME, N_REGIONKEY, N_COMMENT, 
        R_NAME, R_COMMENT
    ) VALUES (:L_ORDERKEY, :L_LINENUMBER, :L_PARTKEY, :L_SUPPKEY, :L_QUANTITY, 
              :L_EXTENDEDPRICE, :L_DISCOUNT, :L_TAX, :L_RETURNFLAG, :L_LINESTATUS, 
              :L_SHIPDATE, :L_COMMITDATE, :L_RECEIPTDATE, :L_SHIPINSTRUCT, :L_SHIPMODE, 
              :L_COMMENT, :O_CUSTKEY, :O_ORDERSTATUS, :O_TOTALPRICE, :O_ORDERDATE, 
              :O_ORDERPRIORITY, :O_CLERK, :O_SHIPPRIORITY, :O_COMMENT, 
              :C_NAME, :C_ADDRESS, :C_NATIONKEY, :C_PHONE, :C_ACCTBAL, 
              :C_MKTSEGMENT, :C_COMMENT, :P_NAME, :P_MFGR, :P_BRAND, 
              :P_TYPE, :P_SIZE, :P_CONTAINER, :P_RETAILPRICE, :P_COMMENT, 
              :S_NAME, :S_ADDRESS, :S_NATIONKEY, :S_PHONE, :S_ACCTBAL, 
              :S_COMMENT, :N_NAME, :N_REGIONKEY, :N_COMMENT, 
              :R_NAME, :R_COMMENT)
'''

# Load the data and print debug information
load_data(file_path, query)

# Verify the number of columns
keyspace_metadata = cluster.metadata.keyspaces['ssb']
table_metadata = keyspace_metadata.tables['wide_table']
column_count = len(table_metadata.columns)
print(f'The table "wide_table" has {column_count} columns.')