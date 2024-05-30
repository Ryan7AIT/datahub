from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('ssb')

# Sample data row as a dictionary
sample_data = {
    'L_ORDERKEY': 3587,
    'L_LINENUMBER': 6,
    'L_PARTKEY': 106080,
    'L_SUPPKEY': 1101,
    'L_QUANTITY': 16.00,
    'L_EXTENDEDPRICE': 17377.28,
    'L_DISCOUNT': 0.01,
    'L_TAX': 0.03,
    'L_RETURNFLAG': 'N',
    'L_LINESTATUS': 'O',
    'L_SHIPDATE': '1996-05-11',
    'L_COMMITDATE': '1996-06-19',
    'L_RECEIPTDATE': '1996-06-04',
    'L_SHIPINSTRUCT': 'COLLECT COD',
    'L_SHIPMODE': 'FOB',
    'L_COMMENT': 'y ruthless dolphins to',
    'O_CUSTKEY': 77359,
    'O_ORDERSTATUS': 'O',
    'O_TOTALPRICE': 234552.97,
    'O_ORDERDATE': '1996-05-10',
    'O_ORDERPRIORITY': '4-NOT SPECIFIED',
    'O_CLERK': 'Clerk#000000443',
    'O_SHIPPRIORITY': 0,
    'O_COMMENT': 'ular patterns detect',
    'C_NAME': 'Customer#000077359',
    'C_ADDRESS': '22,0dkKFDYmDJK2LGyZ3Pbb',
    'C_NATIONKEY': 8,
    'C_PHONE': '18-938-153-8598',
    'C_ACCTBAL': 5874.57,
    'C_MKTSEGMENT': 'FURNITURE',
    'C_COMMENT': 'slyly even packages. slyly even ideas',
    'P_NAME': 'blush pink peru dodger lace',
    'P_MFGR': 'Manufacturer#1',
    'P_BRAND': 'Brand#15',
    'P_TYPE': 'PROMO BURNISHED TIN',
    'P_SIZE': 49,
    'P_CONTAINER': 'SM DRUM',
    'P_RETAILPRICE': 1086.08,
    'P_COMMENT': 'nal acc',
    'S_NAME': 'Supplier#000001101',
    'S_ADDRESS': 'Hr1EBv8bSuV0wcG',
    'S_NATIONKEY': 10,
    'S_PHONE': '20-392-415-6037',
    'S_ACCTBAL': 6509.11,
    'S_COMMENT': 'ular, regular deposits. packages haggle blithely fluffily busy accounts. carefully busy packages',
    'N_NAME': 'INDIA',
    'N_REGIONKEY': 10,
    'N_COMMENT': 'ular deposits boost slyly careful instruc',
    'R_NAME': 'ASIA',
    'R_COMMENT': 'IRAN'
}

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

# Execute the query with the sample data
session.execute(query, sample_data)

print("Insertion successful")
