#Understanding the dataset to explore how the data is present in the database and if there is a need of creating some aggregated tables that can help:
#   * vendor selection for profitability
#   * product pricing optimization
import pandas as pd
import sqlite3

conn = sqlite3.connect('inventory.db')
tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)

for table in tables['name']:
    print('-'*50,f'{table}','-'*50)
    print('count of records : ', pd.read_sql(f'SELECT COUNT(*) as Count FROM {table}', conn)['Count'].values[0])
    print(pd.read_sql(f"SELECT * FROM {table} LIMIT 5",conn))