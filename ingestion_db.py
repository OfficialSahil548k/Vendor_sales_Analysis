import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode ="a"
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(file_path, table_name, engine, chunksize=10000):
    chunks = pd.read_csv(file_path, chunksize=chunksize)
    first_chunk = True
    total_rows = 0
    for chunk in chunks:
        if first_chunk:
            chunk.to_sql(table_name, con=engine, if_exists='replace', index=False)
            first_chunk = False
        else:
            chunk.to_sql(table_name, con=engine, if_exists='append', index=False)
        total_rows += len(chunk)
    print(f"Inserted data into table '{table_name}' with {total_rows} rows (in chunks).")

# Iterate through all CSV files in the 'data' folder
def load_raw_data():
    ''' this function will load CSVs as dataframes and ingest to database.'''
    start = time.time()
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            logging.info(f"Ingesting {file} into DB")
            table_name = os.path.splitext(file)[0].replace(" ", "_").lower()
            file_path = os.path.join('data', file)
            ingest_db(file_path, table_name, engine)
    end = time.time()
    total_time = (end - start)/60
    logging.info("-------------------Ingestion complete-------------------")
    logging.info(f"Total time: {total_time:.2f} minutes")

if name == "__main__":
    load_raw_data()
