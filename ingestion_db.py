import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# Configure logging
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# Create engine globally (or you can pass from external script)
engine = create_engine('sqlite:///inventory.db')


def ingest_db(file_path, table_name, engine, chunksize=10000):
    """Ingest CSV file into SQLite DB in chunks."""
    try:
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

        logging.info(f"✅ Inserted {total_rows} rows into table '{table_name}' from {file_path}")
        print(f"Inserted data into table '{table_name}' with {total_rows} rows (in chunks).")

    except Exception as e:
        logging.error(f"❌ Failed to ingest {file_path} into '{table_name}': {e}")
        print(f"Error processing {file_path}: {e}")


def load_raw_data(data_dir='data'):
    """
    Loads all CSVs from the given directory and ingests them into the DB.
    Each file becomes one table, based on the filename.
    """
    start = time.time()

    if not os.path.exists(data_dir):
        logging.error(f"⚠️ Data folder '{data_dir}' does not exist.")
        return

    for file in os.listdir(data_dir):
        if file.endswith('.csv'):
            file_path = os.path.join(data_dir, file)
            table_name = os.path.splitext(file)[0].replace(" ", "_").lower()
            logging.info(f"📥 Ingesting file: {file}")
            ingest_db(file_path, table_name, engine)

    end = time.time()
    logging.info("------------------- Ingestion Complete -------------------")
    logging.info(f"Total ingestion time: {(end - start)/60:.2f} minutes")


if __name__ == "__main__":
    load_raw_data()
