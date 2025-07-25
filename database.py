import pandas as pd
import os
from sqlalchemy import create_engine

# Create SQLite engine
engine = create_engine('sqlite:///inventory.db')


def ingest_db(df, table_name, engine, chunksize=10000):
    """
    Insert dataframe into SQLite table in chunks to avoid MemoryError.
    """
    df.to_sql(
        table_name,
        con=engine,
        if_exists='replace',
        index=False,
        chunksize=chunksize  # batch insert
    )
    print(f"Inserted data into table '{table_name}' with {df.shape[0]} rows and {df.shape[1]} columns (in chunks).")


# Iterate through all CSV files in the 'data' folder
for file in os.listdir('data'):
    if file.endswith('.csv'):  # safer check
        table_name = os.path.splitext(file)[0].replace(" ", "_").lower()  # sanitize table name
        file_path = os.path.join('data', file)

        # Read CSV file in chunks to avoid memory overload
        chunks = pd.read_csv(file_path, chunksize=10000)

        # Combine all chunks into the DB (replace only for first chunk)
        first_chunk = True
        for chunk in chunks:
            if first_chunk:
                # First chunk replaces table if exists
                chunk.to_sql(table_name, con=engine, if_exists='replace', index=False)
                first_chunk = False
            else:
                # Later chunks append to existing table
                chunk.to_sql(table_name, con=engine, if_exists='append', index=False)

        print(f"Inserted data into table '{table_name}' from file '{file}'.")

