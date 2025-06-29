import os
import json
import math
import pyodbc
import pandas as pd
import boto3

DRIVER = "ODBC Driver 17 for SQL Server"
SERVER = os.getenv("MSSQL_SERVERNAME", "127.0.0.1")
DATABASE = os.getenv("MSSQL_DBNAME", "dbname")
TABLE_NAME = os.getenv("TABLE_NAME")
USERNAME = os.getenv("MSSQL_USERNAME", "krish")
PASSWORD = os.getenv("MSSQL_PASSWORD")
AWS_PROFILE=os.getenv("AWS_PROFILE", "personal")

CONNECTION_STRING = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
)

S3_BUCKET = "krish-mssql-to-parquet-poc"
S3_PREFIX = "exports/"
CHUNK_SIZE = 1000
CHECKPOINT_FILE = "checkpoint.json"


session = boto3.Session(profile_name=AWS_PROFILE)
s3_client = session.client("s3")

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {"last_offset": 0}

def save_checkpoint(offset):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"last_offset": offset}, f)

def get_total_rows(cursor):
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    return cursor.fetchone()[0]

def fetch_chunk(cursor, offset, chunk_size):
    query = f"""
        SELECT *
        FROM {TABLE_NAME}
        ORDER BY [BRCONO] -- Replace with your PK column!
        OFFSET {offset} ROWS
        FETCH NEXT {chunk_size} ROWS ONLY
    """
    return pd.read_sql(query, cursor.connection)

def write_parquet(df, filename):
    df.to_parquet(filename, index=False)

def upload_to_s3(filename):
    key = f"{S3_PREFIX}{os.path.basename(filename)}"
    s3_client.upload_file(filename, S3_BUCKET, key)
    print(f"Uploaded {filename} to s3://{S3_BUCKET}/{key}")

def main():
    checkpoint = load_checkpoint()
    last_offset = checkpoint["last_offset"]

    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()

    total_rows = get_total_rows(cursor)
    total_chunks = math.ceil(total_rows / CHUNK_SIZE)
    current_chunk = last_offset // CHUNK_SIZE

    print(f"Total rows: {total_rows}, total chunks: {total_chunks}")

    try:
        while last_offset < total_rows:
            print(f"Exporting chunk starting at offset {last_offset}...")

            df = fetch_chunk(cursor, last_offset, CHUNK_SIZE)
            if df.empty:
                print("No more data to export.")
                break

            filename = f"{TABLE_NAME}_{current_chunk:06d}.parquet"
            write_parquet(df, filename)
            upload_to_s3(filename)

            # Clean up local file
            os.remove(filename)

            last_offset += len(df)
            current_chunk += 1

            save_checkpoint(last_offset)
            print(f"Checkpoint updated to offset {last_offset}")

    finally:
        cursor.close()
        conn.close()
        print("Export finished or stopped. You can restart safely.")

if __name__ == "__main__":
    main()
