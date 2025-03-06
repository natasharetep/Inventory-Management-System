import pandas as pd
from db_connect import get_connection
import logging

def import_csv_to_sql(file_path, table_name):
    logging.basicConfig(
        filename="database_logs.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    try:
        conn = get_connection()
        if conn is None:
            raise Exception("Database connection failed!")

        cursor = conn.cursor()

        df = pd.read_csv(file_path)
        print(f" Read {len(df)} records from {file_path}")

        for index, row in df.iterrows():  # FIXED iterrows()
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['?' for _ in row.values])  # SQL placeholders
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            logging.info(f"Executing Query: {sql} with values {tuple(row.values)}")
            cursor.execute(sql, tuple(row.values))  # FIXED SQL Injection issue

        conn.commit()
        print(" Data imported successfully!")

    except Exception as e:
        logging.error(f" Error Occurred: {str(e)}")
        print(f" Error: {str(e)}")

    finally:
        if conn:
            conn.close()

# Import both CSVs into SQL
import_csv_to_sql("products.csv", "Products")
import_csv_to_sql("transactions.csv", "Transactions")
