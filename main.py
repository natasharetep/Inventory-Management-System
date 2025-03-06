import pandas as pd
from db_connect import get_connection
import logging

def import_csv_to_sql(file_path, table_name, identity_column):
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

        # Exclude the identity column
        columns = [col for col in df.columns if col.lower() != identity_column.lower()]
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['?' for _ in columns])  # Parameterized placeholders

        sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        for index, row in df.iterrows():
            values = [row[col] for col in columns]  # Get values without identity column
            cursor.execute(sql, values)  # Use parameterized query

        conn.commit()
        print(" Data imported successfully!")

    except Exception as e:
        logging.error(f" Error Occurred: {str(e)}")
        print(f" Error: {str(e)}")

    finally:
        if conn:
            conn.close()

# Import both CSVs into SQL
import_csv_to_sql("products.csv", "Products", "ProductID")  # Exclude ProductID
import_csv_to_sql("transactions.csv", "Transactions", "TransactionID")  # Exclude TransactionID
