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
        engine = get_connection() 
        if engine is None:
            raise Exception("Database connection failed!")

       

        df = pd.read_csv(file_path)
        print(f" Read {len(df)} records from {file_path}")

        # Exclude the identity column
        if identity_column in df.columns:
            df = df.drop(columns=[identity_column])

        # ✅ Use `to_sql()` for bulk insert (better than looping `execute()`)
        df.to_sql(table_name, engine, if_exists="append", index=False)

        print("✅ Data imported successfully!")
        logging.info(f"Successfully imported {len(df)} records into {table_name}")

    except Exception as e:
        logging.error(f" Error Occurred: {str(e)}")
        print(f" Error: {str(e)}")

    

# Import both CSVs into SQL
import_csv_to_sql("products.csv", "Products", "ProductID")  # Exclude ProductID
import_csv_to_sql("transactions.csv", "Transactions", "TransactionID")  # Exclude TransactionID
