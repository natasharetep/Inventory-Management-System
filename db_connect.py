from sqlalchemy import create_engine
import logging

def get_connection():
    """Create and return an SQLAlchemy engine for SQL Server."""
    server = "localhost"
    database = "InventoryDB"

    connection_string = f"mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"

    engine = create_engine(connection_string)
    return engine  # ✅ Returning an SQLAlchemy engine

def logging_report():

    logging.basicConfig(
        filename="database_logs.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s")
    
    return logging

# import pyodbc

# def get_connection():
#     conn = pyodbc.connect(
#         "DRIVER={SQL Server};"
#             "SERVER=localhost;" 
#             "DATABASE=InventoryDB;"  
#             "Trusted_Connection=yes;"
#     )
#     return conn
