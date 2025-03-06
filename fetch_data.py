import pandas as pd
from db_connect import get_connection
import logging



# def fetch_data_from_sql(table_name):
#     logging.basicConfig(
#         filename="database_logs.log",
#         level=logging.INFO,
#         format="%(asctime)s - %(levelname)s - %(message)s")
    
#     try:
#         engine = get_connection()
#         if engine is None:
#             raise Exception("Database connection failed!")

#         query = f"SELECT * FROM {table_name}"  
#         df = pd.read_sql(query, con=engine)  

         
#         return df

#     except Exception as e:
#         print(f"Error: {str(e)}")
#         logging.error(f"Error occured while fetching the data : {str(e)}")
#         return None

# products_df = fetch_data_from_sql("Products")
# transactions_df = fetch_data_from_sql("Transactions")

# print(products_df.head())
# print(transactions_df.head())




def fetch_low_stock_products(threshold):
    """Fetch products where stock is below the given threshold."""
    logging.basicConfig(
        filename="database_logs.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s")

    try:
        engine = get_connection()
        query = f"SELECT * FROM Products WHERE StockQuantity < {threshold}"
        df = pd.read_sql_query(query, con= engine)
        
        return df

    except Exception as e:
        logging.error(f"Error fetching low-stock products: {str(e)}")
        print(f"Error: {str(e)}")
        return pd.DataFrame()  # Return empty DataFrame on error

# Fetch products with stock < 10
print("\nFetch low stocks ")
#threshold = int(input("Enter the number : "))
low_stock_df = fetch_low_stock_products(15)
print("📉 Low Stock Products:")
print(low_stock_df)

def fetch_sales_Report():
    logging.basicConfig(
        filename="database_logs.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s")
    try:
        engine = get_connection()
        if engine is None:
            raise Exception("Database connection is failed !")
    
        query ="""
                select p.ProductID, 
                    t.Date,
                    p.Name, 
                    p.Category, 
                    p.Price,
                    sum(t.Quantity) as Total_sales_Quantity,
                    count(t.TransactionID) as Total_Sales_Transactions,
                    p.price * sum(t.Quantity) as Total_Price
                    
                from Products p
                join Transactions t
                    on p.ProductID = t.ProductID
                    and t.Type ='Sale'
                group by p.ProductID,p.Name, p.Category, p.Price, t.Date
                Order by t.Date DESC 
            """
        df = pd.read_sql(query, con=engine)
        return df
    
    except Exception as e:
        logging.error(f'Error Occured : {str(e)}')


fetch_sales_Report()