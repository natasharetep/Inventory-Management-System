import pandas as pd
from db_connect import get_connection, logging_report
from fetch_data import fetch_low_stock_products, fetch_sales_Report, fetch_revenue_Report



def generate_low_stock_report(threshold=15):
    log_value = logging_report()
    df = fetch_low_stock_products(threshold)
    if df is not None and not df.empty:
        
        df.to_csv('stock_report.csv')
        print("\nReport generated successfully!")

generate_low_stock_report()

def generate_sales_report():
    df = fetch_sales_Report()
    if df is not None and not df.empty:
        df.to_csv('Sales Report.csv')
        print("\nSales report generated!")

generate_sales_report()

def generate_revenue_report():
    df = fetch_revenue_Report()
    if df is not None and not df.empty:
        df.to_csv('Revenue Report.csv')
        print("\nSales report generated!")

generate_revenue_report()