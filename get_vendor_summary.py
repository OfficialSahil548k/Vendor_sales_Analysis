import sqlite3
from sqlalchemy import create_engine
import pandas as pd
import logging
from ingestion_db import ingest_db
import os

logging.basicConfig(
    filename = "logs/get_vendor_summary.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode="a" 
)


def create_vendor_summary(conn):
    '''this function will merge all the different tables to get the overall vendor summary and adding new columns to the resulting data'''
    vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS (
    SELECT
        VendorNumber,
        SUM(Freight) AS FreightCost
    FROM vendor_invoice
    GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
         FROM purchases p
        JOIN purchase_prices pp
        ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),
    SalesSummary As(
        SELECT 
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
    FROM sales
    GROUP BY VendorNo, Brand
    )
        SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC""", conn)

    return vendor_sales_summary

def clean_data(df):
    '''this function will clean data'''
    #changing datatype to float 
    df['Volume'] = df['Volume'].astype('float64')

    #filling missing value with 0
    df.fillna(0, inplace = True)

    #removing spacing between categorial columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    #creating new columns for better analysis
    df['GrossProfit'] = df['TotalSalesDollars']-df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit']/df['TotalSalesDollars'])*100
    df['StockTurnOver'] = df['TotalSalesQuantity']/df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars']/df['TotalPurchaseDollars']

    return df

    

if __name__ == '__main__':
    conn = sqlite3.connect('inventory.db')
    engine = create_engine('sqlite:///inventory.db')
    path = r"D:\DATA_ANALYST_MAJOR\data\vendor_sales_summary.csv"
    
    try:
        logging.info("Creating vendor summary table...")
        summary_df = create_vendor_summary(conn)
        logging.info("Vendor summary created successfully.")
        logging.debug(summary_df.head().to_string())

        logging.info("Cleaning data...")
        clean_df = clean_data(summary_df)
        logging.info("Data cleaned successfully.")
        logging.debug(clean_df.head().to_string())

        clean_df.to_csv(path, index=False)
        logging.info(f"Cleaned data exported to CSV at {path}")

        logging.info("Ingesting cleaned DataFrame directly into DB...")
        clean_df.to_sql("vendor_sales_summary", con=engine, if_exists='replace', index=False)
        logging.info("Data ingestion completed.")
    
    except Exception as e:
        logging.error(f"Error occurred: {e}")
