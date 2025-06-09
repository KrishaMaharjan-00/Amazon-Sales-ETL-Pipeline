import pandas as pd
import numpy as np
import logging
import os
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime, date

load_dotenv()


class DataExtractor:
    def __init__(self):
        self.csv_path = os.getenv('RAW_CSV_PATH')

    def extract_csv_data(self):
        return pd.read_csv(self.csv_path)


class DataTransformer:
    def __init__(self):
        self.cols_to_filter = ['actual_price', 'discounted_price', 'discount_percentage', 'rating_count', 'rating']
        self.text_columns = ['product_name', 'user_name', 'review_title', 'review_content', 'about_product']
        self.subset = ['product_id', 'review_id', 'user_id']


    def remove_extra_characters(self, df, cols_to_filter):
        for col in cols_to_filter:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r'[\$,£,₹,%,|]','',regex=True)
                .str.strip()
                .replace('', np.nan)
                .astype(float)
            )
        return df
    

    def fill_missing_values(self, df):
        df['rating'] = df['rating'].fillna(0)
        df['discount_percentage'] = df['discount_percentage'].fillna(0)
        df['review_content'] = df['review_content'].fillna("No review provided")
        return df
        
    
    def remove_duplicates(self, df, subset):
        return df.drop_duplicates(subset=subset)


    def trim_whitespace(self, df, text_columns):
        for col in text_columns:
            df[col] = df[col].astype(str).str.strip()
        return df


    def standardize_text_casing(self, df):
        df['product_name'] = df['product_name'].str.title()
        df['user_name'] = df['user_name'].str.title()
        df['review_title'] = df['review_title'].str.capitalize()
        df['category'] = df['category'].str.lower()
        return df
    

    def filter_invalid_records(self, df):
        df['rating'] = df['rating'].astype(float)
        df = df[(df['rating']>=1) & (df['rating']<=5)]
        df = df[df['discount_percentage'] <=100 ]
        df = df[(df['actual_price'] >=0) & (df['discounted_price'] >=0)]
        return df


    def feature_engineering(self, df):
        # calculated discount_value
        df['discount_value'] = df['actual_price'] - df['discounted_price']

        # calculate is_discounted flag
        df['is_discounted'] = df['actual_price'] > df['discounted_price']

        # calculate weighted rating_score
        df['rating_score'] = df['rating'] * df['rating_count']

        # calculate price_difference_pct
        df['price_difference_pct'] = np.where(
            df['actual_price'] !=0,
            ((df['actual_price'] - df['discounted_price']) / df['actual_price']) * 100,
            0
        )
        return df


    def transform_data(self, df):
        transformed_df = self.remove_extra_characters(df, cols_to_filter=self.cols_to_filter)
        transformed_df = self.fill_missing_values(transformed_df)
        transformed_df = self.remove_duplicates(transformed_df, self.subset)
        transformed_df = self.trim_whitespace(transformed_df, self.text_columns)
        transformed_df = self.standardize_text_casing(transformed_df)
        transformed_df = self.filter_invalid_records(transformed_df)
        transformed_df = self.feature_engineering(transformed_df)
        return transformed_df


class DatabaseConfigurer:
    def __init__(self):
        self.db_server = os.getenv("DB_SERVER")
        self.db_name = os.getenv("DB_NAME")
        self.db_user = os.getenv("DB_USER")
        self.db_pwd = os.getenv("DB_PWD")
        self.db_driver = os.getenv("DB_DRIVER")


    def get_db_engine_connector(self):
        odbc_params = f"DRIVER={self.db_driver};SERVER=tcp:{self.db_server};\
        DATABASE={self.db_name};UID={self.db_user};PWD={self.db_pwd}"
        connection_string = f"mssql+pyodbc:///?odbc_connect={odbc_params}"
        engine = create_engine(connection_string)
        return engine

    def get_database_connection(self):
        conn = pyodbc.connect(
            "DRIVER={"
            + self.db_driver
            + "};SERVER="
            + self.db_server
            + ";DATABASE="
            + self.db_name
            + ";UID="
            + self.db_user
            + ";PWD="
            + self.db_pwd
        )
        cur = conn.cursor()
        return conn, cur

    def close_database_connection(self, conn, cur):
        conn.commit()
        cur.close()
        conn.close()

    def execute_query(self, query, result=0):
        conn, cur = self.get_database_connection()
        if result==1:
            result = cur.execute(query).fetchall()
            self.close_database_connection(conn, cur)
            return result
        else:
            cur.execute(query)
        self.close_database_connection(conn, cur)



class DataLoader:
    def __init__(self):
        self.db_con = DatabaseConfigurer()


    def insert_data_into_db_table(self, df, table_name, schema_name):
        engine = self.db_con.get_db_engine_connector()
        return df.to_sql(table_name, engine, if_exists="replace", index=False, schema=schema_name)
    

class CommonUtils:
    def add_audit_columns(df):
        df['created_at'] = datetime.now()
        df['created_by'] = 'system'
        return df


class StreamFileLogger:
    def get_logger(default_log_level=logging.INFO) -> object:
        logging.basicConfig(level=default_log_level)
        logger = logging.getLogger()
        logger.setLevel(default_log_level)

        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        
        # formatter = CustomFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        if logger.hasHandlers():
            logger.handlers.clear()

        # Stream Handling
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # File Handling
        file_path = "./log_files"

        # Create file path if it does not exists
        if not os.path.exists(file_path):
            os.mkdir(file_path)

        d = str(date.today())
        file_output = os.path.join(file_path, "Sales_Data_" + d + ".log")
        fh = logging.FileHandler(file_output)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger

if __name__ == '__main__':
    # logger
    logger = StreamFileLogger.get_logger()

    # extract
    logger.info("Raw: Extraction Started")
    extractor = DataExtractor()
    extracted_df = extractor.extract_csv_data()
    logger.info("Raw: Extracted Successfully")

    # load
    logger.info("Raw: Loading Data")
    loader = DataLoader()
    loader.insert_data_into_db_table(extracted_df, os.getenv("LANDING_TABLE"), os.getenv("LANDING_SCHEMA"))
    logger.info("Raw: Loaded Successfully")


    # transform
    logger.info("Transformation: Started")
    transformer = DataTransformer()
    transformed_df = transformer.transform_data(extracted_df)
    transformed_df['sales_id'] = range(1, len(transformed_df) + 1)
    logger.info("Transformation: Completed Successfully")

    logger.info("Transformation: Loading Data")
    loader.insert_data_into_db_table(transformed_df, os.getenv("STAGING_TABLE"), os.getenv("STAGING_SCHEMA"))
    logger.info("Transformation: Loaded Successfully")


    # dim tables
    logger.info("Dim Product: Creation Started")
    product_df = transformed_df[['product_id', 'product_name', 'category', 'about_product', 'img_link', 'product_link']]
    CommonUtils.add_audit_columns(product_df)
    loader.insert_data_into_db_table(product_df, os.getenv("PRODUCT_TABLE"), os.getenv("FINAL_SCHEMA"))
    logger.info("Dim Product: Created Successfully")


    logger.info("Dim User: Creation Started")
    user_df = transformed_df[['user_id', 'user_name']]
    CommonUtils.add_audit_columns(user_df)
    loader.insert_data_into_db_table(user_df, os.getenv("USER_TABLE"), os.getenv("FINAL_SCHEMA"))
    logger.info("Dim User: Created Successfully")


    logger.info("Dim Review: Creation Started")
    review_df = transformed_df[['review_id', 'review_title', 'review_content']]
    CommonUtils.add_audit_columns(review_df)
    loader.insert_data_into_db_table(review_df, os.getenv("REVIEW_TABLE"), os.getenv("FINAL_SCHEMA"))
    logger.info("Dim Review: Created Successfully")


    
    
    # fact tables
    logger.info("Fact Sales: Creation Started")
    sales_df = transformed_df[['sales_id', 'product_id', 'user_id', 'review_id', 'discounted_price', 'actual_price', 'discount_percentage', 'rating', 'rating_count', 'discount_value', 'is_discounted', 'rating_score', 'price_difference_pct']]
    CommonUtils.add_audit_columns(sales_df)
    loader.insert_data_into_db_table(sales_df, os.getenv("SALES_TABLE"), os.getenv("FINAL_SCHEMA"))
    logger.info("Fact Sales: Created Successfully")


    















