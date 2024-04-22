import pandas as pd
import db_extract
import json
import logging
from db_extract import *
##  Grammy 

def read_db():
    query = "SELECT * FROM grammy;"

    connection = db_extract.connect_bd()

    df_db = None
    if connection is not None:
        df_db = db_extract.extract_data(connection, query)

    if connection is not None:
        connection.dispose()
        logging.info("Closed connection")

    if df_db is not None:
        return df_db.to_json(orient='records')
    else:
        logging.warning("No data could be extracted from the database.")
        return None
    

    
def transform_db(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_db")

    if str_data is None:
        logging.warning("No data were found to transform.")
        return None

    try:
        json_data = json.loads(str_data)
        grammy_df = pd.json_normalize(data=json_data)
        logging.info(f"DataFrame columns: {grammy_df.columns.tolist()}")
        num_columns_before = grammy_df.shape[1]
        logging.info(f"The DataFrame has {num_columns_before} columns before the transformation.")
        logging.info("Null values in the DataFrame before transformations:")
        logging.info(grammy_df.isnull().sum())

        # Transformations
        grammy_df['artist'] = grammy_df['artist'].fillna(grammy_df['workers'].str.extract(r'\(([^)]+)\)')[0])
        grammy_df['artist'].fillna(grammy_df['workers'], inplace=True)  
        grammy_df['workers'].fillna(grammy_df['artist'], inplace=True)
        grammy_df['category'] = grammy_df['category'].str.replace(r'[-()]', '', regex=True)  
        grammy_df.loc[grammy_df['category'].str.contains('Artist', case=False), ['artist', 'workers']] = grammy_df['nominee']  
        grammy_df['artist'] = grammy_df['artist'].fillna(grammy_df['nominee'])  
        grammy_df['workers'] = grammy_df['workers'].fillna(grammy_df['nominee'])
        grammy_df = grammy_df.rename(columns={'winner': 'grammy_nominee'})  
        grammy_df.drop([2279, 2379, 2475, 2569, 4527, 4575], inplace=True)  
        grammy_df.drop(columns=['published_at', 'updated_at', 'img'], inplace=True)  

        logging.info(f"Finished transformations")
        
        num_columns_after = grammy_df.shape[1]
        logging.info(f"The DataFrame has {num_columns_after} columns after the transformation.")

        return grammy_df.to_json(orient='records')

    except json.JSONDecodeError as e:
        logging.error("Error decoding JSON data.")
        return None
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        return None
