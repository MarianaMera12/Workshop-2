import pandas as pd
import logging
import json 
import db_extract


def merge(**kwargs):
    ti = kwargs["ti"]
    
    json_data = json.loads(ti.xcom_pull(task_ids="transformation_db"))
    grammy_df = pd.json_normalize(data=json_data)

    json_data = json.loads(ti.xcom_pull(task_ids="transformation_csv"))
    spotify_df = pd.json_normalize(data=json_data)
    
    # Merge 
    merge_sg = spotify_df.merge(grammy_df, how='left', left_on='track_name', right_on='nominee')
    merge_sg['grammy_nominee'] = merge_sg['grammy_nominee'].notna().astype(int)
    merge_sg.drop([37417, 37660], inplace=True)
    merge_sg.drop(columns=['year', 'title', 'category', 'nominee', 'artist', 'workers'], inplace=True)
    

    logging.info("Data merging has been performed successfully.")

    num_rows = merge_sg.shape[0]
    num_cols = merge_sg.shape[1]
    logging.info(f"Number of records: {num_rows}")
    logging.info(f"Number of columns: {num_cols}")
    
    csv_file_path = "./data/merge_result.csv"
    merge_sg.to_csv(csv_file_path, index=False)
    logging.info(f"The result of the merge has been saved in: {csv_file_path}")
    
    return merge_sg.to_json(orient='records')



def load(**kwargs):
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="merge"))
    load_data = pd.json_normalize(json_data)
    logging.info("Loading data")

    connection = db_extract.connect_bd()
    
    if connection is not None:
        try:

            db_extract.create_merge_table(connection)
            
            db_extract.insert_merge_data(load_data, connection)
            
            connection.dispose()
            
            logging.info("Data has been loaded into the merge table")
        except Exception as e:
            logging.error("Error loading data into the merge table:", e)
    else:
        logging.error("Unable to connect to the database")
