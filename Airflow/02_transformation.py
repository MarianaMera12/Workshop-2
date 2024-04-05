import pandas as pd
import db_extract
import json
import logging
##  Grammy 

def read_db():
    grammy_df = db_extract.extract_data()
    logging.info("Extracción finalizada")
    logging.debug('Los datos extraídos son: ', grammy_df)
    return grammy_df.to_json(orient='records')

def transform_db(**kwargs):
    logging.info("the kwargs are: ", kwargs)
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_db")
    json_data = json.loads(str_data)
    grammy_df = pd.json_normalize(data=json_data)
    #logging.info(f"data is: {data}")
    #function transform

    grammy_df['artist'] = grammy_df['artist'].fillna(grammy_df['workers'].str.extract(r'\(([^)]+)\)')[0])
    grammy_df = grammy_df.loc[grammy_df[grammy_df['category'].str.contains('Artist', case=False)].index, ['artist', 'workers']] = grammy_df[grammy_df['category'].str.contains('Artist', case=False)][['nominee', 'nominee']]
    grammy_df = grammy_df.drop([2261, 2359, 2454, 2547, 4525, 4573], inplace=True)
    grammy_df = grammy_df.drop(columns=['published_at', 'updated_at', 'img'])

    return grammy_df.to_json(orient='records')

