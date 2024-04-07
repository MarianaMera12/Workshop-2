import pandas as pd
import logging
import json 
import db_extract



def merge(**kwargs):
    ti = kwargs["ti"]
    
    # Obtener los datos transformados de la base de datos
    db_data = ti.xcom_pull(task_ids="transform_db")
    if db_data is None:
        logging.warning("No se encontraron datos transformados para la fusión.")
        return None
    
    # Cargar los datos transformados de la base de datos como JSON
    try:
        json_data = json.loads(db_data)
    except json.JSONDecodeError as e:
        logging.error(f"Error al decodificar JSON: {e}")
        return None
    
    # Convertir los datos JSON en un DataFrame de Pandas
    grammy_df = pd.json_normalize(data=json_data)
    
    # Obtener los datos transformados del archivo CSV
    csv_data = ti.xcom_pull(task_ids="transform_csv")
    if csv_data is None:
        logging.warning("No se encontraron datos transformados del archivo CSV para la fusión.")
        return None
    
    # Cargar los datos transformados del archivo CSV como JSON
    try:
        json_data = json.loads(csv_data)
    except json.JSONDecodeError as e:
        logging.error(f"Error al decodificar JSON: {e}")
        return None
    
    # Convertir los datos JSON en un DataFrame de Pandas
    spotify_df = pd.json_normalize(data=json_data)
    
    # Fusionar los DataFrames
    merge_sg = grammy_df.merge(spotify_df, how='inner', left_on=['nominee', 'artist'], right_on=['track_name', 'artists'])

    logging.info("Se ha realizado la fusión de datos con éxito.")
    return merge_sg.to_json(orient='records')



def load(**kwargs):
    ti = kwargs["ti"]
    json_data = json.loads(ti.xcom_pull(task_ids="merge"))
    data = pd.json_normalize(data=json_data)

    logging.info("Cargando datos...")
    
    db_extract.insert_data(data)
    
    logging.info("Los datos se han cargado en: tracks")