import pandas as pd
import db_extract
import json
import logging
##  Grammy 

def read_db():
    csv_file_path = "./data/the_grammy_awards.csv"
    query = "SELECT * FROM grammy;"
    connection = db_extract.connect_bd()
    df_csv = db_extract.read_csv(csv_file_path)
    table_name = 'grammy'

    if connection is not None and df_csv is not None:
        db_extract.insert_data(df_csv, connection, table_name)
        
    # Extraer datos de la base de datos en un DataFrame
    df_db = db_extract.extract_data(connection, query)

    # Cerrar la conexión a la base de datos
    if connection is not None:
        connection.dispose()
        logging.info("Conexión cerrada")  # Cambio aquí

    if df_db is not None:
        return df_db.to_json(orient='records')
    else:
        logging.warning("No se pudieron extraer datos de la base de datos.")  # Cambio aquí
        return None
    

def transform_db(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_db")

    if str_data is None:
        logging.warning("No se encontraron datos para transformar.")
        return None

    try:
        json_data = json.loads(str_data)
        grammy_df = pd.json_normalize(data=json_data)
        logging.info(f"Columnas del DataFrame: {grammy_df.columns.tolist()}")
        num_columns_before = grammy_df.shape[1]
        logging.info(f"El DataFrame tiene {num_columns_before} columnas antes de la transformación.")

        # Realizar transformaciones en los datos
        grammy_df['artist'] = grammy_df['artist'].fillna(grammy_df['workers'].str.extract(r'\(([^)]+)\)')[0])
        grammy_df.drop([2261, 2359, 2454, 2547, 4525, 4573], inplace=True)
        grammy_df.drop(columns=['published_at', 'updated_at', 'img'], inplace=True)

        logging.info(f"Transformaciones finalizadas")
        
        num_columns_after = grammy_df.shape[1]
        logging.info(f"El DataFrame tiene {num_columns_after} columnas después de la transformación.")

        return grammy_df.to_json(orient='records')

    except Exception as e:
        logging.error(f"Error durante la transformación de datos: {e}")
        return None
