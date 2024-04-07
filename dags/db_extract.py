import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv

def connect_bd():
    # Cargar variables de entorno desde el archivo .env
    load_dotenv()

    try:
        # Crear la cadena de conexión
        db_uri = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

        # Crear la conexión al motor SQLAlchemy
        engine = create_engine(db_uri)

        print("Conexión exitosa a la base de datos PostgreSQL")
        return engine
    except Exception as error:
        print("Error al conectar a la base de datos PostgreSQL:", error)
        return None

def read_csv(file_path):
    try:
        # Leer el archivo CSV en un DataFrame
        df = pd.read_csv(file_path)
        print("Datos del archivo CSV cargados correctamente en un DataFrame de Pandas")
        return df
    except Exception as e:
        print("Error al cargar datos desde el archivo CSV:", e)
        return None

def insert_data(df, connection, table_name):
    try:
        # Insertar datos en la tabla de la base de datos
        df.to_sql(table_name, connection, if_exists='append', index=False)
        print("Datos insertados correctamente en la tabla", table_name)
    except Exception as e:
        print("Error al insertar datos en la tabla:", e)

def extract_data(connection, query):
    try:
        # Extraer datos de la base de datos
        df = pd.read_sql_query(query, connection)
        print("Datos extraídos correctamente desde la base de datos")
        return df
    except Exception as e:
        print("Error al extraer datos desde la base de datos:", e)
        return None

# Ejemplo de uso de las funciones
if __name__ == "__main__":
    # Ruta al archivo CSV
    csv_file_path = "./data/the_grammy_awards.csv"

    # Consulta SQL para extraer datos de la base de datos
    query = "SELECT * FROM grammy;"

    # Obtener la conexión a la base de datos
    connection = connect_bd()

    # Cargar datos desde el archivo CSV en un DataFrame
    df_csv = read_csv(csv_file_path)

    # Nombre de la tabla en la base de datos
    table_name = 'grammy'

    # Insertar datos del archivo CSV en la tabla
    if connection is not None and df_csv is not None:
        insert_data(df_csv, connection, table_name)
        
    # Extraer datos de la base de datos en un DataFrame
    df_db = extract_data(connection, query)

    # Insertar datos extraídos de la base de datos en la tabla
    if connection is not None and df_db is not None:
        insert_data(df_db, connection, table_name)
        
    # Cerrar la conexión a la base de datos
    connection.dispose()
    print("Conexión cerrada")
