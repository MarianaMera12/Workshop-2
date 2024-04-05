import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv

def connect_bd():
    # Cargar variables de entorno desde el archivo .env
    load_dotenv()

    try:
        # Crear conexión a la base de datos
        connection = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        print("Conexión exitosa a la base de datos PostgreSQL")
        return connection
    except (Exception, psycopg2.Error) as error:
        print("Error al conectar a la base de datos PostgreSQL:", error)
        return None


def extract_data(connection, query):
    try:
        # Ejecutar la consulta SQL y leer los resultados en un DataFrame de Pandas
        df = pd.read_sql_query(query, connection)
        print("Datos extraídos correctamente en un DataFrame de Pandas")
        return df
    except Exception as e:
        print("Error al extraer datos:", e)
        return None

def create_table():
    pass

def insert_data():
    pass



# Ejemplo de uso de las funciones
if __name__ == "__main__":
    # Obtener la conexión a la base de datos
    connection = connect_bd()

    # Consulta SQL para extraer los datos
    query = "SELECT * FROM public.grammy;"

    # Extraer los datos y crear un DataFrame
    if connection is not None:
        df = extract_data(connection, query)
        # Imprimir el DataFrame resultante
        if df is not None:
            print(df)
        # Cerrar la conexión a la base de datos
        connection.close()
        print("Conexión cerrada")
