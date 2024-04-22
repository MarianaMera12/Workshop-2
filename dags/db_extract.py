import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

def connect_bd():

    load_dotenv()

    try:
        db_uri = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

        engine = create_engine(db_uri)

        print("Successful connection to PostgreSQL database")
        return engine
    except Exception as error:
        print("Error connecting to PostgreSQL database:", error)
        return None


def extract_data(connection, query):
    try:
        df = pd.read_sql_query(query, connection)
        print("Data extracted correctly from the database")
        return df
    except Exception as e:
        print("Error when extracting data from the database:", e)
        return None

def create_merge_table(engine):
    try:

        create_table_query = """
        CREATE TABLE IF NOT EXISTS mergesg (
            artists VARCHAR(1000),  
            album_name VARCHAR(1000),  
            track_name VARCHAR(1000),  
            popularity FLOAT,
            duration_ms INTEGER,
            explicit BOOLEAN,
            danceability FLOAT,
            energy FLOAT,
            key INTEGER,
            loudness FLOAT,
            mode INTEGER,
            speechiness FLOAT,
            acousticness FLOAT,
            instrumentalness FLOAT,
            liveness FLOAT,
            valence FLOAT,
            tempo FLOAT,
            time_signature INTEGER,
            track_genre TEXT,
            num_artists INTEGER,
            secondary_artist VARCHAR(1000),  
            genre_category TEXT,
            popularity_category TEXT,
            grammy_nominee INT
        );
        """
        with engine.connect() as con:
            con.execute(create_table_query)
        print("Merge table created correctly")
    except Exception as e:
        print("Error when creating the merge table:", e)


def insert_merge_data(df, connection):
    try:

        df.to_sql('mergesg', connection, if_exists='append', index=False)
        print("Merge data correctly inserted in merge table")
    except Exception as e:
        print("Error inserting merge data into the merge table:", e) 




if __name__ == "__main__":

    query = "SELECT * FROM grammy;"

    connection = connect_bd()

    df_db = extract_data(connection, query)

    connection.dispose()
    print("Closed connection")