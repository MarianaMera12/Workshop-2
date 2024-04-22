import pandas as pd
import logging
import json 

music_genres = {
    "Pop": ["pop", "pop-film", "j-pop", "k-pop", "latino", "latin", "party", "dance", "disco", "club", "trance", "techno", "synth-pop", "reggaeton", "reggae", "hip-hop", "r-n-b", "rap", "edm", "electronic", "dancehall", "dubstep", "dub", "disco", "dance", "club", "comedy", "happy"],
    "Rock": ["rock", "psych-rock", "punk-rock", "rock-n-roll", "rockabilly", "indie", "indie-pop", "alt-rock", "metalcore", "metal", "heavy-metal", "hard-rock", "hardcore", "grunge", "goth", "grunge", "garage", "emo"],
    "Acoustic": ["acoustic", "singer-songwriter", "songwriter", "sad", "romance", "folk", "country", "blues", "ballad"],
    "World": ["world-music", "tango", "samba", "salsa", "sertanejo", "samba", "pagode", "forro", "flamenco", "cantopop", "mandopop"],
    "Instrumental": ["classical", "piano", "jazz", "instrumental", "ambient", "new-age", "opera", "orchestral"],
    "Experimental": ["experimental", "alternative", "avant-garde", "noise", "industrial", "idm", "glitch", "drone"],
    "Ethnic": ["african", "iranian", "turkish", "swedish", "spanish", "french", "german", "celtic", "indian", "malay"],
    "Children": ["kids", "children", "disney", "animation", "cartoon"]
}

## Spotify 
def assign_genre(genre, music_genres):
    for category, keywords in music_genres.items():
        if any(keyword in genre for keyword in keywords):
            return category
    return 'Other'

def read_csv():
    spotify_df = pd.read_csv("./data/spotify_dataset.csv")
    logging.info("Extraction completed")
    return spotify_df.to_json(orient='records')


def transform_csv(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_csv")
    json_data = json.loads(str_data)
    spotify_df = pd.json_normalize(data=json_data)
    #logging.info(f"data is: {spotify_df}")
    #function transform

    spotify_df = spotify_df.drop(spotify_df[spotify_df['track_id'] == '1kR4gIb7nGxHPI3D2ifs59'].index, axis=0)
    spotify_df = spotify_df.drop_duplicates(subset="track_id", keep='first', inplace=False)
    spotify_df['split_artists'] = spotify_df['artists'].str.split(';')
    spotify_df['num_artists'] = spotify_df['split_artists'].apply(len)
    spotify_df['secondary_artist'] = spotify_df['split_artists'].apply(lambda x: x[1] if len(x) > 1 else None)
    spotify_df['secondary_artist'] = spotify_df['secondary_artist'].fillna('No second artist')
    spotify_df['genre_category'] = spotify_df['track_genre'].apply(lambda x: assign_genre(x, music_genres))
    spotify_df['popularity_category'] = pd.cut(spotify_df['popularity'], bins=[0, 33, 66, 100], labels=['Low', 'Medium', 'High'], include_lowest=True)
    spotify_df = spotify_df.drop(columns=['Unnamed: 0', 'split_artists', 'track_id'])
    
    num_columns_after = spotify_df.shape[1]
    logging.info(f"The DataFrame has {num_columns_after} columns after the transformation.")
    logging.info(f"Finished transformations")
    return spotify_df.to_json(orient='records')