import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from dotenv import load_dotenv
import os
import time
import snowflake.connector


load_dotenv()

#Spotify credentials
CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")

# Snowflake credentials
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
ACCOUNT = os.getenv("ACCOUNT")
WAREHOUSE = os.getenv("WAREHOUSE")
DATABASE = os.getenv("DATABASE")
SCHEMA = os.getenv("SCHEMA")


# Connect to Snowflake
try:
    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
        )
    cur = conn.cursor()
except Exception as e:
    print("Error while connecting to Snowflake!")
    print(f"Error: {e}")

try: 
    cur.execute("USE SCHEMA SPOTIFY.DEV;")
except Exception as e:
    print("Schema error!")
    print(f"Error: {e}")

try: 
    cur.execute("SELECT DISTINCT artist_id FROM DIM_ARTISTS;")
except Exception as e:
    print("Cannot get artist ids!")
    print(f"Error: {e}")

artist_ids = [item[0] for item in cur.fetchall()]

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=REDIRECT_URI,
    ))

artist_genre = {}
counter = 0
for artist_id in artist_ids:
    counter+=1
    info = sp.artist(artist_id)
    artist_genre[artist_id] = info['genres']
    if counter % 10 == 0:
        time.sleep(3)


artist_genre_df = pd.DataFrame.from_dict(artist_genre, orient='index').reset_index().rename(columns={"index": "artist_id"})

for i, column in enumerate(list(artist_genre_df.columns)[1:]):
    artist_genre_df = artist_genre_df.rename(columns={f"{column}": f"genre_{i+1}"})

artist_genre_df.to_csv("artist_genre.csv", index=False)
