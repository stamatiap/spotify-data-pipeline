import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

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

# Use Spotify Schema
try: 
    conn.cursor().execute("USE SCHEMA SPOTIFY.RAW;")
except Exception as e:
    print("Schema error!")
    print(f"Error: {e}")


# Create the tables
try:
    cur.execute("""CREATE OR REPLACE TABLE raw_playlists
                (id string,
                uri string,
                name string,
                owner_id string,
                owner_uri string,
                owner_display_name string,
                tracks_total integer);""")
except Exception as e:
    print("Cannot create playlist table!")
    print(f"Error: {e}")


try:
    cur.execute("""CREATE OR REPLACE TABLE raw_tracks
                (id string,
                included_in string,
                reccobeats_id string,
                acousticness float,
                danceability float,
                energy float,
                instrumentalness float,
                key float,
                liveness float,
                loudness float,
                mode float,
                speechiness float,
                tempo float,
                valence float);""")
except Exception as e:
    print("Cannot create tracks table!")
    print(f"Error: {e}")


try:
    cur.execute("""CREATE OR REPLACE TABLE raw_listening_history
                (played_at datetime,
                track_id string,
                track_uri string,
                track_name string);""")
except Exception as e:
    print("Cannot create listening_history table!")
    print(f"Error: {e}")


# Load data - raw_playlists
try:
    cur.execute("PUT file://final_playlist_table.csv @%raw_playlists")
    cur.execute("""COPY INTO raw_playlists FROM @%raw_playlists
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)""")
except Exception as e:
    print("Cannot load raw_playlist table!")
    print(f"Error: {e}")

# Load data - raw_tacks
try:
    cur.execute("PUT file://final_tracks_table.csv @%raw_tracks")
    cur.execute("""COPY INTO raw_tracks FROM @%raw_tracks
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)""")
except Exception as e:
    print("Cannot load raw_tracks table!")
    print(f"Error: {e}")

# Load data - raw_listening_history
try:
    cur.execute("PUT file://final_listening_history_table.csv @%raw_listening_history")
    cur.execute("""COPY INTO raw_listening_history FROM @%raw_listening_history
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)""")
except Exception as e:
    print("Cannot load raw_listening_history table!")
    print(f"Error: {e}")

cur.close()