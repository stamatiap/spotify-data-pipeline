import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")


sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=REDIRECT_URI,
    scope="playlist-read-private,user-top-read,user-library-read,user-read-recently-played,user-follow-read,playlist-read-collaborative"))

my_playlists = sp.current_user_playlists(limit=50)
my_listening_history = sp.current_user_recently_played(limit=50)
my_playlists_df = pd.json_normalize(my_playlists['items'])
my_listening_history_df = pd.json_normalize(my_listening_history['items'])


my_follower_urls = []
for i in range(10):
    my_follower_urls.append(os.getenv(f'MY_FOLLOWER_URL{i+1}'))
print(my_follower_urls)

my_follower_ids = []
for id in my_follower_urls:
    print(id.split("/")[-1].split("?")[0])
    my_follower_ids.append(id.split("/")[-1].split("?")[0])

follower_playlists_df = pd.DataFrame()
for id in my_follower_ids:
    follower_playlists = sp.user_playlists(id)
    df = pd.json_normalize(follower_playlists['items'])
    follower_playlists_df = pd.concat([follower_playlists_df, df], ignore_index=True)
    

all_playlists_df = pd.concat([my_playlists_df, follower_playlists_df])
print(all_playlists_df)


playlist_tracks = {}
for pl_id in all_playlists_df['uri']:
    offset=0
    while True:
        response = sp.playlist_items(pl_id,
                                    offset=offset,
                                    fields='items.track.id,total',
                                    additional_types=['track'])

        if len(response['items']) == 0:
            break

        #print(response['items'])
        playlist_tracks[pl_id] = response['items']
        offset = offset + len(response['items'])
        print(offset, "/", response['total'])
    break 


print(playlist_tracks)
