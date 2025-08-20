import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from dotenv import load_dotenv
import os
import http.client
import json
from itertools import islice
import time

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


my_follower_ids = []
for id in my_follower_urls:
    my_follower_ids.append(id.split("/")[-1].split("?")[0])

follower_playlists_df = pd.DataFrame()
for id in my_follower_ids:
    follower_playlists = sp.user_playlists(id)
    df = pd.json_normalize(follower_playlists['items'])
    follower_playlists_df = pd.concat([follower_playlists_df, df], ignore_index=True)
    

all_playlists_df = pd.concat([my_playlists_df, follower_playlists_df])

print("Getting playlists' tracks! ")
playlist_tracks = {}
for pl_id in all_playlists_df['uri']:
    offset=0
    while True:
        response = sp.playlist_items(pl_id,
                                    offset=offset,
                                    fields='items,total',
                                    additional_types=['track'])

        if len(response['items']) == 0:
            break

        playlist_tracks[pl_id] = response['items']
        offset = offset + len(response['items'])
        print(offset, "/", response['total'])


all_tracks = {}
playlist_tracks_info = {}
for playlist_id, playlist in playlist_tracks.items():
    all_tracks[playlist_id] = []
    for item in playlist:
        if item['track'] is not None:
            all_tracks[playlist_id].append(item['track']['id'])
            playlist_tracks_info[item['track']['id']] = {'name': item['track']['name'], 'uri': item['track']['uri'], 'popularity': item['track']['popularity'], 'duration': item['track']['duration_ms'], 'artists': [{artist_item['id']: artist_item['name']} for artist_item in item['track']['artists']]}
            if 'included_in' not in playlist_tracks_info[item['track']['id']].keys():
                playlist_tracks_info[item['track']['id']] = {'included_in': [playlist_id]}
            else:
                playlist_tracks_info[item['track']['id']['included_in']].append(playlist_id)

playlist_tracks_info_df = pd.DataFrame.from_dict(playlist_tracks_info, orient='index').reset_index().rename(columns={'index': 'id'})

response = sp.tracks(list(my_listening_history_df['track.id']))


def batched(iterable, n, *, strict=False):
    if n < 1:
        raise ValueError('n must be at least one')
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        if strict and len(batch) != n:
            raise ValueError('batched(): incomplete batch')
        yield batch

conn = http.client.HTTPSConnection("api.reccobeats.com")
payload = ''
headers = {
  'Accept': 'application/json'
}

print("Getting tracks! ")
response_content = []
counter = 0
for playlist_id, track_ids in all_tracks.items():
    
    for batch in batched(track_ids, 10):
        counter += 1 
        id_str = ""
        for track_id in batch:
            id_str = id_str + f"ids={track_id}&"
        id_str = id_str[:-1]
        
        conn.request("GET", f"/v1/track?{id_str}", payload, headers)
        res = conn.getresponse()
        data = res.read()
        response_content.append(json.loads(data)['content'])
        if counter % 50 == 0:
            time.sleep(3)

response_content = [x for xs in response_content for x in xs]
reccobeats_ids = {}
for track in response_content:
    spotify_id = track['href'].split('/')[-1]
    reccobeats_ids[spotify_id] = track['id']


conn = http.client.HTTPSConnection("api.reccobeats.com")

print("Getting track features! ")
counter = 0
track_audio_features = {}
for spotify_id, track_id in reccobeats_ids.items():
    conn.request("GET", f"/v1/track/{track_id}/audio-features", payload, headers)
    res = conn.getresponse()
    track_audio_features[spotify_id] = json.loads(res.read())
    if counter % 50 == 0:
        time.sleep(3)


track_audio_features_df = pd.DataFrame.from_dict(track_audio_features, orient='index').reset_index().rename(columns={'id': 'reccobeats_id','index': 'id'})

final_playlist_table = all_playlists_df[['id', 'uri', 'name', 'owner.id', 'owner.uri', 'owner.display_name', 'tracks.total']]
final_tracks_table = playlist_tracks_info_df.merge(track_audio_features_df, on='id')
final_listening_history_table = my_listening_history_df[['played_at', 'track.id', 'track.uri', 'track.name']]


print(final_playlist_table)
print(final_tracks_table)
print(final_listening_history_table)

final_playlist_table.to_csv("final_playlist_table.csv")
final_tracks_table.to_csv("final_tracks_table.csv")
final_listening_history_table.to_csv("final_listening_history_table.csv")