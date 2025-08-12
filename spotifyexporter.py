#!/usr/bin/env python3

import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from datetime import datetime

# Timestamp for filenames
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")


## ─── Authentication ──────────────────────────────────────────────────────────
# Load Spotify API credentials from environment variables
SPOTIPY_CLIENT_ID     = os.getenv('client_id')
SPOTIPY_CLIENT_SECRET = os.getenv('client_secret')
SPOTIPY_REDIRECT_URI  = os.getenv('redirect_uri')

# Authenticate and create Spotify client
try:
    print("Authenticating with Spotify...")
    sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(
            client_id=SPOTIPY_CLIENT_ID,
            client_secret=SPOTIPY_CLIENT_SECRET,
            redirect_uri=SPOTIPY_REDIRECT_URI,
            scope=(
                'user-read-private user-read-email '
                'user-library-read user-follow-read '
                'user-top-read user-read-recently-played'
            )
        )
    )
    print("Authentication successful.")
except Exception as e:
    print(f"Authentication failed: {e}")
    exit()


## ─── Helper Functions ────────────────────────────────────────────────────────

# Paginate through any Spotify “.items”-based endpoint
def paginate_results(fetch_function, limit=50, **kwargs):
    results = []
    offset = 0
    while True:
        response = fetch_function(limit=limit, offset=offset, **kwargs)
        items = response.get('items', [])
        results.extend(items)
        if len(items) < limit:
            break
        offset += limit
    return results

# Unified CSV export: takes a dict of DataFrames and writes them
def export_csv(dataframes: dict, timestamp: str):
    for name, df in dataframes.items():
        filename = f'spotify_{name}_{timestamp}.csv'
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"{name.replace('_', ' ').title()} data saved to {filename}")


## ─── Data-Fetching Functions ────────────────────────────────────────────────

def fetch_user_profile():
    print("Fetching user profile data...")
    profile = sp.current_user()
    return {
        'User ID':      profile['id'],
        'Display Name': profile['display_name'],
        'Email':        profile['email'],
        'Country':      profile['country'],
        'Followers':    profile['followers']['total']
    }

def fetch_saved_tracks():
    print("Fetching saved tracks...")
    items = paginate_results(sp.current_user_saved_tracks)
    data = []
    for item in items:
        tr = item['track']
        data.append({
            'Track Name':    tr['name'],
            'Artist':        ', '.join(a['name'] for a in tr['artists']),
            'Album':         tr['album']['name'],
            'Release Date':  tr['album']['release_date'],
            'Duration (ms)': tr['duration_ms']
        })
    return data

def fetch_recently_played():
    print("Fetching recently played tracks...")
    results = []
    try:
        items = sp.current_user_recently_played(limit=50).get('items', [])
        for item in items:
            tr = item['track']
            results.append({
                'Track Name': tr['name'],
                'Artist':     ', '.join(a['name'] for a in tr['artists']),
                'Album':      tr['album']['name'],
                'Played At':  item['played_at']
            })
        print(f"Retrieved {len(results)} recently played tracks.")
    except Exception as e:
        print(f"Error fetching recently played tracks: {e}")
    return results

def fetch_followed_artists():
    print("Fetching followed artists...")
    items = []
    after = None
    while True:
        resp = sp.current_user_followed_artists(limit=50, after=after)
        batch = resp['artists']['items']
        items.extend(batch)
        if len(batch) < 50:
            break
        after = batch[-1]['id']
    data = []
    for art in items:
        data.append({
            'Artist Name': art['name'],
            'Artist ID':   art['id'],
            'Genres':      ', '.join(art['genres']),
            'Followers':   art['followers']['total'],
            'Popularity':  art['popularity']
        })
    return data

def fetch_playlists():
    print("Fetching user playlists...")
    items = paginate_results(sp.current_user_playlists)
    data = []
    for pl in items:
        data.append({
            'Playlist ID':   pl['id'],
            'Playlist Name': pl['name'],
            'Description':   pl.get('description', ''),
            'Tracks Count':  pl['tracks']['total'],
            'Public':        pl['public']
        })
    return data

def fetch_playlist_tracks(playlist_id, playlist_name):
    limit, offset = 100, 0
    records = []
    while True:
        resp = sp.playlist_items(
            playlist_id,
            offset=offset,
            limit=limit,
            fields=(
                'items('
                    'added_by(id),added_at,'
                    'track(name,artists(name),album(name),duration_ms)'
                '),total'
            )
        )
        items = resp.get('items', [])
        if not items:
            break
        for item in items:
            tr = item.get('track', {})
            ab = item.get('added_by') or {}
            records.append({
                'Playlist ID':    playlist_id,
                'Playlist Name':  playlist_name,
                'Track Name':     tr.get('name', ''),
                'Artist':         ', '.join(a.get('name','') for a in tr.get('artists', [])),
                'Album':          tr.get('album', {}).get('name', ''),
                'Duration (ms)':  tr.get('duration_ms', 0),
                'Added By ID':    ab.get('id', 'Unknown'),
                'Added At':       item.get('added_at', 'Unknown')
            })
        offset += limit
        if len(items) < limit:
            break
    return records

def fetch_top_artists():
    print("Fetching top artists...")
    items = paginate_results(sp.current_user_top_artists, limit=50)
    data = []
    for art in items:
        data.append({
            'Artist Name': art['name'],
            'Genres':      ', '.join(art['genres']),
            'Followers':   art['followers']['total'],
            'Popularity':  art['popularity']
        })
    return data

def fetch_saved_albums():
    print("Fetching saved albums...")
    items = paginate_results(sp.current_user_saved_albums)
    data = []
    for item in items:
        alb = item['album']
        data.append({
            'Album Name':   alb['name'],
            'Artist':       ', '.join(a['name'] for a in alb['artists']),
            'Release Date': alb['release_date'],
            'Total Tracks': alb['total_tracks']
        })
    return data


## ─── Main Execution ─────────────────────────────────────────────────────────

# Fetch all data
user_data         = fetch_user_profile()
tracks_data       = fetch_saved_tracks()
recent_data       = fetch_recently_played()
artists_data      = fetch_followed_artists()
playlists_meta    = fetch_playlists()

# Fetch tracks for each playlist
print("Fetching tracks for each playlist...")
playlist_tracks = []
for pl in playlists_meta:
    print(f"  • {pl['Playlist Name']}...")
    playlist_tracks.extend(
        fetch_playlist_tracks(pl['Playlist ID'], pl['Playlist Name'])
    )

top_artists_data  = fetch_top_artists()
saved_albums_data = fetch_saved_albums()


# Prepare DataFrames
frames = {
    'user_data':                      pd.DataFrame([user_data]),
    'saved_tracks':                   pd.DataFrame(tracks_data),
    'recently_played_tracks':         pd.DataFrame(recent_data),
    'followed_artists':               pd.DataFrame(artists_data),
    'playlists':                      pd.DataFrame(playlists_meta),
    'playlist_tracks':                pd.DataFrame(playlist_tracks),
    'top_artists':                    pd.DataFrame(top_artists_data),
    'saved_albums':                   pd.DataFrame(saved_albums_data),
}

# Export everything to CSV
export_csv(frames, timestamp)

print("All data has been successfully exported.")
