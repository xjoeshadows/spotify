#!/usr/bin/env python3
import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from datetime import datetime

# Set up your Spotify API credentials from environment variables
SPOTIPY_CLIENT_ID = os.getenv('client_id')
SPOTIPY_CLIENT_SECRET = os.getenv('client_secret')
SPOTIPY_REDIRECT_URI = os.getenv('redirect_uri')

# Authenticate with Spotify
try:
    print("Authenticating with Spotify...")
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=SPOTIPY_CLIENT_ID,
                                                   client_secret=SPOTIPY_CLIENT_SECRET,
                                                   redirect_uri=SPOTIPY_REDIRECT_URI,
                                                   scope='user-read-private user-read-email user-library-read user-follow-read user-top-read user-read-recently-played'))
    print("Authentication successful.")
except Exception as e:
    print(f"Authentication failed: {e}")
    exit()

# Get user profile data
print("Fetching user profile data...")
user_profile = sp.current_user()
user_data = {
    'User ID': user_profile['id'],
    'Display Name': user_profile['display_name'],
    'Email': user_profile['email'],
    'Country': user_profile['country'],
    'Followers': user_profile['followers']['total'],
}

# Function to paginate through results
def paginate_results(fetch_function, limit=50):
    results = []
    offset = 0
    while True:
        response = fetch_function(limit=limit, offset=offset)
        results.extend(response['items'])
        if len(response['items']) < limit:
            break
        offset += limit
    return results

# Function to get all followed artists
def get_all_followed_artists():
    results = []
    limit = 50
    after = None

    print("Fetching followed artists...")
    while True:
        if after:
            response = sp.current_user_followed_artists(limit=limit, after=after)
        else:
            response = sp.current_user_followed_artists(limit=limit)

        results.extend(response['artists']['items'])
        if len(response['artists']['items']) < limit:
            break
        after = response['artists']['items'][-1]['id']

    return results

import time

# Function to get the most recently played tracks
def get_recently_played_tracks():
    results = []
    limit = 50

    print("Fetching recently played tracks...")
    try:
        response = sp.current_user_recently_played(limit=limit)
        if not response['items']:
            print("No recently played tracks found.")
            return results  # Return empty if no items are found

        for item in response['items']:
            track = item['track']
            results.append({
                'Track Name': track['name'],
                'Artist': ', '.join(artist['name'] for artist in track['artists']),
                'Album': track['album']['name'],
                'Played At': item['played_at'],
            })

        print(f"Retrieved {len(results)} recently played tracks.")
    except Exception as e:
        print(f"Error fetching recently played tracks: {e}")

    return results

# Get saved tracks
print("Fetching saved tracks...")
saved_tracks = paginate_results(sp.current_user_saved_tracks)
tracks_data = []
for item in saved_tracks:
    track = item['track']
    tracks_data.append({
        'Track Name': track['name'],
        'Artist': ', '.join(artist['name'] for artist in track['artists']),
        'Album': track['album']['name'],
        'Release Date': track['album']['release_date'],
        'Duration (ms)': track['duration_ms'],
    })

# Get recently played tracks
recent_tracks_data = get_recently_played_tracks()
print(f"Retrieved {len(recent_tracks_data)} recently played tracks.")

# Get followed artists
followed_artists = get_all_followed_artists()
artists_data = []
print(f"Retrieved {len(followed_artists)} followed artists.")

for artist in followed_artists:
    artists_data.append({
        'Artist Name': artist['name'],
        'Artist ID': artist['id'],
        'Genres': ', '.join(artist['genres']),
        'Followers': artist['followers']['total'],
        'Popularity': artist['popularity'],
    })

# Get user playlists
print("Fetching user playlists...")
playlists = paginate_results(sp.current_user_playlists)
playlists_data = []
for item in playlists:
    playlists_data.append({
        'Playlist Name': item['name'],
        'Description': item.get('description', 'No description available'),
        'Tracks Count': item['tracks']['total'],
        'Public': item['public'],
    })

# Get top artists
print("Fetching top artists...")
top_artists = paginate_results(sp.current_user_top_artists, limit=50)
top_artists_data = []
print(f"Retrieved {len(top_artists)} top artists.")

for artist in top_artists:
    top_artists_data.append({
        'Artist Name': artist['name'],
        'Genres': ', '.join(artist['genres']),
        'Followers': artist['followers']['total'],
        'Popularity': artist['popularity'],
    })

# Get saved albums
print("Fetching saved albums...")
saved_albums = paginate_results(sp.current_user_saved_albums)
albums_data = []
print(f"Retrieved {len(saved_albums)} saved albums.")

for item in saved_albums:
    album = item['album']
    albums_data.append({
        'Album Name': album['name'],
        'Artist': ', '.join(artist['name'] for artist in album['artists']),
        'Release Date': album['release_date'],
        'Total Tracks': album['total_tracks'],
    })

# Create DataFrames
print("Creating DataFrames for the collected data...")
user_df = pd.DataFrame([user_data])
tracks_df = pd.DataFrame(tracks_data)
artists_df = pd.DataFrame(artists_data)
playlists_df = pd.DataFrame(playlists_data)
top_artists_df = pd.DataFrame(top_artists_data)
recent_tracks_df = pd.DataFrame(recent_tracks_data)
albums_df = pd.DataFrame(albums_data)

# Generate a timestamp for file naming
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Save to CSV with timestamp and provide feedback
try:
    print("Saving data to CSV files...")
    user_df.to_csv(f'spotify_user_data_{timestamp}.csv', index=False)
    print(f"User data saved to spotify_user_data_{timestamp}.csv")
    
    tracks_df.to_csv(f'spotify_saved_tracks_{timestamp}.csv', index=False)
    print(f"Saved tracks data saved to spotify_saved_tracks_{timestamp}.csv")
    
    artists_df.to_csv(f'spotify_followed_artists_{timestamp}.csv', index=False)
    print(f"Followed artists data saved to spotify_followed_artists_{timestamp}.csv")
    
    playlists_df.to_csv(f'spotify_playlists_{timestamp}.csv', index=False)
    print(f"Playlists data saved to spotify_playlists_{timestamp}.csv")
    
    top_artists_df.to_csv(f'spotify_top_artists_{timestamp}.csv', index=False)
    print(f"Top artists data saved to spotify_top_artists_{timestamp}.csv")
    
    recent_tracks_df.to_csv(f'spotify_recently_played_tracks_{timestamp}.csv', index=False)
    print(f"Recently played tracks data saved to spotify_recently_played_tracks_{timestamp}.csv")
    
    albums_df.to_csv(f'spotify_saved_albums_{timestamp}.csv', index=False)
    print(f"Saved albums data saved to spotify_saved_albums_{timestamp}.csv")
    
    print("All data has been successfully exported to CSV files.")
except Exception as e:
    print(f"An error occurred while saving data to CSV files: {e}")
