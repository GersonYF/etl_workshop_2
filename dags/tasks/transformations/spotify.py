import pandas as pd
import numpy as np

from .utils import normalize_string


def set_index(spotify_df):
    """Set the index of the DataFrame to start from 1."""

    spotify_df.index = spotify_df["Unnamed: 0"].map(lambda x: int(x)+1)
    spotify_df = spotify_df.rename_axis("index")
    spotify_df = spotify_df.drop(columns=["Unnamed: 0", "track_id"])
    return spotify_df

def remove_duplicates(spotify_df):
    """Remove duplicate rows from the DataFrame."""

    spotify_df = spotify_df.drop_duplicates(keep='first')
    return spotify_df

def remove_na(spotify_df):
    """Remove rows with missing values in the 'track_name' column."""

    spotify_df = spotify_df.dropna(subset=['track_name'])
    return spotify_df

def normalize_name(spotify_df):
    """Normalize the 'track_name' column."""

    spotify_df['normalized_name'] = spotify_df['track_name'].apply(normalize_string)
    return spotify_df

def map_key(spotify_df):
    """Map the 'key' column to the corresponding note."""

    key_mapping = {0: 'Do', 1: 'Do#', 2: 'Re', 3: 'Reb', 4: 'Mi', 5: 'Fa', 6: 'Fa#', 7: 'Sol', 8: 'Sol#', 9: 'La', 10: 'Lab', 11: 'Si'}
    spotify_df['key'] = spotify_df['key'].map(key_mapping)
    return spotify_df

def categorize_tempo(spotify_df):
    """Categorize the 'tempo' column into four categories: Slow, Moderate, Fast, Very Fast."""

    bin_edges = [spotify_df['tempo'].min(), 100, 120, 160, spotify_df['tempo'].max()]
    bin_labels = ['Slow', 'Moderate', 'Fast', 'Very Fast']
    spotify_df['tempo_category'] = pd.cut(spotify_df['tempo'], bins=bin_edges, labels=bin_labels, include_lowest=True)
    return spotify_df

def create_mood(spotify_df):
    """Create a new column 'mood' based on the 'valence' and 'energy' columns."""

    valence_median = spotify_df['valence'].median()
    energy_median = spotify_df['energy'].median()
    spotify_df['mood'] = np.select(
        [
            (spotify_df['valence'] > valence_median) & (spotify_df['energy'] > energy_median),
            (spotify_df['valence'] <= valence_median) & (spotify_df['energy'] <= energy_median),
            (spotify_df['valence'] <= valence_median) & (spotify_df['energy'] > energy_median),
            (spotify_df['valence'] > valence_median) & (spotify_df['energy'] <= energy_median)
        ], 
        [
            'Happy', 
            'Sad', 
            'Energetic', 
            'Chill'
        ], 
        default='Neutral'
    )
    return spotify_df

def create_energy_dance_index(spotify_df):
    """Create a new column 'energy_dance_index' based on the 'danceability' and 'energy' columns."""

    spotify_df['energy_dance_index'] = spotify_df['danceability'] * spotify_df['energy']
    return spotify_df

def categorize_popularity(spotify_df):
    """Categorize the 'popularity' column into three categories: Low, Medium, High."""

    bin_edges = [spotify_df['popularity'].min(), spotify_df['popularity'].quantile(0.33), spotify_df['popularity'].quantile(0.66), spotify_df['popularity'].max()]
    bin_labels = ['Low', 'Medium', 'High']
    spotify_df['popularity_index'] = pd.cut(spotify_df['popularity'], bins=bin_edges, labels=bin_labels, include_lowest=True)
    return spotify_df

def flag_performance_type(spotify_df):
    """Flag the 'performance' column based on the 'liveness' column."""

    spotify_df['performance'] = np.where(spotify_df['liveness'] > 0.8, 'Live Performance', 'Studio Recorded')
    return spotify_df

