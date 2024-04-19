from .utils import normalize_string

def filter_data(grammy_df):
    """Filter the data based on certain conditions."""

    same_nominee_artist_df = grammy_df[grammy_df["nominee"] == grammy_df["artist"]]
    songs_nominations = grammy_df[~grammy_df["category"].isin(same_nominee_artist_df["category"])]

    return songs_nominations


def remove_na(grammy_df):
    """Remove rows with missing values in the 'nominee' column."""

    grammy_df = grammy_df.dropna(subset=['nominee'])
    return grammy_df


def normalize_name(grammy_df):
    """Normalize the 'nominee' column."""

    grammy_df['normalized_name'] = grammy_df['nominee'].apply(normalize_string)
    return grammy_df
