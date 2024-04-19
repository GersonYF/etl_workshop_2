from .transformations import spotify as sp
from .transformations import grammy as gr


def transform_db(ti):
    grammy_winners_df = ti.xcom_pull(task_ids='read_db')

    songs_nominations_df = gr.filter_data(grammy_winners_df)
    songs_nominations_df = gr.remove_na(songs_nominations_df)
    songs_nominations_df = gr.normalize_name(songs_nominations_df)

    return songs_nominations_df



def transform_csv(ti):
    """ Perform transformations on the Spotify dataset."""

    spotify_df = ti.xcom_pull(task_ids='read_csv')
    spotify_df = sp.set_index(spotify_df)
    spotify_df = sp.remove_duplicates(spotify_df)
    spotify_df = sp.remove_na(spotify_df)
    spotify_df = sp.normalize_name(spotify_df)
    spotify_df = sp.map_key(spotify_df)
    spotify_df = sp.categorize_tempo(spotify_df)
    spotify_df = sp.create_mood(spotify_df)
    spotify_df = sp.create_energy_dance_index(spotify_df)
    spotify_df = sp.categorize_popularity(spotify_df)
    spotify_df = sp.flag_performance_type(spotify_df)

    return spotify_df