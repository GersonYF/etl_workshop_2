import pandas as pd

def merge(ti):
    spotify_df = ti.xcom_pull(task_ids='transform_csv')
    grammies_df = ti.xcom_pull(task_ids='transform_db')

    print(spotify_df.head())
    print(grammies_df.head())

    spotify_df['grammy_nominee'] = spotify_df['normalized_name'].isin(grammies_df['normalized_name'])

    return spotify_df
