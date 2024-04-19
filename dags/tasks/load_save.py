import os
import pandas as pd
from .core.config import settings
from .drive import upload_file
from sqlalchemy import create_engine


data_dir = '/opt/airflow/data'


engine = create_engine(
    str(settings.POSTGRES_URI), 
    echo=settings.POSTGRES_ECHO,
    pool_size=max(5, settings.POSTGRES_POOL_SIZE),
)


def read_csv():
    """ Read the spotify_dataset.csv file and return a pandas DataFrame. """
    csv_file_path = os.path.join(data_dir, 'spotify_dataset.csv')
    df = pd.read_csv(csv_file_path, sep=',')

    return df


def read_db():
    """ Read the grammy table from the database and return a pandas DataFrame. """
    columns = ["year","title","published_at","updated_at","category","nominee","artist","workers","img","winner"]
    
    df  = pd.read_sql_table(
        'raw_table',
        con=engine,
        columns=columns,
    )

    return df


def load(ti):
    df = ti.xcom_pull(task_ids='merge')
    csv_file_path = os.path.join(data_dir, 'clean_data.csv')
    df.to_csv(csv_file_path, index=False)

    return csv_file_path


def store(ti):
    csv_file_path = ti.xcom_pull(task_ids='load')
    upload_file(csv_file_path, '1IMyQAnm-PvcwxjIiGW-skvrWsb7Zernv')
