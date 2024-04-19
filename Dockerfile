FROM apache/airflow:2.8.3

RUN pip install --no-cache-dir pydantic[email] pydantic-settings psycopg2-binary==2.9.2 pandas numpy matplotlib seaborn wordcloud scipy statsmodels asyncpg SQLAlchemy uuid7 pydrive2 apache-airflow-providers-google
