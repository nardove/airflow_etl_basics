import os
import tweepy
import mysql.connector
import pandas as pd

from sqlalchemy import create_engine
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


DB_HOST = os.environ.get('MYSQL_HOST')
DB_USER = os.environ.get('MYSQL_USER')
DB_PASS = os.environ.get('MYSQL_PASS')
DB_PORT = os.environ.get('MYSQL_PORT')

CONSUMER_KEY = os.environ.get('TWITTER_API_KEY')
CONSUMER_SECRET = os.environ.get('TWITTER_API_SECRET')
ACCESS_TOKEN = os.environ.get('TWITTER_ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.environ.get('TWITTER_ACCESS_TOKEN_SECRET')


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

try:
    api = tweepy.API(auth)
except tweepy.TweepError:
    print('ERROR! Failed to authenticate')


def _get_tweets() -> list:
    keyword = 'covid'
    search = api.search(
        q=keyword,
        geocode='55.378051,-3.435973,1000mi',
        result_type='recent',
        lang='en',
        include_entities=False)
    tweets = []
    for tweet in search:
        t = {
            'tweet_id':tweet.id,
            'content':tweet.text,
            'created_at':tweet.created_at,
            'location':tweet.geo
        }
        tweets.append(t)
    return tweets


def _store_data_to_db() -> None:
    DB_NAME = 'tweets_101_etl'
    TABLE_NAME = 'tweets'
    
    mysql_db = mysql.connector.connect(
        host = DB_HOST,
        user = DB_USER,
        passwd = DB_PASS,
        # database = 'DB_NAME'
    )

    mysql_cursor = mysql_db.cursor()
    
    mysql_cursor.execute("SHOW DATABASES")
    # Create a list of existing databases
    db_list = [str(i[0]) for i in mysql_cursor]
    
    # Check if DB exists, if not create one
    if DB_NAME in db_list:
        mysql_cursor.execute(f"USE {DB_NAME}")
    else:
        # Create DB
        mysql_cursor.execute(f'CREATE DATABASE IF NOT EXISTS {DB_NAME}')
    
    # Create a table
    query = """
    CREATE TABLE IF NOT EXISTS {table}(
        id INT(10) ZEROFILL NOT NULL AUTO_INCREMENT PRIMARY KEY,
        tweet_id VARCHAR(20),
        content VARCHAR(200),
        created_at DATETIME,
        location VARCHAR(100)
    )
    """.format(table=TABLE_NAME)
    mysql_cursor.execute(query)

    # SQLALchemy setup
    db = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db)

    data = pd.DataFrame(_get_tweets())
    data.to_sql(TABLE_NAME, engine, if_exists='append', index=False)

    mysql_db.commit()
    mysql_db.close()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='tweeter_101_dag',
    default_args=default_args,
    description='First DAG with ETL process',
    # schedule_interval=timedelta(days=1)
    schedule_interval=timedelta(minutes=30)
)


get_tweets = PythonOperator(
    task_id='get_tweets',
    python_callable=_get_tweets,
    dag=dag
)

store_data_to_db = PythonOperator(
    task_id='store_data_to_db',
    python_callable=_store_data_to_db,
    dag=dag
)

notify = BashOperator(
    task_id='notify',
    bash_command='echo "Operation Completed!"',
    dag=dag
)

get_tweets >> store_data_to_db >> notify
