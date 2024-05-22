from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import pandahouse as ph
import pandas as pd
import numpy as np


default_args = {
    'owner': 'm-utebaeva',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 5, 20),
}

schedule_interval = '0 7 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def mutebaeva_dag():
    #здесь были параметры подключения
    connection = { 
        'host': "...",
        'password': "...",
        'user': '...',
        'database': "..."
    }
    
    connection_test = {
        'host': "...",
        'password': "...",
        'user': '...',
        'database': "..."
    }
    
    
    @task(retries=4)
    def get_feed_actions():
        query_feed_actions = """
            select date(time) as event_date,
                   countIf(action='view') as views,
                   countIf(action='like') as likes,
                   user_id
            from {db}.feed_actions
            where toDate(time) = yesterday()
            group by user_id, event_date
        """
        feed = ph.read_clickhouse(query_feed_actions, connection=connection)
        return feed

    
    @task(retries=4)
    def get_message_actions():
        query_message_actions = """
            select event_date,
                user_id,
                os,
                gender,
                age,
                messages_sent,
                users_sent,
                messages_received,
                users_received
            from (select date(time) as event_date,
                        user_id,
                        os,
                        gender,
                        age,
                        count(receiver_id) as messages_sent,
                        count(distinct receiver_id) as users_sent
                from {db}.message_actions
                where event_date = yesterday()
                group by event_date, user_id, os, gender, age) as t1
            join 
                (select date(time) as event_date,
                        receiver_id,
                        count(user_id) as messages_received,
                        count(distinct user_id) as users_received
                        from {db}.message_actions
                        where event_date = yesterday()
                        group by event_date, receiver_id) as t2
            on t1.user_id = t2.receiver_id and t1.event_date = t2.event_date
        """
        message = ph.read_clickhouse(query_message_actions, connection=connection)
        return message
    
    
    @task(retries=4)
    def merge_tables(feed, message):
        merged = feed.merge(message, how='outer', on=['user_id', 'event_date']).fillna(0)
        return merged
    
    
    @task(retries=4)
    def sliece_os(merged):
        os = merged.copy().drop(['gender', 'age'], axis=1)
        os = os.groupby(['os', 'event_date']).sum()
        os.reset_index(inplace=True)
        os.rename(columns={'os':'dimension_value'}, inplace=True)
        os.insert(0, 'dimension', pd.Series(len(os) * ['os']))
        os = os[['event_date', 'dimension', 'dimension_value',\
                 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        return os
    
    @task(retries=4)
    def sliece_gender(merged):
        gender = merged.copy().drop(['os', 'age'], axis=1)
        gender = gender.groupby(['gender', 'event_date']).sum()
        gender.reset_index(inplace=True)
        gender.rename(columns={'gender':'dimension_value'}, inplace=True)
        gender.insert(0, 'dimension', pd.Series(len(gender) * ['gender']))
        gender = gender[['event_date', 'dimension', 'dimension_value',\
                         'views', 'likes', 'messages_received', 'messages_sent', 'users_received','users_sent']]
        return gender
    
    @task(retries=4)
    def sliece_age(merged):
        age = merged.copy().drop(['gender', 'os'], axis=1)
        age = age.groupby(['age', 'event_date']).sum()
        age.reset_index(inplace=True)
        age.rename(columns={'age':'dimension_value'}, inplace=True)
        age.insert(0, 'dimension', pd.Series(len(age) * ['age']))
        age = age[['event_date', 'dimension', 'dimension_value',\
                   'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        return age 
    
    @task(retries=4)
    def load_to_test(os, gender, age):
        result = pd.concat([os, gender, age])
        result = result.astype({'messages_received' : int, 'likes': int, 'views': int,\
                                'messages_sent': int,'users_received': int,'users_sent': int})
        query_create = """
                    CREATE TABLE IF NOT EXISTS test.mutebaeva(
                        event_date Date,
                        dimension String,
                        dimension_value String,
                        views Int64,
                        likes Int64,
                        messages_received Int64,
                        messages_sent Int64,
                        users_received Int64,
                        users_sent Int64
                    )
                    ENGINE = MergeTree()
                    ORDER BY event_date
                """
        ph.execute(query_create, connection=connection_test)
        ph.to_clickhouse(df=result, table='mutebaeva', index=False, connection=connection_test)

        
    fd = get_feed_actions()
    msg = get_message_actions()
    mrgd = merge_tables(fd, msg)
    
    os = sliece_os(mrgd)
    gender = sliece_gender(mrgd)
    age = sliece_age(mrgd)
    
    load_to_test(os, gender, age)
       
mutebaeva_dag = mutebaeva_dag()