from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import pandahouse as ph
import io


default_args = {
    'owner': 'm-utebaeva',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 6, 1),
}
schedule_interval = '*/15 * * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def mutebaeva_app_alert_dag():
    #здесь были нужные параметры
    connection = { 
        'host': "...",
        'password': "...",
        'user': '...',
        'database': "..."
    }
    token = '...'
    chat_id = ...
    

    @task(retries=3)
    def from_today():
        q = """
            with (select date_sub(now() , interval 15 minute)) as prev 
            select time, 
                count(distinct t1.user_id) as DAU_feed, 
                count(distinct t2.user_id) as DAU_mess,
                countIf(action='view') as views, 
                countIf(action='like') as likes,
                countIf(action='like') / countIf(action='view') as CTR, 
                count(receiver_id) as send_mess,
                os
            from {db}.feed_actions t1 full outer join {db}.message_actions t2
            on t1.time = t2.time and t1.os = t2.os
            where time between prev and now()
            group by time, os
        """
        today = ph.read_clickhouse(query=q, connection=connection)
        return today


    @task(retries=3)
    def from_yesterday():
        q = """
            with (select date_sub(now() , interval 1 day)) as prev2,
                (select date_sub(prev2 , interval 15 minute)) as prev1
            select time, 
                count(distinct t1.user_id) as DAU_feed, 
                count(distinct t2.user_id) as DAU_mess,
                countIf(action='view') as views, 
                countIf(action='like') as likes,
                countIf(action='like') / countIf(action='view') as CTR, 
                count(receiver_id) as send_mess,
                os
            from {db}.feed_actions t1 full outer join {db}.message_actions t2
            on t1.time = t2.time and t1.os = t2.os
            where time between prev1 and prev2
            group by time, os
        """
        yesterday = ph.read_clickhouse(query=q, connection=connection)
        return yesterday


    def check(df1, df2, alpha=1):
        std = df1.std()
        mean = df1.mean()
        prov = df2.mean()
        if prov < mean - alpha * std or prov > mean + alpha * std:
            return 1
        return 0


    def send_alert(df1, col_name, metric, tek, past, os):
        msg = f"Метрика: {metric}, в срезе {os}.\
                \nТекущее значение {round(tek, 3)}, вчера было {round(past, 3)}, отклонение более 68%."
        bot.sendMessage(chat_id=chat_id, text=msg)
        sns.lineplot(data=df1, x=col_name, y='time', color='green', label=f"{metric} сегодня")
        plt.title(f"{metric}")
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'problem'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)


    @task(retries=3)
    def compare(today, yesterday, alpha=1):
        #для iOS
        df1 = today[today['os'] == 'iOS']
        df2 = yesterday[yesterday['os'] == 'iOS']

        if check(df1.DAU_feed, df2.DAU_feed):
            send_alert(df1, 'DAU_feed', 'DAU ленты новостей', df1.DAU_feed.sum(), df2.DAU_feed.sum(), 'iOS')
        
        if check(df1.DAU_mess, df2.DAU_mess):
            send_alert(df1, 'DAU_mess', 'DAU мессенджера', df1.DAU_mess.sum(), df2.DAU_mess.sum(), 'iOS')

        if check(df1.views, df2.views):
            send_alert(df1, 'views', 'просмотры', df1.views.sum(), df2.views.sum(), 'iOS')

        if check(df1.likes, df2.likes):
            send_alert(df1, 'likes', 'лайки', df1.likes.sum(), df2.likes.sum(), 'iOS')
        
        if check(df1.CTR, df2.CTR):
            send_alert(df1, 'CTR', 'CTR', df1.likes.sum() / df1.views.sum(), df2.likes.sum() / df2.views.sum(), 'iOS')

        if check(df1.send_mess, df2.send_mess):
            send_alert(df1, 'send_mess', 'кол-во сообщений', df1.send_mess.sum(), df2.send_mess.sum(), 'iOS')
        #для Android
        df1 = today[today['os'] == 'Android']
        df2 = yesterday[yesterday['os'] == 'Android']
        if check(df1.DAU_feed, df2.DAU_feed):
            send_alert(df1, 'DAU_feed', 'DAU ленты новостей', df1.DAU_feed.sum(), df2.DAU_feed.sum(), 'Android')
        
        if check(df1.DAU_mess, df2.DAU_mess):
            send_alert(df1, 'DAU_mess', 'DAU мессенджера', df1.DAU_mess.sum(), df2.DAU_mess.sum(), 'Android')

        if check(df1.views, df2.views):
            send_alert(df1, 'views', 'просмотры', df1.views.sum(), df2.views.sum(), 'Android')

        if check(df1.likes, df2.likes):
            send_alert(df1, 'likes', 'лайки', df1.likes.sum(), df2.likes.sum(), 'Android')
        
        if check(df1.CTR, df2.CTR):
            send_alert(df1, 'CTR', 'CTR', df1.likes.sum()/df1.views.sum(), df2.likes.sum()/df2.views.sum(), 'Android')

        if check(df1.send_mess, df2.send_mess):
            send_alert(df1, 'send_mess', 'кол-во сообщений', df1.send_mess.sum(), df2.send_mess.sum(), 'Android')

        
    bot = telegram.Bot(token=token)
    today = from_today()
    yesterday = from_yesterday()
    compare(today, yesterday)
    

mutebaeva_app_alert_dag = mutebaeva_app_alert_dag()