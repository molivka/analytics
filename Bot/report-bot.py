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
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 5, 24),
}
schedule_interval = '0 11 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def mutebaeva_report_dag():
    #здесь были нужные параметры
    connection = { 
        'host': "...",
        'password': "...",
        'user': '...',
        'database': "..."
    }
    token = '...'
    chat_id = ...
    
    
    def send_chart(week, col_name, metirc, img_name, chat_id=...):
        sns.lineplot(data=week, x='date', y=col_name, color='green')
        plt.title(f"{metirc} за прошлую неделю")
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = img_name
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        
    @task(retries=4)
    def send_weekly(day, chat_id=...):
        begin = day - timedelta(days=7)
        msq = f"Статистика за {begin.strftime('%d %b %Y')} - {day.strftime('%d %b %Y')}:\n"
        bot.sendMessage(chat_id=chat_id, text=msq)
        for_week = """
            select toDate(time) as date, 
                count(distinct user_id) as DAU, 
                countIf(action='view') as views, 
                countIf(action='like') as likes, 
                countIf(action='like') / countIf(action='view') as CTR
            from simulator_20240420.feed_actions
            where toDate(time) between yesterday() - 7 and yesterday()
            group by date
        """
        week = ph.read_clickhouse(query=for_week, connection=connection)
        week['date'] = week['date'].dt.day 
        send_chart(week, 'DAU', 'DAU', 'Weekly_DAU', chat_id)
        send_chart(week, 'views', 'Просмотры', 'Weekly_views', chat_id)
        send_chart(week, 'likes', 'Лайки', 'Weekly_likes', chat_id)
        send_chart(week, 'CTR', 'CTR', 'Weekly_CTR', chat_id)


    @task(retries=4)
    def send_daily(chat_id=...):
        for_day = """
            select toDate(time) as date, 
                count(distinct user_id) as DAU, 
                countIf(action='view') as views, 
                countIf(action='like') as likes, 
                round(countIf(action='like') / countIf(action='view'), 4) as CTR
            from simulator_20240420.feed_actions
            where toDate(time) = yesterday()
            group by date
        """
        day = ph.read_clickhouse(query=for_day, connection=connection)
        DAU = day['DAU']
        views = day['views']
        [date, DAU, views, likes, CTR] = day.iloc[0, :]
        msg = f"Доброе утро! \
            \nДанные за {date.strftime('%d %b %Y')}:\
            \nDAU: {DAU} \
            \nВсего просмотров: {views} \
            \nВсего лайков: {likes} \
            \nCTR: {CTR}\n"
        bot.sendMessage(chat_id=chat_id, text=msg)
        return date

    
    bot = telegram.Bot(token=token)
    day = send_daily(chat_id)
    send_weekly(day, chat_id)

mutebaeva_report_dag = mutebaeva_report_dag()