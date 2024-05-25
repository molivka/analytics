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
    'start_date': datetime(2024, 5, 25),
}
schedule_interval = '0 11 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def mutebaeva_app_report_dag():
    #здесь были нужные параметры
    connection = { 
        'host': "...",
        'password': "...",
        'user': '...',
        'database': "..."
    }
    token = '...'
    chat_id = ...
    
    
    def send_chart(data, metirc, img_name, chat_id=...):
        for i in data:
            df, col_name, color, label = i
            sns.lineplot(data=df, x='date', y=col_name, color=color, label=label)
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
        q_mess = """
            select toDate(time) as date,
                count(receiver_id) as messages,
                count(distinct user_id) as DAU_mes
            from {db}.message_actions
            where toDate(time) between yesterday() - 7 and yesterday()
            group by date
        """
        q_feed = """
            select toDate(time) as date, 
                count(distinct user_id) as DAU, 
                countIf(action='view') as views, 
                countIf(action='like') as likes, 
                countIf(action='like') / countIf(action='view') as CTR
            from {db}.feed_actions
            where toDate(time) between yesterday() - 7 and yesterday()
            group by date
        """
        q_source_feed = """
            select toDate(time) as date,
                count(distinct user_id) as users, 
                source
            from {db}.feed_actions
            where toDate(time) between yesterday() - 7 and yesterday()
            group by date, source
        """
        q_source_mess = """
            select toDate(time) as date,
                count(distinct user_id) as users, 
                source
            from {db}.message_actions
            where toDate(time) between yesterday() - 7 and yesterday()
            group by date, source
        """
        mess = ph.read_clickhouse(query=q_mess, connection=connection)
        feed = ph.read_clickhouse(query=q_feed, connection=connection)
        source_feed = ph.read_clickhouse(query=q_source_feed, connection=connection)
        source_mess = ph.read_clickhouse(query=q_source_mess, connection=connection)
 
        mess['date'] = mess['date'].dt.day 
        feed['date'] = feed['date'].dt.day
        source_feed['date'] = source_feed['date'].dt.day 
        source_mess['date'] = source_mess['date'].dt.day  

        source_feed = ph.read_clickhouse(query=q_source_feed, connection=connection)
        source_feed['date'] = source_feed['date'].dt.day
        source_feed = source_feed.pivot_table(index='date', columns='source', values=['date', 'users']).reset_index()
        source_feed.columns = source_feed.columns.droplevel(0)
        source_feed = source_feed.rename(columns={'':'date'})

        source_mess = ph.read_clickhouse(query=q_source_mess, connection=connection)
        source_mess['date'] = source_mess['date'].dt.day
        source_mess = source_mess.pivot_table(index='date', columns='source', values=['date', 'users']).reset_index()
        source_mess.columns = source_mess.columns.droplevel(0)
        source_mess = source_mess.rename(columns={'':'date'})

        data = [(mess, 'DAU_mes', 'green', 'DAU мессенджера'), (feed, 'DAU', 'red', 'DAU ленты новостей')]
        send_chart(data, 'DAU ленты новостей и мессенджера', 'weekly_dau', chat_id)

        data = [(feed, 'views', 'green', 'Кол-во просмотров'), 
                (feed, 'likes', 'blue', 'Кол-во лайков')]
        send_chart(data, 'Метрики по ленте новостей', 'weekly_feed', chat_id)

        data = [(feed, 'CTR', 'red', 'CTR')]
        send_chart(data, 'CTR ленты новостей', 'weekly_feed_ctr', chat_id)

        data = [(source_feed, 'ads', 'green', 'ads'), (source_feed, 'organic', 'red', 'organic')]
        send_chart(data, 'Трафик ленты новостей', 'weekly_feed_traffic', chat_id)

        data = [(source_mess, 'ads', 'green', 'ads'), (source_mess, 'organic', 'red', 'organic')]
        send_chart(data, 'Трафик мессенджера', 'weekly_mess_traffic', chat_id)

        data = [(mess, 'messages', 'green', 'Кол-во сообщений')]
        send_chart(data, 'Метрика по мессенджеру', 'weekly_mess', chat_id)

        return day


    @task(retries=4)
    def send_daily(chat_id=...):
        q_both = """
            select count(distinct t1.user_id) as both_DAU
            from {db}.feed_actions t1
                inner join 
                {db}.message_actions t2
                on toDate(t1.time) = toDate(t2.time) and t1.user_id = t2.user_id
            where toDate(t1.time) = yesterday()
            group by toDate(t1.time)
        """ 
        q_feed = """
            select toDate(time) as date,
                count(distinct user_id) as DAU, 
                countIf(action='like') as likes, 
                countIf(action='view') as views,
                round(countIf(action='like') / countIf(action='view'), 4) as CTR
            from {db}.feed_actions
            where toDate(time) = yesterday()
            group by date
        """
        q_messages = """
            select count(receiver_id) as messages
            from {db}.message_actions
            where toDate(time) = yesterday()
            group by toDate(time) 
        """

        both = ph.read_clickhouse(query=q_both, connection=connection)
        feed = ph.read_clickhouse(query=q_feed, connection=connection)
        messages = ph.read_clickhouse(query=q_messages, connection=connection)

        DAU_both = both['both_DAU'][0]
        [date, DAU, views, likes, CTR] = feed.iloc[0, :]
        cnt_mes = messages['messages'][0]
       
        msg = f"Доброе утро! \
            \nДанные за {date.strftime('%d %b %Y')}:\
            \nDAU пользователей ленты и мессенджера: {DAU_both} \
            \nКоличество отправленных сообщений: {cnt_mes}\
            \nDAU пользователей ленты: {DAU} \
            \nВсего просмотров: {views} \
            \nВсего лайков: {likes} \
            \nCTR: {CTR}\n"
        bot.sendMessage(chat_id=chat_id, text=msg)
        return date
    

    @task(retries=4)
    def report(start, chat_id=...):
        end = start - timedelta(days=7)
        q = """
            select toDate(t1.time) as date, 
                t1.user_id, 
                t1.country, 
                t1.exp_group, 
                t1.os, 
                t1.source, 
                sum(action='like') as likes, 
                sum(action='view') as views, 
                count(distinct receiver_id) as messages
            from {db}.feed_actions t1 
                full outer join 
                    {db}.message_actions t2
                on toDate(t1.time) = toDate(t2.time) and t1.user_id = t2.user_id
            where toDate(t1.time) between yesterday() - 7 and yesterday()
            group by date, user_id, country, exp_group, os, source
            order by date    
        """
        msq = f"Файл с основными данными по пользователям:\n"
        bot.sendMessage(chat_id=chat_id, text=msq)
        data = ph.read_clickhouse(query=q, connection=connection)
        file_object = io.StringIO()
        data.to_csv(file_object)
        file_object.name = f"user info {start.strftime('%d %b %Y')}-{end.strftime('%d %b %Y')}.csv".replace(' ', '_')
        file_object.seek(0)
        bot.sendDocument(chat_id=chat_id, document=file_object)

    
    bot = telegram.Bot(token=token)
    day = send_daily(chat_id)
    start = send_weekly(day, chat_id)
    report(start)


mutebaeva_app_report_dag = mutebaeva_app_report_dag()