from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph

from airflow.decorators import dag, task


# Функция для CH
def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

default_args = {
    'owner': 'a.timoshenko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 9, 9),
}

schedule_interval = '0 7 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_atimoshenko():

    @task()
    def extract_messages():
        query1 = '''
            SELECT user as user_id, event_date, messages_sent, messages_received, users_sent, users_received
            from
            (SELECT reciever_id as user, toDate(time) as event_date, count(*) as messages_received, count(distinct user_id) as users_received,
            toDate(time) as event_date
            FROM simulator_20220820.message_actions 
            WHERE toDate(time) = today() - 1
            group by reciever_id, event_date) t1
            join
            (SELECT user_id as user, toDate(time) as event_date, count(*) as messages_sent, count(distinct reciever_id) as users_sent,
            toDate(time) as event_date
            FROM simulator_20220820.message_actions 
            WHERE toDate(time) = today() - 1
            group by user_id, event_date) t2 using user
            format TSVWithNames'''
        df = ch_get_df(query=query1)
        return df
    
    @task()
    def extract_feed():
        query2 = '''
            SELECT user_id,
            sum(action = 'like') as likes,
            sum(action = 'view') as views,
            toDate(time) as event_date
            FROM simulator_20220820.feed_actions 
            WHERE toDate(time) = today() - 1
            group by user_id, event_date
            format TSVWithNames'''
        df = ch_get_df(query=query2)
        return df
    
    @task() #добавим информацию про пол, возраст, ос
    def extract_info():
        query3 = '''
            select user_id, gender, age, os
            from (
                select user_id, gender, age, os
                from simulator_20220820.feed_actions
                group by user_id, gender, age, os
                union distinct
                select user_id, gender, age, os
                from simulator_20220820.message_actions
                group by user_id, gender, age, os)
            group by user_id, gender, age, os
            format TSVWithNames'''
        df = ch_get_df(query=query3)
        return df
    
    @task()
    def join_messages_feed(df_messages, df_feed, df_info):
        df = df_messages.merge(df_feed, how="outer", on=['user_id', 'event_date']).fillna(0)
        df = df.merge(df_info, how="left", on='user_id')
        return df
    
    @task()
    def groupby_gender(df):
        grouped_df = df[['likes', 'views', 'messages_sent', 'messages_received', 
                         'users_sent', 'users_received', 'gender', 'event_date']].groupby(['gender', 'event_date']).sum().reset_index()
        grouped_df['dimension'] = 'gender'
        grouped_df = grouped_df.rename(columns={'gender': 'dimension_value'})
        return grouped_df
    
    @task()
    def groupby_age(df):
        grouped_df = df[['likes', 'views', 'messages_sent', 'messages_received', 
                         'users_sent', 'users_received', 'age', 'event_date']].groupby(['age', 'event_date']).sum().reset_index()
        grouped_df['dimension'] = 'age'
        grouped_df = grouped_df.rename(columns={'age': 'dimension_value'})
        return grouped_df
    
    @task()
    def groupby_os(df):
        grouped_df = df[['likes', 'views', 'messages_sent', 'messages_received', 
                         'users_sent', 'users_received', 'os', 'event_date']].groupby(['os', 'event_date']).sum().reset_index()
        grouped_df['dimension'] = 'os'
        grouped_df = grouped_df.rename(columns={'os': 'dimension_value'})
        return grouped_df
    
    @task()
    def concat_dfs(df1, df2, df3):
        df = pd.concat([df1, df2, df3])
        return df

    @task()
    def load_todb(df):
        conn_test = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'656e2b0c9c'
                     }
        df = df.astype({'dimension': 'object', 'dimension_value': 'object',
                        'likes': int, 'views': int, 'messages_received': int,
                        'messages_sent': int, 'users_received': int, 'users_sent': int})
        ph.to_clickhouse(df, 'atimoshenko', index=False, connection=conn_test)
    
    df_messages = extract_messages()
    df_feed = extract_feed()
    df_info = extract_info()
    df_joined = join_messages_feed(df_messages, df_feed, df_info)
    df_gender_agg = groupby_gender(df_joined)
    df_age_agg = groupby_age(df_joined)
    df_os_agg = groupby_os(df_joined)
    df_final = concat_dfs(df_gender_agg, df_age_agg, df_os_agg)
    load_todb(df_final)
    
dag_atimoshenko = dag_atimoshenko()