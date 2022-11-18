from datetime import datetime, timedelta
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import requests
import pandahouse as ph

from airflow.decorators import dag, task


# Функция для CH
def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(io.StringIO(r.text), sep='\t')
    return result

default_args = {
    'owner': 'a.timoshenko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 9, 10),
}

schedule_interval = '0 11 * * *'

def test_report(chat=None):
    chat_id = chat or -555114317
    
    my_token = '5500222373:AAFLC9Xen1SKw1SRCAKzwh2iuzYBaYuffl8' 
    bot = telegram.Bot(token=my_token) 
    
    connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220820',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

    q1 = """
    SELECT toDate(time) as day, count(distinct user_id) as DAU
    FROM {db}.feed_actions 
    WHERE toDate(time) between today() - 7 and today() - 1
    group by day;

    """
    q2 = """
    select toDate(time) as day, countIf(user_id, action='view') as views, countIf(user_id, action='like') as likes
    from {db}.feed_actions
    where toDate(time) between today() - 7 and today() - 1
    group by day;
    """
    dau = ph.read_clickhouse(q1, connection=connection)
    vl = ph.read_clickhouse(q2, connection=connection)
    vl['ctr'] = vl.likes/vl.views

    msg = 'Отчёт за ' + str(dau.iloc[6,0])[:10] +\
    '\nDAU = ' + str(dau.iloc[6,1]) +\
    '\nПросмотры = ' + str(vl.iloc[6,1]) +\
    '\nЛайки = ' + str(vl.iloc[6,2]) +\
    '\nCTR = ' + str(round(vl.iloc[6,2]/vl.iloc[6,1], 4))
    bot.sendMessage(chat_id=chat_id, text=msg)

    plot_object = io.BytesIO()
    fig, axes = plt.subplots(2, 2, figsize=(15, 9))

    a1 = sns.lineplot(data=dau, x="day", y="DAU", ax=axes[0,0])
    a1.set(title='DAU')
    a1.set_ylabel('')
    a1.set_xlabel('')

    a2 = sns.lineplot(data=vl, x="day", y="views", ax=axes[0,1])
    a2.set(title='Просмотры')
    a2.set_ylabel('')
    a2.set_xlabel('')

    a3 = sns.lineplot(data=vl, x="day", y="likes", ax=axes[1,0])
    a3.set(title='Лайки')
    a3.set_ylabel('')
    a3.set_xlabel('')

    a4 = sns.lineplot(data=vl, x="day", y="ctr", ax=axes[1,1])
    a4.set(title='CTR')
    a4.set_ylabel('')
    a4.set_xlabel('')

    plt.tight_layout()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'test_plot.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_report_atimoshenko():
    
    @task()
    def make_report():
        test_report()
    
    make_report()
    
dag_report_atimoshenko = dag_report_atimoshenko()

