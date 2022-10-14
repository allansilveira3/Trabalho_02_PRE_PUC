import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args = {
    'owner':"Allan",
    'depends_on_past': False,
    'start_date': datetime(2022,10,12)
}

@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Titanic_dag2'])
def trabalho2_dag2():
    @task
    def total_mean():
        NOME_TABELA = "/tmp/resultados.csv"
        dado = "/tmp/tabela_unica.csv"
        df = pd.read_csv(dado, sep=";")
        res1 = df.groupby(['Sex']).agg({
           'PassengerId':'mean',
           'Fare':'mean',
           'SibSp_Parch':'mean'
        })
        print(res1)
        res1.to_csv(NOME_TABELA, index=False, sep=";")
    
    fim = EmptyOperator(task_id="fim")

    ind = total_mean()

    ind >> fim

execucao = trabalho2_dag2()