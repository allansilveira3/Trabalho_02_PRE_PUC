from ast import Return
from asyncio import Task
from operator import index
import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args = {
    'owner':"Allan",
    'depends_on_past': False,
    'start_date': datetime(2022,10,12)
}

@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Titanic'])
def trabalho2_dag1():

    @task #Aquisção do dado
    def ingestao():
        NOME_DO_ARQUIVO = "/tmp/titanic.csv"
        df = pd.read_csv(URL,sep=';')
        df.to_csv(NOME_DO_ARQUIVO, index=False, header=True, sep=";")
        return NOME_DO_ARQUIVO

    @task
    def ind_passageiros(nome_do_arquivo):
        NOME_TABELA = "/tmp/passageiros_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res = df.groupby(['Sex','Pclass']).agg({
            "PassengerId":"count"
        }).reset_index()
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA

    @task
    def preço_med(nome_do_arquivo):
        NOME_TABELA = "/tmp/preço_medio_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        med = df.groupby(['Sex','Pclass']).agg({
            "Fare":"mean"
        }).reset_index()
        print(med)
        med.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA

    
    @task
    def sum_SibSp_Parch(nome_do_arquivo):
        NOME_TABELA = "/tmp/soma SibSP com Parch.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        df['SibSp_Parch'] = df['SibSp'] + df['Parch']
        soma = df.groupby(['Sex','Pclass']).agg({
            'SibSp_Parch':'sum'
        }).reset_index()           
        print(soma)
        soma.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA

    @task
    def unir(tab1,tab2,tab3):
        NOME_TABELA = "/tmp/tabela_unica.csv"
        df1 = pd.read_csv(tab1, sep=";")
        df2 = pd.read_csv(tab2, sep=";")
        df3 = pd.read_csv(tab3, sep=";")
        df12 = df1.merge(df2,on=['Sex','Pclass'], how='inner')
        df123 = df12.merge(df3,on=['Sex','Pclass'], how='inner')
        print(df123)
        df123.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA    

    inicio = DummyOperator(task_id="Início")
    fim = DummyOperator(task_id="Fim")

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def end():
        print("Terminou")
    triggerdag = TriggerDagRunOperator(
        task_id = 'trigga_trabalho2_dag2',
        trigger_dag_id = "trabalho2_dag2"
    )

    ing = ingestao()
    indicador = ind_passageiros(ing)
    pre_med = preço_med(ing)
    soma_t = sum_SibSp_Parch(ing)
    total = unir(indicador,pre_med,soma_t)
   
    
    inicio >> ing >> [indicador,pre_med,soma_t] >> total >> fim >> triggerdag

execucao = trabalho2_dag1()




