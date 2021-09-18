import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import display
import random as random
from datetime import datetime
import pandas as pd
from plotly.offline import plot, iplot, init_notebook_mode
import plotly.io as pio
import plotly.figure_factory as ff
from plotly.subplots import make_subplots
import warnings
import folium
#warnings.filter
import plotly.express as px
import seaborn as sns

import matplotlib.pyplot as plt

plt.style.use("ggplot")


def get_database():
    from pymongo import MongoClient
    import pymongo

    CONNECTION_STRING = "mongodb+srv://jemimafpassos:041092@cluster0.owswo.mongodb.net/myFirstDatabase"
  
    from pymongo import MongoClient
    client = MongoClient(CONNECTION_STRING)

    return client['Projeto_Final_EngdeDados']
dbname = get_database()
collection_name = dbname["dados_global_covid"]

detalhes_itens = collection_name.find().sort('date')
df = pd.DataFrame(list(detalhes_itens))
df = df.fillna("Não Informado")
df['date'] =  pd.to_datetime(df['date'],format='%Y-%m-%d')
df['month'] = df['date'].dt.month


#display(df)

def teste():
    
    detalhes_itens = collection_name.find().sort('If Closed due to COVID19 When')#.sort('date')
    df = pd.DataFrame(list(detalhes_itens))
    #g = df.sort_values(by='If Closed due to COVID19 When').head(10) #ascending=False).head(10)
    #tab = pd.DataFrame({"Nome do filme":g['Series_Title'],"Nota" :g['Meta_score'],"Gênero":g['Genre']})
    display(df)
def teste1():
    detalhes_itens = collection_name.find({"date":"2021-01-02"})
    df = pd.DataFrame(list(detalhes_itens)).fillna(0)
    dff= df[df['continent']!=0]
    g = dff.sort_values(by='total_deaths',ascending=False).head(10)
    #display(g)
    dt2= collection_name.find({"date":"2021-02-02"})
    df2 = pd.DataFrame(list(dt2)).fillna(0)
    dff2= df2[df2['continent']!=0]
    h = dff2.sort_values(by='total_deaths',ascending=False).head(10)
    #display(h)
    dt3= collection_name.find({"date":"2021-03-02"})
    df3 = pd.DataFrame(list(dt3)).fillna(0)
    dff3= df3[df3['continent']!=0]
    
    i = dff3.sort_values(by='total_deaths',ascending=False).head(10)
    #display(i)
   
    dt4= collection_name.find({"date":"2021-04-02"})
    dt5= collection_name.find({"date":"2021-05-02"})
    dt6= collection_name.find({"date":"2021-05-25"})
    dffinal= pd.merge(df, df2, how = 'inner', on = 'location')
    #dff19= dffinal[dffinal['continent']!=0]
    h = dffinal.sort_values(by='total_deaths',ascending=False).head(10)
    
    
    display(h)
def teste2():
    
    detalhes_itens2 = collection_name.find({"location": "World"})
    
    dfteste = pd.DataFrame(list(detalhes_itens2))

    plt.figure(figsize=(10,8))
    plt.plot(dfteste['date'],dfteste[''])

    plt.show()
def teste5():
    detalhes_itens = collection_name.find({"date":"2021-01-02"})
    df = pd.DataFrame(list(detalhes_itens)).fillna(0)
    dff= df[df['continent']!=0]
    g = dff.sort_values(by='total_deaths',ascending=False).head(10)
    x = g.groupby('month')
    x.describe()
    plt.style.use("ggplot")
    teste_x = (x.mean()["new_deaths"]).values
    x1 =  np.arange(len(teste_x))

    plt.bar(x1, teste_x, width=0.25, label = 'new_deaths', color = 'b')

    meses = ['jan','fev','mar','abril','maio']
    plt.xlabel('Meses')
    plt.ylabel('new_deaths')
    plt.xticks([x + 0.25 for x in range(len(teste_x))], meses)
    plt.title("Quantidade de novos óbitos globais por COVID-19 nos primeiros 5 meses de 2021")

    plt.show()
    #x.mean()
    #x.mean().new_deaths
    #x.mean().people_fully_vaccinated
#FUNCIONANDO
def teste6():
    detalhes_itens = collection_name.find()
    df = pd.DataFrame(list(detalhes_itens)).fillna(0)
    dff= df[df['continent']!=0]
    df_agrupado = dff.groupby(['date', 'location'])['total_cases', 'total_deaths','new_cases'].sum().reset_index()
    df2=df_agrupado.sort_values(by='total_cases', ascending=False)
    
    plt.figure(figsize=(35, 25))
    sns.barplot(x=df2['date'], y=df2['total_deaths'], dodge=False, palette='husl')
    plt.xticks(rotation=90)
    init_notebook_mode(connected=True)
    fig = px.line(df2,
                x="date",
                y='total_cases',
                color='location',
                height=600,
                title='Casos Confirmados',
                color_discrete_sequence = px.colors.qualitative.Dark2 )
    fig.show()

    fig = px.line(df2,
                x="date",
                y='total_deaths',
                color='location',
                height=900,
                title='Mortes Confirmadas',
                color_discrete_sequence = px.colors.qualitative.Dark2)
    fig.show()
#FUNCIONANDO
def casosdiarios():
    detalhes_itens = collection_name.find()
    df = pd.DataFrame(list(detalhes_itens)).fillna(0)
    dff= df[df['continent']!=0]
    df_agrupado = dff.groupby(['date', 'location'])['total_cases', 'total_deaths','new_cases'].sum().reset_index()
    df2=df_agrupado.sort_values(by='total_cases', ascending=False)
    for i in range(50,len(df2['total_cases'])):
        fig, ax = plt.subplots(figsize=(18, 5))
        # sns.lineplot(np.arange(len(dados['Data']))[:i], dados['CA'][:i], ax=ax, color='k', linestyle='dashed', label="Valores acumulados")

        clrs = ['darkblue' if (x < 13) else 'red' for x in np.arange(len(df2['date'])) ]
        sns.barplot(np.arange(len(df2['date']))[:i], df2['new_cases'][:i], palette=clrs, ax=ax, label=None)

        plt.xticks(np.arange(len(df2['date']))[:i], df2['date'][:i], rotation=90, fontsize=12)
        plt.yticks(fontsize=12)
        plt.title('Novos casos por dia ', fontsize=16)
        ax.set_frame_on(False)
        ax.tick_params(axis='both', which='both', length=0)
        plt.grid(True, alpha=0.3)

        plt.legend(fontsize=14)
        plt.savefig('imgs/'+str(i)+'.png', dpi=100, format='png', bbox_inches='tight')
        plt.show()
        plt.close('all')
#periodo = (df[(df_italia['date'] > '2021-05-02') & (df['date'] < '2021-05-24')])
#FUNCIONANDO
def evolucaoinglaterra():
    detalhes_itens2 = collection_name.find({"location": "United Kingdom"})
    df= pd.DataFrame(list(detalhes_itens2))
    df= df.fillna(0)

    df_agrupado = df.groupby(['date', 'location','continent', 'people_vaccinated'])['new_cases', 'new_deaths', 'population'].sum().reset_index()

    fig = px.line(df_agrupado,
            x='date',
            y='people_vaccinated',
            color='location',
            height=600,
            title='Casos Confirmados',
            color_discrete_sequence = px.colors.qualitative.Dark2)
    fig.show()

    
        


#teste1()
#teste2()
#display(df)
#df.dtypes
#teste5()
#teste6()
#casosdiarios()
#evolucaoinglaterra()