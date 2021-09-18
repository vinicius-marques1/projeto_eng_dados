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




def stringency3():
    
    stringency_index = '#21bf73'
    detalhes_itens = collection_name.find({'location':'Brazil'})
    df = pd.DataFrame(list(detalhes_itens)).fillna(0)


    temp = df.groupby('date')['stringency_index', 'new_deaths'].sum().reset_index()
    fig = px.area(temp, 
                x="date",
                y= 'stringency_index',
                #y="Count", 
                height=600,
                title='Rigor',
                color_discrete_sequence = [stringency_index])
    fig.update_layout(xaxis_rangeslider_visible=True)
    fig.show()
def novasmortesbrasil2():
    detalhes_itens2 = collection_name.find()
    df= pd.DataFrame(list(detalhes_itens2))
    df= df.fillna(0)
    df_agrupado = df.loc[df['continent'] != 'World']
    df_agrupado = df_agrupado.loc[df_agrupado['continent'] != 'Asia']
    df_agrupado = df_agrupado.loc[df_agrupado['continent'] != 'Africa']
    df_agrupado = df_agrupado.loc[df_agrupado['continent'] != 'North America']
    df_agrupado = df_agrupado.loc[df_agrupado['continent'] != 'Europe']
    df_agrupado = df_agrupado.loc[df_agrupado['continent'] != 'Oceania']
    df_agrupado = df_agrupado.loc[df_agrupado['continent'] != 'Oceania']
    dff= df_agrupado[df_agrupado['continent']!=0]
    df2=dff.sort_values(by='total_deaths', ascending=False)
    fig = px.line(df2,
                x="date",
                y='total_deaths',
                color='location',
                height=600,
                title='Casos Confirmados',
                color_discrete_sequence = px.colors.qualitative.Dark2 )
    fig.show()