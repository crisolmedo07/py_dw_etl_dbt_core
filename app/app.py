# import
import os
import pandas as pd
import streamlit as st 
import plotly.express as px
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from dotenv import load_dotenv


# import das variables de ambiente
load_dotenv()

commodities = ['CL=F' , 'GC=F' , 'SI=F'] # activos de bolsa de valores

# busca las variables de ambientes con la biblioteca dotenv
DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD') 
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')
    
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

def get_data():
    query = f"""
    select 
        *
    from public.dm_commodities as p
    order by p.valor desc ;
    """
    df = pd.read_sql( query , engine)
    return df

# Configurar a pag do Streamlit
st.set_page_config(page_title = 'Dashboard de Commodities' , layout = 'wide')

# Titulo Dashboard
st.title('Dashboard Commodities')

# Descripción
st.write("""
Este Dashboard es para mostrar los datos de commodities y sus transacciones
""")

df = get_data()

# Mostrar dataframe
st.dataframe( df)
 
 
# Crear gráfico de barras invertido
fig = px.bar(df, x='valor', y='simbolo', orientation='h', title='Valores de Commodities')

# Mostrar gráfico en el dashboard
st.plotly_chart(fig) 
