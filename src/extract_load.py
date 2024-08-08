from sqlalchemy import create_engine
from dotenv import load_dotenv
import yfinance as yf
import pandas as pd
import os
import time 

# import variables de ambiente

load_dotenv()

commodities = ['CL=F' , 'GC=F' , 'SI=F']

# busca las variables de ambientes con la biblioteca dotenv
DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD') 
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')
    
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

def buscar_datos_commodiesties(simbolo , periodo = '5y' , intervalo = '1d'):
    ticker = yf.Ticker(simbolo)
    datos = ticker.history(period = periodo , interval = intervalo)[['Close']]
    datos['simbolo'] = simbolo  
    return datos

def buscar_todos_datos_commodiesties(commodities):
    todos_datos = []
    for simbolo in commodities:
        datos = buscar_datos_commodiesties(simbolo)
        todos_datos.append(datos)
    return pd.concat(todos_datos)


def guardar_en_bd(df , schema = 'public'):
    start_time = time.time()
    df.to_sql('commodities' , engine , if_exists = 'replace' , index = True , index_label = 'Date' , schema = schema )
    end_time = time.time()
    print(f"Tiempo de inserccion: {end_time - start_time:.2f} seg...")

if __name__ == "__main__":
    datos_concatenados = buscar_todos_datos_commodiesties(commodities)
    guardar_en_bd(datos_concatenados)
