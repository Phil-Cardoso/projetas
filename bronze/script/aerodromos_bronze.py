from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.context import *
import re
from datetime import date
import requests
import json
import os

# ============================================================================

# Iniciando spark

spark = SparkSession.builder.appName("minhaAplicacao").getOrCreate()

LOCATION = '/silver/data/VRA_tratado'
vra = spark.read.format('parquet').load(LOCATION)

# ============================================================================

# Definindo colunas com info de ICAO
colunas = ['icao_aerodromo_destino',
           'icao_aerodromo_origem', 'icao_empresa_aerea']

# Lista que recebera ICAO unico
list_icao = []

# Pegando ICAO do arquivo VAR
for x in colunas:

    icao_apoio = vra.select(x).distinct().collect()

    for y in icao_apoio:

        if y[0] not in list_icao:

            list_icao.append(y[0])

# ============================================================================

# Acessando os dados da API 'https://rapidapi.com/Active-api/api/airport-info/'
url = "https://airport-info.p.rapidapi.com/airport"

# lista para receber os dados da api
df = []

# lop de icao
log_icao = []

# key encontrada no doc do site
key = 'ce19d0164fmsh3d383efc0e85ce5p16dcb1jsnb1a4a3c79541'

# Loop para pegar os dados da API

for icao in list_icao:
    querystring = {"icao": f"{icao}"}

    headers = {
        "X-RapidAPI-Key": f"{key}",
        "X-RapidAPI-Host": "airport-info.p.rapidapi.com"
    }

    response = requests.request(
        "GET", url, headers=headers, params=querystring)

    # Convertendo response para json

    response = json.loads(response.text)

    # verificando se icao existe

    if 'error' in list(response.keys()):

        log_icao.append({'icao': icao, 'status': 'n encontrado'})

        pass

    else:

        log_icao.append({'icao': icao, 'status': 'ok'})
        df.append(response)

# ============================================================================

# transformando lista em spark df com parallelize
rdd = spark.sparkContext.parallelize(df)

aerodromos = rdd.toDF()

# ============================================================================

# salvando dados de aerodromos
LOCATION = '/content/bronze/aerodromos/'
aerodromos.write.mode('overwrite').parquet(LOCATION)

# salvando log_icao
rdd = spark.sparkContext.parallelize(log_icao)

log_icao = rdd.toDF()

LOCATION = '/content/logs/aerodromos/'

log_icao \
    .repartition(1) \
    .write \
    .mode('append') \
    .option("header", "true") \
    .option("delimiter", ";") \
    .csv(LOCATION)

# ============================================================================

# Alterando nome do log

data = date.today()
data = str(data).replace('-', '')

for x in os.listdir(LOCATION):
    if '.csv' in x and '.crc' not in x:
        arquivo = LOCATION + x

        os.rename(arquivo, LOCATION + f'aero_{data}.csv')

# ============================================================================

# Apagando arquivo que não seja o log
for x in os.listdir(LOCATION):

    if 'aero' not in x:

        try:
            os.remove(LOCATION+x)
        except:
            pass
