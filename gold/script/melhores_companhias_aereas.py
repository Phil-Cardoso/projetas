from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.context import *
import tratar_texto

# ============================================================================

# Iniciando spark
spark = SparkSession.builder.appName("minhaAplicacao").getOrCreate()

# ============================================================================

# Fazendo a leitura dos arquivos

LOCATION = '/silver/data/VRA_tratado'
vra = spark.read.format('parquet').load(LOCATION)

LOCATION = '/silver/data/AIR_CIA_tratado/'
air_cia = spark.read.format('parquet').load(LOCATION)

LOCATION = '/bronze/data/aerodromos/'
aerodromos = spark.read.format('parquet').load(LOCATION)

# ============================================================================

# Criando views

vra.createOrReplaceTempView("vra")
air_cia.createOrReplaceTempView("air_cia")
aerodromos.createOrReplaceTempView("aerodromos")

# ============================================================================

# Criando views qtd_origem

query_2 = """
SELECT v.icao_aerodromo_origem as icao
,cia.razao_social
,count(*) as qtd_partida
FROM vra v

left join (SELECT icao, razao_social FROM air_cia) cia
on v.icao_empresa_aerea = cia.icao

where v.partida_real is not null

group by 1,2

"""
qtd_origem = spark.sql(query_2)

qtd_origem.createOrReplaceTempView("qtd_origem")

# ============================================================================

# Criando views qtd_destino

query_3 = """
SELECT v.icao_aerodromo_destino as icao
,cia.razao_social
,count(*) as qtd_destino
FROM vra v

left join (SELECT icao, razao_social FROM air_cia) cia
on v.icao_empresa_aerea = cia.icao

where v.chegada_real is not null

group by 1,2

"""

qtd_destino = spark.sql(query_3)

qtd_destino.createOrReplaceTempView("qtd_destino")

# ============================================================================

# juntando views de quantidade

query_4 = """
SELECT ifnull(ori.icao, des.icao) as icao
,ifnull(ori.razao_social, des.razao_social) as razao_social
,ifnull(ori.qtd_partida, 0) as qtd_partida
,ifnull(des.qtd_destino, 0) as qtd_destino
,ifnull(ori.qtd_partida, 0) + 
ifnull(des.qtd_destino, 0) as qtd_pousos_decolagens

FROM qtd_origem ori
full join qtd_destino des
on ori.icao = des.icao
"""

qtd_viagens = spark.sql(query_4)

qtd_viagens.createOrReplaceTempView("qtd_viagens")
# ============================================================================

# Colocando Razão social e Nome do Aeroporto

query_5 = """

select nome_aeroporto
,icao
,razao_social
,qtd_partida
,qtd_destino
,qtd_pousos_decolagens 
from

(

select cia.name as nome_aeroporto
,via.icao
,via.razao_social
,via.qtd_partida
,via.qtd_destino
,via.qtd_pousos_decolagens 
,row_number() over (partition by cia.name order by via.qtd_destino desc) as row

from qtd_viagens via

-- air_cia
left join (SELECT name, icao FROM aerodromos) cia
on via.icao = cia.icao

where via.razao_social is not null

)

where row = 1
"""

df = spark.sql(query_5)
# ============================================================================

# Criando view melhores_companhias_aereas

df.createOrReplaceTempView("melhores_companhias_aereas")
