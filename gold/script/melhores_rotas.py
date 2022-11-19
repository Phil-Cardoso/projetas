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

query_1 = """

select razao_social, name_origem, icao_aerodromo_origem, estado_uf_origem, name_destino, icao_aerodromo_destino, estado_uf_destino from (

-- Query para criar tabela com rank

SELECT * ,row_number() over (partition by razao_social order by qtd desc) as row FROM

(

-- Query para criar tabela com quantidades

SELECT a.razao_social 

-- Dados de origem

,ori.name_origem
,v.icao_aerodromo_origem
,ori.estado_uf_origem

-- Dados de destino

,des.name_destino
,v.icao_aerodromo_destino
,des.estado_uf_destino

-- Coluna de apoio
,count(concat(v.icao_empresa_aerea, '-', v.icao_aerodromo_origem, '-', v.icao_aerodromo_destino)) as qtd

-- VRA
FROM vra v

-- AIR_CIA
left join air_cia a
on v.icao_empresa_aerea = a.icao

-- aerodromos origem
left join (SELECT icao,  name as name_origem, concat(county, ' / ', country_iso) as estado_uf_origem FROM aerodromos) ori
on v.icao_aerodromo_origem = ori.icao

-- aerodromos destino
left join (SELECT icao,  name as name_destino, concat(county, ' / ', country_iso) as estado_uf_destino FROM aerodromos) des
on v.icao_aerodromo_origem = des.icao

where ori.name_origem is not null
and des.name_destino is not null

group by a.razao_social 
,ori.name_origem
,v.icao_aerodromo_origem
,ori.estado_uf_origem
,des.name_destino
,v.icao_aerodromo_destino
,des.estado_uf_destino

)

)

where row = 1
and razao_social is not null
"""

sqlDF = spark.sql(query_1)
# ============================================================================
# Criando view melhores_rotas
sqlDF.createOrReplaceTempView("melhores_rotas")
