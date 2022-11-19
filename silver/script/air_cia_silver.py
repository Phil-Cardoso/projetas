from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.context import *
import tratar_texto

# ============================================================================

# Iniciando spark
spark = SparkSession.builder.appName("minhaAplicacao").getOrCreate()

# ============================================================================

# Carregar os dados de AIR_CIA
LOCATION = '/bronze/data/AIR_CIA/'

air_cia = spark.read.options(header='True', delimiter=';').csv(LOCATION)

# ============================================================================

# Pegando colunas da AIR_CIA
old_col_air_cia = air_cia.columns

# Normalizando nome das colunas
new_col_air_cia = tratar_texto.norm_cab(old_col_air_cia)

# Trocando o nome das colunas de AIR_CIA
for coluna in list(new_col_air_cia.keys()):
    air_cia = air_cia.withColumnRenamed(coluna, new_col_air_cia[coluna])

# ============================================================================

# Separar a coluna 'ICAO IATA' (icao_iata)
air_cia = air_cia.withColumn('apoio', split(col('icao_iata'), ' ')) \
    .withColumn('icao', col('apoio')[0]) \
    .withColumn('iata', col('apoio')[1]) \
    .drop('apoio', 'icao_iata')

# ============================================================================

# tratando colunas de data
air_cia = air_cia.withColumn('data_decisao_operacional', to_date(
    col("data_decisao_operacional"), "dd/MM/yyyy"))

# ============================================================================

# Salvando AIR_CIA tratado em formato parquet
LOCATION = '/silver/data/AIR_CIA_tratado/'
air_cia.write.mode('overwrite').parquet(LOCATION)
