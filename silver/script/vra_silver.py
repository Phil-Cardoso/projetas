from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.context import *
import re
import tratar_texto
import unicodedata

# ============================================================================

# Iniciando spark
spark = SparkSession.builder.appName("minhaAplicacao").getOrCreate()

# ============================================================================

# Carregar os dados de VRA
LOCATION = '/bronze/data/VRA/'

vra = spark.read.format('json').load(LOCATION)

# ============================================================================

# Pegando colunas da VRA
old_col_vra = vra.columns

# regex para separar por letrar maiúscula
r = re.compile(r'[A-Z][a-z]+')

for x in range(len(old_col_vra)):

    # Tirando acento do texto

    coluna_antiga = old_col_vra[x]
    coluna_nova = ''.join(caracter for caracter in unicodedata.normalize('NFKD', coluna_antiga)
                          if not unicodedata.combining(caracter))

    # Tratando texto com ICAO
    if 'ICAO' in coluna_nova:

        coluna_nova = coluna_nova.replace('ICAO', '')

        coluna_nova = 'ICAO ' + ' '.join(r.findall(coluna_nova))

    # Tratando sem com ICAO
    else:

        coluna_nova = ' '.join(r.findall(coluna_nova))

    old_col_vra[x] = coluna_nova

# Normalizando nome das colunas
new_col_vra = tratar_texto.norm_cab(old_col_vra)

# Trocando o nome das colunas de vra
for x in range(len(vra.columns)):

    coluna_antiga = vra.columns[x]
    coluna_nova = list(new_col_vra.values())[x]

    vra = vra.withColumnRenamed(coluna_antiga, coluna_nova)

# ============================================================================

# tratando colunas de data

vra = vra.withColumn('chegada_prevista', to_timestamp(col('chegada_prevista'), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn('chegada_real', to_timestamp(col('chegada_real'), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn('partida_prevista', to_timestamp(col('partida_prevista'), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn('partida_real', to_timestamp(col('partida_real'), 'yyyy-MM-dd HH:mm:ss')) \

# ============================================================================

# Salvando VRA tratado em formato parquet
LOCATION = '/silver/data/VRA_tratado/'
vra.write.mode('overwrite').parquet(LOCATION)
