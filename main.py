from pyspark.sql.types import *
import pandas as pd
import pyspark 
import os 
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col

# Start a Spark session
spark = SparkSession.builder.appName("s33ding").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("NU_ANO", IntegerType()),
    StructField("NU_INSCRICAO", StringType()),
    StructField("TP_FAIXA_ETARIA", StringType()),
    StructField("Idade_Calculada", IntegerType()),
    StructField("TP_SEXO", StringType()),
    StructField("TP_ESTADO_CIVIL", StringType()),
    StructField("TP_COR_RACA", StringType()),
    StructField("TP_NACIONALIDADE", StringType()),
    StructField("TP_ST_CONCLUSAO", StringType()),
    StructField("TP_ANO_CONCLUIU",StringType()),
    StructField("TP_ESCOLA", StringType()),
    StructField("TP_ENSINO", StringType()),
    StructField("TP_DEPENDENCIA_ADM_ESC", StringType()),
    StructField("TP_LOCALIZACAO_ESC", StringType()),
    StructField("TP_SIT_FUNC_ESC", StringType()),
    StructField("CO_UF_PROVA", IntegerType()),
    StructField("SG_UF_PROVA", StringType()),
    StructField("CO_MUNICIPIO_PROVA", IntegerType()),
    StructField("NO_MUNICIPIO_PROVA", StringType()),
    StructField("NOTA_CN_CIENCIAS_DA_NATUREZA", DoubleType()),
    StructField("NOTA_CH_CIENCIAS_HUMANAS", DoubleType()),
    StructField("NOTA_LC_LINGUAGENS_E_CODIGOS", DoubleType()),
    StructField("NOTA_MT_MATEMATICA", DoubleType()),
    StructField("NOTA_REDACAO", DoubleType()),
    StructField("NOTA_MEDIA_5_NOTAS", DoubleType()),
    StructField("TP_Lingua", IntegerType())
])

## Start a Spark session
df = spark.read.csv("dataset/enem.csv", header=True, inferSchema=True)

cols_float = [
    "NOTA_CN_CIENCIAS_DA_NATUREZA", 
    "NOTA_CH_CIENCIAS_HUMANAS", 
    "NOTA_LC_LINGUAGENS_E_CODIGOS", 
    "NOTA_MT_MATEMATICA", 
    "NOTA_REDACAO", 
    "NOTA_MEDIA_5_NOTAS"
]

# Clean the 'NU_ANO' column by removing leading zeros
cols_int = ["NU_ANO","CO_UF_PROVA"]

for col_nm in cols_int:
    df = df.withColumn(col_nm, regexp_replace(col(col_nm), "^0+", ""))
    df = df.withColumn(col_nm, df[col_nm].cast(IntegerType()))

for col_nm in cols_float:
    df = df.withColumn(col_nm, df[col_nm].cast(IntegerType()))

df = df.drop("TP_Lingua")
df.write.parquet("dataset/enem.parquet") 

df.show()
