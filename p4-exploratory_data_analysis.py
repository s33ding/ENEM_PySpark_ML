from pyspark.sql.types import *
import pandas as pd
import pyspark 
import os 
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
from shared_func import generate_html_report
from shared_func import add_section

# Start a Spark session
spark = SparkSession.builder.appName("s33ding").getOrCreate()

# Perform your data processing and analysis

# Generate HTML report
title = "PySpark Report"


## Start a Spark session
df = spark.read.csv("dataset/enem.csv", header=True, inferSchema=True)

#defining a problem
nmb_of_rows = df.count()

p1 = f"""Number of rows: {nmb_of_rows}"""
print(p1)


col_names = df.columns

df_dtypes = df.dtypes

p2 = f"""
    Data Types: 
    {df_dtypes}
"""

(df.columns)


#Visually Inspecting Data / EDA

#col_nm = ""
#df.describe([col_nm]).show()

#Mean
#pyspark.sql.functions.mean(col_nm)
#df.agg({col_nm: 'mean'}).collect()

#Skewness
#pyspark.sql.functions.skewness(col_nm)

#Minimum
#pyspark.sql.functions.min(col_nm)

#Covariance
#df.cov('SALESCLOSEPRICE', 'YEARBUILT')

#Correlation
#corr(col1, col2)

#df.sample(withReplacement=False, fraction=0.5, seed=42).count()
# Convert the sample to a Pandas DataFrame
#pandas_df = sample_df.toPandas()# Plot itsns.distplot(pandas_df)

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

df = df.withColumn("CO_MUNICIPIO_PROVA", regexp_replace(col("CO_MUNICIPIO_PROVA"), "\.0", ""))

df = df.drop("TP_Lingua")

lst_num_data = [
    "Idade_Calculada",
    "NOTA_CN_CIENCIAS_DA_NATUREZA",
    "NOTA_CH_CIENCIAS_HUMANAS", 
    "NOTA_LC_LINGUAGENS_E_CODIGOS",
    "NOTA_MT_MATEMATICA",
    "NOTA_REDACAO"
]

#rmv outliers
for my_col in lst_num_data:
    # Calculate values used for outlier filtering
    mean_val = df.agg({my_col: "mean"}).collect()[0][0]
    stddev_val = df.agg({my_col: "stddev"}).collect()[0][0]

    # Create three standard deviation (μ ± 3σ) lower and upper bounds for data
    low_bound = mean_val - (3 * stddev_val)
    hi_bound = mean_val + (3 * stddev_val)

    # Filter the data to fit between the lower and upper bounds
    df = df.where((df[my_col] < hi_bound) & (df[my_col] > low_bound))

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

df.coalesce(1).write.mode("overwrite").parquet("dataset/enem.parquet") 
df.show()

# Generate the final HTML report 
html_report = generate_html_report(title, content)
# Save the report to a file
with open("media/report.html", "w") as f:
    f.write(html_report)
