{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 48,
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql.types import *\n",
    "import pandas as pd\n",
    "import pyspark \n",
    "import os \n",
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.functions import regexp_replace, col, udf, rank, asc, sum as spark_sum\n",
    "from pyspark.sql import functions as F\n",
    "import matplotlib.pyplot as plt\n",
    "import seaborn as sns\n",
    "from pyspark.sql.window import Window\n",
    "\n",
    "from pyspark.ml.feature import StringIndexer, VectorAssembler\n",
    "from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, GBTClassifier\n",
    "from pyspark.ml import Pipeline\n",
    "from pyspark.ml.stat import ChiSquareTest\n",
    "from pyspark.sql import Row"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 49,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[('NU_ANO', 'int'),\n",
       " ('NU_INSCRICAO', 'bigint'),\n",
       " ('TP_FAIXA_ETARIA', 'string'),\n",
       " ('Idade_Calculada', 'int'),\n",
       " ('TP_SEXO', 'string'),\n",
       " ('TP_ESTADO_CIVIL', 'string'),\n",
       " ('TP_COR_RACA', 'string'),\n",
       " ('TP_NACIONALIDADE', 'string'),\n",
       " ('TP_ST_CONCLUSAO', 'string'),\n",
       " ('TP_ANO_CONCLUIU', 'string'),\n",
       " ('TP_ESCOLA', 'string'),\n",
       " ('TP_ENSINO', 'string'),\n",
       " ('TP_DEPENDENCIA_ADM_ESC', 'string'),\n",
       " ('TP_LOCALIZACAO_ESC', 'string'),\n",
       " ('TP_SIT_FUNC_ESC', 'string'),\n",
       " ('CO_UF_PROVA', 'int'),\n",
       " ('SG_UF_PROVA', 'string'),\n",
       " ('CO_MUNICIPIO_PROVA', 'string'),\n",
       " ('NO_MUNICIPIO_PROVA', 'string'),\n",
       " ('NOTA_CN_CIENCIAS_DA_NATUREZA', 'float'),\n",
       " ('NOTA_CH_CIENCIAS_HUMANAS', 'float'),\n",
       " ('NOTA_LC_LINGUAGENS_E_CODIGOS', 'float'),\n",
       " ('NOTA_MT_MATEMATICA', 'float'),\n",
       " ('NOTA_REDACAO', 'float'),\n",
       " ('NOTA_MEDIA_5_NOTAS', 'double')]"
      ]
     },
     "execution_count": 49,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Start a Spark session\n",
    "spark = SparkSession.builder.appName(\"s33ding\").getOrCreate()\n",
    "# Read the Parquet file into a DataFrame\n",
    "df = spark.read.parquet(\"dataset/enem.parquet\")\n",
    "df.dtypes"
   ]
  },
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "CATEGORICAL"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 46,
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "Exception in thread \"serve-DataFrame\" java.net.SocketTimeoutException: Accept timed out\n",
      "\tat java.base/sun.nio.ch.NioSocketImpl.timedAccept(NioSocketImpl.java:708)\n",
      "\tat java.base/sun.nio.ch.NioSocketImpl.accept(NioSocketImpl.java:752)\n",
      "\tat java.base/java.net.ServerSocket.implAccept(ServerSocket.java:675)\n",
      "\tat java.base/java.net.ServerSocket.platformImplAccept(ServerSocket.java:641)\n",
      "\tat java.base/java.net.ServerSocket.implAccept(ServerSocket.java:617)\n",
      "\tat java.base/java.net.ServerSocket.implAccept(ServerSocket.java:574)\n",
      "\tat java.base/java.net.ServerSocket.accept(ServerSocket.java:532)\n",
      "\tat org.apache.spark.security.SocketAuthServer$$anon$1.run(SocketAuthServer.scala:65)\n",
      "23/06/16 22:46:13 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.\n",
      "23/06/16 22:46:13 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.\n",
      "23/06/16 22:46:13 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+------+----------------+------------------+----+\n",
      "|              column|pValue|degreesOfFreedom|        statistics|rank|\n",
      "+--------------------+------+----------------+------------------+----+\n",
      "|  CO_MUNICIPIO_PROVA|   0.0|          8730.0|112434.56683746527|   1|\n",
      "|         SG_UF_PROVA|   0.0|           130.0| 61781.07546856713|   2|\n",
      "|TP_DEPENDENCIA_AD...|   0.0|            20.0|12340.469420002684|   3|\n",
      "|           TP_ESCOLA|   0.0|            10.0|11449.389923838318|   4|\n",
      "|     TP_FAIXA_ETARIA|   0.0|            40.0| 7474.192419265729|   5|\n",
      "|  TP_LOCALIZACAO_ESC|   0.0|            10.0| 5037.361454483375|   6|\n",
      "|     TP_SIT_FUNC_ESC|   0.0|            20.0| 4771.274784582533|   7|\n",
      "|    TP_NACIONALIDADE|   0.0|            20.0|4064.5254548267576|   8|\n",
      "|           TP_ENSINO|   0.0|            10.0| 3495.915659554101|   9|\n",
      "|     TP_ANO_CONCLUIU|   0.0|            75.0| 3441.480956706109|  10|\n",
      "|     TP_ST_CONCLUSAO|   0.0|            10.0|3189.8778349073636|  11|\n",
      "|     TP_ESTADO_CIVIL|   0.0|            20.0| 2636.916548478804|  12|\n",
      "|             TP_SEXO|   0.0|             5.0|259.53366622851814|  13|\n",
      "+--------------------+------+----------------+------------------+----+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "\n",
    "\n",
    "# Read the Parquet file into a DataFrame\n",
    "df = spark.read.parquet(\"dataset/enem.parquet\")\n",
    "df.dtypes\n",
    "results = []\n",
    "\n",
    "# Define a list with the names of the categorical columns\n",
    "categorical_columns = ['TP_FAIXA_ETARIA', 'TP_SEXO', 'TP_ESTADO_CIVIL', 'TP_NACIONALIDADE', \n",
    "                       'TP_ST_CONCLUSAO', 'TP_ANO_CONCLUIU', 'TP_ESCOLA', 'TP_ENSINO', \n",
    "                       'TP_DEPENDENCIA_ADM_ESC', 'TP_LOCALIZACAO_ESC', 'TP_SIT_FUNC_ESC', \n",
    "                       'SG_UF_PROVA', 'CO_MUNICIPIO_PROVA']\n",
    "\n",
    "# Define the target variable\n",
    "target = 'TP_COR_RACA'\n",
    "\n",
    "# Assuming 'df' is your DataFrame\n",
    "for column in categorical_columns:\n",
    "    # Convert the categorical variable and the target to numeric\n",
    "    indexer_target = StringIndexer(inputCol=target, outputCol=target+\"_index\")\n",
    "    df_indexed_target = indexer_target.fit(df).transform(df)\n",
    "\n",
    "    indexer_feature = StringIndexer(inputCol=column, outputCol=column+\"_index\")\n",
    "    df_indexed = indexer_feature.fit(df_indexed_target).transform(df_indexed_target)\n",
    "\n",
    "    # Conduct the chi-square test\n",
    "    assembler = VectorAssembler(\n",
    "        inputCols=[column+\"_index\"],\n",
    "        outputCol=\"features\")\n",
    "\n",
    "    df_assembled = assembler.transform(df_indexed)\n",
    "\n",
    "    r = ChiSquareTest.test(df_assembled, \"features\", target+\"_index\").head()\n",
    "\n",
    "    # Store the result in a list and convert numpy types to Python types\n",
    "    results.append((column, float(r.pValues[0]), float(r.degreesOfFreedom[0]), float(r.statistics[0])))\n",
    "\n",
    "    # Define the schema\n",
    "schema = StructType([\n",
    "    StructField(\"column\", StringType(), True),\n",
    "    StructField(\"pValue\", FloatType(), True),\n",
    "    StructField(\"degreesOfFreedom\", DoubleType(), True),\n",
    "    StructField(\"statistics\", DoubleType(), True)\n",
    "])\n",
    "\n",
    "# Convert the results to DataFrame\n",
    "df_results = spark.createDataFrame(results, schema)\n",
    "\n",
    "df_results.createOrReplaceTempView(\"chi_squared_results\")\n",
    "spark.sql(\"\"\"\n",
    "    SELECT *,\n",
    "    RANK() OVER (ORDER BY statistics DESC) as rank\n",
    "    FROM chi_squared_results\n",
    "\"\"\").show()\n",
    "\n",
    "df_results.write.mode(\"overwrite\").parquet(\"data_for_dashboards/featuares_categ_tp_cor_raca/\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 44,
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "('NOTA_MT_MATEMATICA', 0.5857740182655987)\n",
      "('NOTA_LC_LINGUAGENS_E_CODIGOS', 0.16058507931252256)\n",
      "('NOTA_CN_CIENCIAS_DA_NATUREZA', 0.1459015499100855)\n",
      "('NOTA_CH_CIENCIAS_HUMANAS', 0.10459298635425907)\n",
      "('NOTA_REDACAO', 0.0031463661575339553)\n"
     ]
    }
   ],
   "source": [
    "# Create SparkSession\n",
    "spark = SparkSession.builder.getOrCreate()\n",
    "\n",
    "# Read Parquet file into DataFrame\n",
    "df = spark.read.parquet(\"dataset/enem.parquet\")\n",
    "\n",
    "# Select relevant columns\n",
    "selected_cols = ['NOTA_CN_CIENCIAS_DA_NATUREZA', 'NOTA_CH_CIENCIAS_HUMANAS', 'NOTA_LC_LINGUAGENS_E_CODIGOS', 'NOTA_MT_MATEMATICA', 'NOTA_REDACAO', 'TP_COR_RACA']\n",
    "selected_df = df.select(selected_cols)\n",
    "\n",
    "# Handle missing values if necessary\n",
    "\n",
    "# Convert categorical column to numeric labels\n",
    "indexer = StringIndexer(inputCol='TP_COR_RACA', outputCol='label')\n",
    "indexed_df = indexer.fit(selected_df).transform(selected_df)\n",
    "\n",
    "# Assemble features into a vector\n",
    "assembler = VectorAssembler(inputCols=['NOTA_CN_CIENCIAS_DA_NATUREZA', 'NOTA_CH_CIENCIAS_HUMANAS', 'NOTA_LC_LINGUAGENS_E_CODIGOS', 'NOTA_MT_MATEMATICA', 'NOTA_REDACAO'], outputCol='features')\n",
    "assembled_df = assembler.transform(indexed_df)\n",
    "\n",
    "# Split data into training and testing sets\n",
    "train_data, test_data = assembled_df.randomSplit([0.7, 0.3], seed=42)\n",
    "\n",
    "# Train RandomForestClassifier model\n",
    "rf = RandomForestClassifier(labelCol='label', featuresCol='features')\n",
    "model = rf.fit(train_data)\n",
    "\n",
    "# Extract feature importances\n",
    "importances = model.featureImportances\n",
    "\n",
    "# Define the feature importances\n",
    "feature_importances = []\n",
    "# Print feature importances\n",
    "feature_importances = list(zip(['NOTA_CN_CIENCIAS_DA_NATUREZA', 'NOTA_CH_CIENCIAS_HUMANAS', 'NOTA_LC_LINGUAGENS_E_CODIGOS', 'NOTA_MT_MATEMATICA', 'NOTA_REDACAO'], importances))\n",
    "feature_importances.sort(key=lambda x: x[1], reverse=True)\n",
    "for feature, importance in feature_importances:\n",
    "    res = (feature, importance)\n",
    "    print(res)\n",
    "\n",
    "df = pd.DataFrame(feature_importances, columns=[\"Features\",\"Importance\"])\n",
    "df.to_parquet('data_for_dashboards/featuares_num_tp_cor_raca/tp_cor_raca.parquet')"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.3"
  },
  "orig_nbformat": 4
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
