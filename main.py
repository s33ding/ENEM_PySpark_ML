import pandas as pd
import pyspark 
import os 
from pyspark.sql import SparkSession



#pandasDF = pd.read_excel("dataset/enem.xlsx")

## Start a Spark session
#spark = SparkSession.builder.appName("s33ding").getOrCreate()
#
#df = spark.createDataFrame(pandasDF)
#df.show()
df  = pd.read_excel("dataset/enem.xlsx")
