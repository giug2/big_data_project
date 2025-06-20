#!/usr/bin/env python3


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, avg, count, collect_set
from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField
import argparse


# Parsing degli argomenti
parser = argparse.ArgumentParser()
parser.add_argument("-input", type=str, help="Path del file di input")
#parser.add_argument("-output", type=str, help="Path del file di output")
args = parser.parse_args()


# Avvia sessione Spark
spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .appName("Job1_sparksql") \
    .getOrCreate()


# Definisce lo schema del dataset
schema = StructType([
    StructField(name="make_name", dataType=StringType(), nullable=True),
    StructField(name="model_name", dataType=StringType(), nullable=True),
    StructField(name="price", dataType=DoubleType(), nullable=True),
    StructField(name="year", dataType=IntegerType(), nullable=True)
])


# Lettura del dataset in formato CSV
df = spark.read \
    .csv(args.input, schema=schema) \
    .select("make_name", "model_name", "price", "year") \
    .createOrReplaceTempView("job1_dataset")


# Query Spark SQL
query = """
SELECT
    make_name,
    model_name,
    COUNT(*) AS numero_auto,
    MIN(price) AS prezzo_minimo,
    MAX(price) AS prezzo_massimo,
    ROUND(AVG(price), 2) AS prezzo_medio,
    COLLECT_SET(year) AS anni_presenti
FROM job1_dataset
GROUP BY make_name, model_name
ORDER BY make_name, model_name
"""


# Esegue la query
model_stats = spark.sql(query)
model_stats.createOrReplaceTempView("model_statistics")


# Visualizza la lista di anni in una stringa separandoli con una virgola
model_stats = model_stats \
    .withColumn("anni_presenti", concat_ws(",", col("anni_presenti")))


# Mostra le prime 10 righe
model_stats.show(10, truncate=False)


# Termina la sessione Spark
spark.stop()