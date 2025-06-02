#!/usr/bin/env python3


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, avg, count, collect_set
from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField
import argparse


# Parsing degli argomenti
parser = argparse.ArgumentParser()
parser.add_argument("-input", type=str, help="Path del file di input")
parser.add_argument("-output", type=str, help="Path del file di output")
args = parser.parse_args()


# Avvia sessione Spark
spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .appName("Job3_sparksql") \
    .getOrCreate()


# Definisce lo schema del dataset
schema = StructType([
    StructField(name="model_name", dataType=StringType(), nullable=True),
    StructField(name="horsepower", dataType=DoubleType(), nullable=True),
    StructField(name="engine_displacement", dataType=DoubleType(), nullable=True),
    StructField(name="price", dataType=DoubleType(), nullable=True)
])


# Lettura del dataset con lo schema definito
df = spark.read \
    .csv(args.input, schema=schema) \
    .select("make_name", "horsepower", "price", "engine_displacement") \
    .createOrReplaceTempView("job3_dataset")


# Query
query = """
SELECT
    a.model_name AS modello_base,
    b.model_name AS modello_simile,
    b.horsepower,
    b.engine_displacement,
    b.price
FROM cars a
JOIN cars b
ON abs(a.horsepower - b.horsepower) / a.horsepower <= 0.1
AND abs(a.engine_displacement - b.engine_displacement) / a.engine_displacement <= 0.1
"""


# Esegue la query
gruppi = spark.sql(query)
gruppi.createOrReplaceTempView("gruppi")

# Calcola statistiche
result = spark.sql("""
SELECT
    modello_base,
    COUNT(DISTINCT modello_simile) AS modelli_simili,
    ROUND(AVG(price), 2) AS prezzo_medio_gruppo,
    FIRST(modello_simile) KEEP (DENSE_RANK FIRST ORDER BY horsepower DESC) AS modello_top_potenza
FROM gruppi
GROUP BY modello_base
ORDER BY prezzo_medio_gruppo DESC
""")

# Mostra i risultati
result.show(10, truncate=False)
