#!/usr/bin/env python3

from pyspark.sql import SparkSession
import argparse


# Parsing degli argomenti
parser = argparse.ArgumentParser()
parser.add_argument("-input", type=str, help="Path al file CSV di input")
#parser.add_argument("-output", type=str, help="Path del file di output")
args = parser.parse_args()


# Avvio sessione Spark
spark = SparkSession.builder \
    .appName("SparkCore_Job1") \
    .getOrCreate()

rdd = spark.sparkContext.textFile(args.input)


# Ottieni intestazione
header = rdd.first()


# Rimuovi intestazione
rdd_no_header = rdd.filter(lambda row: row != header)


# Elaborazione
processed_RDD = rdd_no_header \
    .map(lambda line: line.split(",")) \
    .filter(lambda x: len(x) == 4 and x[2] != '' and x[3] != '') \
    .map(lambda x: (
        (x[0], x[1]),  # make_name, model_name
        (1, float(x[2]), float(x[2]), float(x[2]), set([int(x[3])]))
    )) \
    .reduceByKey(lambda v1, v2: (
        v1[0] + v2[0],
        v1[1] + v2[1],
        min(v1[2], v2[2]),
        max(v1[3], v2[3]),
        v1[4].union(v2[4])
    )) \
    .map(lambda x: {
        "make_name": x[0][0],
        "model_name": x[0][1],
        "num_cars": x[1][0],
        "min_price": x[1][2],
        "max_price": x[1][3],
        "avg_price": round(x[1][1] / x[1][0], 2),
        "years": sorted(list(x[1][4]))
    })


# Stampa i primi 10 risultati
for row in processed_RDD.take(10):
    print(row)


spark.stop()
