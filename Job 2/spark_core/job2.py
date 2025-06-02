#!/usr/bin/env python3

from pyspark.sql import SparkSession
import os
import argparse


# Parsing degli argomenti
parser = argparse.ArgumentParser()
parser.add_argument("-input", type=str, help="Path del file di input")
parser.add_argument("-output", type=str, help="Path del file di output")
args = parser.parse_args()


# Funzione che mappa la fascia di prezzo
def categoria_prezzo(x):
    if x > 50000:
        return "alto"
    elif x > 20000:
        return "medio"
    else:
        return "basso"


# Funzione per trovare le 3 parole più frequenti
def top3(description: str):

    word_counts = {}
    for word in description.split():
        if len(word) > 0 and word.isalpha():
            word_counts[word] = word_counts.get(word, 0) + 1

    sorted_words = sorted(word_counts.items(), key=lambda x: (x[1]), reverse=True)
    top_3 = list(map(lambda x: x[0], sorted_words[:3]))

    return top_3


# Avvia sessione Spark
spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .appName("Job2_sparkcore") \
    .getOrCreate()

rdd = spark.sparkContext.textFile(args.input)


# Preprocessing RDD
# Mantengo solo i record con campo daysonmarket intero
processed_RDD = rdd \
    .map(f=lambda line: line.split(",")) \
    .filter(f=lambda x: x[1].isdigit()) \
    .map(f=lambda x: (x[0], int(x[1]), x[2], categoria_prezzo(float(x[7])), int(x[8]))) \
    .map(f=lambda x: ((x[0], x[4], x[3]), (1, x[1], x[2]))) \
    .reduceByKey(func=lambda a, b:(
        a[0] + b[0],        # somma contatori
        a[1] + b[1],        # somma giorni
        a[2] + b[2]         # concatena descrizioni
    )) \
    .map(lambda x: (
            x[0][0],                        # città
            x[0][1],                        # anno
            x[0][2],                        # categoria prezzo
            x[1][1],                        # numero auto
            round(x[1][1] / x[1][0], 2),    # avg giorni sul mercato
            top3(x[1][2])                   # top 3 parole
        )
    )


# Stampo 10 righe di output
for line in processed_RDD.take(10):
    print(line)


# Termina sessione Spark
spark.stop()