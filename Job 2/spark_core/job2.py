#!/usr/bin/env python3

from pyspark.sql import SparkSession
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
# Funzione per trovare le 3 parole più frequenti, escludendo le stopwords
def top3(description: str):
    stopwords = {
        "the", "and", "with", "for", "from", "this", "that", "your", "you", "all", "are"
    }
    word_counts = {}
    for word in description.split():
        word = word.lower()
        if len(word) > 1 and word.isalpha() and word not in stopwords:
            word_counts[word] = word_counts.get(word, 0) + 1
    sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
    return [word for word, count in sorted_words[:3]]



# Avvia sessione Spark
spark = SparkSession.builder \
    .appName("Job2_sparkcore") \
    .getOrCreate()

rdd = spark.sparkContext.textFile(args.input)


# Rimuovi intestazione
header = rdd.first()
rdd_no_header = rdd.filter(lambda row: row != header)


# Preprocessing RDD
processed_RDD = rdd_no_header \
    .map(lambda line: line.split(",")) \
    .filter(lambda x: len(x) == 5 and x[1].isdigit() and x[3] != '' and x[4].isdigit()) \
    .map(lambda x: (
        x[0],                             # city
        int(x[1]),                        # daysonmarket
        x[2],                             # description
        categoria_prezzo(float(x[3])),   # prezzo -> categoria
        int(x[4])                         # year
    )) \
    .map(lambda x: (
        (x[0], x[4], x[3]),              # chiave: (city, year, categoria prezzo)
        (1, x[1], x[2])                  # valore: (conteggio, daysonmarket, descrizione)
    )) \
    .reduceByKey(lambda a, b: (
        a[0] + b[0],                     # totale auto
        a[1] + b[1],                     # totale giorni
        a[2] + " " + b[2]                # concatena descrizioni
    )) \
    .map(lambda x: (
        x[0][0],                         # city
        x[0][1],                         # year
        x[0][2],                         # categoria prezzo
        x[1][0],                         # numero auto
        round(x[1][1] / x[1][0], 2),     # avg daysonmarket
        top3(x[1][2])                    # top 3 parole
    ))

# Stampa 10 righe
for line in processed_RDD.take(10):
    print(line)

# Ferma Spark
spark.stop()
