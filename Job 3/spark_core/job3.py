#!/usr/bin/env python3

from pyspark.sql import SparkSession
import os
import argparse


# Parsing degli argomenti
parser = argparse.ArgumentParser()
parser.add_argument("-input", type=str, help="Path del file di input")
parser.add_argument("-output", type=str, help="Path del file di output")
args = parser.parse_args()


# Avvia sessione Spark
spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .appName("Job3_sparkcore") \
    .getOrCreate()

rdd = spark.sparkContext.textFile(args.input)


# Parsing
def parsing(line):
    try:
        fields = line.split(",")
        modello = fields[46]
        cavalli = float(fields[28])
        disp = float(fields[15])
        prezzo = float(fields[49])
        if modello and cavalli > 0 and disp > 0 and prezzo > 0:
            return (modello, cavalli, disp, prezzo)
    except:
        pass
    return None

data = rdd.map(parsing).filter(lambda x: x is not None)


# Raggruppamento per celle
def crea_celle(record):
    modello, cavalli, disp, prezzo = record
    cavalli_bin = int(cavalli / 10)
    disp_bin = int(disp / 0.1)
    return ((cavalli_bin, disp_bin), [(modello, cavalli, disp, prezzo)])


# Raggruppa in bin e unisci liste
binned = data.map(crea_celle).reduceByKey(lambda a, b: a + b)


# Funzione per trovare gruppi simili all'interno di ogni bin
def trovo_familiarita(group):
    _, cars = group
    res = []

    for i in range(len(cars)):
        base = cars[i]
        modello_i, cavalli_i, disp_i, prezzo_i = base
        group_members = []

        for j in range(len(cars)):
            modello_j, cavalli_j, disp_j, prezzo_j = cars[j]
            if abs(cavalli_i - cavalli_j)/cavalli_i <= 0.1 and abs(disp_i - disp_j)/disp_i <= 0.1:
                group_members.append((modello_j, cavalli_j, disp_j, prezzo_j))

        if group_members:
            prezzos = [x[3] for x in group_members]
            prezzo_medio = round(sum(prezzos) / len(prezzos), 2)
            modello_max_potenza = max(group_members, key=lambda x: x[1])[0]
            res.append((modello_i, len(group_members), prezzo_medio, modello_max_potenza))

    return res


# Applica per ogni bin e unisci risultati
risultati = binned.flatMap(trovo_familiarita)


# Rimuovi duplicati per modellolo
uq_res = risultati.reduceByKey(lambda a, b: a if a[1] >= b[1] else b)

# Mostra i primi 10 risultati
for row in uq_res.take(10):
    print(row)
