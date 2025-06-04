#!/usr/bin/env python3

import sys
from collections import defaultdict


# Inizializzazione delle variabili
current_key = None
count = 0
total_price = 0.0
min_price = float('inf')
max_price = float('-inf')
years = set()


# Funzione per calcolare le statistiche richieste
def stats():
    if current_key:
        avg_price = total_price / count
        years_list = ','.join(sorted(years))
        print(f"{current_key}\tNumero totale auto: {count}, Prezzo minimo: {min_price}, Prezzo massimo: {max_price}, Prezzo medio: {avg_price:.2f}, Anni: [{years_list}]")


# Scansione di ogni record del mapper
for line in sys.stdin:
    try:
        key, value = line.strip().split("\t1")
        price_str, year = value.split("\t")
        price = float(price_str)

        if key != current_key and current_key is not None:
            stats()
            count = 0
            total_price = 0.0
            min_price = float('inf')
            max_price = float('-inf')
            years = set()

        # Aggiornamento delle statistiche
        current_key = key
        count += 1
        total_price += price
        min_price = min(min_price, price)
        max_price = max(max_price, price)
        years.add(year)

    except Exception:
        continue

# Per l'ultimo blocco
stats()  
