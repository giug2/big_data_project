#!/usr/bin/env python3


import sys
import json


# Legge ogni record
for line in sys.stdin:
    city, year, categoria, num_cars, avg_giorni, lista_parole = line.strip().split("\t")

    word_counter = {}
    for word_count in lista_parole:
        obj = json.loads(word_count)