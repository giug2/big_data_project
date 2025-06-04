#!/usr/bin/env python3

import sys


# Mappa l'auto in base alla fascia di price
def fascia_prezzo(price: str):
    price = float(price)

    if price >= 50000:
        return "alto"
    elif price >= 20000 and price < 50000:
        return "medio"
    else:
        return "basso"


# Per ogni record del csv prende i campi di interesse
for line in sys.stdin:
    city, daysonmarket, description, price, year = line.strip().split(",")

    try:
        daysonmarket = int(daysonmarket)
        fascia_prezzo = fascia_prezzo(price)
        description = ",".join(description.split())

        # Stampa i record 
        print(f"{city}::{year}::{fascia_prezzo}\t1::{daysonmarket}::[{description}]")
    except ValueError:
        continue