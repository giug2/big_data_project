#!/usr/bin/env python3
import sys
import re


def fascia_prezzo(price: str):
    try:
        price = float(price)
    except ValueError:
        return None
    if price >= 50000:
        return "alto"
    elif 20000 <= price < 50000:
        return "medio"
    else:
        return "basso"



for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    fields = line.split(',')
    if len(fields) < 5:
        continue

    city, daysonmarket, description, price, year = fields[0], fields[1], fields[2], fields[3], fields[4]

    try:
        daysonmarket_int = int(daysonmarket)
        price_tag = fascia_prezzo(price)
        if price_tag is None:
            continue

        # Tokenizzo e pulisco descrizione: solo parole a-z separate da virgola
        description = description.lower()
        words = re.findall(r'\b[a-z]+\b', description)
        description_clean = ",".join(words)

        print(f"{city}::{year}::{price_tag}\t1::{daysonmarket_int}::{description_clean}")

    except ValueError:
        continue
