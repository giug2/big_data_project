#!/usr/bin/env python3
import sys


def risultato(key: str, num_auto: int, tot_giorni: int, conta_parole: dict):
    if num_auto == 0:
        avg_giorni = 0
    else:
        avg_giorni = round(tot_giorni / num_auto, 2)

    top3 = sorted(conta_parole.items(), key=lambda x: x[1], reverse=True)[:3]
    top3_words = [word for word, count in top3]

    key_tab = key.replace("::", "\t")
    print(f"{key_tab}\t{num_auto}\t{avg_giorni}\t{','.join(top3_words)}")



conta_parole = {}
current_key = None
num_auto = 0
tot_giorni = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        key, value = line.split("\t")
        counter_str, giorni_mercato_str, descrizione = value.split("::", 2)
        counter = int(counter_str)
        giorni_mercato = int(giorni_mercato_str)
    except ValueError:
        continue

    if current_key and key != current_key:
        risultato(current_key, num_auto, tot_giorni, conta_parole)
        conta_parole = {}
        num_auto = 0
        tot_giorni = 0

    num_auto += counter
    tot_giorni += giorni_mercato

    # descrizione ora è parole separate da virgola, split facile
    words = descrizione.split(",") if descrizione else []

    for word in words:
        word = word.strip()
        if word:
            conta_parole[word] = conta_parole.get(word, 0) + 1

    current_key = key

# Stampa ultima chiave
if current_key:
    risultato(current_key, num_auto, tot_giorni, conta_parole)
