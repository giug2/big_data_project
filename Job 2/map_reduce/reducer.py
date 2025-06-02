#!/usr/bin/env python3

import sys


# Funzione per
def risultato(
    key: str, num_auto: int,
    tot_giorni: int, conta_parole: dict) -> None:
    
    # Ordino le parole per frequenza decrescente
    word_dict = dict(sorted(conta_parole.items(), key=lambda item: item[1], reverse=True))
    # Estrae le prime 3
    top3 = list(map(lambda x: x[0], word_dict.items()))[:3]

    if num_auto == 0:
        avg_giorni = 0
    else:
        avg_giorni = round(tot_giorni / num_auto, 2)

    key = key.replace("::","\t")

    print(f"{key}\t{num_auto}\t{avg_giorni}\t{top3}")



# Inizializza variabili
conta_parole = {}
current_key = None
num_auto = 0
tot_giorni = 0

# Scannerizza ogni record prodotto dal mapper
for line in sys.stdin:
    key, value = line.strip().split("\t")
    counter, giorni_mercato, descrizione = value.split("::", 2)

    if key != current_key:
        risultato(key, num_auto, tot_giorni, conta_parole)
        conta_parole = {}
        num_auto = 0
        tot_giorni = 0

    num_auto += int(counter)
    tot_giorni += int(giorni_mercato)

    descrizione = descrizione.strip("[]")
    words = descrizione.split(",") if descrizione else []

    for word in words:
        if word not in conta_parole:
            conta_parole[word] = 1
        else:
            conta_parole[word] += 1

    current_key = key