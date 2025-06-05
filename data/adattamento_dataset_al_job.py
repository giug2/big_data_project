import pandas as pd


# Percorso del dataset
dataset_csv = 'dataset_pulito.csv'

# Job1
colonne_da_estrarre1 = ['make_name', 'model_name', 'price', 'year']

# Processamento per chunks data la grandezza del dataset
chunks1 = pd.read_csv(dataset_csv, usecols=colonne_da_estrarre1, chunksize=10000)
lista_chunks1 = []


for chunk in chunks1:
    lista_chunks1.append(chunk)


# Creazione un dataset apposito per il primo job
nuovo_df1 = pd.concat(lista_chunks1)
nuovo_df1.to_csv("job1_dataset.csv", index=False)

print(f"\nDataset primo job creato")


# Job2
colonne_da_estrarre = ['city', 'daysonmarket', 'description', 'price', 'year']

# Processamento per chunks data la grandezza del dataset
chunks = pd.read_csv(dataset_csv, usecols=colonne_da_estrarre, chunksize=10000)
lista_chunks = []


for chunk in chunks:
    lista_chunks.append(chunk)


# Creazione un dataset apposito per il primo job
nuovo_df = pd.concat(lista_chunks)
nuovo_df.to_csv("job2_dataset.csv", index=False)

print(f"\nDataset primo job creato")