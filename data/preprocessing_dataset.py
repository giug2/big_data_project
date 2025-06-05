import pandas as pd


# Percorso del dataset
dataset_csv = 'used_cars_data.csv'
colonne_da_estrarre = ['make_name', 'model_name', 'price', 'year', 'city', 'daysonmarket', 'description']


# Legge solo le colonne selezionate
df = pd.read_csv(dataset_csv, usecols=colonne_da_estrarre)


# Salva numero iniziale di righe
n_totale = len(df)


# Valori NULL
nulli = df.isnull().sum()
tot_nulli = df.isnull().any(axis=1).sum()
print("Valori nulli per colonna:\n", nulli)
print(f"Righe con almeno un valore nullo: {tot_nulli} ({tot_nulli/n_totale:.2%})")


# Rimuove righe con valori nulli
df.dropna(subset=colonne_da_estrarre, inplace=True)


# Stringhe vuote
mask_vuote = (df['make_name'].str.strip() == '') | (df['model_name'].str.strip() == '')
righe_vuote = mask_vuote.sum()
print(f"Righe con campi 'make_name' o 'model_name' vuoti: {righe_vuote} ({righe_vuote/n_totale:.2%})")

df = df[~mask_vuote]


# Prezzi <= 0
prezzi_invalidi = (df['price'] <= 0).sum()
print(f"Righe con prezzo <= 0: {prezzi_invalidi} ({prezzi_invalidi/n_totale:.2%})")

df = df[df['price'] > 0]

# Anni fuori range
anni_invalidi = ((df['year'] < 1950) | (df['year'] > 2025)).sum()
print(f"Righe con anni non realistici: {anni_invalidi} ({anni_invalidi/n_totale:.2%})")

df = df[(df['year'] >= 1950) & (df['year'] <= 2025)]

q1 = df['price'].quantile(0.25)
q3 = df['price'].quantile(0.75)
iqr = q3 - q1
lim_inf = q1 - 1.5 * iqr
lim_sup = q3 + 1.5 * iqr

n_outlier = ((df['price'] < lim_inf) | (df['price'] > lim_sup)).sum()
print(f"Outlier di prezzo rilevati (ma non rimossi): {n_outlier} ({n_outlier/len(df):.2%})")

# Salvataggio finale
df.reset_index(drop=True, inplace=True)
df.to_csv("dataset_pulito.csv", index=False)

print(f"\nPulizia completata. Righe finali: {len(df)} (da {n_totale})")
