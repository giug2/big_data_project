import pandas as pd

# Percorso al tuo dataset
dataset_path = 'Job 2\job2_dataset.csv'

# Carica il dataset
df = pd.read_csv(dataset_path)

# Shuffle per rimescolare i dati prima di fare le divisioni (opzionale)
df = df.sample(frac=1, random_state=42).reset_index(drop=True)

# Percentuali di benchmark
percentuali = [0.1, 0.3, 0.5, 0.7]

# Salva ogni benchmark in un file separato
for p in percentuali:
    benchmark = df.iloc[:int(len(df) * p)]
    filename = f'benchmark_{int(p*100)}.csv'
    benchmark.to_csv(filename, index=False)
    print(f'Creato {filename} con {len(benchmark)} righe')

