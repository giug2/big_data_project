import os
import time
import subprocess
import matplotlib.pyplot as plt


# Inizializzazione delle tecnologie e dei benchmark
tools = ["map_reduce", "spark_core", "hive"]
fractions = [0.1, 0.3, 0.5, 0.7, 1.0]
fraction_labels = [f"{int(f * 100)}%" for f in fractions]


# Creazione cartella per log e grafico
os.makedirs("logs", exist_ok=True)

execution_data = {tool: [] for tool in tools}


for tool in tools:
    print(f"\n--- Esecuzione per {tool} in Job 2 ---")

    for f, label in zip(fractions, fraction_labels):
        dataset_name = f"data-{label}" if f < 1.0 else "data_cleaned"
        print(f"Eseguo {tool} Job 2 su dataset: {dataset_name}")

        script_path = os.path.join(tool, "run.sh")
#        if not os.path.isfile(script_path):
#            print(f"Script non trovato: {script_path}")
#            execution_data[tool].append(None)
#            continue

        start = time.time()
        process = subprocess.run(
            ["bash", "run.sh", dataset_name],
            cwd=os.path.join(tool),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        end = time.time()

        elapsed = end - start
        execution_data[tool].append(elapsed)

        # Salvataggio log
        log_dir = os.path.join("logs", tool)
        os.makedirs(log_dir, exist_ok=True)

        with open(os.path.join(log_dir, f"stdout-{label}.txt"), "wb") as f_out:
            f_out.write(process.stdout)
        with open(os.path.join(log_dir, f"stderr-{label}.txt"), "wb") as f_err:
            f_err.write(process.stderr)

        print(f"Completato in {elapsed:.2f} secondi")

# Plot dei risultati
plt.figure(figsize=(10, 6))
colors = {
    "map_reduce": "red",
    "spark_core": "blue",
    "hive": "green"
}

x_pos = list(range(len(fractions)))
for tool in tools:
    if any(t is None for t in execution_data[tool]):
        continue  
    plt.plot(
        x_pos,
        execution_data[tool],
        marker="x",
        label=tool,
        color=colors[tool]
    )

plt.xticks(x_pos, fraction_labels)
plt.xlabel("Frazione del dataset")
plt.ylabel("Tempo di esecuzione (s)")
plt.title(f"Benchmark per Job 2")
plt.grid(True)
plt.legend()
plt.tight_layout()

graph_path = os.path.join("logs", f"benchmark_job2.png")
plt.savefig(graph_path, dpi=300)
print(f"Grafico salvato in {graph_path}")
