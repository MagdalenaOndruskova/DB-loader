import pandas as pd

df = pd.read_csv('./jams.csv')

# Zistíme maximá pre číselné stĺpce
for col in ['id', 'jam_length', 'delay', 'speed_kmh', 'speed']:
    if col in df.columns:
        print(f"{col}: max = {df[col].max()} | min = {df[col].min()}")