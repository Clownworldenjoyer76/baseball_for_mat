import os
import pandas as pd
import unicodedata
import re

def strip_accents(text):
    if not isinstance(text, str):
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    return ''.join(c for c in normalized if not unicodedata.combining(c))

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = strip_accents(name)
    name = re.sub(r"[^\w\s,]", "", name)  # remove punctuation except comma
    parts = [part.strip().capitalize() for part in name.split(",")]
    if len(parts) == 2:
        return f"{parts[0]}, {parts[1]}"
    return name.strip().title()

def process_file(input_path, output_path):
    df = pd.read_csv(input_path)
    if "last_name, first_name" in df.columns:
        df["last_name, first_name"] = df["last_name, first_name"].apply(normalize_name)
        df.to_csv(output_path, index=False)

os.makedirs("data/normalized", exist_ok=True)

process_file("data/Data/batters.csv", "data/normalized/batters_normalized.csv")
process_file("data/Data/pitchers.csv", "data/normalized/pitchers_normalized.csv")