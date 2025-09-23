import requests
import pandas as pd

# Identifiant de la station (Sherbrooke)
station_id = 48371

# Années à télécharger
annees = range(2019, 2026)

# Liste pour stocker les DataFrames
dataframes = []

for annee in annees:
    # Construire l'URL du CSV
    url = f"https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={station_id}&Year={annee}&Month=1&Day=1&timeframe=2&submit=Download+Data"
    
    print(f"Téléchargement des données pour {annee}...")
    
    # Lire le CSV depuis l’URL
    df = pd.read_csv(url, skiprows=0)
    
    # Ajouter à la liste
    dataframes.append(df)

# Fusionner tous les DataFrames en un seul
df_final = pd.concat(dataframes, ignore_index=True)

# Sauvegarder dans un fichier CSV unique
df_final.to_csv("sherbrooke_meteo_2019_2025.csv", index=False)

print("Fichier créé : sherbrooke_meteo_2019_2025.csv")
