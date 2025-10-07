####################################################################
#CRÉATEUR : TOMMY LARIVIERE
#DATE_DE_CREATION : 25-09-2025
#DATE_DE_MODIFICATION : 06-10-2025
####################################################################

import pandas as pd
from pymongo import MongoClient


# Lire tous les CSV avec le bon séparateur
df_all = pd.read_csv("sherbrooke_summer_hourly_weather2025.csv", sep=',', encoding='utf-8-sig')


# Remplacer les valeurs manquantes ou symboles d'erreur ('M', 'LegendMM', '') par None 
# pour assurer la compatibilité avec MongoDB et éviter les erreurs lors de l'insertion
df_all.replace(['M', 'LegendMM', ''], None, inplace=True)

# Convertir en dictionnaires
data = df_all.to_dict(orient='records')

# Connexion à MongoDB Atlas
client = MongoClient("mongodb+srv://anwar:mimimomo01@projet2.vcv1jsv.mongodb.net/")
db = client["meteo_db"]
collection = db["sherbrooke"]

# Insérer toutes les données
collection.insert_many(data)

print(f"{len(data)} documents importés dans MongoDB !")


