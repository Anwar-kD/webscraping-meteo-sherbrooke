####################################################################
#CRÉATEUR : BRAHIM DAHOU
#DATE_DE_CREATION : 26-09-2025
#DATE_DE_MODIFICATION : 04-10-2025
####################################################################
from pymongo import MongoClient

# Connexion à MongoDB Atlas
client = MongoClient("mongodb+srv://anwar:mimimomo01@projet2.vcv1jsv.mongodb.net/")
db = client["meteo_db"]
collection = db["sherbrooke"]

#1. Trouver les températures les plus élevées (> 30°C)
result1 = collection.find(
    {"temperature": {"$gt": 30}},
    {"date": 1, "time": 1, "temperature": 1, "_id": 0}
).sort("temperature", -1)

print("Températures > 30°C:")
for doc in result1:
    print(doc)

#2. Données d'une date spécifique
#Toutes les données du 26 septembre 2025
result2 = collection.find(
    {"date": "2025-09-26"},
    {"time": 1, "temperature": 1, "_id": 0}
).sort("time", 1)

print("\nDonnées du 26 septembre 2025:")
for doc in result2:
    print(doc)

# 3. Trouver les heures les plus froides (< 8°C)
result3 = collection.find(
    {"temperature": {"$lt": 8}},
    {"date": 1, "time": 1, "temperature": 1, "_id": 0}
).sort("temperature", 1)

print("\nTempératures < 8°C:")
for doc in result3:
    print(doc)