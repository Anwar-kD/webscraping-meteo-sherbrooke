####################################################################
#CRÉATEUR : ANWAR OUKRID
#DATE_DE_CREATION : 25-09-2025
#DATE_DE_MODIFICATION : 07-10-2025
####################################################################

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import random, time

# Configuration
station_id = "48371"  # Sherbrooke
base_url = "https://climate.weather.gc.ca/climate_data/hourly_data_e.html"

# Headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

weather_data = []

# Fonction pour scraper une journée
def scrape_day(date):
    url = f"{base_url}?StationID={station_id}&Year={date.year}&Month={date.month}&Day={date.day}&timeframe=1"
    print(f"Scraping {date.strftime('%Y-%m-%d')}")
    day_data = []

    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')

        table = soup.find('table', class_='table')
        if table:
            rows = table.find_all('tr')[1:]  # Skip header
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 3:
                    time_cell = cells[0].get_text().strip()
                    temp_cell = cells[1].get_text().strip()
                    try:
                        temp = float(temp_cell)
                        day_data.append({
                            'date': date.strftime('%Y-%m-%d'),
                            'time': time_cell,
                            'temperature': temp
                        })
                    except:
                        pass

        # Petit délai aléatoire pour éviter surcharge
        time.sleep(random.uniform(0.5, 1.5))

    except Exception as e:
        print(f"Erreur pour {date}: {e}")

    return day_data


# Générer les dates (107 derniers jours)
dates = [datetime.now() - timedelta(days=i) for i in range(107)]

# Exécuter en parallèle
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(scrape_day, d) for d in dates]
    for future in as_completed(futures):
        weather_data.extend(future.result())

# Sauvegarder
if weather_data:
    df = pd.DataFrame(weather_data)
    df.to_csv('sherbrooke_summer_hourly_weather2025.csv', index=False)
    print(f"Données sauvegardées: {len(weather_data)} entrées")
    print(df.head())
else:
    print("Aucune donnée récupérée")
