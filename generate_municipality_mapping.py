# generate_municipality_mapping.py
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Scrape Wikipedia for Danish municipalities
url = "https://en.wikipedia.org/wiki/List_of_municipalities_of_Denmark"
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

# Find the municipalities table
table = soup.find('table', class_='wikitable')
df = pd.read_html(str(table))[0]

# Create mapping dictionary
municipality_map = {}
for _, row in df.iterrows():
    code = row['Code']
    name_en = row['Municipality']
    name_da = row['Danish name']
    region = row['Region']
    
    municipality_map[code] = {
        'municipality_name': name_en,
        'municipality_name_danish': name_da,
        'region_name': region
    }

# Save to CSV for dbt seed
mapping_df = pd.DataFrame.from_dict(municipality_map, orient='index')
mapping_df.to_csv('seeds/municipality_region_mapping.csv')