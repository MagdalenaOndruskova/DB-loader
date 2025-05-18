import requests
import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta
from pytz import timezone
import os

# Base API URL
jam_api_url = "https://gis.brno.cz/ags1/rest/services/Hosted/WazeJams/FeatureServer/0/"
event_api_url = "https://gis.brno.cz/ags1/rest/services/Hosted/WazeAlerts/FeatureServer/0/"

# Optional helper function (replace with actual logic if needed)
def fix_encoding(value):
    try:
        fixed = value.replace('Ã¡', 'á')
        fixed = fixed.replace('Ã\xad', 'í')
        fixed = fixed.replace('Åˆ', 'ň')
        fixed = fixed.replace('Ã½', 'ý')
        fixed = fixed.replace('Å™', 'ř')
        fixed = fixed.replace('Å¾', 'ž')
        fixed = fixed.replace('Ä�', 'č')
        fixed = fixed.replace('Å½', 'Ž')
        fixed = fixed.replace('Ã©', 'é')
        fixed = fixed.replace('Ä›', 'ě')
        fixed = fixed.replace('Å¡', 'š')
        fixed = fixed.replace('Å˜', 'Ř')
        fixed = fixed.replace('Å\xa0', 'Š')
        fixed = fixed.replace('ÄŒ', 'Č')
        fixed = fixed.replace('Å¯', 'ů')
        fixed = fixed.replace('Ãš', 'Ú')
        fixed = fixed.replace('Ãº', 'ú')
        fixed = fixed.replace('Ã¼º', 'ü')
        fixed = fixed.replace('Ã¼', 'ü')
        fixed = fixed.replace('Ã¶', 'ö')
        fixed = fixed.replace('Â»', '»')  # ď Ď ä

        return fixed
    except Exception:
        return ''


def reverse_encoding(value):
    try:
        fixed = value.replace('á', 'Ã¡')
        fixed = fixed.replace('í', 'Ã\xad')
        fixed = fixed.replace('ň', 'Åˆ')
        fixed = fixed.replace('ý', 'Ã½')
        fixed = fixed.replace('ř', 'Å™')
        fixed = fixed.replace('ž', 'Å¾')
        fixed = fixed.replace('č', 'Ä�')
        fixed = fixed.replace('Ž', 'Å½')
        fixed = fixed.replace('é', 'Ã©')
        fixed = fixed.replace('ě', 'Ä›')
        fixed = fixed.replace('š', 'Å¡')
        fixed = fixed.replace('Ř', 'Å˜')
        fixed = fixed.replace('Š', 'Å\xa0')
        fixed = fixed.replace('Č', 'ÄŒ')
        fixed = fixed.replace('ů', 'Å¯')
        fixed = fixed.replace('Ú', 'Ãš')
        fixed = fixed.replace('ú', 'Ãº')
        fixed = fixed.replace('ü', 'Ã¼º')
        fixed = fixed.replace('ü', 'Ã¼')
        fixed = fixed.replace('ö', 'Ã¶')
        fixed = fixed.replace('»', 'Â»')
        return fixed
    except Exception:
        return ''

def get_part_data(api_url, start_time, end_time, final_df, out_fields="*", out_streets=None):
    query = f"city='Brno' AND pubMillis > TIMESTAMP '{start_time}' AND pubMillis <= TIMESTAMP '{end_time}'"

    if out_streets:
        query_streets = ""
        for street in out_streets:
            street_encoded = reverse_encoding(street)
            if len(query_streets) < 1:
                query_streets = f"street='{street_encoded}'"
            else:
                query_streets = f"{query_streets} OR street='{street_encoded}'"
        query = f"{query} AND ({query_streets})"

    url = f"{api_url}query?where=({query})&outFields={out_fields}&outSR=4326&f=geojson"
    response = requests.get(url)

    if response.status_code == 200:
        content = response.content.decode('utf-8')
        gdf = gpd.read_file(content)
        gdf['pubMillis'] = pd.to_datetime(gdf['pubMillis'], unit='ms')

        # Convert to Bratislava timezone
        def localize_to_bratislava(timestamp):
            bratislava_tz = timezone('Europe/Bratislava')
            return timestamp.tz_localize('UTC').tz_convert(bratislava_tz)

        gdf['pubMillis'] = gdf['pubMillis'].apply(localize_to_bratislava)
        gdf['pubMillis'] = pd.to_datetime(gdf['pubMillis'].dt.strftime('%Y-%m-%d %H:%M:%S'))

        gdf['street'] = gdf.apply(lambda row: fix_encoding(row['street']), axis=1)

        if final_df is None:
            final_df = gdf
        else:
            final_df = pd.concat([final_df, gdf], ignore_index=True)
    return final_df


def get_data(from_time='2023-11-04', to_time='2023-11-10',
             api_url=jam_api_url, type_api="JAMS", out_fields=None, out_streets=None):
    final_df = None
    date_range = pd.date_range(start=pd.to_datetime(from_time), end=pd.to_datetime(to_time) + timedelta(days=1), freq='1D')

    if type_api == "JAMS" and not out_fields:
        out_fields = "*"
        # out_fields = "pubMillis,level,delay,speedKMH,length,street,blockingAlertUuid"
    if type_api == "ALERTS" and not out_fields:
        # out_fields = "pubMillis,subtype,street,type,latitude,longitude"
        out_fields = "*"
    #
    os.makedirs("data_jams", exist_ok=True)
    os.makedirs("data_alerts", exist_ok=True)

    for i in range(len(date_range) - 1):
        start_time = date_range[i] - timedelta(hours=2)  # offset for early events
        end_time = date_range[i + 1]
        day_str = date_range[i].strftime('%Y-%m-%d')
        print(f"Fetching data  for {day_str}...")

        daily_df = get_part_data(api_url, start_time, end_time, None, out_fields, out_streets)

        if daily_df is not None and not daily_df.empty:
            file_path = f"data_jams/{day_str}.geojson"
            daily_df.to_file(file_path, driver='GeoJSON')
            print(f"✅ Saved {len(daily_df)} records to {file_path}")
        else:
            print("⚠️ No data_jamms for this day.")

    print("✅ All done!")

# Example usage
# get_data(from_time="2025-04-26", to_time="2025-05-18", api_url=event_api_url, type_api="ALERTS")
get_data(from_time="2025-04-26", to_time="2025-05-18")

