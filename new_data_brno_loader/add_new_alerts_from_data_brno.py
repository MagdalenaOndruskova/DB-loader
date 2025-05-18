import binascii
import glob
import geopandas as gpd
import pandas as pd
import os
from datetime import datetime
from shapely.wkb import dumps as wkb_dumps
import psycopg2

def ensure_alerts_columns(df_merged):
    required_columns = [
        'uuid', 'country', 'city', 'type', 'subtype', 'street',
        'report_rating', 'confidence', 'reliability', 'road_type',
        'magvar', 'report_by_municipality_user', 'report_description',
        'location', 'published_at', 'last_updated', 'active'
    ]

    if 'geometry' in df_merged.columns:
        # Previesť Point na WKB v HEX
        df_merged['location'] = df_merged['geometry'].apply(
            lambda geom: binascii.hexlify(wkb_dumps(geom, hex=False)).decode() if geom else None)
    else:
        df_merged['location'] = None

    for col in required_columns:
        if col not in df_merged.columns:
            if col == 'active':
                df_merged[col] = False
            elif col in ['report_rating', 'confidence', 'reliability', 'magvar']:
                df_merged[col] = 0
            elif col == 'report_by_municipality_user':
                df_merged[col] = False
            else:
                df_merged[col] = ''

    default_time = pd.Timestamp("2024-01-01 00:00:00")
    df_merged['published_at'] = pd.to_datetime(df_merged['published_at']).fillna(default_time)
    df_merged['last_updated'] = pd.to_datetime(df_merged['last_updated']).fillna(default_time)

    df_merged = df_merged[required_columns]
    return df_merged

def load_existing_alerts(csv_path):
    if os.path.exists(csv_path):
        df_existing = pd.read_csv(csv_path, parse_dates=['published_at', 'last_updated'])
    else:
        df_existing = pd.DataFrame()
    return df_existing

def save_alerts_to_csv(df_existing, df_new, csv_path):
    import os
    import pandas as pd

    # Skombinuj oba dataframe
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)

    if df_combined.empty:
        print("Žiadne alerty na uloženie.")
        return

    # Ulož ako nový súbor (prepíše existujúci)
    df_combined.to_csv(csv_path, index=False)
    print(f"Uložených {len(df_combined)} alertov do {csv_path}")

def process_csv(file_path, final_path):
    # Načítanie CSV súboru, dátumy načítame ako datetime
    df = pd.read_csv(file_path, parse_dates=['published_at', 'last_updated'])
    print(len(df))

    # Explicitne konvertujeme na datetime
    df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce')
    df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce')

    # Definovanie dátumu pre filter
    cutoff_date = pd.Timestamp('2024-04-01', tz='UTC')
    fill_date = pd.Timestamp('2024-01-01', tz='UTC')
    df['last_updated'] = df['last_updated'].fillna(fill_date)

    # Oprava chybných dátumov: kde last_updated < published_at
    mask = df['last_updated'] < df['published_at']
    df.loc[mask, 'last_updated'] = fill_date

    print(df['last_updated'].isna().sum())
    print("Celkový počet záznamov:", len(df))
    print("Počet záznamov s last_updated >= cutoff_date:", len(df[df['last_updated'] >= cutoff_date]))
    print("Počet záznamov s last_updated < cutoff_date:", len(df[df['last_updated'] < cutoff_date]))
    print("Súčet predchádzajúcich dvoch:",len(df[df['last_updated'] >= cutoff_date]) + len(df[df['last_updated'] < cutoff_date]))

    # Záznamy, kde last_updated >= cutoff_date (budeme z nich počítať priemernú duration)
    recent_records = df[df['last_updated'] >= cutoff_date].copy()

    # Vypočítame duration pre tieto záznamy ako rozdiel last_updated - published_at v sekundách
    recent_records['duration'] = (recent_records['last_updated'] - recent_records['published_at']).dt.total_seconds()

    # Priemerná dĺžka trvania v sekundách
    avg_duration_seconds = recent_records['duration'].mean()

    # Záznamy, kde last_updated < cutoff_date (tieto budeme upravovať)
    old_records = df[df['last_updated'] < cutoff_date].copy()

    # Prepiseme last_updated na published_at + avg_duration pre staré záznamy
    old_records['last_updated'] = old_records['published_at'] + pd.to_timedelta(avg_duration_seconds, unit='s')

    # Spojíme späť upravené staré a nezmenené nové záznamy
    result_df = pd.concat([old_records, recent_records.drop(columns=['duration'])], ignore_index=True)
    result_df.to_csv(final_path, index=False)
    print(len(result_df))
    return result_df


if __name__ == '__main__':
    csv_path = './alerts.csv'
    final_path = './alerts_updated.csv'
    process_csv(csv_path, final_path)

    df_existing = load_existing_alerts(final_path)
    df_existing['subtype'].fillna('', inplace=True)
    df_existing['report_description'].fillna('', inplace=True)
    df_existing['street'].fillna('', inplace=True)
    boolean_cols = ['report_by_municipality_user', 'active']
    for col in boolean_cols:
        df_existing[col] = df_existing[col].map({'f': False, 't': True})
    existing_uuids = set(df_existing['uuid']) if not df_existing.empty else set()

    start_date = datetime.strptime('2025-04-25', '%Y-%m-%d').date()
    end_date = datetime.strptime('2025-05-18', '%Y-%m-%d').date()

    file_list = glob.glob("../brno_data/data_alerts/*.geojson")

    selected_files = []
    for f in file_list:
        base_name = os.path.basename(f)
        date_str = base_name.replace('.geojson', '')
        try:
            file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            if start_date <= file_date <= end_date:
                selected_files.append(f)
        except ValueError:
            continue

    if not selected_files:
        print("Žiadne GeoJSON súbory v požadovanom dátumovom rozsahu v mene súboru.")
        # exit()

    selected_alerts_list = []
    for f in selected_files:
        df_file = gpd.read_file(f)
        selected_alerts_list.append(df_file)

    df_new = gpd.GeoDataFrame(pd.concat(selected_alerts_list, ignore_index=True))

    # Odstránime už existujúce podľa uuid
    df_new = df_new[~df_new['uuid'].isin(existing_uuids)]

    if df_new.empty:
        print("Žiadne nové alerty po odfiltrovaní existujúcich UUID.")
        # exit()

    # Road type korekcie
    if 'roadType' in df_new.columns:
        df_new['road_type'] = df_new['roadType'].astype(str)
    else:
        if 'road_type' not in df_new.columns:
            df_new['road_type'] = ''

    # Výpočet priemernej doby trvania alertov z existujúceho CSV
    df_existing_for_duration = df_existing.copy()
    # Uistime sa, že dátumy sú datetime
    df_existing_for_duration['published_at'] = pd.to_datetime(df_existing_for_duration['published_at'], errors='coerce')
    df_existing_for_duration['last_updated'] = pd.to_datetime(df_existing_for_duration['last_updated'], errors='coerce')

    df_existing_for_duration['duration_hours'] = (
        (df_existing_for_duration['last_updated'] - df_existing_for_duration['published_at']).dt.total_seconds() / 3600
    )

    finished_alerts = df_existing_for_duration[df_existing_for_duration['last_updated'].notna()]

    avg_duration = (
        finished_alerts
        .groupby(['street', 'road_type', 'type', 'subtype'])['duration_hours']
        .mean()
        .reset_index()
    )

    avg_duration_global = (
        df_existing_for_duration
        .groupby(['road_type', 'type', 'subtype'])['duration_hours']
        .mean()
        .reset_index()
    )

    avg_duration['road_type'] = avg_duration['road_type'].astype(str)
    avg_duration_global['road_type'] = avg_duration_global['road_type'].astype(str)

    df_new = df_new.merge(avg_duration, on=['street', 'road_type', 'type', 'subtype'], how='left')
    df_new = df_new.merge(avg_duration_global, on=['road_type', 'type', 'subtype'], how='left', suffixes=('', '_global'))

    df_new['duration_hours'] = df_new['duration_hours'].fillna(df_new['duration_hours_global'])
    df_new.drop(columns=['duration_hours_global'], inplace=True)

    # published_at - ak je pubMillis, pouzi ho, inak pouzi existujuce published_at
    if 'pubMillis' in df_new.columns:
        df_new['published_at'] = pd.to_datetime(df_new['pubMillis'], unit='ms')
    else:
        df_new['published_at'] = pd.to_datetime(df_new['published_at'])

    df_new['last_updated'] = df_new['published_at'] + pd.to_timedelta(df_new['duration_hours'], unit='h')
    df_new['active'] = False

    df_new = ensure_alerts_columns(df_new)
    csv_path = 'alerts.csv'
    save_alerts_to_csv(df_existing, df_new, csv_path)

    final_path = './alerts_updated2.csv'
    process_csv(csv_path, final_path)
