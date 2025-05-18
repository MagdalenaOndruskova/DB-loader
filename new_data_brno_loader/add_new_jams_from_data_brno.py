import binascii
import glob
import geopandas as gpd
import pandas as pd
import os
from datetime import datetime
import warnings
from shapely.wkb import dumps as wkb_dumps


warnings.filterwarnings("ignore")

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
        fixed = fixed.replace('Â»', '»')
        return fixed
    except Exception:
        return ''

def ensure_jams_columns(df_merged):
    required_columns = [
        'id', 'country', 'city', 'jam_level', 'speed_kmh', 'jam_length', 'turn_type',
        'uuid', 'street', 'end_node', 'start_node', 'speed', 'road_type', 'delay',
        'blocking_alert_uuid', 'published_at', 'last_updated', 'active', 'jam_line'
    ]

    # Previesť geometriu (LineString) na WKB HEX
    if 'jam_line' in df_merged.columns:
        df_merged['jam_line'] = df_merged['jam_line'].apply(
            lambda geom: binascii.hexlify(wkb_dumps(geom, hex=False)).decode() if geom else None
        )
    else:
        df_merged['jam_line'] = None
    for col in required_columns:
        if col not in df_merged.columns:
            if col in ['jam_level', 'speed_kmh', 'jam_length', 'speed', 'delay']:
                df_merged[col] = 0
            elif col == 'active':
                df_merged[col] = False
            else:
                df_merged[col] = ''

    default_time = pd.Timestamp("2024-01-01 00:00:00")
    df_merged['published_at'] = pd.to_datetime(df_merged['published_at'], errors='coerce').fillna(default_time)
    df_merged['last_updated']  = pd.to_datetime(df_merged['last_updated'],  errors='coerce').fillna(default_time)

    df_merged = df_merged[required_columns]
    return df_merged

def load_existing_jams(csv_path):
    if os.path.exists(csv_path):
        df_existing = pd.read_csv(csv_path, parse_dates=['published_at', 'last_updated'])
    else:
        df_existing = pd.DataFrame()
    return df_existing

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


def save_jams_to_csv(df_existing, df_new, csv_path):

    # Skombinuj oba dataframe
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)

    if df_combined.empty:
        print("Žiadne jams na uloženie.")
        return

    # Ulož ako nový súbor (prepíše existujúci)
    df_combined.to_csv(csv_path, index=False)
    print(f"Uložených {len(df_combined)} jams do {csv_path}")


if __name__ == '__main__':
    csv_path = './jams.csv'
    final_path = 'jams.csv'
    process_csv(csv_path, final_path)

    df_existing = load_existing_jams(final_path)
    df_existing['end_node'].fillna('', inplace=True)
    df_existing['start_node'].fillna('', inplace=True)
    df_existing['street'].fillna('', inplace=True)
    boolean_cols = [ 'active']
    for col in boolean_cols:
        df_existing[col] = df_existing[col].map({'f': False, 't': True})
    existing_uuids = set(df_existing['uuid']) if not df_existing.empty else set()

    existing_uuids = set(df_existing['uuid']) if not df_existing.empty else set()

    start_date = datetime.strptime('2025-05-18', '%Y-%m-%d').date()
    # start_date = datetime.strptime('2024-04-24', '%Y-%m-%d').date()
    end_date   = datetime.strptime('2025-05-18', '%Y-%m-%d').date()
    # end_date   = datetime.strptime('2025-04-25', '%Y-%m-%d').date()

    file_list = glob.glob("../brno_data/data_jams/*.geojson")

    selected_files = []
    for f in file_list:
        base_name = os.path.basename(f)
        date_str  = base_name.replace('.geojson', '')
        try:
            file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            if start_date <= file_date <= end_date:
                selected_files.append(f)
        except ValueError:
            continue

    if not selected_files:
        print("Žiadne GeoJSON súbory v požadovanom rozsahu.")
        exit()

    all_jams = []
    for f in selected_files:
        df_file = gpd.read_file(f)

        # Premapovanie stĺpcov na očakávané názvy
        df_file = df_file.rename(columns={
            'level': 'jam_level',
            'length': 'jam_length',
            'geometry': 'jam_line',
            'speedKMH': 'speed_kmh',
            'turnType': 'turn_type',
            'roadType': 'road_type',
        })

        # Opravíme encoding textových polí
        for col in ['street', 'end_node', 'start_node']:
            if col in df_file.columns:
                df_file[col] = df_file[col].fillna('').astype(str).apply(fix_encoding)

        all_jams.append(df_file)

    df_new = gpd.GeoDataFrame(pd.concat(all_jams, ignore_index=True))
    df_new = df_new[~df_new['uuid'].isin(existing_uuids)]

    if df_new.empty:
        print("Žiadne nové jams po filtrovaní.")
        exit()

    # Konverzia dátumov pre výpočty
    df_existing['published_at'] = pd.to_datetime(df_existing['published_at'], errors='coerce')
    df_existing['last_updated']  = pd.to_datetime(df_existing['last_updated'],  errors='coerce')

    # Výpočet duration_hours
    df_existing['duration_hours'] = (
        (df_existing['last_updated'] - df_existing['published_at'])
        .dt.total_seconds() / 3600
    )
    finished_jams = df_existing[df_existing['last_updated'].notna()]

    # Detailná priemerka podľa street + parametrov
    avg_duration = (
        finished_jams
        .groupby([
            'street','jam_level','speed_kmh','jam_length',
            'speed','road_type','delay'
        ])['duration_hours']
        .mean()
        .reset_index()
    )

    # Globálna priemerka bez street
    avg_duration_global = (
        finished_jams
        .groupby([
            'jam_level','speed_kmh','jam_length',
            'speed','road_type','delay'
        ])['duration_hours']
        .mean()
        .reset_index()
    )

    avg_duration['road_type']        = avg_duration['road_type'].astype(str)
    avg_duration_global['road_type'] = avg_duration_global['road_type'].astype(str)

    df_new = df_new.merge(
        avg_duration,
        on=['street','jam_level','speed_kmh','jam_length','speed','road_type','delay'],
        how='left'
    )
    df_new = df_new.merge(
        avg_duration_global,
        on=['jam_level','speed_kmh','jam_length','speed','road_type','delay'],
        how='left',
        suffixes=('','_global')
    )

    df_new['duration_hours'] = df_new['duration_hours'].fillna(df_new['duration_hours_global'])
    df_new.drop(columns=['duration_hours_global'], inplace=True)

    # published_at z pubMillis ak existuje
    if 'pubMillis' in df_new.columns:
        df_new['published_at'] = pd.to_datetime(df_new['pubMillis'], unit='ms')
    else:
        df_new['published_at'] = pd.to_datetime(df_new['published_at'], errors='coerce')

    df_new['last_updated'] = df_new['published_at'] + pd.to_timedelta(df_new['duration_hours'], unit='h')
    df_new['active']       = False

    df_new = ensure_jams_columns(df_new)
    csv_path = 'jams.csv'
    save_jams_to_csv(df_existing, df_new, csv_path)

    final_path = './jams_updated2.csv'
    process_csv(csv_path, final_path)

