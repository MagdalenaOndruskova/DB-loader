import os
import glob
import binascii
import pandas as pd
import geopandas as gpd
from datetime import datetime
from shapely.wkb import dumps as wkb_dumps
from shapely.geometry import LineString

pd.options.mode.chained_assignment = None  # Potlačenie warnings

def load_existing_jams(path):
    if not os.path.exists(path):
        return pd.DataFrame()

    df = pd.read_csv(path, parse_dates=['published_at', 'last_updated'])

    # Konverzia stĺpcov na datetime
    df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce')
    df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce')

    df['duration_hours'] = (
        (df['last_updated'] - df['published_at']).dt.total_seconds() / 3600
    )
    return df[df['duration_hours'].notna()]

def get_geojson_files_in_range(folder, start_date, end_date):
    file_list = glob.glob(os.path.join(folder, "*.geojson"))
    selected = []

    for f in file_list:
        try:
            date_str = os.path.basename(f).replace('.geojson', '')
            file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            if start_date <= file_date <= end_date:
                selected.append(f)
        except ValueError:
            continue

    return selected

def compute_avg_duration_seconds(df_existing):
    # Filter cez IQR (Interquartile Range)
    q1 = df_existing['duration_hours'].quantile(0.02)
    q3 = df_existing['duration_hours'].quantile(0.98)
    iqr = q3 - q1
    filtered = df_existing[(df_existing['duration_hours'] >= q1 - 1.5 * iqr) &
                           (df_existing['duration_hours'] <= q3 + 1.5 * iqr)]

    avg_seconds = filtered['duration_hours'].mean() * 3600 if not filtered.empty else 3600.0
    return avg_seconds
    # return df_existing['duration_hours'].mean() * 3600 if not df_existing.empty else 3600 * 1.0  # default 1h

def fix_encoding(value):
    replacements = {
        'Ã¡': 'á', 'Ã\xad': 'í', 'Åˆ': 'ň', 'Ã½': 'ý', 'Å™': 'ř',
        'Å¾': 'ž', 'Ä�': 'č', 'Å½': 'Ž', 'Ã©': 'é', 'Ä›': 'ě',
        'Å¡': 'š', 'Å˜': 'Ř', 'Å\xa0': 'Š', 'ÄŒ': 'Č', 'Å¯': 'ů',
        'Ãš': 'Ú', 'Ãº': 'ú', 'Ã¼º': 'ü', 'Ã¼': 'ü', 'Ã¶': 'ö',
        'Â»': '»'
    }
    for bad, good in replacements.items():
        value = value.replace(bad, good)
    return value

def filter_and_convert_linestrings(df):
    df = df[df['jam_line'].apply(lambda geom: isinstance(geom, LineString))].copy()
    df['jam_line'] = df['jam_line'].apply(
        lambda geom: binascii.hexlify(wkb_dumps(geom, hex=False)).decode() if geom else None
    )
    return df


def apply_avg_duration(df_new, avg_seconds):
    df_new['published_at'] = pd.to_datetime(
        df_new['pubMillis'], unit='ms', errors='coerce'
    ) if 'pubMillis' in df_new.columns else pd.to_datetime(df_new['published_at'], errors='coerce')

    df_new['duration_hours'] = avg_seconds / 3600
    df_new['last_updated'] = df_new['published_at'] + pd.to_timedelta(df_new['duration_hours'], unit='h')

    df_new['active'] = False
    df_new = filter_and_convert_linestrings(df_new)
    return df_new


def ensure_jams_columns(df):
    required = [
        'id', 'country', 'city', 'jam_level', 'speed_kmh', 'jam_length', 'turn_type',
        'uuid', 'street', 'end_node', 'start_node', 'speed', 'road_type', 'delay',
        'blocking_alert_uuid', 'published_at', 'last_updated', 'active', 'jam_line'
    ]
    for col in required:
        if col not in df.columns:
            df[col] = 0 if col in ['jam_level', 'speed_kmh', 'jam_length', 'speed', 'delay'] else ''
    df['active'] = df['active'].astype(bool)
    return df[required]

def fix_invalid_last_updated(df):
    # Istota, že stĺpce sú typu datetime

    df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce').dt.floor('s')
    df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce').dt.floor('s')
    # df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce')
    # df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce')

    # Výpočet priemernej dĺžky z platných záznamov
    valid_mask = df['last_updated'] >= df['published_at']
    valid_durations = (df.loc[valid_mask, 'last_updated'] - df.loc[valid_mask, 'published_at'])

    if not valid_durations.empty:
        avg_duration = valid_durations.mean()
    else:
        avg_duration = pd.Timedelta(hours=1)  # fallback: 1 hodina

    # Oprava neplatných záznamov
    invalid_mask = df['last_updated'] < df['published_at']
    df.loc[invalid_mask, 'last_updated'] = df.loc[invalid_mask, 'published_at'] + avg_duration

    # Aktualizácia duration_hours
    df['duration_hours'] = (df['last_updated'] - df['published_at']).dt.total_seconds() / 3600

    return df


def main(start_time, end_time, csv_path, new_file):
    geojson_folder = "../brno_data/data_jams"
    start_date = datetime.strptime(start_time, "%Y-%m-%d").date()
    end_date   = datetime.strptime(end_time, "%Y-%m-%d").date()

    df_existing = load_existing_jams(csv_path)
    existing_uuids = set(df_existing['uuid']) if not df_existing.empty else set()



    avg_seconds = compute_avg_duration_seconds(df_existing)

    files = get_geojson_files_in_range(geojson_folder, start_date, end_date)
    if not files:
        print("Žiadne GeoJSON súbory v požadovanom rozsahu.")
        return

    all_new = []
    for path in files:
        df = gpd.read_file(path)
        df = df.rename(columns={
            'level': 'jam_level',
            'length': 'jam_length',
            'geometry': 'jam_line',
            'speedKMH': 'speed_kmh',
            'turnType': 'turn_type',
            'roadType': 'road_type'
        })

        for col in ['street', 'end_node', 'start_node']:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str).apply(fix_encoding)

        all_new.append(df)

    df_new = pd.concat(all_new, ignore_index=True)
    df_new = df_new[~df_new['uuid'].isin(existing_uuids)]
    if df_new.empty:
        print("Žiadne nové zápchy.")
        return

    df_new = apply_avg_duration(df_new, avg_seconds)

    df_new = ensure_jams_columns(df_new)

    df_final = pd.concat([df_existing, df_new], ignore_index=True)

    # df_final = fix_invalid_last_updated(df_final)

    df_final.to_csv(new_file, index=False)

    print(f"Uložených {len(df_final)} zápch do {new_file}")


if __name__ == '__main__':
    start_time = "2024-04-25"
    end_time = "2025-05-18"
    main(start_time, end_time, "jams.csv", "jams_updated2.csv")

