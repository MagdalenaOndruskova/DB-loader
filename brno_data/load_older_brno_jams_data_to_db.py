import glob
import geopandas as gpd
import pandas as pd
import psycopg2

from queries import queries_inserting_data
from cons.CONF_DB import DB_CONFIG_BRNO
from psycopg2.extras import execute_values
from shapely.wkb import dumps as wkb_dumps


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


def load_alerts_from_db(db_config, new_jams):
    try:
        with queries_inserting_data.connect(db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                            SELECT * from jams
                        """)
                jams = cursor.fetchall()


                # Get column names from the cursor description
                columns = [desc[0] for desc in cursor.description]

                # Create the DataFrame
                df_jams = pd.DataFrame(jams, columns=columns)

                # Make sure times are datetime
                df_jams['published_at'] = pd.to_datetime(df_jams['published_at'])
                df_jams['last_updated'] = pd.to_datetime(df_jams['last_updated'])

                # Filter only finished alerts (i.e., alerts that are not active)
                finished_jams = df_jams[df_jams['last_updated'].notna()].copy()
                #
                # Calculate duration in hours
                finished_jams['duration_hours'] = (finished_jams['last_updated'] - finished_jams[
                    'published_at']).dt.total_seconds() / 3600

                df_jams['duration_hours'] = (df_jams['last_updated'] - df_jams[
                    'published_at']).dt.total_seconds() / 3600

                # Group by street, roadtype, type, and subtype, then calculate average duration
                avg_duration = (
                    finished_jams
                    .groupby(['street', 'road_type', 'jam_level'])['duration_hours']
                    .mean()
                    .reset_index()
                )
                #
                avg_duration_global = (
                    df_jams
                    .groupby(['road_type', 'jam_level'])['duration_hours']
                    .mean()
                    .reset_index()
                )
                new_jams['road_type'] = new_jams["roadType"]
                new_jams['jam_level'] = new_jams["level"]

                new_jams['road_type'] = new_jams['road_type'].astype(int)
                avg_duration['road_type'] = avg_duration['road_type'].astype(int)
                avg_duration_global['road_type'] = avg_duration_global['road_type'].astype(int)

                df_merged = new_jams.merge(avg_duration, on=['street', 'road_type', 'jam_level',], how='left')


                pass

            # conn.commit()
    except Exception as e:
        print(f"[ERROR] {e}")


if __name__ == '__main__':
    # JAMS
    file_list = glob.glob("./data_jams/2025-03*.geojson")
    df = gpd.GeoDataFrame(pd.concat([gpd.read_file(f) for f in file_list], ignore_index=True))

    # Find duplicates based on 'uuid'
    duplicate_df = df[df.duplicated(subset='uuid', keep=False)].copy()

    # Sort by 'uuid'
    duplicate_df = duplicate_df.sort_values(by='uuid')

    # drop duplicates, keep only first
    # duplicates probably by wrong export
    df_unique = df.drop_duplicates(subset='uuid', keep='first')

    # keep only Brno
    df_brno = df_unique[df_unique['city'] == 'Brno']
    df_brno['endNode'] = df_brno.apply(lambda row: fix_encoding(row['endNode']), axis=1)
    load_alerts_from_db(DB_CONFIG_BRNO, df_brno)
    pass


