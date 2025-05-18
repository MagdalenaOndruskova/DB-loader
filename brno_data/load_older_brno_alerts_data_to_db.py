import glob
import geopandas as gpd
import pandas as pd
import psycopg2

from queries import queries_inserting_data
from cons.CONF_DB import DB_CONFIG_BRNO
from psycopg2.extras import execute_values
from shapely.wkb import dumps as wkb_dumps


def insert_new_alerts(df, connection):
    """
    Inserts new alert records into the 'alerts' table in PostgreSQL.
    Skips any records where the UUID already exists in the table.

    Parameters:
    - df: Processed pandas DataFrame containing alerts.
    - connection: psycopg2 connection to the PostgreSQL database.
    """

    with connection.cursor() as cursor:
        # Fetch existing UUIDs from the alerts table
        cursor.execute("SELECT uuid FROM alerts")
        existing_uuids = set(row[0] for row in cursor.fetchall())

        # Filter out rows with UUIDs already in the table
        df_new = df[~df['uuid'].isin(existing_uuids)]

        if df_new.empty:
            print("No new alerts to insert.")
            return

        # Prepare rows for insertion
        rows = []
        for _, row in df_new.iterrows():
            rows.append((
                row['uuid'],
                row['country'],
                row['city'],
                row['type'],
                row['subtype'],
                row['street'],
                row['report_rating'],
                row['confidence'],
                row['reliability'],
                row['road_type'],
                row['magvar'],
                row['report_by_municipality_user'],
                row['report_description'],
                psycopg2.Binary(wkb_dumps(row['location'])) if row['location'] else None,
                row['published_at'],
                row['last_updated'],
                row['active']
            ))

        # Define SQL for inserting
        insert_sql = """
            INSERT INTO alerts (
                uuid, country, city, type, subtype, street,
                report_rating, confidence, reliability, road_type,
                magvar, report_by_municipality_user, report_description,
                location, published_at, last_updated, active
            ) VALUES %s
        """

        # Use execute_values for efficient bulk insert
        execute_values(cursor, insert_sql, rows)
        connection.commit()
        print(f"Inserted {len(rows)} new alerts.")


def ensure_alerts_columns(df_merged):
    """
    This function processes the given DataFrame by:
    1. Ensuring that the required columns are present with default values.
    2. Creating a 'location' column from 'longitude' and 'latitude' (or other specified columns).
    3. Returning the processed DataFrame.

    Parameters:
    - df_merged: The DataFrame to be processed.

    Returns:
    - Processed DataFrame with required columns and created 'location' geometry.
    """

    # Define the list of required columns
    required_columns = [
        'uuid', 'country', 'city', 'type', 'subtype', 'street',
        'report_rating', 'confidence', 'reliability', 'road_type',
        'magvar', 'report_by_municipality_user', 'report_description',
        'location', 'published_at', 'last_updated', 'active'
    ]

    # Assign 'geometry' to 'location'
    if 'geometry' in df_merged.columns:
        df_merged['location'] = df_merged['geometry']
    else:
        df_merged['location'] = None  # fallback if 'geometry' is missing

    # Check for missing columns and add them with default values
    for col in required_columns:
        if col not in df_merged.columns:
            if col == 'active':
                df_merged[col] = False
            elif col == 'report_rating' or col == 'confidence' or col == 'reliability' or col == 'magvar':
                df_merged[col] = 0
            elif col == 'report_by_municipality_user':
                df_merged[col] = False
            else:  # text columns
                df_merged[col] = ''

    default_time = pd.Timestamp("2024-01-01 00:00:00")

    df_merged['published_at'] = df_merged['published_at'].fillna(default_time)
    df_merged['last_updated'] = df_merged['last_updated'].fillna(default_time)

    # Now select only the required columns and drop any others
    df_merged = df_merged[required_columns]

    # Return the processed DataFrame
    return df_merged


def load_alerts_from_db(db_config, new_alerts):
    try:
        with queries_inserting_data.connect(db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                            SELECT * from alerts
                        """)
                alerts = cursor.fetchall()


                # Get column names from the cursor description
                columns = [desc[0] for desc in cursor.description]

                # Create the DataFrame
                df_alerts = pd.DataFrame(alerts, columns=columns)

                # Make sure times are datetime
                df_alerts['published_at'] = pd.to_datetime(df_alerts['published_at'])
                df_alerts['last_updated'] = pd.to_datetime(df_alerts['last_updated'])

                # Filter only finished alerts (i.e., alerts that are not active)
                finished_alerts = df_alerts[df_alerts['last_updated'].notna()].copy()

                # Calculate duration in hours
                finished_alerts['duration_hours'] = (finished_alerts['last_updated'] - finished_alerts[
                    'published_at']).dt.total_seconds() / 3600

                df_alerts['duration_hours'] = (df_alerts['last_updated'] - df_alerts[
                    'published_at']).dt.total_seconds() / 3600

                # Group by street, roadtype, type, and subtype, then calculate average duration
                avg_duration = (
                    finished_alerts
                    .groupby(['street', 'road_type', 'type', 'subtype'])['duration_hours']
                    .mean()
                    .reset_index()
                )

                avg_duration_global = (
                    df_alerts
                    .groupby(['road_type', 'type', 'subtype'])['duration_hours']
                    .mean()
                    .reset_index()
                )

                # Drop rows from df_geo where uuid already exists in df_alerts
                new_alerts = new_alerts[~new_alerts['uuid'].isin(df_alerts['uuid'])]
                new_alerts['road_type'] = new_alerts["roadType"]

                new_alerts['road_type'] = new_alerts['road_type'].astype(str)
                avg_duration['road_type'] = avg_duration['road_type'].astype(str)
                avg_duration_global['road_type'] = avg_duration_global['road_type'].astype(str)
                # Join
                df_merged = new_alerts.merge(avg_duration, on=['street', 'road_type', 'type', 'subtype'], how='left')

                # For rows where there's no match in avg_duration, join the global value from avg_duration_global
                df_merged = df_merged.merge(avg_duration_global, on=['road_type', 'type', 'subtype'], how='left',
                                            suffixes=('', '_global'))

                # Fill missing values in the merged columns
                df_merged['duration_hours'] = df_merged['duration_hours'].fillna(df_merged['duration_hours_global'])
                df_merged.drop(columns=['duration_hours_global'], inplace=True)

                # Step 4: Calculate last_updated from pubMillis + duration_hours (if needed for df_alerts)
                df_merged['published_at'] = pd.to_datetime(df_merged['pubMillis'], unit='ms')
                df_merged['last_updated'] = df_merged['published_at'] + pd.to_timedelta(df_merged['duration_hours'],
                                                                                        unit='h')
                df_merged['active'] = False

                prepared_old_alerts_data = ensure_alerts_columns(df_merged)

                insert_new_alerts(prepared_old_alerts_data, conn)
                pass

            # conn.commit()
    except Exception as e:
        print(f"[ERROR] {e}")


if __name__ == '__main__':
    # ALERTS
    file_list = glob.glob("./data_alerts/*.geojson")
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
    load_alerts_from_db(DB_CONFIG_BRNO, df_brno)
    pass


