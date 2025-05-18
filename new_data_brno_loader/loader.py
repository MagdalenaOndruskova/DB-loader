import binascii
import json
import psycopg2
import pandas as pd
from datetime import datetime
from pyproj import Transformer
from psycopg2.extras import execute_values
from shapely import LineString, wkb, MultiLineString

# --- DB CONFIG ---
conn = psycopg2.connect(
    dbname='traffic_brno',
    user='analyticity_brno',
    password='waze_admin',
    host='localhost',
    port=5433
)

# --- Transformer pre EPSG:5514 -> 4326 ---
transformer = Transformer.from_crs("EPSG:5514", "EPSG:4326", always_xy=True)


def safe_int(val):
    if val is None:
        return -1
    try:
        return int(val)
    except:
        return -1


def safe_float(val):
    if val is None:
        return -1.0
    try:
        return float(val)
    except:
        return -1.0


def load_nehody():
    # --- Načítanie GeoJSON ---
    with open('./nehody.geojson', 'r', encoding='utf-8') as f:
        data = json.load(f)

    columns = [
        "p1", "p36", "p37", "p2a", "p2b", "p6", "p7", "p8", "p9", "p10", "p11", "p12",
        "p13a", "p13b", "p13c", "p14", "p15", "p16", "p17", "p18", "p19", "p20", "p21",
        "p22", "p23", "p24", "p27", "p28", "p34", "p35", "p39", "p44", "p45a", "p47",
        "p48a", "p49", "p50a", "p50b", "p51", "p52", "p53", "p55a", "p57", "p58",
        "p5a", "p8a", "p11a"
    ]

    int_fields = {
        "p1", "p2b", "p6", "p7", "p8", "p9", "p10", "p11", "p12", "p13a", "p13b", "p13c",
        "p14", "p15", "p16", "p17", "p18", "p19", "p20", "p21", "p22", "p23", "p24",
        "p27", "p28", "p34", "p35", "p44", "p45a", "p48a", "p49", "p50a", "p50b",
        "p51", "p52", "p53", "p55a", "p57", "p58", "p5a", "p8a", "p11a"
    }

    nehody_records = []

    for feature in data['features']:
        props = feature['properties']
        coords = feature['geometry']['coordinates']
        x, y = coords[0], coords[1]
        lon4326, lat4326 = transformer.transform(x, y)

        row = []
        for key in columns:
            val = props.get(key)
            if val is None:
                row.append(-1 if key in int_fields else None)
            else:
                if key in int_fields:
                    try:
                        row.append(int(float(val)))
                    except:
                        row.append(-1)
                else:
                    row.append(val)

        # Dátum p2a
        try:
            p2a_date = datetime.strptime(props.get("p2a", ""), "%d/%m/%Y").date()
        except:
            p2a_date = None
        row[3] = p2a_date  # prepísať správnu hodnotu dátumu

        # Súradnice a geom/geog
        row.extend([
            x,
            y,
            f'SRID=5514;POINT({x} {y})',
            f'SRID=4326;POINT({lon4326} {lat4326})'
        ])

        nehody_records.append(tuple(row))

    # --- Execute batch insert ---
    execute_values(cur, """
        INSERT INTO nehody (
            p1, p36, p37, p2a, p2b, p6, p7, p8, p9, p10, p11, p12,
            p13a, p13b, p13c, p14, p15, p16, p17, p18, p19, p20, p21,
            p22, p23, p24, p27, p28, p34, p35, p39, p44, p45a, p47,
            p48a, p49, p50a, p50b, p51, p52, p53, p55a, p57, p58,
            p5a, p8a, p11a, x, y,
            geom, geog
        ) VALUES %s
    """, nehody_records, template="""
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
         %s, %s, %s, %s, %s,
         ST_GeomFromText(%s, 5514),
         ST_GeogFromText(%s)
        )
    """, page_size=500)

    print(f"✅ Nehody: {len(nehody_records)} records loaded.")


def load_jams():
    # Načítanie CSV so správnym parsovaním dátumov a potlačením DtypeWarning
    jams_df = pd.read_csv('./jams_updated2.csv', parse_dates=['published_at', 'last_updated'], low_memory=False)
    jams_df = jams_df.where(pd.notnull(jams_df), None)

    jams_records = []

    for _, row in jams_df.iterrows():
        try:
            # Geometria z WKB HEX na LineString alebo MultiLineString
            wkb_hex = row['jam_line']
            wkb_bytes = binascii.unhexlify(wkb_hex)
            geom = wkb.loads(wkb_bytes)

            if isinstance(geom, MultiLineString):
                # Spojenie všetkých čiar do jednej
                coords = []
                for line in geom.geoms:
                    coords.extend(line.coords)
                linestring = LineString(coords)
            elif isinstance(geom, LineString):
                linestring = geom
            else:
                raise ValueError(f"Unsupported geometry type: {geom.geom_type}")

            record = (
                safe_int(row['id']),
                row.get('country'),
                row.get('city'),
                safe_int(row['jam_level']),
                safe_int(row['speed_kmh']),
                safe_int(row['jam_length']),
                row.get('turn_type'),
                row.get('uuid'),
                row.get('street'),
                row.get('end_node'),
                row.get('start_node'),
                safe_float(row['speed']),
                safe_int(row['road_type']),
                safe_int(row['delay']),
                row.get('blocking_alert_uuid'),
                row['published_at'],
                row['last_updated'],
                row.get('active', True),
                linestring.wkt
            )
            jams_records.append(record)
        except Exception as e:
            print(f"❌ JAMS: Error while processing row: {e}")

    # Deduplicate jams_records by (uuid, published_at)
    unique_records = {}
    for rec in jams_records:
        key = (rec[7], rec[15])
        unique_records[key] = rec
    jams_records = list(unique_records.values())

    # Batch insert into DB
    execute_values(cur, """
        INSERT INTO jams (
            id, country, city, jam_level, speed_kmh, jam_length, turn_type,
            uuid, street, end_node, start_node, speed, road_type, delay,
            blocking_alert_uuid, published_at, last_updated, active, jam_line
        ) VALUES %s
        ON CONFLICT (uuid, published_at) DO UPDATE SET
            last_updated = EXCLUDED.last_updated,
            active = EXCLUDED.active;
    """, jams_records, template="""
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
         ST_SetSRID(ST_GeomFromText(%s), 4326))
    """, page_size=500)

    print(f"✅ Jams: {len(jams_records)} records loaded.")


def load_alerts():
    alerts_df = pd.read_csv('alerts.csv', parse_dates=['published_at', 'last_updated'], low_memory=False)
    alerts_df = alerts_df.where(pd.notnull(alerts_df), None)

    alerts_records = []

    for _, row in alerts_df.iterrows():
        try:
            # Parsovanie location z WKB HEX na bod
            wkb_hex = row['location']
            wkb_bytes = binascii.unhexlify(wkb_hex)
            point = wkb.loads(wkb_bytes)
            x, y = point.x, point.y

            record = (
                row['uuid'],
                row.get('country'),
                row.get('city'),
                safe_int(row.get('report_rating')),
                (row.get('report_by_municipality_user', 'f') in ['t', 'true', True]),
                safe_int(row.get('confidence')),
                safe_int(row.get('reliability')),
                row.get('type'),
                row.get('subtype'),
                row.get('street'),
                safe_int(row.get('road_type')),
                safe_int(row.get('magvar')),
                row.get('report_description'),
                x,
                y,
                row['published_at'],
                row['last_updated'],
                row.get('active', True)
            )
            alerts_records.append(record)
        except Exception as e:
            print(f"❌ ALERTS: Error while processing alert with uuid: {row.get('uuid')}: {e}")

    # Deduplicate alerts_records by (uuid, published_at)
    unique_alerts = {}
    for rec in alerts_records:
        key = (rec[0], rec[15])  # uuid, published_at
        unique_alerts[key] = rec  # posledný výskyt vyhrá

    alerts_records = list(unique_alerts.values())

    execute_values(cur, """
        INSERT INTO alerts (
            uuid, country, city, report_rating, report_by_municipality_user,
            confidence, reliability, type, subtype, street, road_type, magvar,
            report_description, location, published_at, last_updated, active
        ) VALUES %s
        ON CONFLICT (uuid, published_at) DO UPDATE SET
            last_updated = EXCLUDED.last_updated,
            active = TRUE
    """, alerts_records, template="""
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
         ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s)
    """, page_size=500)

    print(f"✅ Alerts: {len(alerts_records)} records loaded.")


if __name__ == '__main__':
    cur = conn.cursor()

    load_nehody()
    load_jams()
    load_alerts()

    # --- Commit ---
    conn.commit()
    cur.close()
    conn.close()
