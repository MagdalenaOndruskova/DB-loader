import requests
import psycopg2
from shapely.geometry import LineString

from queries.queries_functions import deactive_queries


def get_data(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def connect(db_config):
    return psycopg2.connect(**db_config)


def insert_alerts(cursor, alerts):
    for alert in alerts:
        try:
            # print(f"Processing alert {alert}: {alert['uuid']}")
            # print("x =", alert["location"].get("x"))
            # print("y =", alert["location"].get("y"))
            # print("pubMillis =", alert.get("pubMillis"))

            if (not isinstance(alert["location"]["x"], (int, float))
                    or not isinstance(alert["location"]["y"], (int, float))
                    or not isinstance(alert["pubMillis"],(int, float))):
                print(f"Skipping alert {alert} due to invalid coordinates or pubMillis")
                continue

            cursor.execute("""
                INSERT INTO alerts (uuid, country, city, report_rating, report_by_municipality_user,
                    confidence, reliability, type, subtype, street, road_type, magvar,
                    report_description, location, published_at, last_updated, active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326), to_timestamp(%s / 1000.0), now(), TRUE)
                ON CONFLICT (uuid, published_at) DO UPDATE SET
                    last_updated = now(),
                    active = TRUE;
            """, (
                alert["uuid"],
                alert.get("country"),
                alert.get("city"),
                alert.get("reportRating"),
                alert.get("reportByMunicipalityUser", "false") == "true",
                alert.get("confidence"),
                alert.get("reliability"),
                alert.get("type"),
                alert.get("subtype"),
                alert.get("street"),
                alert.get("roadType"),
                alert.get("magvar"),
                alert.get("reportDescription"),
                alert["location"]["x"],
                alert["location"]["y"],
                alert["pubMillis"]
            ))
        except Exception as e:
            print(e)

    deactive_queries(cursor, "alerts")


def insert_jams(cursor, jams):
    for jam in jams:
        coords = [(pt["x"], pt["y"]) for pt in jam["line"]]
        linestring = LineString(coords)
        cursor.execute("""
            INSERT INTO jams (uuid, country, jam_level, city, speed_kmh, jam_length, turn_type,
                end_node, start_node, speed, road_type, delay, street, published_at, jam_line,
                blocking_alert_uuid, last_updated, active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,
                to_timestamp(%s / 1000.0), ST_SetSRID(ST_GeomFromText(%s), 4326),
                %s, now(), TRUE)
            ON CONFLICT (uuid, published_at) DO UPDATE SET
                last_updated = now(),
                active = TRUE;
        """, (
            str(jam["uuid"]),
            jam.get("country"),
            jam.get("level"),
            jam.get("city"),
            jam.get("speedKMH"),
            jam.get("length"),
            jam.get("turnType"),
            jam.get("endNode"),
            jam.get("startNode"),
            jam.get("speed"),
            jam.get("roadType"),
            jam.get("delay"),
            jam.get("street"),
            jam.get("pubMillis"),
            linestring.wkt,
            jam.get("blockingAlertUuid")
        ))

    deactive_queries(cursor, "jams")


def extract_segments_from_jams(jams):
    """
    Extracts segments from jam entries in JSON and returns a list of tuples
    for insertion into the 'segments' table.
    """
    segments_data = []

    for jam in jams:
        jam_id = jam.get("id")
        for segment in jam.get("segments", []):
            from_node = segment.get("fromNode")
            to_node = segment.get("toNode")
            segment_id = segment.get("ID")
            is_forward = segment.get("isForward")
            segments_data.append((jam_id, from_node, to_node, segment_id, is_forward))

    return segments_data

