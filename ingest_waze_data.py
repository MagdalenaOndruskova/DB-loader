from datetime import datetime
import time
from cons.CONF_DB import DB_CONFIG_BRNO, DB_CONFIG_JMK, DB_CONFIG_ORP_MOST
from cons.FEED_ULRS import FEED_URL_JMK, FEED_URL_ORP_MOST
from queries.QUERIES import INSERT_SEGMENTS_QUERY
from queries.queries_inserting_data import get_data, connect, insert_jams, insert_alerts, extract_segments_from_jams
from queries.queries_functions import run_statistics


def main_loop(db_config, alerts, jams):

    print(f"[{datetime.now()}] Fetching data...")
    conn = connect(db_config)

    try:

        conn.autocommit = True

        with conn.cursor() as cursor:
            insert_alerts(cursor, alerts)
            print(f"[{datetime.now()}] Alerts ingested successfully.")
            insert_jams(cursor, jams)
            print(f"[{datetime.now()}] Jams ingested successfully.")
            segments = extract_segments_from_jams(jams)
            cursor.executemany(INSERT_SEGMENTS_QUERY, segments)

            print(f"[{datetime.now()}] Segments ingested successfully.")

                #TODO: move to separate script with scheduler
            run_statistics(cursor, "25.04.2024 00:00")
            # run_statistics(cursor, "28.04.2025 20:00")
            # run_statistics(cursor)

        conn.commit()

        print(f"[{datetime.now()}] FULL DATA ingested successfully.")
    except Exception as e:
        print(f"[ERROR] {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    while True:
        # BRNO A JMK
        data = get_data(FEED_URL_JMK)
        alerts_brno_jmk = data.get("alerts", [])
        jams_brno_jmk = data.get("jams", [])

        # for Brno, filter only for city Brno
        alerts_brno = [alert for alert in alerts_brno_jmk if alert.get('city') == 'Brno']
        jams_brno = [jam for jam in jams_brno_jmk if jam.get('city') == 'Brno']
        print(f"[{datetime.now()}] INGESTING DATA FOR BRNO")
        main_loop(DB_CONFIG_BRNO, alerts_brno, jams_brno)
        print(f"="*75)

        # for JMK - everything but city Brno
        alerts_jmk = [alert for alert in alerts_brno_jmk if alert.get('city') != 'Brno']
        jams_jmk = [jam for jam in jams_brno_jmk if jam.get('city') != 'Brno']
        print(f"[{datetime.now()}] INGESTING DATA FOR JMK")
        main_loop(DB_CONFIG_JMK, alerts_jmk, jams_jmk)
        print(f"="*75)

        # ORP MOST
        data = get_data(FEED_URL_ORP_MOST)
        alerts_orp_most = data.get("alerts", [])
        jams_orp_most = data.get("jams", [])
        print(f"[{datetime.now()}] INGESTING DATA FOR ORP MOST")
        main_loop(DB_CONFIG_ORP_MOST, alerts_orp_most, jams_orp_most)
        print(f"="*75)

        # data_jams updates every 2 minutes -> sleep for 2 minutes
        time.sleep(120)
