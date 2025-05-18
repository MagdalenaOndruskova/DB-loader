from datetime import datetime, timedelta

from queries.QUERIES import DEACTIVATE_OLD_ALERTS_QUERY, SUM_STATISTICS_INSERT_QUERY, JAM_HOURLY_QUERY, ALERT_HOURLY_QUERY
from helpers import round_to_hour


def deactive_queries(cursor, table_name):
    """
    Functions set for alert/query active to False, if the last update was older than 5 minutes.

    :param cursor:
    :param table_name:
    :return:
    """
    query = DEACTIVATE_OLD_ALERTS_QUERY.format(table=table_name)
    cursor.execute(query)
    affected_rows = cursor.rowcount
    print(f"{affected_rows} alert(s) deactivated in '{table_name}'.")


def insert_sum_statistics(cursor, start_time, jam_stats, alert_stats):
    """
    Function inserts calculated statistics to the database table

    :param cursor:
    :param start_time:
    :param jam_stats:
    :param alert_stats:
    :return:
    """
    data = (
        start_time,
        jam_stats[0] if jam_stats[0] else 0,
        alert_stats[0] if alert_stats[0] else 0,
        jam_stats[1] if jam_stats[1] else 0,
        jam_stats[2] if jam_stats[2] else 0,
        jam_stats[3] if jam_stats[3] else 0,
        jam_stats[4] if jam_stats[4] else 0
    )

    cursor.execute(SUM_STATISTICS_INSERT_QUERY, data)


def run_statistics(cursor, stat_time_str=None):
    """
    Calculates and inserts statistics for one or multiple hours.

    - If no time is given, runs for the current UTC hour only.
    - If time is given (e.g. '17.04.2025 17:00'), runs for every full hour
      from that time up to (but excluding) the current UTC hour.

    :param cursor: psycopg2 cursor
    :param stat_time_str: Optional string "DD.MM.YYYY HH:MM"
    """
    now = round_to_hour(datetime.utcnow())

    # Determine starting hour
    if stat_time_str:
        try:
            start_time = datetime.strptime(stat_time_str, "%d.%m.%Y %H:%M")
            start_time = round_to_hour(start_time)
        except ValueError:
            raise ValueError("Invalid date format. Use 'DD.MM.YYYY HH:MM'")
    else:
        # Only current hour
        start_time = now

    # Loop for each full hour
    while start_time < now:
        calculate_statistics_step(cursor, start_time)
        # Move to next hour
        start_time += timedelta(hours=1)

    if not stat_time_str:
        calculate_statistics_step(cursor, start_time)


def calculate_statistics_step(cursor, start_time):
    """
    Function calculates statistics for given hour (based on start time)

    :param cursor: psycopg2 cursor
    :param start_time: Hour for which statistics are calculated
    """
    end_time = start_time + timedelta(hours=1)

    # Fetch stats
    cursor.execute(JAM_HOURLY_QUERY, (start_time, end_time, start_time, start_time))
    jam_stats = cursor.fetchone()

    cursor.execute(ALERT_HOURLY_QUERY, (start_time, end_time, start_time, start_time))
    alert_stats = cursor.fetchone()

    # Store to database
    insert_sum_statistics(cursor, start_time, jam_stats, alert_stats)

    print(f"Processed stats for hour: {start_time.strftime('%Y-%m-%d %H:%M')}")