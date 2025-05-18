DEACTIVATE_OLD_ALERTS_QUERY = """
            UPDATE {table}
            SET active = FALSE
            WHERE active = TRUE
            AND last_updated < NOW() - INTERVAL '5 minutes';
            """

SUM_STATISTICS_INSERT_QUERY = """
        INSERT INTO sum_statistics (
            stat_time, total_active_jams, total_active_alerts,
            avg_speed_kmh, avg_jam_length, avg_delay, avg_jam_level
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (stat_time) DO UPDATE
        SET
            total_active_jams = EXCLUDED.total_active_jams,
            total_active_alerts = EXCLUDED.total_active_alerts,
            avg_speed_kmh = EXCLUDED.avg_speed_kmh,
            avg_jam_length = EXCLUDED.avg_jam_length,
            avg_delay = EXCLUDED.avg_delay,
            avg_jam_level = EXCLUDED.avg_jam_level;
    """

# Query for jams in given hour
JAM_HOURLY_QUERY = """
            SELECT 
                COUNT(*) AS total_jams,
                AVG(speed_kmh)::FLOAT AS avg_speed_kmh,
                AVG(jam_length)::FLOAT AS avg_jam_length,
                AVG(delay)::FLOAT AS avg_delay,
                AVG(jam_level)::FLOAT AS avg_jam_level
            FROM jams
            WHERE (published_at >= %s AND last_updated < %s) OR 
                (published_at <= %s AND last_updated >= %s)
        """

# Query for alerts in given hour
ALERT_HOURLY_QUERY = """
            SELECT COUNT(*) AS total_alerts
            FROM alerts
            WHERE (published_at >= %s AND last_updated < %s) OR 
                (published_at <= %s AND last_updated >= %s)
        """

INSERT_SEGMENTS_QUERY = """
    INSERT INTO segments (jam_id, from_node, to_node, segment_id, is_forward)
    VALUES (%s, %s, %s, %s, %s);
    """