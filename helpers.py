def round_to_hour(dt):
    """Rounds the given datetime to the hour (no minutes, seconds, or microseconds)."""
    return dt.replace(minute=0, second=0, microsecond=0)
