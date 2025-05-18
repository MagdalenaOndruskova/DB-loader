-- Rozšírenia
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS jams (
    id BIGINT,
    country TEXT,
    city TEXT,
    jam_level INTEGER,
    speed_kmh INTEGER,
    jam_length INTEGER,
    turn_type TEXT,
    uuid INTEGER,
    street TEXT,
    end_node TEXT,
    start_node TEXT,
    speed FLOAT,
    road_type INTEGER,
    delay INTEGER,
    blocking_alert_uuid UUID,
    published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_updated TIMESTAMPTZ DEFAULT now(),
    active BOOLEAN DEFAULT TRUE,
    -- Toto pole uloží geografickú líniu (napr. trasu dopravnej zápchy) s presnými výpočtami vzdialeností na zakrivenej Zemi.
    jam_line GEOGRAPHY(LINESTRING, 4326),
    PRIMARY KEY (uuid, published_at)
);

SELECT create_hypertable('jams', 'published_at', if_not_exists => TRUE);


-- Tabuľka ALERTS
CREATE TABLE IF NOT EXISTS alerts (
    uuid UUID,
    country TEXT,
    city TEXT,
    type TEXT,
    subtype TEXT,
    street TEXT,
    report_rating INTEGER,
    confidence INTEGER,
    reliability INTEGER,
    road_type INTEGER,
    magvar INTEGER,
    report_by_municipality_user BOOLEAN,
    report_description TEXT,
    location GEOGRAPHY(POINT, 4326),
    published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_updated TIMESTAMPTZ DEFAULT now(),
    active BOOLEAN DEFAULT TRUE,

    PRIMARY KEY (uuid, published_at)

);

SELECT create_hypertable('alerts', 'published_at', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS segments (
    id SERIAL PRIMARY KEY,
    jam_id BIGINT,
    from_node BIGINT,
    to_node BIGINT,
    segment_id BIGINT,
    is_forward BOOLEAN
);


CREATE TABLE IF NOT EXISTS sum_statistics (
    stat_time TIMESTAMPTZ PRIMARY KEY,  -- Rounded to the hour
    total_active_jams INTEGER,
    total_active_alerts INTEGER,
    avg_speed_kmh FLOAT,
    avg_jam_length FLOAT,
    avg_delay FLOAT,
    avg_jam_level FLOAT
);

CREATE TABLE IF NOT EXISTS nehody (
    p1 BIGINT PRIMARY KEY,
    p36 TEXT,
    p37 TEXT,
    p2a DATE,
    p2b INTEGER,
    p6 INTEGER,
    p7 INTEGER,
    p8 INTEGER,
    p9 INTEGER,
    p10 INTEGER,
    p11 INTEGER,
    p12 INTEGER,
    p13a INTEGER,
    p13b INTEGER,
    p13c INTEGER,
    p14 INTEGER,
    p15 INTEGER,
    p16 INTEGER,
    p17 INTEGER,
    p18 INTEGER,
    p19 INTEGER,
    p20 INTEGER,
    p21 INTEGER,
    p22 INTEGER,
    p23 INTEGER,
    p24 INTEGER,
    p27 INTEGER,
    p28 INTEGER,
    p34 INTEGER,
    p35 INTEGER,
    p39 TEXT,
    p44 INTEGER,
    p45a INTEGER,
    p47 TEXT,
    p48a INTEGER,
    p49 INTEGER,
    p50a INTEGER,
    p50b INTEGER,
    p51 INTEGER,
    p52 INTEGER,
    p53 INTEGER,
    p55a INTEGER,
    p57 INTEGER,
    p58 INTEGER,
    p5a INTEGER,
    p8a INTEGER,
    p11a INTEGER,
    x FLOAT,
    y FLOAT,
    geom GEOMETRY(POINT, 5514),
    geog GEOGRAPHY(POINT, 4326)
);

SELECT create_hypertable('nehody', 'p2a', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_jams_jam_line ON jams USING GIST(jam_line);
CREATE INDEX IF NOT EXISTS idx_alerts_location ON alerts USING GIST(location);
CREATE INDEX IF NOT EXISTS idx_accidents_geom ON nehody USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_accidents_geog ON nehody USING GIST(geog);
