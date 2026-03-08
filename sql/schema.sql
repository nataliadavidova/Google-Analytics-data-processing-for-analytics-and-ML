-- schema.sql
-- SQLite schema for GA sessions + hits (events log)

PRAGMA foreign_keys = ON;

-- =========================
-- Table: sessions
-- =========================
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    client_id TEXT,

    -- Dates/times stored as TEXT in ISO format for SQLite:
    -- visit_date: 'YYYY-MM-DD'
    -- visit_time: 'HH:MM:SS'
    visit_date TEXT,
    visit_time TEXT,
    visit_number INTEGER,

    -- UTM
    utm_source TEXT,
    utm_medium TEXT,
    utm_campaign TEXT,
    utm_adcontent TEXT,
    utm_keyword TEXT,

    -- Device
    device_category TEXT,
    device_os TEXT,
    device_brand TEXT,
    device_model TEXT,
    device_screen_resolution TEXT,
    device_browser TEXT,

    -- Geo
    geo_country TEXT,
    geo_city TEXT
);

-- Helpful indexes for analytics
CREATE INDEX IF NOT EXISTS idx_sessions_visit_date ON sessions(visit_date);
CREATE INDEX IF NOT EXISTS idx_sessions_utm_medium ON sessions(utm_medium);
CREATE INDEX IF NOT EXISTS idx_sessions_geo_country ON sessions(geo_country);

-- =========================
-- Table: hits (events)
-- =========================
CREATE TABLE IF NOT EXISTS hits (
    -- Surrogate PK because (session_id, hit_number) is not unique
    hit_id INTEGER PRIMARY KEY AUTOINCREMENT,

    session_id TEXT NOT NULL,

    -- hit_date stored as TEXT 'YYYY-MM-DD'
    -- hit_time stored as INTEGER (nullable) - as in your cleaned rules
    hit_date TEXT,
    hit_time INTEGER,
    hit_number INTEGER,

    hit_type TEXT,
    hit_referer TEXT,
    hit_page_path TEXT,

    event_category TEXT,
    event_action TEXT,
    event_label TEXT,

    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

-- Indexes for joins + funnel queries
CREATE INDEX IF NOT EXISTS idx_hits_session_id ON hits(session_id);
CREATE INDEX IF NOT EXISTS idx_hits_event_action ON hits(event_action);
CREATE INDEX IF NOT EXISTS idx_hits_event_category ON hits(event_category);
CREATE INDEX IF NOT EXISTS idx_hits_hit_date ON hits(hit_date);
