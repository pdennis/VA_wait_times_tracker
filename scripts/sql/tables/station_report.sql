drop table if exists station_report;
CREATE TABLE IF NOT EXISTS station_report
(
    id         SERIAL PRIMARY KEY,
    station_id TEXT NOT NULL,
    file_name  text,
    report     bytea,
    uploaded   TIMESTAMP WITH TIME ZONE default NOW()
);