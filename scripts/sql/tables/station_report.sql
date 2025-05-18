drop table if exists station_report;
CREATE TABLE IF NOT EXISTS station_report
(
    id         SERIAL PRIMARY KEY,
    station_id TEXT                     NOT NULL,
    file_name  text                     NOT NULL,
    size       int                      NOT NULL,
    report     bytea                    NOT NULL,
    hash       bytea                    not null unique,
    downloaded TIMESTAMP WITH TIME ZONE not null default NOW()
);

CREATE INDEX IF NOT EXISTS ix_station_report_station_id ON station_report USING btree (station_id);
