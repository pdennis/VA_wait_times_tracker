drop table if exists station_report CASCADE;
CREATE TABLE IF NOT EXISTS station_report
(
    report_id   SERIAL PRIMARY KEY,
    station_id  TEXT                     NOT NULL,
    file_name   text                     NOT NULL,
    size        int                      NOT NULL,
    report      bytea                    NOT NULL,
    report_hash bytea                    not null unique,
    downloaded  TIMESTAMP WITH TIME ZONE not null default NOW()
);

CREATE INDEX IF NOT EXISTS ix_station_report_station_id ON station_report (station_id);
CREATE INDEX IF NOT EXISTS ix_station_report_downloaded ON station_report (downloaded);
CREATE INDEX IF NOT EXISTS ix_station_report_downloaded_date ON station_report ((timezone('UTC', downloaded)::date));
ALTER TABLE station_report
    ADD CONSTRAINT fk_station_report_station_station_id
        FOREIGN KEY (station_id) REFERENCES station (station_id) ON UPDATE CASCADE;

drop table if exists wait_time_report;
CREATE TABLE IF NOT EXISTS wait_time_report
(
    station_id       TEXT NOT NULL,
    report_id        INT  NOT NULL,
    report_date      DATE NOT NULL,
    appointment_type TEXT NOT NULL,
    established      REAL,
    new              REAL,
    source           TEXT,
    PRIMARY KEY (station_id, report_date, appointment_type)
);

CREATE INDEX IF NOT EXISTS ix_wait_time_report_report_id ON wait_time_report (report_id);
CREATE INDEX IF NOT EXISTS ix_wait_time_report_date ON wait_time_report (report_date);
CREATE INDEX IF NOT EXISTS ix_wait_time_appointment_type ON wait_time_report (appointment_type);
ALTER TABLE wait_time_report
    ADD CONSTRAINT fk_wait_time_report_station_station_id
        FOREIGN KEY (station_id) REFERENCES station (station_id) ON UPDATE CASCADE;
ALTER TABLE wait_time_report
    ADD CONSTRAINT fk_wait_time_report_station_report_report_id
        FOREIGN KEY (report_id) REFERENCES station_report (report_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE;

drop table if exists satisfaction_report;
CREATE TABLE IF NOT EXISTS satisfaction_report
(
    station_id       TEXT NOT NULL,
    report_id        INT  NOT NULL,
    report_date      DATE NOT NULL,
    appointment_type TEXT NOT NULL,
    percent          DECIMAL(5, 2),
    PRIMARY KEY (station_id, report_date, appointment_type)
);

CREATE INDEX IF NOT EXISTS ix_satisfaction_report_report_id ON satisfaction_report (report_id);
CREATE INDEX IF NOT EXISTS ix_satisfaction_report_report_date ON satisfaction_report (report_date);
CREATE INDEX IF NOT EXISTS ix_satisfaction_report_appointment_type ON satisfaction_report (appointment_type);
ALTER TABLE satisfaction_report
    ADD CONSTRAINT fk_satisfaction_report_station_station_id
        FOREIGN KEY (station_id) REFERENCES station (station_id) ON UPDATE CASCADE;
ALTER TABLE satisfaction_report
    ADD CONSTRAINT fk_satisfaction_report_station_report_report_id
        FOREIGN KEY (report_id) REFERENCES station_report (report_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE;