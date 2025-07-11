drop table if exists station_report CASCADE;
CREATE TABLE IF NOT EXISTS station_report
(
    report_id   SERIAL PRIMARY KEY,
    station_id  TEXT                     NOT NULL,
    file_name   text                     NOT NULL,
    size        int                      NOT NULL,
    report      bytea                    NOT NULL,
    report_hash bytea                    NOT NULL UNIQUE,
    downloaded  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_station_report_station_id ON station_report (station_id);
CREATE INDEX IF NOT EXISTS ix_station_report_downloaded ON station_report (downloaded);
CREATE INDEX IF NOT EXISTS ix_station_report_downloaded_date ON station_report ((timezone('UTC', downloaded)::date));
ALTER TABLE station_report
    ADD CONSTRAINT fk_station_report_station_station_id
        FOREIGN KEY (station_id) REFERENCES station (station_id) ON UPDATE CASCADE;

drop view if exists last_station_report;
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
    PRIMARY KEY (station_id, report_id, report_date, appointment_type)
);

CREATE INDEX IF NOT EXISTS ix_wait_time_report_station_id ON wait_time_report (station_id);
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

drop view if exists last_station_report;
create or replace view last_station_report
as
select station_id,
       min(report_date)            as first_report_date,
       max(report_date)            as last_report_date,
       count(distinct report_date) as total_reports
from wait_time_report
group by station_id
order by station_id, last_report_date desc;

drop view if exists delinquent_stations;
create or replace view delinquent_stations
as
select w.station_id,
       w.last_report_date,
       extract(day from (now() - w.last_report_date)) as delinquence,
       w.total_reports,
       f.state,
       c.cd119fp                                      as district,
       f.facility,
       f.website
from (select station_id,
             max(report_date)            as last_report_date,
             count(distinct report_date) as total_reports
      from wait_time_report
      group by station_id
      order by station_id, last_report_date desc) w,
     station s,
     facility f,
     congress c
where w.last_report_date < (now() - interval '7 days')
  and s.station_id = w.station_id
  and f.fid = s.fid
  and c.geoid = f.geoid
order by last_report_date desc, state, station_id;

drop view if exists delinquent_station_appointments;
create or replace view delinquent_station_appointments
as
select w.station_id,
       w.appointment_type,
       extract(day from (now() - w.last_reported)) as delinquence,
       w.last_reported,
       w.total_reports,
       f.state,
       c.cd119fp                                   as district,
       f.facility,
       f.website
from station_appointment_type w,
     station s,
     facility f,
     congress c
where w.last_reported < (now() - interval '7 days')
  and s.station_id = w.station_id
  and f.fid = s.fid
  and c.geoid = f.geoid
order by w.last_reported desc, state, appointment_type;

drop view if exists wait_time_report_7_v;
drop table if exists wait_time_report_7;
CREATE TABLE IF NOT EXISTS wait_time_report_7
(
    station_id        TEXT NOT NULL,
    report_id         INT  NOT NULL,
    report_date       DATE NOT NULL,
    appointment_type  TEXT NOT NULL,
    established_avg   REAL,
    established_std   REAL,
    established_count INT,
    established_sum   REAL,
    established_sumx2 REAL,
    established_min   REAL,
    established_max   REAL,
    established_q1    REAL,
    established_q2    REAL,
    established_q3    REAL,
    new_avg           REAL,
    new_std           REAL,
    new_count         INT,
    new_sum           REAL,
    new_sumx2         REAL,
    new_min           REAL,
    new_max           REAL,
    new_q1            REAL,
    new_q2            REAL,
    new_q3            REAL,
    PRIMARY KEY (station_id, report_id, report_date, appointment_type)
);

CREATE INDEX IF NOT EXISTS ix_wait_time_report_7_station_id ON wait_time_report_7 (station_id);
CREATE INDEX IF NOT EXISTS ix_wait_time_report_7_report_id ON wait_time_report_7 (report_id);
CREATE INDEX IF NOT EXISTS ix_wait_time_report_7_date ON wait_time_report_7 (report_date);
CREATE INDEX IF NOT EXISTS ix_wait_time_appointment_7_type ON wait_time_report_7 (appointment_type);
ALTER TABLE wait_time_report_7
    ADD CONSTRAINT fk_wait_time_report_7_station_station_id
        FOREIGN KEY (station_id) REFERENCES station (station_id) ON UPDATE CASCADE;
ALTER TABLE wait_time_report_7
    ADD CONSTRAINT fk_wait_time_report_7_station_report_report_id
        FOREIGN KEY (report_id) REFERENCES station_report (report_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE;

drop view if exists wait_time_report_7_v;
create or replace view wait_time_report_7_v
as
select station_id,
       report_id,
       report_date,
       extract(isodow from report_date) as report_dow,
       appointment_type,
       established_avg                  as established,
       established_std,
       established_q2,
       new_avg                          as new,
       new_std,
       new_q2
from wait_time_report_7;

drop view if exists wait_time_report_28_v;
drop table if exists wait_time_report_28;
CREATE TABLE IF NOT EXISTS wait_time_report_28
(
    station_id        TEXT NOT NULL,
    report_id         INT  NOT NULL,
    report_date       DATE NOT NULL,
    appointment_type  TEXT NOT NULL,
    established_avg   REAL,
    established_std   REAL,
    established_count INT,
    established_sum   REAL,
    established_sumx2 REAL,
    established_min   REAL,
    established_max   REAL,
    established_q1    REAL,
    established_q2    REAL,
    established_q3    REAL,
    new_avg           REAL,
    new_std           REAL,
    new_count         INT,
    new_sum           REAL,
    new_sumx2         REAL,
    new_min           REAL,
    new_max           REAL,
    new_q1            REAL,
    new_q2            REAL,
    new_q3            REAL,
    PRIMARY KEY (station_id, report_id, report_date, appointment_type)
);

CREATE INDEX IF NOT EXISTS ix_wait_time_report_28_station_id ON wait_time_report_28 (station_id);
CREATE INDEX IF NOT EXISTS ix_wait_time_report_28_report_id ON wait_time_report_28 (report_id);
CREATE INDEX IF NOT EXISTS ix_wait_time_report_28_report_date ON wait_time_report_28 (report_date);
CREATE INDEX IF NOT EXISTS ix_wait_time_report_28_appointment_type ON wait_time_report_28 (appointment_type);
ALTER TABLE wait_time_report_28
    ADD CONSTRAINT fk_wait_time_report_28_station_station_id
        FOREIGN KEY (station_id) REFERENCES station (station_id) ON UPDATE CASCADE;
ALTER TABLE wait_time_report_28
    ADD CONSTRAINT fk_wait_time_report_28_station_report_report_id
        FOREIGN KEY (report_id) REFERENCES station_report (report_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE;

drop view if exists wait_time_report_28_v;
create or replace view wait_time_report_28_v
as
select station_id,
       report_id,
       report_date,
       extract(isodow from report_date) as report_dow,
       appointment_type,
       established_avg                  as established,
       established_std,
       established_q2,
       new_avg                          as new,
       new_std,
       new_q2
from wait_time_report_28;

drop view if exists wait_time_report_90_v;
drop table if exists wait_time_report_90;
CREATE TABLE IF NOT EXISTS wait_time_report_90
(
    station_id        TEXT NOT NULL,
    report_id         INT  NOT NULL,
    report_date       DATE NOT NULL,
    appointment_type  TEXT NOT NULL,
    established_avg   REAL,
    established_std   REAL,
    established_count INT,
    established_sum   REAL,
    established_sumx2 REAL,
    established_min   REAL,
    established_max   REAL,
    established_q1    REAL,
    established_q2    REAL,
    established_q3    REAL,
    new_avg           REAL,
    new_std           REAL,
    new_count         INT,
    new_sum           REAL,
    new_sumx2         REAL,
    new_min           REAL,
    new_max           REAL,
    new_q1            REAL,
    new_q2            REAL,
    new_q3            REAL,
    PRIMARY KEY (station_id, report_id, report_date, appointment_type)
);

CREATE INDEX IF NOT EXISTS ix_wait_time_report_90_station_id ON wait_time_report_90 (station_id);
CREATE INDEX IF NOT EXISTS ix_wait_time_report_90_report_id ON wait_time_report_90 (report_id);
CREATE INDEX IF NOT EXISTS ix_wait_time_report_90_report_date ON wait_time_report_90 (report_date);
CREATE INDEX IF NOT EXISTS ix_wait_time_report_90_appointment_type ON wait_time_report_90 (appointment_type);
ALTER TABLE wait_time_report_90
    ADD CONSTRAINT fk_wait_time_report_90_station_station_id
        FOREIGN KEY (station_id) REFERENCES station (station_id) ON UPDATE CASCADE;
ALTER TABLE wait_time_report_90
    ADD CONSTRAINT fk_wait_time_report_90_station_report_report_id
        FOREIGN KEY (report_id) REFERENCES station_report (report_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE;

create or replace view wait_time_report_90_v
as
select station_id,
       report_id,
       report_date,
       appointment_type,
       established_avg as established,
       established_std,
       established_q2,
       new_avg         as new,
       new_std,
       new_q2
from wait_time_report_90;

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


drop table if exists station_appointment_type;
CREATE TABLE IF NOT EXISTS station_appointment_type
(
    station_id       TEXT NOT NULL,
    appointment_type TEXT NOT NULL,
    total_reports    INT  NOT NULL,
    first_reported   DATE NOT NULL,
    last_reported    DATE NOT NULL,
    PRIMARY KEY (station_id, appointment_type)
);

CREATE INDEX IF NOT EXISTS ix_station_appointment_type_station_id ON station_appointment_type (station_id);
CREATE INDEX IF NOT EXISTS ix_station_appointment_type_appointment_type ON station_appointment_type (appointment_type);

ALTER TABLE station_appointment_type
    ADD CONSTRAINT fk_station_appointment_type_station_id
        FOREIGN KEY (station_id) REFERENCES station (station_id) ON UPDATE CASCADE;
