CREATE TABLE IF NOT EXISTS facilities
(
    station_id      TEXT NOT NULL,
    facility        TEXT NOT NULL,
    website         TEXT,
    address         TEXT NOT NULL,
    mailing_address TEXT,
    state           TEXT NOT NULL,
    phone           TEXT,
    created         TIMESTAMP WITH TIME ZONE default NOW(),
    updated         TIMESTAMP WITH TIME ZONE default NOW(),
    reporting       BOOLEAN,
    last_report     TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (station_id, facility)
);

CREATE INDEX IF NOT EXISTS ix_facilities_station_id ON facilities USING btree (station_id);
CREATE INDEX IF NOT EXISTS ix_facilities_facility ON facilities USING btree (facility);
CREATE INDEX IF NOT EXISTS ix_facilities_website ON facilities USING btree (website);
CREATE INDEX IF NOT EXISTS ix_facilities_address ON facilities USING btree (address);
CREATE INDEX IF NOT EXISTS ix_facilities_state ON facilities USING btree (state);
CREATE INDEX IF NOT EXISTS ix_facilities_last_report ON facilities USING btree (last_report);

CREATE TABLE IF NOT EXISTS facilities_shuttered
(
    station_id      TEXT NOT NULL,
    facility        TEXT NOT NULL,
    website         TEXT,
    address         TEXT NOT NULL,
    mailing_address TEXT,
    state           TEXT NOT NULL,
    phone           TEXT,
    awol            TIMESTAMP WITH TIME ZONE default NOW(),
    reporting       BOOLEAN,
    last_report     TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (station_id, facility)
);

CREATE INDEX IF NOT EXISTS ix_facilities_shuttered_station_id ON facilities USING btree (station_id);
CREATE INDEX IF NOT EXISTS ix_facilities_shuttered_facility ON facilities_shuttered USING btree (facility);
CREATE INDEX IF NOT EXISTS ix_facilities_shuttered_website ON facilities_shuttered USING btree (website);
CREATE INDEX IF NOT EXISTS ix_facilities_shuttered_address ON facilities_shuttered USING btree (address);
CREATE INDEX IF NOT EXISTS ix_facilities_shuttered_state ON facilities_shuttered USING btree (state);
CREATE INDEX IF NOT EXISTS ix_facilities_shuttered_awol ON facilities_shuttered USING btree (awol);
CREATE INDEX IF NOT EXISTS ix_facilities_shuttered_last_report ON facilities_shuttered USING btree (last_report);

--
-- The Facilities Updater job posts records to this table. It is used to
-- update the primary Facilities table as well as to post records
-- to Facilities_Shuttered when facilities are closed.
--
CREATE TABLE IF NOT EXISTS facilities_staging
(
    station_id TEXT NOT NULL,
    facility   TEXT NOT NULL,
    website    TEXT,
    address    TEXT,
    state      TEXT NOT NULL,
    phone      TEXT
);

CREATE INDEX IF NOT EXISTS ix_facilities_staging_sid_facility ON facilities_staging USING btree (station_id, facility);
CREATE INDEX IF NOT EXISTS ix_facilities_staging_station_id ON facilities_staging USING btree (station_id);
CREATE INDEX IF NOT EXISTS ix_facilities_staging_facility ON facilities_staging USING btree (facility);
CREATE INDEX IF NOT EXISTS ix_facilities_staging_website ON facilities_staging USING btree (website);
CREATE INDEX IF NOT EXISTS ix_facilities_staging_address ON facilities_staging USING btree (address);
CREATE INDEX IF NOT EXISTS ix_facilities_staging_state ON facilities_staging USING btree (state);
