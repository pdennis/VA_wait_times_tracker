drop table if exists facility;
CREATE TABLE IF NOT EXISTS facility
(
    station_id      TEXT NOT NULL,
    facility        TEXT NOT NULL,
    address         TEXT NOT NULL,
    website         TEXT,
    mailing_address TEXT,
    state           TEXT NOT NULL,
    phones          TEXT,
    created         TIMESTAMP WITH TIME ZONE default NOW(),
    updated         TIMESTAMP WITH TIME ZONE default NOW(),
    PRIMARY KEY (station_id, facility, address)
);

CREATE INDEX IF NOT EXISTS ix_facility_station_id ON facility USING btree (station_id);
CREATE INDEX IF NOT EXISTS ix_facility_facility ON facility USING btree (facility);
CREATE INDEX IF NOT EXISTS ix_facility_website ON facility USING btree (website);
CREATE INDEX IF NOT EXISTS ix_facility_address ON facility USING btree (address);
CREATE INDEX IF NOT EXISTS ix_facility_mailing_address ON facility USING btree (mailing_address);
CREATE INDEX IF NOT EXISTS ix_facility_state ON facility USING btree (state);

drop table if exists facility_shuttered;
CREATE TABLE IF NOT EXISTS facility_shuttered
(
    station_id      TEXT NOT NULL,
    facility        TEXT NOT NULL,
    address         TEXT NOT NULL,
    website         TEXT,
    mailing_address TEXT,
    state           TEXT NOT NULL,
    phones          TEXT,
    awol            TIMESTAMP WITH TIME ZONE default NOW(),
    PRIMARY KEY (station_id, facility)
);

CREATE INDEX IF NOT EXISTS ix_facility_shuttered_station_id ON facility USING btree (station_id);
CREATE INDEX IF NOT EXISTS ix_facility_shuttered_facility ON facility_shuttered USING btree (facility);
CREATE INDEX IF NOT EXISTS ix_facility_shuttered_website ON facility_shuttered USING btree (website);
CREATE INDEX IF NOT EXISTS ix_facility_shuttered_address ON facility_shuttered USING btree (address);
CREATE INDEX IF NOT EXISTS ix_facility_shuttered_state ON facility_shuttered USING btree (state);
CREATE INDEX IF NOT EXISTS ix_facility_shuttered_awol ON facility_shuttered USING btree (awol);

drop table if exists station;
CREATE TABLE IF NOT EXISTS station
(
    station_id     TEXT primary key,
    prefix         text unique,
    active         bool,
    awol           bool                     default False,
    total_failures int                      default 0,
    last_report    TIMESTAMP WITH TIME ZONE,
    last_failure   TIMESTAMP WITH TIME ZONE,
    created        TIMESTAMP WITH TIME ZONE default NOW(),
    updated        TIMESTAMP WITH TIME ZONE default NOW()
);

--
-- The Facility Updater job posts records to this table. It is used to
-- update the primary facility table as well as to post records
-- to facility_Shuttered when a facility is closed.
--
drop table if exists facility_staging;
CREATE TABLE IF NOT EXISTS facility_staging
(
    station_id         TEXT NOT NULL,
    facility           TEXT NOT NULL,
    website            TEXT,
    address            TEXT,
    state              TEXT NOT NULL,
    phone              TEXT,
    mailing_address    TEXT,
    normalized_address TEXT,
    address_parts      tiger.norm_addy,
    phones             text[]
);

CREATE INDEX IF NOT EXISTS ix_facility_staging_sid_facility ON facility_staging USING btree (station_id, facility);
CREATE INDEX IF NOT EXISTS ix_facility_staging_station_id ON facility_staging USING btree (station_id);
CREATE INDEX IF NOT EXISTS ix_facility_staging_facility ON facility_staging USING btree (facility);
CREATE INDEX IF NOT EXISTS ix_facility_staging_website ON facility_staging USING btree (website);
CREATE INDEX IF NOT EXISTS ix_facility_staging_address ON facility_staging USING btree (address);
CREATE INDEX IF NOT EXISTS ix_facility_staging_state ON facility_staging USING btree (state);

CREATE OR REPLACE FUNCTION strip_mailing_address(str text) RETURNS text
    RETURNS NULL ON NULL INPUT
AS
$body$
DECLARE
    v_token_pos int;
    v_str       text;
BEGIN
    v_token_pos = position('Mailing Address: ' in str);
    if v_token_pos > 0 then
        v_str := trim(substring(str, 1, v_token_pos - 1));
        if RIGHT(v_str, 1) = ',' then
            v_str := LEFT(v_str, LENGTH(v_str) - 1);
        end if;
    else
        v_str := str;
    end if;

    return trim(v_str);
END;
$body$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION extract_mailing_address(str text) RETURNS text
    RETURNS NULL ON NULL INPUT
AS
$body$
DECLARE
    v_token_pos int;
    v_str       text;
BEGIN
    v_token_pos = position('Mailing Address: ' in str);
    if v_token_pos > 0 then
        v_str := trim(substring(str, v_token_pos + length('Mailing Address: ')));
    else
        v_str := NULL;
    end if;

    return v_str;
END;
$body$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION extract_phones(str text) RETURNS text
    RETURNS NULL ON NULL INPUT
AS
$body$
DECLARE
    v_token_pos int;
    v_str       text;
BEGIN
    v_token_pos = position('Mailing Address: ' in str);
    if v_token_pos > 0 then
        v_str := trim(substring(str, v_token_pos + length('Mailing Address: ')));
    else
        v_str := NULL;
    end if;

    return v_str;
END;
$body$ LANGUAGE plpgsql;

CREATE or replace AGGREGATE array_accum (anycompatiblearray)
    (
    sfunc = array_cat,
    stype = anycompatiblearray,
    initcond = '{}'
    );

create or replace function public.array_merge(arr1 anyarray, arr2 anyarray)
    returns anyarray
    language sql
    immutable
as
$$
select array_agg(distinct elem order by elem)
from (select unnest(arr1) elem
      union
      select unnest(arr2)) s
$$;

create or replace aggregate array_merge_agg(anyarray) (
    sfunc = array_merge,
    stype = anyarray
    );

CREATE OR REPLACE PROCEDURE update_facilities()
    LANGUAGE plpgsql
AS
$body$
DECLARE
    r_count integer;
BEGIN
    INSERT INTO facility (station_id,
                          facility,
                          address,
                          website,
                          mailing_address,
                          state,
                          phones)
    select station_id,
           facility,
           coalesce(min(address), '')                              as address,
           min(website)                                            as website,
           string_agg(distinct mailing_address, ', ')              as mailing_address,
           min(state)                                              as state,
           array_to_string(array_merge_agg(distinct phones), ', ') as phones
    from facility_staging
    group by station_id, facility, normalized_address
    on conflict (station_id, facility, address)
        do update
        set website         = EXCLUDED.website,
            phones          = EXCLUDED.phones,
            mailing_address = EXCLUDED.mailing_address,
            updated         = now();
    GET DIAGNOSTICS r_count := ROW_COUNT;
    raise notice '% new rows inserted', r_count;

END;
$body$;

CREATE OR REPLACE PROCEDURE update_stations()
    LANGUAGE plpgsql
AS
$body$
DECLARE
    r_count integer;
BEGIN
    INSERT INTO station (station_id)
    select distinct station_id
    from facility
    order by station_id
    on conflict DO NOTHING;
    GET DIAGNOSTICS r_count := ROW_COUNT;
    raise notice '% new rows inserted', r_count;
END;
$body$;