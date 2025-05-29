drop table if exists facility;
CREATE TABLE IF NOT EXISTS facility
(
    fid             SERIAL NOT NULL UNIQUE,
    station_id      TEXT   NOT NULL,
    facility        TEXT   NOT NULL,
    address         TEXT   NOT NULL,
    website         TEXT,
    mailing_address TEXT,
    state           TEXT   NOT NULL,
    phones          TEXT,
    geom            GEOMETRY,
    geoid           CHARACTER VARYING(4),
    created         TIMESTAMP WITH TIME ZONE default NOW(),
    updated         TIMESTAMP WITH TIME ZONE default NOW(),
    PRIMARY KEY (station_id, facility, address)
);

CREATE INDEX IF NOT EXISTS ix_facility_fid ON facility USING btree (fid);
CREATE INDEX IF NOT EXISTS ix_facility_station_id ON facility USING btree (station_id);
CREATE INDEX IF NOT EXISTS ix_facility_facility ON facility USING btree (facility);
CREATE INDEX IF NOT EXISTS ix_facility_website ON facility USING btree (website);
CREATE INDEX IF NOT EXISTS ix_facility_address ON facility USING btree (address);
CREATE INDEX IF NOT EXISTS ix_facility_mailing_address ON facility USING btree (mailing_address);
CREATE INDEX IF NOT EXISTS ix_facility_state ON facility USING btree (state);
CREATE INDEX IF NOT EXISTS ix_facility_geom ON facility USING gist (geom);
CREATE INDEX IF NOT EXISTS ix_facility_geoid ON facility USING btree (geoid);
CREATE INDEX ix_facility_facility_state ON facility ((facility || ' - ' || state));

ALTER TABLE facility
    ADD CONSTRAINT fk_facility_congress_geoid
        FOREIGN KEY (geoid) REFERENCES congress (geoid) ON UPDATE CASCADE;

drop table if exists facility_shuttered;
CREATE TABLE IF NOT EXISTS facility_shuttered
(
    fid             INT  NOT NULL,
    station_id      TEXT NOT NULL,
    facility        TEXT NOT NULL,
    address         TEXT NOT NULL,
    website         TEXT,
    mailing_address TEXT,
    state           TEXT NOT NULL,
    phones          TEXT,
    geom            GEOMETRY,
    geoid           CHARACTER VARYING(4),
    awol            TIMESTAMP WITH TIME ZONE default NOW(),
    created         TIMESTAMP WITH TIME ZONE default NOW(),
    updated         TIMESTAMP WITH TIME ZONE default NOW(),
    PRIMARY KEY (station_id, facility)
);
CREATE INDEX IF NOT EXISTS ix_facility_shuttered_fid ON facility_shuttered USING btree (fid);
CREATE INDEX IF NOT EXISTS ix_facility_shuttered_station_id ON facility_shuttered USING btree (station_id);
CREATE INDEX IF NOT EXISTS ix_facility_shuttered_station_id ON facility_shuttered USING btree (station_id);
CREATE INDEX IF NOT EXISTS ix_facility_shuttered_facility ON facility_shuttered USING btree (facility);
CREATE INDEX IF NOT EXISTS ix_facility_shuttered_website ON facility_shuttered USING btree (website);
CREATE INDEX IF NOT EXISTS ix_facility_shuttered_address ON facility_shuttered USING btree (address);
CREATE INDEX IF NOT EXISTS ix_facility_shuttered_state ON facility_shuttered USING btree (state);
CREATE INDEX IF NOT EXISTS ix_facility_shuttered_awol ON facility_shuttered USING btree (awol);
CREATE INDEX IF NOT EXISTS ix_facility_shuttered_geom ON facility_shuttered USING gist (geom);
CREATE INDEX IF NOT EXISTS ix_facility_shuttered_geoid ON facility_shuttered USING btree (geoid);

ALTER TABLE facility_shuttered
    ADD CONSTRAINT fk_facility_shuttered_congress_geoid
        FOREIGN KEY (geoid) REFERENCES congress (geoid) ON UPDATE CASCADE;

drop table if exists station cascade;
CREATE TABLE IF NOT EXISTS station
(
    fid            INT  NOT NULL UNIQUE,
    station_id     TEXT PRIMARY KEY,
    prefix         TEXT,
    legacy         BOOL NOT NULL            DEFAULT FALSE,
    active         BOOL,
    germane        BOOL NOT NULL            DEFAULT TRUE,
    awol           BOOL NOT NULL            DEFAULT FALSE,
    total_failures INT  NOT NULL            DEFAULT 0,
    last_report    TIMESTAMP WITH TIME ZONE,
    last_failure   TIMESTAMP WITH TIME ZONE,
    created        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated        TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_station_fid ON station USING btree (fid);
CREATE INDEX IF NOT EXISTS ix_station_prefix ON station USING btree (prefix);

ALTER TABLE station
    ADD CONSTRAINT ic_station_station_id_prefix_unique UNIQUE (station_id, prefix);

ALTER TABLE station
    ADD CONSTRAINT fk_station_facility_fid
        FOREIGN KEY (fid) REFERENCES facility (fid) ON UPDATE CASCADE;


drop table if exists station_legacy;
CREATE TABLE IF NOT EXISTS station_legacy
(
    station_id TEXT primary key
);

--
-- The Facility Updater job posts records to this table. It is used to
-- update the primary facility table as well as to post records
-- to facility_shuttered when a facility is closed.
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
    INSERT INTO station (fid, station_id)
    select distinct fid, station_id
    from facility
    order by station_id
    on conflict DO NOTHING;
    GET DIAGNOSTICS r_count := ROW_COUNT;
    raise notice '% new rows inserted', r_count;
END;
$body$;

CREATE OR REPLACE FUNCTION geocode(address text)
    RETURNS geometry
AS
$$
    import requests
    geom = None
    try:
        payload = {'address': address, 'benchmark': "8", 'format': 'json'}
        base_geocode = 'https://geocoding.geo.census.gov/geocoder/locations/onelineaddress'
        r = requests.get(base_geocode, params=payload)
        result = r.json()['result'] if r and r.json() and 'result' in r.json() else None
        if result and 'addressMatches' in result and result['addressMatches']:
            coords = r.json()['result']['addressMatches'][0]['coordinates']
            lon = coords['x']
            lat = coords['y']
            geom = f'SRID=4326;POINT({lon} {lat})'
        else:
            import os
            token = os.environ.get("MAPBOX_API")
            payload = {'q': address, 'access_token': token}
            base_geocode = 'https://api.mapbox.com/search/geocode/v6/forward'
            r = requests.get(base_geocode, params=payload)
            result = r.json()['features'][0] if r and r.json() and 'features' in r.json() else None
            if result and r.json()['features'] and 'geometry' in result and result['geometry']:
                coords = result['geometry']['coordinates']
                geom = f'SRID=4326;POINT({coords[0]} {coords[1]})'
    except Exception as e:
        plpy.notice(f'address failed: {address}')
        plpy.notice(f'error: {e}')
        geom = None
    return geom
$$
    LANGUAGE 'plpython3u';
