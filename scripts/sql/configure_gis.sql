--
-- This script configures the initial Bridge Postgresql Database.
-- As it destroys any existing databases named 'bridge', this
-- script is VERY DESTRUCTIVE!! You must explicitly uncomment
-- the drop and create commands if you want to recreate and
-- configure the database for any reason,
--
-- Or, you may run this script from the command line as is to
-- add GIS components to an existing database via the command:
-- psql bridge -f configure_gis.sql
--

-- DROP DATABASE IF EXISTS bridge;

-- CREATE DATABASE bridge;

-- \connect bridge;

create extension pg_prewarm;
CREATE EXTENSION postgis;
CREATE EXTENSION fuzzystrmatch;
CREATE EXTENSION postgis_tiger_geocoder;
CREATE EXTENSION address_standardizer;
CREATE EXTENSION address_standardizer_data_us;
SELECT set_geocode_setting('use_pagc_address_parser', 'true');
commit;
