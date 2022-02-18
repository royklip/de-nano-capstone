DROP TABLE IF EXISTS public.staging_airport;
CREATE TABLE IF NOT EXISTS public.staging_airport (
    ident varchar(7) NOT NULL,
    "type" varchar(256) NOT NULL,
    "name" varchar(256) NOT NULL,
    elevation_ft int,
    continent varchar(2),
    iso_country varchar(2),
    iso_region varchar(7),
    municipality varchar(256),
    gps_code varchar(4),
    iata_code varchar(3),
    local_code varchar(7),
	lattitude numeric(18,0),
	longitude numeric(18,0)
);

DROP TABLE IF EXISTS public.staging_temperature;
CREATE TABLE IF NOT EXISTS public.staging_temperature (
    dt date NOT NULL,
    AverageTemperature numeric(18,3) NOT NULL,
    AverageTemperatureUncertainty numeric(18,3) NOT NULL,
    City varchar(256) NOT NULL,
    Country varchar(256) NOT NULL,
    Latitude varchar(256) NOT NULL,
    Longitude varchar(256) NOT NULL
);

DROP TABLE IF EXISTS public.staging_cities;
CREATE TABLE IF NOT EXISTS public.staging_cities (
    City varchar(256) NOT NULL,
    "State" varchar(256) NOT NULL,
    "Median Age" numeric(3,1) NOT NULL,
    "Male Population" int,
    "Female Population" int,
    "Total Population" int NOT NULL,
    "Number of Veterans" int,
    "Foreign-born" varchar(256),
    "Average Household Size" numeric(3,2),
    "State Code" varchar(2) NOT NULL,
    Race varchar(256) NOT NULL,
    "Count" int NOT NULL
);

DROP TABLE IF EXISTS public.staging_immigration;
CREATE TABLE IF NOT EXISTS public.staging_immigration (
    cicid double precision,
    i94yr double precision,
    i94mon double precision,
    i94cit double precision,
    i94res double precision,
    i94port varchar(256),
    arrdate date,
    i94mode double precision,
    i94addr varchar(256),
    depdate date,
    i94bir double precision,
    i94visa double precision,
    "count" double precision,
    dtadfile varchar(256),
    visapost varchar(256),
    occup varchar(256),
    entdepa varchar(256),
    entdepd varchar(256),
    entdepu varchar(256),
    matflag varchar(256),
    biryear double precision,
    dtaddto varchar(256),
    gender varchar(256),
    insnum varchar(256),
    airline varchar(256),
    admnum double precision,
    fltno varchar(256),
    visatype varchar(256)
);

DROP TABLE IF EXISTS public.i94addrl;
CREATE TABLE IF NOT EXISTS public.i94addrl (
    code varchar(2),
    "name" varchar(256)
);

DROP TABLE IF EXISTS public.i94cntyl;
CREATE TABLE IF NOT EXISTS public.i94cntyl (
    code int,
    "name" varchar(256)
);

DROP TABLE IF EXISTS public.i94model;
CREATE TABLE IF NOT EXISTS public.i94model (
    code int,
    "name" varchar(256)
);

DROP TABLE IF EXISTS public.i94prtl;
CREATE TABLE IF NOT EXISTS public.i94prtl (
    code varchar(3),
    "name" varchar(256)
);

DROP TABLE IF EXISTS public.i94visa;
CREATE TABLE IF NOT EXISTS public.i94visa (
    code int,
    "name" varchar(256)
);