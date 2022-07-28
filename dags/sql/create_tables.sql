DROP TABLE IF EXISTS public.staging_airport;
CREATE TABLE IF NOT EXISTS public.staging_airport (
    ident varchar(7) PRIMARY KEY,
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
	longitude numeric(18,0),
    "state" varchar(2)
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
    "Foreign-born" int,
    "Average Household Size" numeric(3,2),
    "State Code" varchar(2) NOT NULL,
    Race varchar(256) NOT NULL,
    "Count" int NOT NULL
);

DROP TABLE IF EXISTS public.staging_immigration;
CREATE TABLE IF NOT EXISTS public.staging_immigration (
    cicid int PRIMARY KEY,
    i94yr int,
    i94mon int,
    i94cit int,
    i94res int,
    i94port varchar(256),
    arrdate date,
    i94mode int,
    i94addr varchar(256),
    depdate date,
    i94bir int,
    i94visa int,
    "count" int,
    dtadfile varchar(256),
    visapost varchar(256),
    occup varchar(256),
    entdepa varchar(256),
    entdepd varchar(256),
    entdepu varchar(256),
    matflag varchar(256),
    biryear int,
    dtaddto varchar(256),
    gender varchar(256),
    insnum varchar(256),
    airline varchar(256),
    admnum int,
    fltno varchar(256),
    visatype varchar(256)
);

DROP TABLE IF EXISTS public.state_codes;
CREATE TABLE IF NOT EXISTS public.state_codes (
    state_code varchar(2) SORTKEY PRIMARY KEY,
    state_name varchar(256)
);

DROP TABLE IF EXISTS public.country_codes;
CREATE TABLE IF NOT EXISTS public.country_codes (
    country_code int SORTKEY PRIMARY KEY,
    country_name varchar(256)
);

DROP TABLE IF EXISTS public.mode_codes;
CREATE TABLE IF NOT EXISTS public.mode_codes (
    mode_code int SORTKEY PRIMARY KEY,
    mode_name varchar(256)
);

DROP TABLE IF EXISTS public.airport_codes;
CREATE TABLE IF NOT EXISTS public.airport_codes (
    airport_code varchar(3) SORTKEY PRIMARY KEY,
    airport_name varchar(256),
    state_code varchar(256)
);

DROP TABLE IF EXISTS public.visa_codes;
CREATE TABLE IF NOT EXISTS public.visa_codes (
    visa_code int SORTKEY PRIMARY KEY,
    visa_name varchar(256)
);

DROP TABLE IF EXISTS public.airports;
CREATE TABLE IF NOT EXISTS public.airports (
    airport_id varchar(7) SORTKEY PRIMARY KEY,
    "type" varchar(256) NOT NULL,
    "name" varchar(256) NOT NULL,
    elevation_ft int,
    continent varchar(2),
    country_code varchar(2),
    state_code varchar(7),
    municipality varchar(256),
    gps_code varchar(4),
    airport_code varchar(3),
    local_code varchar(7),
	lattitude numeric(18,0),
	longitude numeric(18,0)
);

DROP TABLE IF EXISTS public.cities;
CREATE TABLE IF NOT EXISTS public.cities (
    city_id INT IDENTITY(0,1) PRIMARY KEY,
    city varchar(256) NOT NULL,
    state_code varchar(2) SORTKEY NOT NULL,
    median_age numeric(3,1) NOT NULL,
    male_pop int,
    female_pop int,
    total_pop int NOT NULL,
    nr_veterans int,
    foreign_born int,
    avg_household numeric(3,2),
    race varchar(256) NOT NULL,
    "count" int NOT NULL
);

DROP TABLE IF EXISTS public.immigration;
CREATE TABLE IF NOT EXISTS public.immigration (
    immigration_id int DISTKEY PRIMARY KEY,
    "year" int,
    "month" int,
    cit_country_code int,
    res_country_code int,
    airport_code varchar(256),
    arrdate date,
    mode_code int,
    airport_state_code varchar(256),
    depdate date,
    age int,
    visa_code int,
    "count" int,
    visapost varchar(256),
    occup varchar(256),
    biryear int,
    gender varchar(256),
    insnum varchar(256),
    airline varchar(256),
    admnum int,
    fltno varchar(256),
    visatype varchar(256)
);