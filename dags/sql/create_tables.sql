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
    cicid varchar(256) NOT NULL,
    i94yr int NOT NULL,
    i94mon int NOT NULL,
    i94cit int NOT NULL,
    i94res int NOT NULL,
    i94port varchar(3) NOT NULL,
    arrdate varchar(256) NOT NULL,
    i94mode int NOT NULL,
    i94addr varchar(2),
    depdate int,
    i94bir int NOT NULL,
    i94visa int NOT NULL,
    "count" int NOT NULL,
    dtadfile varchar(256) NOT NULL,
    visapost varchar(3),
    occup varchar(3),
    entdepa varchar(1) NOT NULL,
    entdepd varchar(1),
    entdepu int,
    matflag varchar(1),
    biryear int NOT NULL,
    dtaddto int NOT NULL,
    gender varchar(1),
    insnum varchar(256),
    airline varchar(10),
    admnum int NOT NULL,
    fltno varchar(256),
    visatype varchar(2) NOT NULL
);