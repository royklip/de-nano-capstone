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

CREATE TABLE IF NOT EXISTS public.staging_temperature (
    dt date NOT NULL,
    AverageTemperature numeric(18,3) NOT NULL,
    AverageTemperatureUncertainty numeric(18,3) NOT NULL,
    City varchar(256) NOT NULL,
    Country varchar(256) NOT NULL,
    Latitude varchar(256) NOT NULL,
    Longitude varchar(256) NOT NULL
);

CREATE TABLE IF NOT EXISTS public.staging_cities (
    City varchar(256) NOT NULL,
    "State" varchar(256) NOT NULL,
    "Median Age" numeric(3,1) NOT NULL,
    "Male Population" int NOT NULL,
    "Female Population" int NOT NULL,
    "Total Population" int NOT NULL,
    "Number of Veterans" int NOT NULL,
    "Foreign-born" varchar(256) NOT NULL,
    "Average Household Size" numeric(3,2) NOT NULL,
    "State Code" varchar(2) NOT NULL,
    Race varchar(256) NOT NULL,
    "Count" int NOT NULL
);