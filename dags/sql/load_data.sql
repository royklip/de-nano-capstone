INSERT INTO airports 
SELECT sta.ident as airport_id,
    sta.type,
    sta.name,
    sta.elevation_ft,
    sta.continent,
    sta.iso_country as country_code,
    sta.state as state_code,
    sta.municipality,
    sta.gps_code,
    sta.iata_code as airport_code,
    sta.local_code,
	sta.lattitude,
	sta.longitude
FROM staging_airport sta;

INSERT INTO cities (city,
    state_code,
    median_age,
    male_pop,
    female_pop,
    total_pop,
    nr_veterans,
    foreign_born,
    avg_household,
    race,
    "count"
) SELECT stc.City as city,
    stc."State Code" as state_code,
    stc."Median Age" as median_age,
    stc."Male Population" as male_pop,
    stc."Female Population" as female_pop,
    stc."Total Population" as total_pop,
    stc."Number of Veterans" as nr_veterans,
    stc."Foreign-born" as foreign_born,
    stc."Average Household Size" as avg_household,
    stc.Race as race,
    stc."Count" as "count"
FROM staging_cities stc;