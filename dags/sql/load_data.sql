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
FROM staging_airport sta