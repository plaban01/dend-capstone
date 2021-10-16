class SqlQueries:
    dim_country_insert = ("""
    INSERT INTO public.dim_country(country_code, country_name)
    SELECT country_code, country_name FROM public.staging_countries
    """)
    
    dim_states_insert = ("""
    INSERT INTO public.dim_States(state_code, state_name)
    SELECT state_code, state_name FROM public.staging_states
    """)
    
    dim_ports_insert = ("""
    INSERT INTO public.dim_Ports(state_id, port_code, port_name)
    SELECT ds.state_id, sp.port_code, sp.port_name
    FROM public.staging_ports sp LEFT OUTER JOIN public.dim_states ds
    on sp.state_code = ds.state_code
    """)
    
    dim_travelmodes_insert = ("""
    INSERT INTO public.dim_TravelModes(mode)
    SELECT DISTINCT travel_mode 
    FROM public.staging_immigration
    WHERE travel_mode NOT IN (
        SELECT mode FROM public.dim_TravelModes
    )
    """)
    
    dim_visatypes_insert = ("""
    INSERT INTO public.dim_VisaCategories(name)
    SELECT DISTINCT visa_category 
    FROM public.staging_immigration
    WHERE visa_category NOT IN (
        SELECT name FROM public.dim_VisaCategories
    )    
    """)
    
    dim_airport_types_insert = ("""
    INSERT INTO public.dim_AirportTypes (airport_type)
    SELECT DISTINCT type FROM public.staging_airports
    """)
    
    dim_time_insert = ("""
    INSERT INTO dim_time
    SELECT ts, 
        date_part('year', sas_ts) as year,
        date_part('month', sas_ts) as month,
        date_part('day', sas_ts) as day
            
    FROM
    (
        SELECT DISTINCT arrdate as ts, timestamp '1960-01-01 00:00:00 +00:00' + (arrdate * INTERVAL '1 day') as sas_ts 
        FROM staging_immigration
        UNION
        SELECT DISTINCT depdate as ts, timestamp '1960-01-01 00:00:00 +00:00' + (depdate * INTERVAL '1 day') as sas_ts 
        FROM staging_immigration
    )
    WHERE ts IS NOT NULL 
    AND ts NOT IN (
        SELECT ts FROM dim_time
    )
    """)
    
    dim_airports_insert = ("""
    INSERT INTO public.dim_Airports (
        airport_name, airport_type_id, state_id, continent, elevation_ft, 
        municipality, gps_code, iata_code, local_code, coordinates
    )
    SELECT sa.name as airport_name,
           da.airport_type_id,
           ds.state_id,
           sa.continent,
           sa.elevation_ft,
           sa.municipality,
           sa.gps_code,
           sa.iata_code,
           sa.local_code,
           sa.coordinates
    FROM public.staging_airports sa
    INNER JOIN public.dim_AirportTypes da
    ON sa.type = da.airport_type
    INNER JOIN public.dim_States ds
    ON ds.state_code = SPLIT_PART(sa.iso_region, '-', 2)
    WHERE sa.iso_country = 'US'
    """)
    
    fact_demographics_insert = ("""
    INSERT INTO fact_demographics (
        city_id, median_age, male_population, female_population, total_population, veterans,
        foreign_born, avg_household_size, race, count
    )
    SELECT dp.port_id as city_id,
           sd."Median Age" as median_age,
           sd."Male Population" as male_population,
           sd."Female Population" as female_population,
           sd."Total Population" as total_population,
           sd."Number of Veterans" as veterans,
           sd."Foreign-born" as foreign_born,
           sd."Average Household Size" as avg_household_size,
           sd.Race as race,
           sd.Count as count
    FROM public.staging_demographics sd
    INNER JOIN public.dim_Ports dp
    ON UPPER(sd.City) = dp.port_name
    """)
    
    fact_immigration_insert = ("""
    INSERT INTO fact_immigration(
        cicid, citizenship_country_id, residence_country_id, port_id, arrival_date, arrdate,
        departure_date, depdate, travel_mode_id, age, visa_category_id, visatype, airline, fltno
    )
    SELECT  si.cicid, 
            dc_cit.country_id as citizenship_country_id,
            dc_res.country_id as residence_country_id,
            dp.port_id,
            si.arrival_date,
            si.arrdate,
            si.departure_date,
            si.depdate,
            dt.travel_mode_id,
            si.i94bir as age,
            dv.visa_category_id,
            si.visatype,
            si.airline,
            si.fltno            
    FROM staging_immigration si
    INNER JOIN dim_Country dc_cit
    on si.i94cit = dc_cit.country_code
    INNER JOIN dim_Country dc_res
    on si.i94res = dc_res.country_code
    INNER JOIN dim_Ports dp
    on si.i94port = dp.port_code
    INNER JOIN dim_TravelModes dt
    on si.travel_mode = dt.mode
    INNER JOIN dim_VisaCategories dv
    on si.visa_category = dv.name
    """)

    immigration_row_count = "SELECT COUNT(*) from fact_immigration WHERE arrival_date = TO_DATE('{execution_date}', 'YYYY-MM--DD')"
    
    row_count_query = "SELECT COUNT(*) FROM {table}"

    country_table_null_count = "SELECT COUNT(*) FROM dim_Country where country_name IS NULL"

    state_table_null_count = "SELECT COUNT(*) FROM dim_States where state_name IS NULL"

    ports_table_null_count = "SELECT COUNT(*) FROM dim_Ports where port_code IS NULL"

    travel_modes_null_count = "SELECT COUNT(*) FROM dim_TravelModes where mode IS NULL"

    visa_categories_null_count = "SELECT COUNT(*) FROM dim_VisaCategories where name IS NULL"

    airport_types_null_count = "SELECT COUNT(*) FROM dim_AirportTypes where airport_type IS NULL"

    time_table_null_count = "SELECT COUNT(*) FROM dim_time where ts IS NULL"

    fact_immigration_null_arrival_date_count = "SELECT COUNT(*) FROM fact_immigration where arrdate IS NULL"