CREATE TABLE public.staging_countries (
    country_code varchar(3),
    country_name varchar(100)
);

CREATE TABLE public.staging_states (
    state_code varchar(25),
    state_name varchar(100)
);

CREATE TABLE public.staging_ports (
    port_code varchar(3),
    port_name varchar(100),
    state_code varchar(25)
);

CREATE TABLE public.staging_demographics (
    city varchar(200),
    state varchar(200),
    "Median Age" decimal,
    "Male Population" int,
    "Female Population" int,
    "Total Population" int,
    "Number of Veterans" int,
    "Foreign-born" int,
    "Average Household Size" decimal,
    "Stage Code" varchar(2),
    Race varchar(200),
    Count int
);

CREATE TABLE public.staging_airports (
    ident varchar(15),
    type varchar(30),
    name varchar(200),
    elevation_ft numeric(18,0),
    continent varchar(2),
    iso_country varchar(2),
    iso_region varchar(15),
    municipality varchar(100),
    gps_code varchar(15),
    iata_code varchar(15),
    local_code varchar(15),
    coordinates varchar(500)
);

CREATE TABLE public.staging_temperatures (
    dt varchar(10),
    AverageTemperature decimal,
    AverageTemperatureUncertainty decimal,
    City varchar(100),
    Country varchar(100),
    Latitude varchar(100),
    Longitude varchar(100)
);

CREATE TABLE public.staging_immigration (
    cicid double precision,
    arrdate double precision,
    arrival_date date,
    i94cit integer,
    i94res integer,
    i94port varchar(3),
    travel_mode varchar(25),
    i94addr varchar(5),
    depdate double precision,
    departure_date date,
    i94bir integer,
    visa_category varchar(25),
    biryear integer,
    gender varchar(1),
    airline varchar(5),
    fltno varchar(30),
    visatype varchar(5)
);

CREATE TABLE public.dim_Country (
    country_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    country_code varchar(3) NOT NULL UNIQUE,
    country_name varchar(100) NOT NULL UNIQUE
);

CREATE TABLE public.dim_States (
    state_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state_code varchar(2) NOT NULL UNIQUE,
    state_name varchar(100) NOT NULL UNIQUE
);

CREATE TABLE public.dim_Ports (
    port_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state_id int REFERENCES dim_States(state_id),
    port_code varchar(3) NOT NULL UNIQUE,
    port_name varchar(100) UNIQUE
);

CREATE TABLE public.dim_TravelModes (
    travel_mode_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mode varchar(25) NOT NULL
);

CREATE TABLE public.dim_VisaCategories (
    visa_category_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name varchar(50) NOT NULL
);

CREATE TABLE public.dim_AirportTypes (
    airport_type_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    airport_type varchar(50) NOT NULL
);

CREATE TABLE dim_time (
    ts int NOT NULL UNIQUE PRIMARY KEY,
    year int NOT NULL,
    month int NOT NULL,
    day int NOT NULL
);

CREATE TABLE public.dim_Airports (
    airport_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    airport_name varchar(200),
    airport_type_id int REFERENCES dim_AirportTypes(airport_type_id),
    state_id int REFERENCES dim_States(state_id),
    continent varchar(50),
    elevation_ft int,
    municipality varchar(100),
    gps_code varchar(25),
    iata_code varchar(25),
    local_code varchar(25),
    coordinates varchar(256)
);

CREATE TABLE fact_demographics (
    demographics_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    city_id int REFERENCES dim_Ports(port_id),
    median_age decimal,
    male_population int,
    female_population int,
    total_population int,
    veterans int,
    foreign_born int,
    avg_household_size decimal,
    race varchar(100),
    count int
);

CREATE TABLE fact_immigration (
    immigration_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cicid double precision,
    citizenship_country_id int REFERENCES dim_Country(country_id),
    residence_country_id int REFERENCES dim_Country(country_id),
    port_id int REFERENCES dim_Ports(port_id),
    arrival_date date NOT NULL,
    arrdate double precision NOT NULL,
    departure_date date,
    depdate double precision,
    travel_mode_id int REFERENCES dim_TravelModes(travel_mode_id),
    age integer,
    gender varchar(1),
    visa_category_id int REFERENCES dim_VisaCategories(visa_category_id),
    visatype varchar(10),
    airline varchar(5),
    fltno varchar(10)
);