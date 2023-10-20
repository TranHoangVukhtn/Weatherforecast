-- create user table
CREATE TABLE IF NOT EXISTS users (
    firstname TEXT NOT NULL,
    lastname  TEXT NOT NULL,
    country TEXT NOT NULL,
    username TEXT NOT NULL,
    password TEXT NOT NULL,
    email TEXT NOT NULL
);


-- CREATE TABLE IF NOT EXISTS location_tmp (
--     latitude VARCHAR(255) NOT NULL,
--     longitude VARCHAR(255) NOT NULL,
--     city VARCHAR(255) NULL,
--     state VARCHAR(255) NULL,
--     postcode VARCHAR(255) NULL,
--     country VARCHAR(255) NULL,
--     PRIMARY KEY (latitude,longitude)
-- );

-- CREATE TABLE IF NOT EXISTS location (
--     latitude VARCHAR(255) NOT NULL,
--     longitude VARCHAR(255) NOT NULL,
--     city VARCHAR(255) NULL,
--     state VARCHAR(255) NULL,
--     postcode VARCHAR(255) NULL,
--     country VARCHAR(255) NULL,
--     PRIMARY KEY (latitude,longitude)
-- );


CREATE TABLE IF NOT EXISTS df_astro (
    date TEXT,
    sunrise TEXT,
    sunset TEXT,
    moonrise TEXT,
    moonset TEXT,
    moon_phase VARCHAR(20),
    moon_illumination INT
);


CREATE TABLE IF NOT EXISTS df_hour (
    time_epoch INT,
    time TEXT,
    temp_c DECIMAL(5, 2),
    temp_f DECIMAL(5, 2),
    is_day BOOLEAN,
    condition VARCHAR(50),
    wind_mph DECIMAL(5, 2),
    wind_kph DECIMAL(5, 2),
    wind_degree INT,
    wind_dir VARCHAR(20),
    pressure_mb DECIMAL(6, 2),
    pressure_in DECIMAL(6, 2),
    precip_mm DECIMAL(6, 2),
    precip_in DECIMAL(6, 2),
    humidity DECIMAL(5, 2),
    cloud INT,
    feelslike_c DECIMAL(5, 2),
    feelslike_f DECIMAL(5, 2),
    windchill_c DECIMAL(5, 2),
    windchill_f DECIMAL(5, 2),
    heatindex_c DECIMAL(5, 2),
    heatindex_f DECIMAL(5, 2),
    dewpoint_c DECIMAL(5, 2),
    dewpoint_f DECIMAL(5, 2),
    will_it_rain BOOLEAN,
    chance_of_rain INT,
    will_it_snow BOOLEAN,
    chance_of_snow INT,
    vis_km DECIMAL(5, 2),
    vis_miles DECIMAL(5, 2),
    gust_mph DECIMAL(5, 2),
    gust_kph DECIMAL(5, 2),
    uv INT
);



CREATE TABLE IF NOT EXISTS df_day (
    date DATE,
    maxtemp_c DECIMAL(4, 1),
    maxtemp_f DECIMAL(4, 1),
    mintemp_c DECIMAL(4, 1),
    mintemp_f DECIMAL(4, 1),
    avgtemp_c DECIMAL(4, 1),
    avgtemp_f DECIMAL(4, 1),
    maxwind_mph DECIMAL(4, 1),
    maxwind_kph DECIMAL(4, 1),
    totalprecip_mm DECIMAL(5, 2),
    totalprecip_in DECIMAL(4, 2),
    avgvis_km DECIMAL(4, 1),
    avgvis_miles DECIMAL(4, 1),
    avghumidity DECIMAL(4, 1),
    condition VARCHAR(50),
    uv DECIMAL(4, 1)
);

