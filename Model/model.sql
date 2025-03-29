-- Create the Dim_Date table in PostgreSQL
CREATE TABLE model.Dim_Date (
    date DATE PRIMARY KEY,
    day_of_week VARCHAR(10),
    week_of_year INT,
    month INT,
    quarter INT,
    year INT
);

-- Populate the Dim_Date table with dates from 2020-01-01 to 2030-12-31
INSERT INTO model.Dim_Date (date, day_of_week, week_of_year, month, quarter, year)
SELECT
    d::DATE AS date,
    TO_CHAR(d, 'Day') AS day_of_week,
    EXTRACT(WEEK FROM d)::INT AS week_of_year,
    EXTRACT(MONTH FROM d)::INT AS month,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    EXTRACT(YEAR FROM d)::INT AS year
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, INTERVAL '1 day') AS d;



CREATE TABLE model.Dim_Practitioner (
    practitioner_dim_id SERIAL PRIMARY KEY,
    practitioner_id INT NOT NULL,
    practitioner_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    contract_type VARCHAR(50),
    manager_name VARCHAR(100),
	location_id INT,
	location_name VARCHAR(100),
    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN DEFAULT TRUE
);



CREATE TABLE model.Dim_Item (
    item_id SERIAL PRIMARY KEY,
    item_name VARCHAR(100) NOT NULL,
    department VARCHAR(100),
    category VARCHAR(100)
);


-- Create the Dim_Location table in PostgreSQL
CREATE TABLE model.Dim_Location (
    location_id SERIAL PRIMARY KEY,
    location_name VARCHAR(100) NOT NULL,
    address VARCHAR(255)
);


CREATE TABLE model.Fact_Performance (
    date DATE NOT NULL,
    practitioner_dim_id INT NOT NULL,
    location_id INT NOT NULL,
    target_hour DECIMAL(5, 1),
    actual_hour DECIMAL(5, 1),
    total_billing DECIMAL(10, 1)
);


CREATE TABLE model.Fact_Appointments (
    date DATE NOT NULL,
    practitioner_dim_id INT NOT NULL,
    location_id INT NOT NULL,
	item_id INT,
	number_appoiments INT,
    actual_hour DECIMAL(5, 1),
    total_billing DECIMAL(10, 1)
);





INSERT INTO model.Dim_Location (location_name)
SELECT DISTINCT location
FROM public.input
WHERE location IS NOT NULL;


INSERT INTO model.Dim_Item (item_name)
SELECT DISTINCT item
FROM public.input
WHERE item IS NOT NULL;


truncate table model.Dim_Practitioner;
INSERT INTO model.Dim_Practitioner (
    practitioner_id,
    practitioner_name,
    email,
    contract_type,
    manager_name,
    location_id,
    location_name,
    is_current
)
SELECT DISTINCT
    "Staff ID"::INT AS practitioner_id,
    "Staff name" AS practitioner_name,
    LOWER(REPLACE("Staff name", ' ', '.')) || '@springboard.ca' AS email,
    NULL AS contract_type,  -- Assuming contract type is not provided
    manager AS manager_name,
    loc.location_id,        -- Joining to get the location_id from Dim_Location
    loc.location_name,      -- Joining to get the location_name from Dim_Location
    TRUE AS is_current
FROM public.input AS i
JOIN model.Dim_Location AS loc ON i.location = loc.location_name
WHERE "Staff ID" IS NOT NULL;


TRUNCATE TABLE model.Fact_Performance;
INSERT INTO model.Fact_Performance (date, practitioner_dim_id, location_id, target_hour, actual_hour, total_billing)
SELECT
    aggregated_input.date,
    p.practitioner_dim_id,
    l.location_id,
    COALESCE(SUM(t.target_hour), 0) AS target_hour,      -- Use target hours if available, else default to 0
    COALESCE(SUM(aggregated_input.total_actual_hour), 0) AS actual_hour,  -- Pre-aggregated actual hours
    COALESCE(SUM(aggregated_input.total_billing), 0) AS total_billing     -- Pre-aggregated billing total
FROM
    (
        -- Aggregate input data to get actual hours and billing at the daily level for each practitioner and location
        SELECT
            i."Purchase Date" AS date,
            CAST(i."Staff ID" AS INT) AS practitioner_id,
            i.location,
            SUM(i."Actual hour") AS total_actual_hour,    -- Sum of actual hours
            SUM(i.total) AS total_billing                 -- Sum of total billing
        FROM
            public.input AS i
        GROUP BY
            i."Purchase Date",
            CAST(i."Staff ID" AS INT),
            i.location
    ) AS aggregated_input
JOIN
    model.Dim_Practitioner AS p ON aggregated_input.practitioner_id = p.practitioner_id
    AND p.is_current = TRUE  -- Only join with current practitioner records
JOIN
    model.Dim_Location AS l ON aggregated_input.location = l.location_name
LEFT JOIN
    public.target AS t ON aggregated_input.practitioner_id = CAST(t."Staff ID" AS INT) 
                        AND aggregated_input.date = t.appt_date
GROUP BY
    aggregated_input.date,
    p.practitioner_dim_id,
    l.location_id;
	
	
TRUNCATE TABLE model.Fact_Appointments;	
INSERT INTO model.Fact_Appointments (date, practitioner_dim_id, location_id, item_id, number_appoiments, actual_hour, total_billing)
SELECT
    i."Purchase Date" AS date,
    p.practitioner_dim_id,
    l.location_id,
    it.item_id,
    COUNT(i.item) AS number_appointments,               -- Count of appointments for each item
    COALESCE(SUM(i."Actual hour"), 0) AS actual_hour,   -- Sum of actual hours
    COALESCE(SUM(i.total), 0) AS total_billing          -- Sum of total billing
FROM
    public.input AS i
JOIN
    model.Dim_Practitioner AS p ON CAST(i."Staff ID" AS INT) = p.practitioner_id
    AND p.is_current = TRUE  -- Only include current practitioner records
JOIN
    model.Dim_Location AS l ON i.location = l.location_name
JOIN
    model.Dim_Item AS it ON i.item = it.item_name
GROUP BY
    i."Purchase Date",
    p.practitioner_dim_id,
    l.location_id,
    it.item_id;