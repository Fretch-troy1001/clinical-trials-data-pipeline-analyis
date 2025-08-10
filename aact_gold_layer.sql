--==================================================================
--  Creating the gold schema for the GOLD LAYER
--==================================================================
CREATE SCHEMA IF  NOT EXISTS gold;

--==================================================================
--  Creating the dim_studies for the GOLD LAYER
--==================================================================
/*
dim_studies will contain ll the textual, descriptive information about a trial 
that doesn't change or is not a numeric measure.
this will come from the silver.studies.

Key Column:

`study_key`
	-A new surrogate key (a unique integer) that we will generate to serve as the primary key for this table.

`nct_id`
	-The "natural" or "business" key from the source system, kept for reference and readability
*/

DROP TABLE IF EXISTS gold.dim_studies;

CREATE TABLE gold.dim_studies AS
SELECT
	ROW_NUMBER() OVER(ORDER BY nct_id) AS study_key,
	nct_id,
	brief_title,
	official_title, 
	acronym,
	study_type,
	limitations_and_caveats, 
	overall_status,
	phase,
	why_stopped,
	why_stopped_category,
	has_dmc,
	is_fda_regulated_drug, 
	is_fda_regulated_device,
	patient_registry
FROM silver.studies;

-- Set the study_key as primary key
ALTER TABLE gold.dim_studies
ADD PRIMARY KEY (study_key);


SELECT COUNT(*)
FROM gold.dim_studies
GROUP BY study_key
ORDER BY study_key DESC;



--==============================================
--  Creating the dim_sponsors for the GOLD LAYER
--==============================================


DROP TABLE IF EXISTS gold.dim_sponsors;

CREATE TABLE gold.dim_sponsors AS
SELECT
    -- Assign a unique, sequential number to each row from the clean list below.
    ROW_NUMBER() OVER (ORDER BY clean_sponsor_name) AS sponsor_key,
    clean_sponsor_name AS sponsor_name,
    sponsor_category
FROM (
    -- Get a unique list of every sponsor and their category.
    SELECT DISTINCT
        clean_sponsor_name,
        sponsor_category
    FROM
        silver.sponsors
) AS distinct_sponsors;

-- Add the primary key constraint to the new surrogate key
ALTER TABLE gold.dim_sponsors
ADD PRIMARY KEY (sponsor_key);



--==============================================
--  Creating the dim_dates for the GOLD LAYER
--==============================================

--  Drop the table if it already exists (for rerun safety)
DROP TABLE IF EXISTS gold.dim_dates;

-- Create the dim_dates table
CREATE TABLE gold.dim_dates (
    date_key       INTEGER PRIMARY KEY,        -- YYYYMMDD format
    full_date      DATE NOT NULL,              -- Actual date
    year           INTEGER NOT NULL,
    quarter        INTEGER NOT NULL,
    month          INTEGER NOT NULL,
    month_name     TEXT NOT NULL,
    day            INTEGER NOT NULL,
    day_name       TEXT NOT NULL,
    week           INTEGER NOT NULL,
    is_weekend     BOOLEAN NOT NULL
);

-- Step 4: Insert date values using generate_series
INSERT INTO gold.dim_dates (
    date_key, full_date, year, quarter, month, month_name,
    day, day_name, week, is_weekend
)
SELECT 
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
    d AS full_date,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    EXTRACT(MONTH FROM d)::INT AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(DAY FROM d)::INT AS day,
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(WEEK FROM d)::INT AS week,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series(
    DATE '1980-01-01',
    DATE '2030-12-31',
    INTERVAL '1 day'
) d;



--==============================================
--  Creating the dim_condition for the GOLD LAYER
--==============================================

DROP TABLE IF EXISTS gold.dim_conditions;

CREATE TABLE gold.dim_conditions AS
SELECT
    -- Assign a unique, sequential number to each row from the clean list below.
    ROW_NUMBER() OVER (ORDER BY condition_name) AS condition_key,
	condition_name
FROM (
    -- Get a unique list of every condition and their category.
    SELECT DISTINCT
        name AS condition_name
    FROM  silver.conditions
) AS distinct_conditions;

-- Add the primary key constraint to the new surrogate key
ALTER TABLE gold.dim_conditions
ADD PRIMARY KEY (condition_key);


--===================================================
--  Creating the dim_interventions for the GOLD LAYER
--===================================================

DROP TABLE IF EXISTS gold.dim_interventions;

CREATE TABLE gold.dim_interventions AS
SELECT
    -- Assign a unique, sequential number to each row from the clean list below.
    ROW_NUMBER() OVER (ORDER BY intervention_name) AS intervention_key,
	intervention_name,
	intervention_type
FROM (
    -- Get a unique list of every condition and their category.
    SELECT DISTINCT
        name AS intervention_name,
		intervention_type
    FROM  silver.interventions
) AS distinct_interventions;

-- Add the primary key constraint to the new surrogate key
ALTER TABLE gold.dim_interventions
ADD PRIMARY KEY (intervention_key);

--===================================================
--  Creating the fact_interventions for the GOLD LAYER
--===================================================

DROP TABLE IF EXISTS gold.fact_trials;

CREATE TABLE gold.fact_trials AS
SELECT
    -- Assign a unique, sequential number to each row from the clean list below.
    s.nct_id,
	s.enrollment,
	s.target_duration_days,
	number_of_participants_groups,
	ds.study_key, -- foreign key to dim_studies
	COALESCE(dd.date_key,0) AS start_date_key, -- foreign key from start_date to dim_dates
	COALESCE(dd_comp.date_key,0) AS completion_date_key, -- foreign key from completion_date to dim_dates
	COALESCE(dd_prim.date_key,0) AS primary_completion_date_key -- foreign key from primary_completion_date to dim_dates
FROM silver.studies AS s
LEFT JOIN gold.dim_studies AS ds
	ON s.nct_id = ds.nct_id
LEFT JOIN gold.dim_dates AS dd
	ON s.start_date = dd.full_date
LEFT JOIN gold.dim_dates AS dd_comp
	ON s.completion_date = dd_comp.full_date
LEFT JOIN gold.dim_dates AS dd_prim
	ON s.primary_completion_date = dd_prim.full_date;
-- Add the primary key constraint to the new surrogate key
ALTER TABLE gold.fact_trials
ADD PRIMARY KEY(nct_id);

SELECT *
FROM gold.fact_trials;


--===================================================
--  Creating the bridge_trial_sponsors  for the GOLD LAYER
--===================================================
DROP TABLE IF EXISTS gold.bridge_trial_sponsors;

CREATE TABLE gold.bridge_trial_sponsors AS
SELECT
    s.nct_id,
	s.lead_or_collaborator,
	ds.sponsor_key
FROM silver.sponsors AS s
JOIN gold.dim_sponsors AS ds
		ON s.clean_sponsor_name = ds.sponsor_name;




--===================================================
--  Creating the bridge_trial_conditions   for the GOLD LAYER
--===================================================
DROP TABLE IF EXISTS gold.bridge_trial_conditions ;

CREATE TABLE gold.bridge_trial_conditions  AS
SELECT
    s.nct_id,
	dc.condition_key
FROM silver.conditions AS s
JOIN gold.dim_conditions AS dc
		ON s.name = dc.condition_name;


--===================================================
--  Creating the bridge_trial_interventions   for the GOLD LAYER
--===================================================
DROP TABLE IF EXISTS gold.bridge_trial_interventions ;

CREATE TABLE gold.bridge_trial_interventions  AS
SELECT
    s.nct_id,
	di.intervention_key
FROM silver.interventions AS s
JOIN gold.dim_interventions AS di
		ON s.name = di.intervention_name;



--===================================================
--  Creating the gold.opportunity_rank for the GOLD LAYER
--===================================================
DROP TABLE IF EXISTS gold.opportunity_rank;
CREATE TABLE gold.opportunity_rank AS

WITH
-- Step 1: Get the best intervention priority for each trial.
trial_priority AS (
    SELECT
        nct_id,
        MIN(CASE
                -- THE FIX: Use LOWER() to make the comparison case-insensitive
                WHEN LOWER(intervention_type) IN ('drug', 'biological') THEN 1
                WHEN LOWER(intervention_type) = 'dietary supplement'     THEN 2
                ELSE 3
            END) AS best_priority
    FROM silver.interventions
    GROUP BY nct_id
),

-- Step 2: Create a clean, filtered list of trials that meet our strategic criteria.
relevant_trials AS (
    SELECT
        s.nct_id,
        s.phase,
        s.start_date
    FROM
        silver.studies s
    LEFT JOIN
        trial_priority tp ON s.nct_id = tp.nct_id
    WHERE
        -- Filter for relevant trials, handling NULLs for studies without listed interventions.
        COALESCE(tp.best_priority, 99) <= 2 AND s.study_type  = 'INTERVENTIONAL'
),

-- Step 3: Aggregate metrics at the CONDITION level.
condition_metrics AS (
    SELECT
        c.name AS condition_name,
        c.therapeutic_area,
        COUNT(rt.nct_id) AS total_trials,
        COUNT(rt.nct_id) AS drug_trial_count, -- Correct, as we pre-filtered
        SUM(CASE WHEN rt.phase IN ('PHASE 3', 'PHASE 4') THEN 1 ELSE 0 END) AS late_phase_count,
        SUM(CASE WHEN rt.start_date >= '2023-08-09' THEN 1 ELSE 0 END) AS recent_trials,
        SUM(CASE WHEN rt.start_date BETWEEN '2021-08-09' AND '2023-08-08' THEN 1 ELSE 0 END) AS older_trials
    FROM
        relevant_trials rt
    JOIN
        silver.conditions c ON rt.nct_id = c.nct_id
    GROUP BY
        c.name, c.therapeutic_area
),

-- Step 4: Separately, aggregate industry sponsor counts.
condition_sponsors AS (
    SELECT
        c.name AS condition_name,
        COUNT(DISTINCT spon.clean_sponsor_name) AS industry_sponsor_count
    FROM
        relevant_trials rt
    JOIN
        silver.conditions c ON rt.nct_id = c.nct_id
    JOIN
        silver.sponsors spon ON rt.nct_id = spon.nct_id
    WHERE
        spon.sponsor_category = 'Industry'
    GROUP BY
        c.name
)

-- Final Step: Join the pre-aggregated metrics and calculate the score.
SELECT
    cm.condition_name,
    cm.therapeutic_area,
    cm.total_trials,
    (
        (COALESCE(cm.drug_trial_count, 0) * 1.0 / cm.total_trials) * 0.4
        + (1.0 / (1 + COALESCE(cs.industry_sponsor_count, 0))) * 0.3
        + (1.0 / (1 + COALESCE(cm.late_phase_count, 0))) * 0.2
        + (CASE
            WHEN cm.older_trials = 0 AND cm.recent_trials > 0 THEN 1.0
            WHEN cm.older_trials > 0 THEN (CAST(cm.recent_trials AS REAL) - cm.older_trials) / cm.older_trials
            ELSE 0
          END) * 0.1
    ) AS opportunity_score
FROM
    condition_metrics cm
LEFT JOIN
    condition_sponsors cs ON cm.condition_name = cs.condition_name
WHERE
    cm.total_trials > 10
    AND cm.therapeutic_area NOT IN ('Others', 'Social & Behavioral')
    AND cm.condition_name NOT ILIKE '%healthy%';
	


/*
================================================================================
GOLD LAYER DOCUMENTATION: AACT Clinical Trial Star Schema
================================================================================
This document summarizes the purpose and importance of each table in the Gold
Layer. The Gold Layer is structured as a star schema, which is the industry
standard for building fast, scalable, and easy-to-understand analytics models.
*/

/*
================================================================================
TABLE: gold.dim_studies
================================================================================
--- PURPOSE ---
The `dim_studies` table is a dimension table that stores the descriptive,
[cite_start]textual attributes of each clinical trial [cite: 493-495, 503, 506, 509, 512, 544-546, 553]. The "grain" of this table is one row per
unique trial (nct_id). It answers the "what" questions about a trial,
such as its title, status, and phase.

--- IMPORTANCE ---
This table provides the rich, human-readable context for our analysis. When
a user on the dashboard drills through to a specific trial, this table
will provide all the detailed information like the official title, the reason
[cite_start]a trial was stopped, and its regulatory status[cite: 494, 512, 545]. Without it, our
fact table would just be a collection of numbers with no meaning.
*/

/*
================================================================================
TABLES: gold.dim_sponsors, gold.dim_conditions, gold.dim_interventions
================================================================================
--- PURPOSE ---
These dimension tables are designed to be the single source of truth for each
[cite_start]of their respective entities [cite: 639-641]. Each table contains a unique, master list of
every sponsor, condition, or intervention that exists in our dataset, with each
[cite_start]entry having a clean name and a unique surrogate key[cite: 570, 586, 599]. The grain is
one row per unique entity (e.g., one row for 'Pfizer').

--- IMPORTANCE ---
These tables are essential for consistent filtering and grouping. They solve
the problem of messy source data (e.g., "Pfizer Inc." vs. "Pfizer") by creating
one standardized name for each entity. This allows for accurate analysis and
prevents fractured reporting. Furthermore, by joining on a small, integer-based
surrogate key (e.g., `sponsor_key`), the model's performance is significantly
[cite_start]faster than if we had to join on long, text-based names [cite: 643-648].
*/

/*
================================================================================
TABLE: gold.dim_dates
================================================================================
--- PURPOSE ---
This is a dedicated, static lookup table that contains one row for every
[cite_start]single day over a long period (e.g., 1970-2030) [cite: 723-725]. Each date is broken
down into useful, pre-calculated attributes like year, month name, quarter,
and day of the week.

--- IMPORTANCE ---
A date dimension is a cornerstone of any professional business intelligence model.
It centralizes all time-based logic, which dramatically simplifies complex
[cite_start]time-intelligence calculations (like Year-over-Year growth) in Power BI [cite: 727-729]. It
guarantees that time-based metrics are calculated consistently across all reports,
making it an indispensable asset for trend analysis.
*/

/*
================================================================================
TABLE: gold.fact_trials
================================================================================
--- PURPOSE ---
This is the central table in our star schema. Its purpose is to store the
numeric **measures** (the things we want to SUM, COUNT, or AVERAGE) and the
**foreign keys** that link to our dimension tables. The grain of this table is
one row per trial.

--- IMPORTANCE ---
The fact table is the engine for all our quantitative analysis. All of the
KPIs and chart values on our dashboard will be calculated from the measures
in this table (e.g., SUM(enrollment)). Its lean structure (containing only
keys and numbers) is what allows the database and Power BI to perform these
aggregations across millions of rows almost instantly.
*/

/*
================================================================================
TABLES: gold.bridge_* (e.g., gold.bridge_trial_sponsors)
================================================================================
--- PURPOSE ---
Bridge tables are simple mapping tables whose sole purpose is to resolve the
[cite_start]**many-to-many relationships** in our data [cite: 837-841, 845-846]. For example, one trial can have
many sponsors, and one sponsor can have many trials. The bridge table sits
in the middle, holding one row for each valid trial-sponsor link.

--- WHY WE NEED THEM ---
A standard star schema requires clean "one-to-many" relationships. A direct
many-to-many link between a dimension and a fact table creates ambiguity and
[cite_start]can lead to incorrect calculations [cite: 842-844]. The bridge table breaks the complex
many-to-many relationship into two simple and unambiguous "one-to-many"
[cite_start]relationships (e.g., dim_sponsors -> bridge_sponsors <- fact_trials)[cite: 852].

--- WHY USE INSTEAD OF DIRECTLY CONNECTING ---
Directly connecting, for example, `dim_sponsors` to `fact_trials` is not
possible because a trial does not have just one single sponsor; it can have
many. The bridge table is the canonical and most robust solution to this problem,
providing a clear and performant path for filters to flow from the dimension,
[cite_start]through the bridge, to the fact table [cite: 880-884, 887-890]. It is the industry-standard
pattern for modeling this type of complex relationship.
*/