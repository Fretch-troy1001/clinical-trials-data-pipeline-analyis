
-- Create bronze Schema --

CREATE SCHEMA IF NOT EXISTS bronze;



-- Drop Table --
DROP TABLE IF EXISTS bronze.studies;

-- Create the bronze.studies table --

CREATE TABLE bronze.studies(
	nct_id VARCHAR(25),
	nlm_download_date_description VARCHAR(50),
	study_first_submitted_date DATE,
	results_first_submitted_date DATE,
	disposition_first_submitted_date DATE,
	last_update_submitted_date DATE,
	study_first_submitted_qc_date DATE,
	study_first_posted_date DATE,
	study_first_posted_date_type VARCHAR(50),
	results_first_submitted_qc_date DATE,
	results_first_posted_date DATE,
	results_first_posted_date_type VARCHAR(50),
	disposition_first_submitted_qc_date DATE,
	disposition_first_posted_date DATE,
	disposition_first_posted_date_type VARCHAR(50),
	last_update_submitted_qc_date DATE,
	last_update_posted_date DATE,
	last_update_posted_date_type VARCHAR(50),
	start_month_year  VARCHAR(50),
	start_date_type VARCHAR(50),
	start_date DATE,
	verification_month_year VARCHAR(50),
	verification_date DATE,
	completion_month_year VARCHAR(50),
	completion_date_type VARCHAR(50),
	completion_date DATE,
	primary_completion_month_year VARCHAR(50),
	primary_completion_date_type VARCHAR(50),
	primary_completion_date DATE,
	target_duration TEXT,
	study_type VARCHAR(25),
	acronym VARCHAR(50),
	baseline_population TEXT,
	brief_title TEXT,
	official_title TEXT,
	overall_status VARCHAR(50),
	last_known_status VARCHAR(50),
	phase VARCHAR(50),
	enrollment INT,
	enrollment_type VARCHAR(50),
	"source"  TEXT,
	limitations_and_caveats TEXT,
	number_of_arms INT,
	number_of_groups INT,
	why_stopped TEXT,
	has_expanded_access VARCHAR(50),
	expanded_access_type_individual VARCHAR(50),
	expanded_access_type_intermediate VARCHAR(50),
	expanded_access_type_treatment VARCHAR(50),
	has_dmc VARCHAR(50),
	is_fda_regulated_drug VARCHAR(50),
	is_fda_regulated_device  VARCHAR(50),
	is_unapproved_device  VARCHAR(50),
	is_ppsd VARCHAR(50),
	is_us_export VARCHAR(50),
	biospec_retention VARCHAR(50),
	biospec_description TEXT,
	ipd_time_frame TEXT,
	ipd_access_criteria TEXT,
	ipd_url TEXT,
	plan_to_share_ipd VARCHAR(50),
	plan_to_share_ipd_description TEXT,
	created_at TIMESTAMPTZ ,
	updated_at TIMESTAMPTZ ,
	source_class VARCHAR(50),
	delayed_posting VARCHAR(50),
	expanded_access_nctid VARCHAR(50),
	expanded_access_status_for_nctid VARCHAR(50),
	fdaaa801_violation VARCHAR(50),
	baseline_type_units_analyzed VARCHAR(50),
	patient_registry VARCHAR(50)
);

-- Truncate table --

TRUNCATE TABLE bronze.studies;

-- Loading the Data -- 

COPY bronze.studies
FROM 'C:/Temp/data/studies.txt'
WITH (
	FORMAT 'csv',
	DELIMITER '|',
	HEADER 	true
);



-- ==================================================================
-- Creating bronze.sponsors and loading the data from the source
-- ==================================================================
-- Drop bronze.sponsors if exists
DROP TABLE IF EXISTS bronze.sponsors;

CREATE TABLE bronze.sponsors(
	id VARCHAR(50),
	nct_id VARCHAR(50),
	agency_class VARCHAR(50),
	lead_or_collaborator VARCHAR(50),
	name TEXT
);
-- Truncate table --
TRUNCATE TABLE bronze.sponsors;

-- Loading Data to bronze.sponsors --

COPY bronze.sponsors
FROM 'C:/Temp/data/sponsors.txt'
WITH(
	FORMAT 'csv',
	DELIMITER '|',
	HEADER true
);


-- Creating sponsor mapping to fix the issues on sponsors name
DROP TABLE IF EXISTS bronze.sponsor_mapping;

CREATE TABLE bronze.sponsor_mapping
	(
	raw_sponsor_name VARCHAR(150),
	clean_sponsor_name	VARCHAR(150),
	sponsor_category VARCHAR(150)
	);
	
-- Truncate table --
TRUNCATE TABLE bronze.sponsor_mapping;

-- Loading the data to the sponsor mapping that is created from a spreadsheet
COPY bronze.sponsor_mapping
FROM 'C:\Temp\CSVs\AACT\sponsor_mapping.CSV'
WITH(
FORMAT 'csv',
DELIMITER ',',
HEADER true
);


-- ==================================================================
-- Creating bronze.conditions and loading the data from the source
-- ==================================================================
-- Drop bronze.sponsors if exists
DROP TABLE IF EXISTS bronze.conditions;

CREATE TABLE bronze.conditions(
	id VARCHAR(50),
	nct_id VARCHAR(50),
	name TEXT,
	downcase_name TEXT
);

-- Truncate table --
TRUNCATE TABLE bronze.conditions;

-- Loading Data to bronze.conditions --

COPY bronze.conditions
FROM 'C:/Temp/data/conditions.txt'
WITH(
	FORMAT 'csv',
	DELIMITER '|',
	HEADER true
);



-- ==================================================================
-- Creating bronze.interventions and loading the data from the source
-- ==================================================================
-- Drop bronze.sponsors if exists
DROP TABLE IF EXISTS bronze.interventions;

CREATE TABLE bronze.interventions(
	id VARCHAR(50),
	nct_id VARCHAR(50),
	intervention_type VARCHAR(50),
	name TEXT,
	description TEXT
);
-- Truncate table --
TRUNCATE TABLE bronze.interventions;

-- Loading Data to bronze.interventions --
COPY bronze.interventions
FROM 'C:/Temp/data/interventions.txt'
WITH(
	FORMAT 'csv',
	DELIMITER '|',
	HEADER true
);

SELECT *
FROM bronze.interventions
LIMIT 20;


-- Creating intervention mapping to fix the issues on sponsors name
DROP TABLE IF EXISTS bronze.intervention_mapping;

CREATE TABLE bronze.intervention_mapping(
    raw_name VARCHAR(255),
    clean_name VARCHAR(255),
    PRIMARY KEY (raw_name)
);

-- Truncate table --
TRUNCATE TABLE bronze.intervention_mapping;

-- Loading the data to the sponsor mapping that is created from a spreadsheet
COPY bronze.intervention_mapping
FROM 'C:\Temp\CSVs\AACT\intervention_mapping - Sheet1.csv'
WITH(
FORMAT 'csv',
DELIMITER ',',
HEADER true
);






-- ==================================================================
-- Creating bronze.outcomes and loading the data from the source
-- ==================================================================
-- Drop bronze.sponsors if exists
DROP TABLE IF EXISTS bronze.outcomes;

CREATE TABLE bronze.outcomes(
	id VARCHAR(50),
	nct_id VARCHAR(50),
	outcome_type VARCHAR(50),
	title TEXT,
	description TEXT,
	time_frame TEXT,
	population TEXT,
	anticipated_posting_date DATE,
	anticipated_posting_month_year VARCHAR(50),
	units VARCHAR(50),
	units_analyzed VARCHAR(50),
	dispersion_type VARCHAR(50),
	param_type VARCHAR(50)
);
-- Truncate table --
TRUNCATE TABLE bronze.outcomes;

-- Loading Data to bronze.outcomes --
COPY bronze.outcomes
FROM 'C:/Temp/data/outcomes.txt'
WITH(
	FORMAT 'csv',
	DELIMITER '|',
	HEADER true
);

