-- Create the silver schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS silver;

--==================================================================
--  1. Loading bronze.studies table to SILVER LAYER
--==================================================================

-- Drop the table to ensure a fresh build every time the script is run
DROP TABLE IF EXISTS silver.studies;

-- Create the final silver.studies table using a Common Table Expression (CTE)
-- This makes the complex logic to allow us to clean and transform the target_duration_days
CREATE TABLE silver.studies AS
WITH base_cleaning AS (
    SELECT
        nct_id,
        study_first_submitted_date ,
        CASE
            WHEN results_first_submitted_date IS NOT NULL
                THEN 'Report submitted'
            WHEN overall_status IN ('COMPLETED', 'APPROVED_FOR_MARKETING')AND results_first_submitted_date IS NULL
                THEN 'Completed but no report Recorded'
            ELSE 'No Report Submitted'
        END as report_status,
        results_first_submitted_date,
        disposition_first_submitted_date ,
        last_update_submitted_date ,
        study_first_submitted_qc_date ,
        study_first_posted_date ,
        study_first_posted_date_type,
        results_first_submitted_qc_date ,
        results_first_posted_date,
        CASE 
            WHEN overall_status IN ('COMPLETED', 'APPROVED_FOR_MARKETING')AND results_first_submitted_date IS NULL
                THEN 'Completed but study not posted'
            WHEN results_first_submitted_date IS NULL 
                THEN 'Result not posted'
            ELSE results_first_posted_date_type
        END AS results_first_posted_date_type,
        disposition_first_submitted_qc_date ,
        disposition_first_posted_date ,
        disposition_first_posted_date_type,
        last_update_submitted_qc_date ,
        last_update_posted_date,
        last_update_posted_date_type,
        -- start_date_type --
        CASE 
            WHEN start_date IS NULL OR (start_date < '1999-09-17') OR start_date > CURRENT_DATE 
                THEN 'Imputed'-- flag outliers
            WHEN start_date_type IS NULL 
                THEN 'Not Categorized' -- handle nulls
            ELSE start_date_type
        END as start_date_type,
        -- start_date --
        CASE 
            WHEN start_date IS NULL OR (start_date < '1999-09-17') OR start_date > CURRENT_DATE -- handle outliers and nulls
                THEN study_first_submitted_date
            ELSE start_date
        END as start_date,
        verification_date ,
        CASE
            WHEN verification_date IS NULL THEN 'Information Witheld'
            WHEN verification_date  < study_first_submitted_date THEN 'Invalid  - Verification too early'
            WHEN verification_date > CURRENT_DATE THEN 'Invalid - In the future'
            ELSE 'Valid'
        END AS verification_date_type,
        -- completion_date_type --
        CASE
            WHEN completion_date_type IS NOT NULL AND
                    (completion_date < '1999-09-17' 
                    OR completion_date > CURRENT_DATE) THEN 'Imputed' -- flag imputed outliers
            WHEN (overall_status IN  ('COMPLETED', 'APPROVED_FOR_MARKETING')) AND (primary_completion_date IS NULL OR completion_date IS NULL  OR last_update_submitted_date IS NULL)
                THEN 'Imputed'-- flag the imputed primary_completion_date
            WHEN completion_date_type IS NULL  THEN 'ESTIMATED'
            ELSE completion_date_type
        END AS completion_date_type,
        -- completion_date --
        CASE
            WHEN completion_date IS NOT NULL AND
                    (completion_date < '1999-09-17' 
                    OR completion_date > CURRENT_DATE) THEN
                last_update_submitted_date -- handle outliers
            WHEN overall_status IN ('COMPLETED', 'APPROVED_FOR_MARKETING') THEN
                COALESCE(completion_date,last_update_submitted_date) -- Impute missing completed trials
            ELSE completion_date
        END AS completion_date,
        -- primary_completion_date -- 
         CASE
            WHEN primary_completion_date IS NOT NULL AND
                    (primary_completion_date < '1999-09-17' 
                    OR primary_completion_date > CURRENT_DATE) THEN
                last_update_submitted_date -- handle outliers
            WHEN overall_status IN ('COMPLETED', 'APPROVED_FOR_MARKETING') THEN
                COALESCE(primary_completion_date,last_update_submitted_date)
            ELSE primary_completion_date
         END AS primary_completion_date,
        -- primary_completion_date_type -- 
         CASE
            WHEN primary_completion_date_type IS NOT NULL AND
                    (primary_completion_date < '1999-09-17' 
                    OR primary_completion_date > CURRENT_DATE) THEN 'Imputed' -- handle outliers
            WHEN (overall_status IN  ('COMPLETED', 'APPROVED_FOR_MARKETING')) AND (primary_completion_date IS NULL OR completion_date IS NULL  OR last_update_submitted_date IS NULL)
                THEN 'Imputed'-- flag the imputed primary_completion_date
            WHEN primary_completion_date_type IS NULL  THEN 'ESTIMATED'
            ELSE primary_completion_date_type
            END AS primary_completion_date_type,
        target_duration, -- We select the original column to clean it in the next step
        -- study_type --
        CASE
            WHEN study_type IS NULL 
                THEN 'UNKNOWN'
            ELSE study_type
        END AS study_type,
        -- acronym --
        CASE
            WHEN acronym IS NULL 
                THEN '[No Acronym defined]'
            ELSE acronym
        END AS acronym,
        -- baseline_population -- 
        CASE
            WHEN baseline_population IS NULL
                THEN '[Not Defined]'
            ELSE baseline_population
        END AS baseline_population,
        -- brief_title --
        CASE
            WHEN brief_title IS NULL
                THEN '[No Brief Title]' -- handle potential nulls for robustness
            ELSE brief_title
        END AS brief_title,
        -- official_title -- 
        CASE
            WHEN official_title IS NULL
                THEN '[No Official Title]' -- handle missing values
            ELSE official_title
        END AS official_title,
        
        -- overall_status --
        CASE
            WHEN overall_status IN ('RECRUITING', 'ACTIVE_NOT_RECRUITING', 'NOT_YET_RECRUITING', 'ENROLLING_BY_INVITATION') THEN 'Active'
            WHEN overall_status IN ('COMPLETED', 'APPROVED_FOR_MARKETING') THEN 'Complete'
            WHEN overall_status IN ('TERMINATED', 'WITHDRAWN', 'SUSPENDED') THEN 'Stopped'
            ELSE 'Unknown'
        END AS overall_status, 
        
        -- last_known_status --
        CASE 
            WHEN  last_known_status IS NULL THEN 'NO STATUS UPDATE'
            ELSE last_known_status
        END AS last_known_status,
        
        -- phase --
        CASE
            WHEN phase IN ('PHASE1', 'EARLY_PHASE1') THEN 'PHASE 1'
            WHEN phase IN ('PHASE1/PHASE2', 'PHASE2') THEN 'PHASE 2'
            WHEN phase IN ('PHASE2/PHASE3', 'PHASE3') THEN 'PHASE 3'
            WHEN phase IN ('NA',NULL) THEN 'Not Applicable'
            ELSE 'PHASE 4'
        END AS phase,
        -- enrollment --
        CASE
            WHEN enrollment IS NULL THEN 0
            WHEN (study_type = 'OBSERVATIONAL') AND (enrollment > 566401) THEN 566401 -- Cap at 99.7th percentile
            WHEN (study_type = 'INTERVENTIONAL') AND (enrollment > 15886) THEN 15886 -- Cap at 99.7th percentile
            ELSE enrollment
        END AS enrollment,
        CASE
			WHEN enrollment_type IS NULL
				THEN 'Unknown'
			ELSE enrollment_type
		END AS enrollment_type,
        -- source --
        CASE
            WHEN "source" IS NULL
                THEN 'Unknown'
            ELSE "source"
        END AS "source",
        -- limitations_and_caveats --
        CASE
            WHEN limitations_and_caveats IS NULL
                THEN '[Not Defined]'
            ELSE limitations_and_caveats
        END AS limitations_and_caveats,
        
        -- number_of_arms & number_of_groups: Use COALESCE since they are mutually exclusive --
        COALESCE(number_of_arms, number_of_groups, 0) as number_of_participants_groups,
        
        -- why_stopped and its category --
        CASE
            WHEN why_stopped IS NULL THEN 'No Comment'
            ELSE why_stopped
        END AS why_stopped,
        CASE
            WHEN why_stopped ILIKE any (array['%not started%', '%not initiated%', '%canceled%', '%abandoned%', '%not activated%', '%withdrawn%', '%did not start%']) THEN 'Study Not Initiated'
            WHEN why_stopped ILIKE any (array['%interim analysis%', '%futility%', '%dsmb%', '%stopping rule%']) THEN 'Early Termination (Interim Analysis/Futility)'
            WHEN why_stopped ILIKE any (array['%unethical%', '%ethical committee%', '%consent%', '%irb%']) THEN 'Ethical/Regulatory Issues'
            WHEN why_stopped ILIKE any (array['%pharmacokinetic%', '%pk/pd%', '%tolerability%']) THEN 'PK/PD or Tolerability Issues'
            WHEN why_stopped ILIKE any (array['%graft failure%', '%software%', '%unreliable data%', '%technical problem%']) THEN 'Technical/Procedural Failure'
            WHEN why_stopped ILIKE any (array['%recruitment%', '%enrollment%', '%enrolment%', '%accrual%', '%recruit%', '%patients%', '%participants%', '%subjects%', '%inclusion%', '%accrue%']) THEN 'Recruitment Issues'
            WHEN why_stopped ILIKE any (array['%safety%', '%toxicity%', '%adverse event%', '%risk%']) THEN 'Safety Concern'
            WHEN why_stopped ILIKE any (array['%efficacy%', '%endpoint%', '%benefit%', '%objective%', '%superior%', '%therapeutic effect%', '%not effective%', '%no difference%']) THEN 'Efficacy Issues'
            WHEN why_stopped ILIKE any (array['%covid-19%', '%covid%', '%corona pandemic%', '%pandemic%']) THEN 'COVID-19 Related'
            WHEN why_stopped ILIKE any (array['%business%', '%administrative%', '%sponsor%', '%strategic%', '%corporate%', '%development program%', '%decision%']) THEN 'Business/Admin Decision'
            WHEN why_stopped ILIKE any (array['%investigator%', '%pi leaving%', '%pi relocated%', '%personnel%', '%staff%', '%pi left%', '%pi decision%']) THEN 'Investigator/Site Issues'
            WHEN why_stopped ILIKE any (array['%device%', '%drug%', '%supply%', '%resource%', '%equipment%', '%logistic%', '%technical%', '%materials%']) THEN 'Logistical/Resource Issues'
            WHEN why_stopped ILIKE any (array['%approval%', '%fda%', '%regulatory%', '%authorities%']) THEN 'Regulatory/Approval Issues'
            WHEN why_stopped ILIKE any (array['%design%', '%protocol%']) THEN 'Study Design Issues'
            WHEN why_stopped ILIKE any (array['%feasible%', '%feasibility%', '%practical%']) THEN 'Study Feasibility Issues'
			WHEN why_stopped ILIKE any (array['%fund%', '%financial%', '%budget%']) THEN 'Funding Issues'
            WHEN why_stopped IS NOT NULL THEN 'Other'
            ELSE 'No Comment'
        END AS why_stopped_category,
		-- has_expanded_access --
        CASE
		WHEN expanded_access_nctid IS NULL
		THEN 'No'
		ELSE 'Yes'
		END AS has_expanded_access,
		-- expanded_access_type_individual --
        CASE
		WHEN  expanded_access_type_individual IS NULL -- NULL Means 'No'
			THEN 'No'
		ELSE 'Yes'
		END AS expanded_access_type_individual ,
		-- expanded_access_type_intermediate --
        CASE
		WHEN  expanded_access_type_intermediate IS NULL -- NULL Means 'No'
			THEN 'No'
		ELSE 'Yes'
		END AS expanded_access_type_intermediate,
        CASE
		WHEN  expanded_access_type_treatment IS NULL -- NULL Means 'No'
			THEN 'No'
		ELSE 'Yes'
		END AS expanded_access_type_treatment ,
		-- has_dmc --
        CASE
			WHEN  has_dmc = 't'
				THEN 'Yes'
			WHEN has_dmc = 'f'
				THEN 'No'
			ELSE 'Unknown'-- turn nulls into 'unknown'
		END AS has_dmc ,
		-- is_fda_regulated_drug --
		CASE
		WHEN  is_fda_regulated_drug = 't'
			THEN 'Yes'
		WHEN is_fda_regulated_drug = 'f'
			THEN 'No'
		ELSE 'Unknown'-- best way is to turn nulls into 'unknown'
		END AS is_fda_regulated_drug,
		-- is_fda_regulated_device --
        CASE
		WHEN  is_fda_regulated_device = 't'
			THEN 'Yes'
		WHEN is_fda_regulated_device = 'f'
			THEN 'No'
		ELSE 'Unknown'-- best way is to turn nulls into 'unknown'
		END AS is_fda_regulated_device ,
        CASE
		WHEN  is_unapproved_device = 't'
			THEN 'Unapproved'
		WHEN  is_fda_regulated_device = 't' AND is_unapproved_device is null
			THEN 'Approved'
		ELSE 'Not Applicable'-- nulls mean no violations
		END AS is_unapproved_device ,
		-- is_ppsd --
        CASE
		WHEN  is_ppsd = 't'
			THEN 'PPSD study'
		WHEN  is_fda_regulated_device = 't' AND is_ppsd is null
			THEN 'Not PPSD study'
		ELSE 'Not Applicable'-- not applicable to those who are not a device
		END AS is_ppsd,
		-- is_us_export --
        CASE
			WHEN  is_us_export = 't'
				THEN 'Yes'
			WHEN is_us_export = 'f'
				THEN 'No'
		ELSE 'Unknown'-- best way is to turn nulls into 'unknown'
		END AS is_us_export ,
		-- biospec_retention --
        CASE
		WHEN  biospec_retention IS NULL -- best way is to turn nulls into 'unknown'
			THEN 'Unknown'
		ELSE biospec_retention
		END AS biospec_retention ,
		-- biospec_description --
        CASE
		WHEN  biospec_description IS NULL -- best way is to turn nulls into 'Not defined'
			THEN 'Not defined'
		ELSE biospec_description
		END AS biospec_description ,
		-- ipd_time_frame --
        CASE
		WHEN  ipd_time_frame IS NULL -- best way is to turn nulls into 'Not specified'
			THEN 'Not specified'
		ELSE ipd_time_frame
		END AS ipd_time_frame ,
		-- ipd_access_criteria -- 
        CASE
		WHEN  ipd_access_criteria IS NULL -- best way is to turn nulls into 'Not defined'
			THEN 'Not specified'
		ELSE ipd_access_criteria
		END AS ipd_access_criteria ,
		-- ipd_url --
        CASE
		WHEN  ipd_url IS NULL -- best way is to turn nulls into 'Not specified'
			THEN 'Not specified'
		ELSE ipd_url
		END AS ipd_url ,
		-- plan_to_share_ipd --
        CASE
		WHEN  plan_to_share_ipd IS NULL -- best way is to turn nulls into 'Not defined'
			THEN 'Not specified'
		ELSE plan_to_share_ipd
		END AS plan_to_share_ipd,
		-- plan_to_share_ipd_description --
        CASE
		WHEN  plan_to_share_ipd_description IS NULL -- best way is to turn nulls into 'Not defined'
			THEN 'Not specified'
		ELSE plan_to_share_ipd_description
		END AS plan_to_share_ipd_description ,
        created_at ,
        updated_at ,
		-- source_class --
        CASE
		WHEN source_class IS NULL 
			THEN 'UNKNOWN'
		ELSE source_class
		END AS source_class ,
		-- delayed_posting --
        CASE
		WHEN  delayed_posting = 't'
			THEN 'Yes'
		ELSE 'Not Specified' -- NULL means it's either not delayed or not applicable
		END AS delayed_posting,
        expanded_access_nctid,
        -- expanded_access_status_for_nctid --
		CASE
			WHEN expanded_access_status_for_nctid IS NULL
			THEN 'Not Applicable'
			ELSE REPLACE(expanded_access_status_for_nctid, '_', ' ') -- Make it more readable
		END AS expanded_access_status_for_nctid,
		-- fdaaa801_violation --
		CASE
			WHEN  fdaaa801_violation = 't'
				THEN 'Violation'
			ELSE 'No Violation'-- nulls mean no violations 
		END AS fdaaa801_violation,
		-- patient_registry --
	        CASE
			WHEN patient_registry = 't'
				THEN 'Yes'
			WHEN patient_registry = 'f'
				THEN 'No'
			ELSE 'Unknown'
		END AS patient_registry 
    FROM bronze.studies
)
-- ==================================================================
-- Final SELECT: Read from the clean CTE and perform the final complex transformations.
-- ==================================================================
SELECT
    *, -- Select all the already-cleaned columns from the CTE above
    
    -- Create the new, clean numeric duration column
    CASE
        -- First, we calculate the duration in days using an inner CASE statement.
        -- Then, the outer CASE statement checks if that calculated value is an outlier and caps it.
        WHEN 
            (CASE
                WHEN target_duration ILIKE '%Day%'  OR target_duration ILIKE '%Days%' THEN CAST(SPLIT_PART(REGEXP_REPLACE(target_duration, '[^0-9.]', '', 'g'), ' ', 1) AS INTEGER) * 1
                WHEN target_duration ILIKE '%Week%' OR target_duration ILIKE '%Weeks%' THEN CAST(SPLIT_PART(REGEXP_REPLACE(target_duration, '[^0-9.]', '', 'g'), ' ', 1) AS INTEGER) * 7
                WHEN target_duration ILIKE '%Month%' OR target_duration ILIKE '%Months%' THEN CAST(SPLIT_PART(REGEXP_REPLACE(target_duration, '[^0-9.]', '', 'g'), ' ', 1) AS INTEGER) * 30
                WHEN target_duration ILIKE '%Year%' OR target_duration ILIKE '%Years%' THEN CAST(SPLIT_PART(REGEXP_REPLACE(target_duration, '[^0-9.]', '', 'g'), ' ', 1) AS INTEGER) * 365
                ELSE NULL
            END) > 10950 -- Check if the result is greater than our 30-year threshold
        THEN 10950 -- If yes, cap it at 10950
        ELSE
            -- If no, run the same calculation again to get the final value.
            (CASE
                WHEN target_duration ILIKE '%Day%'  OR target_duration ILIKE '%Days%' THEN CAST(SPLIT_PART(REGEXP_REPLACE(target_duration, '[^0-9.]', '', 'g'), ' ', 1) AS INTEGER) * 1
                WHEN target_duration ILIKE '%Week%' OR target_duration ILIKE '%Weeks%' THEN CAST(SPLIT_PART(REGEXP_REPLACE(target_duration, '[^0-9.]', '', 'g'), ' ', 1) AS INTEGER) * 7
                WHEN target_duration ILIKE '%Month%' OR target_duration ILIKE '%Months%' THEN CAST(SPLIT_PART(REGEXP_REPLACE(target_duration, '[^0-9.]', '', 'g'), ' ', 1) AS INTEGER) * 30
                WHEN target_duration ILIKE '%Year%' OR target_duration ILIKE '%Years%' THEN CAST(SPLIT_PART(REGEXP_REPLACE(target_duration, '[^0-9.]', '', 'g'), ' ', 1) AS INTEGER) * 365
                ELSE NULL
            END)
    END as target_duration_days,

    -- Since we capped the outliers to only 30 years, we need to flag it using a new column
    CASE
        WHEN target_duration IS NULL THEN 'Not Provided'
        WHEN 
            (CASE
                WHEN target_duration ILIKE '%Day%'  OR target_duration ILIKE '%Days%' THEN CAST(SPLIT_PART(REGEXP_REPLACE(target_duration, '[^0-9.]', '', 'g'), ' ', 1) AS INTEGER) * 1
                WHEN target_duration ILIKE '%Week%' OR target_duration ILIKE '%Weeks%' THEN CAST(SPLIT_PART(REGEXP_REPLACE(target_duration, '[^0-9.]', '', 'g'), ' ', 1) AS INTEGER) * 7
                WHEN target_duration ILIKE '%Month%' OR target_duration ILIKE '%Months%' THEN CAST(SPLIT_PART(REGEXP_REPLACE(target_duration, '[^0-9.]', '', 'g'), ' ', 1) AS INTEGER) * 30
                WHEN target_duration ILIKE '%Year%' OR target_duration ILIKE '%Years%' THEN CAST(SPLIT_PART(REGEXP_REPLACE(target_duration, '[^0-9.]', '', 'g'), ' ', 1) AS INTEGER) * 365
                ELSE NULL
            END) > 10950
        THEN 'Capped Outlier'
        ELSE 'Actual'
    END as target_duration_flag

FROM base_cleaning;



--==================================================================
-- 2. LOADING THE bronze.sponsors TABLE TO SILVER LAYER
--==================================================================
DROP TABLE IF EXISTS silver.sponsors ;

CREATE TABLE silver.sponsors AS
SELECT
    raw.id,
    raw.nct_id,
    -- Use the mapping logic to get the clean name and category
    COALESCE(map.clean_sponsor_name, raw.name) AS clean_sponsor_name,
    COALESCE(map.sponsor_category, 'Other') AS sponsor_category, 
    raw.lead_or_collaborator
FROM
    bronze.sponsors AS raw
LEFT JOIN
    bronze.sponsor_mapping AS map 
ON 
    raw.name = map.raw_sponsor_name;



--==================================================================
-- 3. LOADING THE bronze.conditions TABLE TO SILVER LAYER
--==================================================================
DROP TABLE IF EXISTS silver.conditions;

CREATE TABLE silver.conditions AS
SELECT
    id,
    nct_id,
    TRIM(
        CASE
            -- If a comma exists...
            WHEN STRPOS(downcase_name, ',') > 0 
            -- Then grab the text before the comma
            THEN SUBSTRING(downcase_name, 1, STRPOS(downcase_name, ',') - 1) 
            -- Otherwise, just use the original name
            ELSE downcase_name 
        END
    ) AS name,
	 -- Final, comprehensive CASE statement for categorizing therapeutic areas
CASE
    -- Oncology
    WHEN downcase_name ILIKE ANY (ARRAY['%cancer%', '%carcinoma%', '%tumor%', '%lymphoma%', '%leukemia%', '%sarcoma%', '%neoplasm%', '%melanoma%', '%myeloma%', '%glioblastoma%']) 
        THEN 'Oncology'
    
    -- Central Nervous System (CNS)
    WHEN downcase_name ILIKE ANY (ARRAY['%neuro%', '%nervous%', '%stroke%', '%alzheimer%', '%parkinson%', '%brain%', '%spinal%', '%dementia%', '%epilepsy%', '%multiple sclerosis%', '%cognitive impairment%']) 
        THEN 'Central Nervous System'
    
    -- Cardiovascular
    WHEN downcase_name ILIKE ANY (ARRAY['%cardiac%', '%heart%', '%vascular%', '%artery%', '%hypertension%', '%atrial fibrillation%', '%atherosclerosis%', '%myocardial infarction%']) 
        THEN 'Cardiovascular'
    
    -- Metabolic Diseases
    WHEN downcase_name ILIKE ANY (ARRAY['%obesity%', '%diabetes%', '%metabolic syndrome%', '%overweight%', '%hypercholesterolemia%', '%insulin resistance%']) 
        THEN 'Metabolic'
    
    -- Infectious Diseases
    WHEN downcase_name ILIKE ANY (ARRAY['%hiv%', '%covid-19%', '%influenza%', '%hepatitis%', '%tuberculosis%', '%malaria%', '%sepsis%', '%pneumonia%', '%infection%']) 
        THEN 'Infectious Disease'
        
    -- Inflammatory & Autoimmune
    WHEN downcase_name ILIKE ANY (ARRAY['%rheumatoid arthritis%', '%crohn''s disease%', '%ulcerative colitis%', '%psoriasis%', '%inflammation%', '%atopic dermatitis%', '%asthma%']) 
        THEN 'Inflammatory & Autoimmune'

    -- Mental Health / Psychiatry
    WHEN downcase_name ILIKE ANY (ARRAY['%depression%', '%anxiety%', '%schizophrenia%', '%bipolar disorder%', '%insomnia%', '%stress%', '%autism%', '%depressive disorder%']) 
        THEN 'Mental Health'

    -- Respiratory Diseases
    WHEN downcase_name ILIKE ANY (ARRAY['%copd%', '%pulmonary%', '%cystic fibrosis%', '%sleep apnea%']) 
        THEN 'Respiratory'
        
    -- Musculoskeletal Disorders
    WHEN downcase_name ILIKE ANY (ARRAY['%osteoarthritis%', '%low back pain%', '%osteoporosis%', '%arthritis%', '%sarcopenia%', '%fibromyalgia%']) 
        THEN 'Musculoskeletal'

    -- Pain & Anesthesiology
    WHEN downcase_name ILIKE ANY (ARRAY['%pain%', '%anesthesia%', '%analgesia%', '%migraine%']) 
        THEN 'Pain & Anesthesiology'

    -- Social / Behavioral (To be excluded later)
    WHEN downcase_name ILIKE ANY (ARRAY['%bully%', '%teen pregnancy%', '%smoking cessation%', '%exercise%', '%physical activity%', '%smoking%']) 
        THEN 'Social & Behavioral'
   
    ELSE 'Others'
END AS therapeutic_area
FROM
    bronze.conditions;

SELECT	name, COUNT(*)
FROM silver.conditions
WHERE therapeutic_area = 'Others'
GROUP BY name
ORDER BY  COUNT(*) DESC
limit 100;
--==================================================================
-- 4. LOADING THE bronze.interventions TABLE TO SILVER LAYER
--==================================================================
DROP TABLE IF EXISTS silver.interventions;

CREATE TABLE silver.interventions AS
SELECT
    raw.id,
    raw.nct_id,
    -- Final logic to handle NULLs and apply mapping
    CASE
        WHEN raw.name IS NULL THEN 'no intervention'
        ELSE COALESCE(map.clean_name, LOWER(TRIM(raw.name)))
    END AS name,
    raw.intervention_type
FROM
    bronze.interventions AS raw
LEFT JOIN
    bronze.intervention_mapping AS map 
ON 
    LOWER(TRIM(raw.name)) = map.raw_name;
	

--==================================================================
-- 5. LOADING THE bronze.outcomes TABLE TO SILVER LAYER
--==================================================================

DROP TABLE IF EXISTS silver.outcomes;

CREATE TABLE silver.outcomes AS
SELECT
    id,
    nct_id,
    outcome_type,
    COALESCE(TRIM(title), 'not specified') AS title, -- there are no nulls but this is just for future proofing
    COALESCE(TRIM(time_frame), 'not specified') AS time_frame, -- replace null values with "not specified"
    COALESCE(TRIM(population), 'not specified') AS population -- replace null values with "not specified"
FROM
    bronze.outcomes;



SELECT sponsor_category, COUNT(*)
FROM silver.sponsors
GROUP BY sponsor_category
ORDER BY COUNT(*) DESC ;


/*
column excluded
description: this is long, unstructured text not suitable for direct dashboard analysis.
units, units_analyzed, dispersion_type, param_type: These columns contain very granular statistical details about the outcome measurement (e.g., param_type might be 'Mean' or 'Median', and dispersion_type might be 'Standard Deviation').
*/




/* 
================DATA EXPLORATION AND ANALYSIS(BASIS FOR SILVER LAYER)=================================
This section will not be run during the loading of the Silver Layer. These are reference code to see
the decisions of how each columns and data where handled during the bronze to silver layer loading.
Select the code you want to run then press `CTRL + /` to uncomment the code before running
*/


-- --=====================================================================================
-- -- 1. Data Cleaning, Standardization, Normalization and Data Enrichment for silver.studies
-- --=====================================================================================


-- ------------------------------  nct_id  ---------------------------
-- -- Check for Null Values

-- SELECT COUNT(nct_id), COUNT(*)
-- FROM silver.studies; -- no null values

-- -- Check for duplicates

-- SELECT nct_id, ROW_NUMBER() OVER(PARTITION BY nct_id) AS dup_count
-- FROM silver.studies
-- ORDER BY dup_count DESC; -- no number greater than 1 meaning no duplicates


-- ------------------------------  nlm_download_date_description  ---------------------------
-- -- Check for Null Values

-- SELECT nlm_download_date_description, COUNT(*)
-- FROM bronze.studies
-- GROUP BY nlm_download_date_description
-- ORDER BY COUNT(*) DESC;
-- -- ALL null VALUES DELETE!

-- SELECT nct_id, ROW_NUMBER() OVER(PARTITION BY nct_id) AS dup_count
-- FROM silver.studies
-- ORDER BY dup_count DESC; -- no number greater than 1 meaning no duplicates



-- ------------------------------  overall_status  ---------------------------
-- -- check for nulls, and distribution

-- SELECT overall_status, COUNT(*)
-- FROM bronze.studies
-- GROUP BY overall_status; -- no null values

-- -- there are too many categories and we need to handle nulls and simplify the overall_status into Active, Completed, Stopped, Unknown
	

-- ------------------------------  last_known_status   ---------------------------
-- -- check for nulls, and distribution


-- SELECT last_known_status, COUNT(*)
-- FROM silver.studies
-- GROUP BY bronze ; -- 465114 null values, convert the nulls to 'No Update'


-- -----------------------------------  phase   ---------------------------------
-- -- check null values
-- SELECT phase , COUNT(*)
-- FROM bronze.studies
-- GROUP BY phase ; -- 128730 null values, we need to handle nulls and put the combination phase to their advance phase to simplify the categorization


-- WITH phase_grouping AS
-- (SELECT CASE
-- 		WHEN phase IN ('PHASE1', 'EARLY_PHASE1') THEN 'PHASE 1'
-- 		WHEN phase IN ('PHASE1/PHASE2', 'PHASE2') THEN 'PHASE 2'
-- 		WHEN phase IN ('PHASE2/PHASE3', 'PHASE3') THEN 'PHASE 3'
-- 		WHEN phase IN ('NA',NULL) THEN 'Not Applicable'
-- 		ELSE 'PHASE 4'
-- 		END AS standardized_phase
-- FROM silver.studies)

-- SELECT standardized_phase, COUNT(*)
-- FROM phase_grouping
-- GROUP BY standardized_phase; 

-- ----------------------------------- study_type   ---------------------------
-- -- check distribution and null values
-- SELECT study_type, COUNT(*)
-- FROM silver.studies
-- GROUP BY study_type; -- replace null values with 'UNKNOWN'



-- SELECT study_type, COUNT(*)
-- FROM silver.studies
-- GROUP BY study_type;

-- ----------------------------------- brief_title   ---------------------------
-- -- check nulls
-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE brief_title IS NULL;
-- -- no null values no need to change



-- ----------------------------------- official_title   ---------------------------

-- -- check distribution and null values
-- SELECT COUNT(*), official_title
-- FROM bronze.studies
-- GROUP BY official_title
-- ORDER BY COUNT(*) DESC; -- replace null values with 'UNKNOWN'
-- -- 9986 null values will be  replaced with [No Official Tile]

-- WITH official_title_cleaned AS
-- (
-- SELECT
-- CASE
-- 	WHEN official_title IS NULL
-- 	 THEN 'No Official Title'
-- 	ELSE official_title
-- END AS official_title
-- FROM bronze.studies
-- )
-- SELECT COUNT(*), official_title
-- FROM  official_title_cleaned 
-- GROUP BY official_title
-- ORDER BY COUNT(*) DESC; -- replace null values with 'UNKNOWN'
-- -- 9986 null values will be  replaced with [No Official Tile]


-- ----------------------------------- baseline_population   ---------------------------
-- -- check nulls
-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE baseline_population IS NULL;
-- -- 521277 null values

-- -- check distribution and null values
-- SELECT baseline_population, COUNT(*)
-- FROM bronze.studies
-- GROUP BY baseline_population
-- ORDER BY COUNT(*) DESC; -- replace null values with 'UNKNOWN'
-- -- all null values will be  replaced with [Not Defined]



-- ----------------------------------- limitations_and_caveats   ---------------------------
-- -- check nulls
-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE limitations_and_caveats IS NULL;
-- -- 530063 null values

-- -- check distribution and null values
-- SELECT COUNT(*),  limitations_and_caveats
-- FROM bronze.studies
-- GROUP BY limitations_and_caveats
-- ORDER BY COUNT(*) DESC; -- replace null values with 'UNKNOWN'
-- -- all null values will be  replaced with [Not Defined]

-- ----------------------------------- acronym   ---------------------------
-- -- check nulls
-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE acronym IS NULL;
-- -- 391692 null values

-- -- check distribution and null values
-- SELECT COUNT(*),  acronym
-- FROM bronze.studies
-- GROUP BY acronym
-- ORDER BY COUNT(*) DESC; -- replace null values with 'UNKNOWN'
-- -- all null values will be  replaced with [No Acronym defined]

-- -----------------------------------  why_stopped   ---------------------------
-- -- check null values

-- SELECT  COUNT(*)
-- FROM silver.studies
-- WHERE why_stopped IS NULL;

-- SELECT why_stopped, COUNT(*)
-- FROM silver.studies
-- GROUP BY why_stopped
-- ; 

-- -- To make a really good categorization lets categorized the reason why the clincal trial stopped, we will call this why_stopped_category


-- WITH CTE AS
-- (SELECT CASE
		    
-- 	    WHEN why_stopped ILIKE any (array['%not started%', '%not initiated%', '%canceled%', '%abandoned%', '%not activated%', '%withdrawn%', '%did not start%']) THEN 'Study Not Initiated'
-- 	    WHEN why_stopped ILIKE any (array['%interim analysis%', '%futility%', '%dsmb%', '%stopping rule%']) THEN 'Early Termination (Interim Analysis/Futility)'
-- 	    WHEN why_stopped ILIKE any (array['%unethical%', '%ethical committee%', '%consent%', '%irb%']) THEN 'Ethical/Regulatory Issues'
-- 	    WHEN why_stopped ILIKE any (array['%pharmacokinetic%', '%pk/pd%', '%tolerability%']) THEN 'PK/PD or Tolerability Issues'
-- 	    WHEN why_stopped ILIKE any (array['%graft failure%', '%software%', '%unreliable data%', '%technical problem%']) THEN 'Technical/Procedural Failure'
-- 	    WHEN why_stopped ILIKE any (array['%recruitment%', '%enrollment%', '%enrolment%', '%accrual%', '%recruit%', '%patients%', '%participants%', '%subjects%', '%inclusion%', '%accrue%']) THEN 'Recruitment Issues'
-- 	    WHEN why_stopped ILIKE any (array['%fund%', '%financial%', '%budget%']) THEN 'Funding Issues'
-- 	    WHEN why_stopped ILIKE any (array['%safety%', '%toxicity%', '%adverse event%', '%risk%']) THEN 'Safety Concern'
-- 	    WHEN why_stopped ILIKE any (array['%efficacy%', '%endpoint%', '%benefit%', '%objective%', '%superior%', '%therapeutic effect%', '%not effective%', '%no difference%']) THEN 'Efficacy Issues'
-- 	    WHEN why_stopped ILIKE any (array['%covid-19%', '%covid%', '%corona pandemic%', '%pandemic%']) THEN 'COVID-19 Related'
-- 	    WHEN why_stopped ILIKE any (array['%business%', '%administrative%', '%sponsor%', '%strategic%', '%corporate%', '%development program%', '%decision%']) THEN 'Business/Admin Decision'
-- 	    WHEN why_stopped ILIKE any (array['%investigator%', '%pi leaving%', '%pi relocated%', '%personnel%', '%staff%', '%pi left%', '%pi decision%']) THEN 'Investigator/Site Issues'
-- 	    WHEN why_stopped ILIKE any (array['%device%', '%drug%', '%supply%', '%resource%', '%equipment%', '%logistic%', '%technical%', '%materials%']) THEN 'Logistical/Resource Issues'
-- 	    WHEN why_stopped ILIKE any (array['%approval%', '%fda%', '%regulatory%', '%authorities%']) THEN 'Regulatory/Approval Issues'
-- 	    WHEN why_stopped ILIKE any (array['%design%', '%protocol%']) THEN 'Study Design Issues'
-- 	    WHEN why_stopped ILIKE any (array['%feasible%', '%feasibility%', '%practical%']) THEN 'Study Feasibility Issues'
-- 		WHEN why_stopped IS NOT NULL THEN 'Other'
--        	ELSE NULL
-- 	 END as 	stopped_reason_category
-- FROM bronze.studies)
-- SELECT stopped_reason_category, COUNT(*)
-- FROM CTE
-- GROUP BY stopped_reason_category
-- ORDER BY COUNT(*) DESC;

-- SELECT stopped_reason_category
-- FROM cte
-- WHERE stopped_reason_category IS NOT NULL
-- 	AND stopped_reason_category NOT IN ('Recruitment Issues','Efficacy Issues', 'Business/Admin Decision', 'Safety Concern', 
-- 	'Funding Issues', 'COVID-19 Related','Investigator/Site Issues','Logistical/Resource Issues','Regulatory/Approval Issues', 'Study Not Initiated',
-- 	'Study Design Issues', 'Study Feasibility Issues', 'Early Termination (Interim Analysis/Futility)', 'Ethical Issues','PK/PD or Tolerability Issues', 'Technical or Procedural Failure' )

-- ----------------------------------- source   ---------------------------
-- -- check nulls
-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE source IS NULL;
-- -- NO null values

-- -- check distribution and null values
-- SELECT COUNT(*),  source
-- FROM bronze.studies
-- GROUP BY source
-- ORDER BY COUNT(*) DESC; -- replace null values with 'UNKNOWN'


-- ----------------------------------  source_class   ---------------------------
-- -- check distribution and null values
-- SELECT source_class,  COUNT(*)
-- FROM silver.studies
-- GROUP BY source_class
-- ORDER BY COUNT(*) DESC;

-- WITH source_class_cleaned AS 
-- (
-- SELECT
-- 	CASE
-- 		WHEN source_class IS NULL 
-- 			THEN 'UNKNOWN'
-- 		ELSE source_class
-- 	END AS source_class
-- FROM bronze.studies
-- )
-- SELECT source_class, COUNT(*)
-- FROM source_class_cleaned
-- GROUP BY source_class
-- ORDER BY  COUNT(*) DESC;


-- ----------------------------------  baseline_type_units_analyzed   ---------------------------
-- -- check distribution and null values
-- SELECT baseline_type_units_analyzed,  COUNT(*)
-- FROM silver.studies
-- GROUP BY baseline_type_units_analyzed
-- ORDER BY COUNT(*) DESC;
-- -- this will not be helpful to our goal so this will be excluded



-- ----------------------------------  patient_registry   ---------------------------
-- -- check distribution and null values
-- SELECT patient_registry,  COUNT(*)
-- FROM silver.studies
-- GROUP BY patient_registry
-- ORDER BY COUNT(*) DESC;


-- WITH patient_registry_cleaned AS 
-- (
-- SELECT
-- 	CASE
-- 		WHEN patient_registry = 't'
-- 			THEN 'Yes'
-- 		WHEN patient_registry = 'f'
-- 			THEN 'No'
-- 		ELSE 'Unknown'
-- 	END AS patient_registry
-- FROM bronze.studies
-- )
-- SELECT patient_registry, COUNT(*)
-- FROM patient_registry_cleaned
-- GROUP BY patient_registry
-- ORDER BY  COUNT(*) DESC;

-- ----------------------------------- target_duration   ---------------------------
-- -- check nulls
-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE target_duration IS NULL;
-- -- 530755 null values

-- -- check distribution and null values
-- SELECT COUNT(*),  target_duration
-- FROM bronze.studies
-- GROUP BY target_duration
-- ORDER BY COUNT(*) DESC; -- replace null values with 'UNKNOWN'
-- -- no need to clean

-- WITH target_duration_days_cleaned AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN (target_duration ILIKE '%Day%'  OR target_duration ILIKE '%Days%') AND (CAST(SPLIT_PART(target_duration, ' ', 1) AS INTEGER) * 1)
-- 	        THEN CAST(SPLIT_PART(target_duration, ' ', 1) AS INTEGER) * 1
	
-- 	    WHEN target_duration ILIKE '%Week%' OR target_duration ILIKE '%Weeks%'
-- 	        THEN CAST(SPLIT_PART(target_duration, ' ', 1) AS INTEGER)* 7
	
-- 	    WHEN target_duration ILIKE '%Month%' OR target_duration ILIKE '%Months%'
-- 	        THEN CAST(SPLIT_PART(target_duration, ' ', 1) AS INTEGER) * 30 -- Using 30 as our approximation
	
-- 	    WHEN target_duration ILIKE '%Year%' OR target_duration ILIKE '%Years%'
-- 	        THEN CAST(SPLIT_PART(target_duration, ' ', 1) AS INTEGER) * 365
-- 	    WHEN target_duration ILIKE '%Day%'  OR target_duration ILIKE '%Days%'
-- 	        THEN CAST(SPLIT_PART(target_duration, ' ', 1) AS INTEGER) * 1
	
-- 	    WHEN target_duration ILIKE '%Week%' OR target_duration ILIKE '%Weeks%'
-- 	        THEN CAST(SPLIT_PART(target_duration, ' ', 1) AS INTEGER)* 7
	
-- 	    WHEN target_duration ILIKE '%Month%' OR target_duration ILIKE '%Months%'
-- 	        THEN CAST(SPLIT_PART(target_duration, ' ', 1) AS INTEGER) * 30 -- Using 30 as our approximation
	
-- 	    WHEN target_duration ILIKE '%Year%' OR target_duration ILIKE '%Years%'
-- 	        THEN CAST(SPLIT_PART(target_duration, ' ', 1) AS INTEGER) * 365
	
-- 	    ELSE NULL -- This handles the original NULLs and any other odd formats
-- 	END AS target_duration_days
-- FROM bronze.studies
-- )
-- SELECT COUNT(*),  target_duration_days
-- FROM target_duration_days_cleaned
-- GROUP BY target_duration_days
-- ORDER BY target_duration_days DESC;






-- ----------------------------------  enrollment   ---------------------------
-- -- check null values
-- SELECT  COUNT(*)
-- FROM silver.studies
-- WHERE enrollment IS NULL; -- 7005 null values, replace null with 0

-- -- Check outliers
-- SELECT MIN(enrollment) ,  MAX(enrollment),   AVG(enrollment)
-- FROM silver.studies; 

-- SELECT study_type, PERCENTILE_DISC(0.997) WITHIN GROUP(ORDER BY enrollment) as percentile_99th
-- FROM silver.studies
-- GROUP BY study_type;
-- -- we will cap the maximum for the observational to 566401 and the observation to 15886 based on 99.7 percentile



-- SELECT brief_title, enrollment
-- FROM silver.studies
-- WHERE enrollment IS NOT NULL
-- ORDER BY enrollment DESC ; 

-- ----------------------------------  enrollment_type   ---------------------------

-- SELECT enrollment_type,COUNT(*) 
-- FROM bronze.studies
-- GROUP BY enrollment_type;

-- WITH enrollment_type_cleaned AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN enrollment_type IS NULL
-- 			THEN 'Unknown'
-- 	ELSE enrollment_type
-- 	END AS enrollment_type
-- FROM bronze.studies
-- )
-- SELECT enrollment_type, COUNT(*)
-- FROM enrollment_type_cleaned
-- GROUP BY enrollment_type;


-- -----------------------------------  number_of_arms   ---------------------------------
-- -- check null values
-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE  number_of_arms IS NULL  ; -- 152619 null values


-- SELECT  COUNT(*)
-- FROM bronze.studies
-- WHERE number_of_arms IS NOT NULL AND number_of_groups IS NOT NULL
-- ;
-- -- nothing was returns, this tells us that these two columns are mutually exlusive

-- SELECT study_type,  COUNT(*)
-- FROM bronze.studies
-- WHERE number_of_arms IS NULL AND number_of_groups IS NOT NULL
-- GROUP BY study_type
-- ;
-- -- based on this, for Observational studies, number_of_group is used to count participants

-- SELECT study_type,  COUNT(*)
-- FROM bronze.studies
-- WHERE number_of_arms IS NOT NULL AND number_of_groups IS NULL
-- GROUP BY study_type
-- ;
-- -- based on this, for Interventional studies, number_of_group is used to count participants

-- -- for the number_of_arms, we will impute all the observational data with zero

-- WITH  number_of_arms_cleaned AS
-- (
-- SELECT
-- 	CASE
-- 		WHEN study_type = 'OBSERVATIONAL' AND number_of_arms IS NULL 
-- 			THEN 0
-- 		ELSE number_of_arms
-- 	END as number_of_arms
-- FROM bronze.studies
-- )

-- SELECT COUNT(*)
-- FROM number_of_arms_cleaned
-- WHERE  number_of_arms IS NULL  ; -- reduced to 25,907 null values which we will not impute but we will flag
-- 	;


-- -----------------------------------  number_of_groups   ---------------------------------

-- -- check null values
-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE  number_of_groups IS NULL  ; -- 462940 null values

-- -- for the number_of_groups, we will impute all the observational data with zero

-- WITH  number_of_groups_cleaned AS
-- (
-- SELECT
-- 	CASE
-- 		WHEN study_type = 'INTERVENTIONAL' AND number_of_groups IS NULL 
-- 			THEN 0
-- 		ELSE number_of_groups
-- 	END as number_of_groups
-- FROM bronze.studies
-- )

-- SELECT COUNT(*)
-- FROM number_of_groups_cleaned
-- WHERE  number_of_groups IS NULL  ; -- reduced to 46,278 null values which we will not impute but we will flag
-- 	;


-- ----------------------------------- new column paticipant_flag   ---------------------------------

-- WITH paticipant_flag_column AS
-- (
-- SELECT number_of_arms, number_of_groups,
-- 	CASE 
-- 		WHEN number_of_arms IS NOT NULL OR number_of_groups IS NOT NULL
-- 			THEN 'Actual'
-- 		WHEN number_of_arms IS NULL AND number_of_groups IS NULL
-- 			THEN 'No Record'
-- 		ELSE 'Imputed'
-- 	END AS paticipant_flag
-- FROM bronze.studies
-- )
-- SELECT*
-- FROM paticipant_flag_column
-- WHERE paticipant_flag IS NULL;

-- -- returns no nulls means everything is covered



-- ----------------------------------- has_dmc   ---------------------------------
-- -- data monitoring committee
-- SELECT has_dmc, COUNT(*)
-- FROM bronze.studies
-- GROUP BY has_dmc;
-- -- returns 97013 nulls 

-- WITH has_dmc_cleaned  AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN  has_dmc = 't'
-- 			THEN 'Yes'
-- 		WHEN has_dmc = 'f'
-- 			THEN 'No'
-- 		ELSE 'Unknown'-- best way is to turn nulls into 'unknown'
-- 	END AS has_dmc
-- FROM bronze.studies
-- )
-- SELECT has_dmc, COUNT(*)
-- FROM has_dmc_cleaned
-- GROUP BY has_dmc;


-- ----------------------------------- is_fda_regulated_drug   ---------------------------------

-- SELECT is_fda_regulated_drug, COUNT(*)
-- FROM bronze.studies
-- GROUP BY is_fda_regulated_drug;
-- -- returns 223100 nulls 

-- WITH is_fda_regulated_drug_cleaned  AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN  is_fda_regulated_drug = 't'
-- 			THEN 'Yes'
-- 		WHEN is_fda_regulated_drug = 'f'
-- 			THEN 'No'
-- 		ELSE 'Unknown'-- best way is to turn nulls into 'unknown'
-- 	END AS is_fda_regulated_drug
-- FROM bronze.studies
-- )
-- SELECT is_fda_regulated_drug, COUNT(*)
-- FROM is_fda_regulated_drug_cleaned
-- GROUP BY is_fda_regulated_drug;


-- ----------------------------------- fdaaa801_violation   ---------------------------------

-- SELECT fdaaa801_violation, COUNT(*)
-- FROM bronze.studies
-- GROUP BY fdaaa801_violation;
-- -- returns 223100 nulls 

-- WITH fdaaa801_violation_cleaned  AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN  fdaaa801_violation = 't'
-- 			THEN 'Violation'
-- 		ELSE 'No Violation'-- nulls mean no violations 
-- 	END AS fdaaa801_violation
-- FROM bronze.studies
-- )
-- SELECT fdaaa801_violation, COUNT(*)
-- FROM fdaaa801_violation_cleaned
-- GROUP BY fdaaa801_violation;


-- ----------------------------------- is_fda_regulated_device   ---------------------------------

-- SELECT is_fda_regulated_device, COUNT(*)
-- FROM bronze.studies
-- GROUP BY is_fda_regulated_device;
-- -- returns 223173 nulls 

-- WITH is_fda_regulated_device_cleaned  AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN  is_fda_regulated_device = 't'
-- 			THEN 'Yes'
-- 		WHEN is_fda_regulated_device = 'f'
-- 			THEN 'No'
-- 		ELSE 'Unknown'-- best way is to turn nulls into 'unknown'
-- 	END AS is_fda_regulated_device
-- FROM bronze.studies
-- )
-- SELECT is_fda_regulated_device, COUNT(*)
-- FROM is_fda_regulated_device_cleaned
-- GROUP BY is_fda_regulated_device;


-- ----------------------------------- is_unapproved_device   ---------------------------------

-- SELECT is_unapproved_device, COUNT(*)
-- FROM bronze.studies
-- GROUP BY is_unapproved_device;
-- -- returns 540904 nulls 

-- WITH is_unapproved_device_cleaned  AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN  is_unapproved_device = 't'
-- 			THEN 'Unapproved'
-- 		WHEN  is_fda_regulated_device = 't' AND is_unapproved_device is null
-- 			THEN 'Approved'
-- 		ELSE 'Not Applicable'-- nulls mean no violations 
-- 	END AS is_unapproved_device
-- FROM bronze.studies
-- )
-- SELECT is_unapproved_device, COUNT(*)
-- FROM is_unapproved_device_cleaned
-- GROUP BY is_unapproved_device;





-- ----------------------------------- is_ppsd   ---------------------------------
-- -- Pediatric Postmarket Surveillance of a Device
-- SELECT is_ppsd, COUNT(*)
-- FROM bronze.studies
-- GROUP BY is_ppsd;
-- -- returns 545274 nulls 

-- WITH is_ppsd_cleaned  AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN  is_ppsd = 't'
-- 			THEN 'PPSD study'
-- 		WHEN  is_fda_regulated_device = 't' AND is_ppsd is null
-- 			THEN 'Not PPSD study'
-- 		ELSE 'Not Applicable'-- nulls mean no violations 
-- 	END AS is_ppsd
-- FROM bronze.studies
-- )
-- SELECT is_ppsd, COUNT(*)
-- FROM is_ppsd_cleaned
-- GROUP BY is_ppsd;





-- ----------------------------------- is_us_export   ---------------------------------

-- SELECT is_us_export, COUNT(*)
-- FROM bronze.studies
-- GROUP BY is_us_export;
-- -- returns 466919 nulls 

-- WITH is_us_export_cleaned  AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN  is_us_export = 't'
-- 			THEN 'Yes'
-- 		WHEN is_us_export = 'f'
-- 			THEN 'No'
-- 		ELSE 'Unknown'-- best way is to turn nulls into 'unknown'
-- 	END AS is_us_export
-- FROM bronze.studies
-- )
-- SELECT is_us_export, COUNT(*)
-- FROM is_us_export_cleaned
-- GROUP BY is_us_export;




-- ----------------------------------- biospec_retention   ---------------------------------
-- SELECT biospec_retention, COUNT(*)
-- FROM bronze.studies
-- GROUP BY biospec_retention;

-- WITH biospec_retention_cleaned  AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN  biospec_retention IS NULL -- best way is to turn nulls into 'unknown'
-- 			THEN 'Unknown'
-- 		ELSE biospec_retention
-- 	END AS is_us_export
-- FROM bronze.studies
-- )
-- SELECT biospec_retention_cleaned, COUNT(*)
-- FROM biospec_retention_cleaned
-- GROUP BY biospec_retention_cleaned;


-- ----------------------------------- biospec_description   ---------------------------------
-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE biospec_description IS NULL;
-- -- 519326 NULLS

-- WITH biospec_description_cleaned  AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN  biospec_description IS NULL -- best way is to turn nulls into 'Not defined'
-- 			THEN 'Not defined'
-- 		ELSE biospec_description
-- 	END AS biospec_description
-- FROM bronze.studies
-- )
-- SELECT biospec_description, COUNT(*)
-- FROM biospec_description_cleaned
-- GROUP BY biospec_description
-- ORDER BY COUNT(*) DESC;




-- ----------------------------------- ipd_time_frame   ---------------------------------
-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE ipd_time_frame IS NULL;
-- -- 518324 NULLS

-- WITH ipd_time_frame_cleaned  AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN  ipd_time_frame IS NULL -- best way is to turn nulls into 'Not defined'
-- 			THEN 'Not specified'
-- 		ELSE ipd_time_frame
-- 	END AS ipd_time_frame
-- FROM bronze.studies
-- )
-- SELECT COUNT(*), ipd_time_frame
-- FROM ipd_time_frame_cleaned
-- GROUP BY ipd_time_frame
-- ORDER BY COUNT(*) DESC;




-- ----------------------------------- plan_to_share_ipd   ---------------------------------

-- SELECT COUNT(*), plan_to_share_ipd
-- FROM bronze.studies
-- GROUP BY plan_to_share_ipd
-- ORDER BY COUNT(*) DESC;
-- -- 272341 NULLS

-- WITH plan_to_share_ipd_cleaned  AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN  plan_to_share_ipd IS NULL -- best way is to turn nulls into 'Not defined'
-- 			THEN 'Not specified'
-- 		ELSE plan_to_share_ipd
-- 	END AS plan_to_share_ipd
-- FROM bronze.studies
-- )
-- SELECT COUNT(*), plan_to_share_ipd
-- FROM plan_to_share_ipd_cleaned
-- GROUP BY plan_to_share_ipd
-- ORDER BY COUNT(*) DESC;



-- ----------------------------------- plan_to_share_ipd_description   ---------------------------------

-- SELECT COUNT(*), plan_to_share_ipd_description
-- FROM bronze.studies
-- GROUP BY plan_to_share_ipd_description
-- ORDER BY COUNT(*) DESC;
-- -- 272341 NULLS

-- WITH plan_to_share_ipd_description_cleaned  AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN  plan_to_share_ipd_description IS NULL -- best way is to turn nulls into 'Not defined'
-- 			THEN 'Not specified'
-- 		ELSE plan_to_share_ipd_description
-- 	END AS plan_to_share_ipd_description
-- FROM bronze.studies
-- )
-- SELECT COUNT(*), plan_to_share_ipd_description
-- FROM plan_to_share_ipd_description_cleaned
-- GROUP BY plan_to_share_ipd_description
-- ORDER BY COUNT(*) DESC;



-- ----------------------------------- expanded_access_nctid   ---------------------------------

-- SELECT COUNT(*), expanded_access_nctid
-- FROM bronze.studies
-- WHERE has_expanded_access =='t'
-- GROUP BY expanded_access_nctid
-- ORDER BY COUNT(*) DESC
-- LIMIT 10;
-- -- 272341 NULLS -need to also correct the `has_expanded_access`, we will not touch this


-- ----------------------------------- has_expanded_access   ---------------------------------

-- -- based on the previous finding we are going to rewrite this column


-- WITH has_expanded_access_cleaned AS
-- (SELECT 
-- 	CASE
-- 		WHEN expanded_access_nctid IS NULL
-- 		THEN 'No'
-- 		ELSE 'Yes'
-- 	END AS has_expanded_access
-- FROM bronze.studies)
-- SELECT COUNT(*), has_expanded_access
-- FROM has_expanded_access_cleaned
-- GROUP BY has_expanded_access
-- ORDER BY  COUNT(*) DESC;

-- ----------------------------------- expanded_access_status_for_nctid   ---------------------------------


-- SELECT COUNT(*), expanded_access_status_for_nctid
-- FROM bronze.studies
-- GROUP BY expanded_access_status_for_nctid
-- ORDER BY COUNT(*) DESC
-- LIMIT 10;
-- -- 272341 NULLS -need to also correct the `has_expanded_access`, we will not touch this


-- SELECT COUNT(*), expanded_access_status_for_nctid
-- FROM bronze.studies
-- WHERE expanded_access_nctid IS NOT NULL
-- GROUP BY expanded_access_status_for_nctid
-- ORDER BY COUNT(*) DESC
-- LIMIT 10;
-- -- this logic is correct. no null values for the ones that have expanded_access_nctid




-- WITH expanded_access_status_for_nctid_cleaned AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN expanded_access_status_for_nctid IS NULL
-- 		THEN 'Not Applicable'
-- 		ELSE expanded_access_status_for_nctid
-- 	END AS expanded_access_status_for_nctid
-- FROM bronze.studies
-- )
-- SELECT COUNT(*), expanded_access_status_for_nctid
-- FROM expanded_access_status_for_nctid_cleaned
-- GROUP BY expanded_access_status_for_nctid
-- ORDER BY  COUNT(*) DESC;



-- ----------------------------------- expanded_access_type_individual   ---------------------------------




-- SELECT COUNT(*), expanded_access_type_individual
-- FROM bronze.studies
-- GROUP BY expanded_access_type_individual
-- ORDER BY COUNT(*) DESC;
-- -- 272341 NULLS

-- WITH expanded_access_type_individual_cleaned  AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN  expanded_access_type_individual IS NULL -- NULL Means 'No'
-- 			THEN 'No'
-- 		ELSE 'Yes'
-- 	END AS expanded_access_type_individual
-- FROM bronze.studies
-- )
-- SELECT COUNT(*), expanded_access_type_individual
-- FROM expanded_access_type_individual_cleaned
-- GROUP BY expanded_access_type_individual
-- ORDER BY COUNT(*) DESC;

-- ----------------------------------- expanded_access_type_intermediate   ---------------------------------




-- SELECT COUNT(*), expanded_access_type_intermediate
-- FROM bronze.studies
-- GROUP BY expanded_access_type_intermediate
-- ORDER BY COUNT(*) DESC;
-- -- 272341 NULLS

-- SELECT COUNT(*), expanded_access_type_intermediate
-- FROM bronze.studies
-- WHERE expanded_access_type_individual IS NOT NULL
-- GROUP BY expanded_access_type_intermediate
-- ORDER BY COUNT(*) DESC;
-- -- there are 16 columns that contradicts the logic that individual and intermediate access are mutually exclusive but we will skip this for now

-- WITH expanded_access_type_intermediate_cleaned  AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN  expanded_access_type_intermediate IS NULL -- NULL Means 'No'
-- 			THEN 'No'
-- 		ELSE 'Yes'
-- 	END AS expanded_access_type_intermediate
-- FROM bronze.studies
-- )
-- SELECT COUNT(*), expanded_access_type_intermediate
-- FROM expanded_access_type_intermediate_cleaned
-- GROUP BY expanded_access_type_intermediate
-- ORDER BY COUNT(*) DESC;


-- ----------------------------------- expanded_access_type_treatment   ---------------------------------


-- SELECT COUNT(*), expanded_access_type_treatment
-- FROM bronze.studies
-- GROUP BY expanded_access_type_treatment
-- ORDER BY COUNT(*) DESC;
-- -- 272341 NULLS


-- -- there are 16 data that contradicts the logic that individual and intermediate access are mutually exclusive but we will skip this for now

-- WITH expanded_access_type_treatment_cleaned  AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN  expanded_access_type_treatment IS NULL -- NULL Means 'No'
-- 			THEN 'No'
-- 		ELSE 'Yes'
-- 	END AS expanded_access_type_treatment
-- FROM bronze.studies
-- )
-- SELECT COUNT(*), expanded_access_type_treatment
-- FROM expanded_access_type_treatment_cleaned
-- GROUP BY expanded_access_type_treatment
-- ORDER BY COUNT(*) DESC;


-- ----------------------------------- delayed_posting   ---------------------------------


-- SELECT COUNT(*), delayed_posting
-- FROM bronze.studies
-- GROUP BY delayed_posting
-- ORDER BY COUNT(*) DESC;
-- -- 272341 NULLS

-- WITH delayed_posting_cleaned  AS
-- (
-- SELECT 
-- 	CASE
-- 		WHEN  delayed_posting IS NULL -- NULL Means 'Not Specified'
-- 			THEN 'Not Specified'
-- 		ELSE 'Yes'
-- 	END AS delayed_posting
-- FROM bronze.studies
-- )
-- SELECT COUNT(*), delayed_posting
-- FROM delayed_posting_cleaned
-- GROUP BY delayed_posting
-- ORDER BY COUNT(*) DESC;


-- -----------------------------------  study_first_submitted_date   ---------------------------

-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE study_first_submitted_date IS NULL;
-- -- NO NULL VALUES


-- -- handle outliers, this is cleanand reasonable
-- SELECT MAX(study_first_submitted_date), MIN(study_first_submitted_date)
-- FROM silver.studies;


-- -----------------------------------  study_first_posted_date   ---------------------------

-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE study_first_posted_date IS NULL;
-- -- NO NULL VALUES



-- SELECT MAX(study_first_submitted_date), MIN(study_first_submitted_date)
-- FROM silver.studies;
-- -- no unreasonable dates



-- -----------------------------------  results_first_submitted_qc_date   ---------------------------

-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE results_first_submitted_qc_date IS NULL;
-- -- NO NULL VALUES



-- SELECT MAX(results_first_submitted_qc_date), MIN(results_first_submitted_qc_date)
-- FROM bronze.studies;
-- -- no unreasonable dates

-- -----------------------------------  results_first_posted_date_type   ---------------------------

-- SELECT overall_status, results_first_posted_date_type
-- FROM bronze.studies
-- WHERE results_first_posted_date_type IS NULL;

-- -- need to handle nulls




-- -----------------------------------  start_date -------------------------------

-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE start_date IS NULL;

-- ;
-- -- handling outliers
-- SELECT MAX(start_date), MIN(start_date)
-- FROM bronze.studies;

-- -- start_date should not be less than  study_first_submitted_date, therefore we impute this outliers with the study_first_submitted_date

-- -- Handling nulls and outliers
-- WITH start_date_cleaned AS
-- (SELECT 
-- CASE 
-- 	WHEN start_date IS NULL OR (start_date < '1999-09-17') OR start_date > CURRENT_DATE -- handle outliers and nulls
-- 	THEN study_first_submitted_date
-- 	ELSE start_date
-- 	END as start_date, study_first_submitted_date
-- FROM bronze.studies)

-- SELECT MAX(start_date), MIN(start_date)
-- FROM start_date_cleaned;

-- ;
-- -----------------------------------  start_date_type   ---------------------------
-- -- we need to flag the imputed data of the start_date
-- WITH start_date_type_cleaned AS
-- (SELECT start_date, 
-- CASE 
-- 	WHEN start_date IS NULL OR (start_date < '1999-09-17') OR start_date > CURRENT_DATE THEN 'Imputed'-- flag outliers
-- 	WHEN start_date_type IS NULL THEN 'Not Categorized' -- handle nulls
-- 	ELSE start_date_type
-- 	END as start_date_type,
-- 	study_first_submitted_date
-- FROM bronze.studies)
-- SELECT COUNT(*)
-- FROM start_date_type_cleaned
-- WHERE start_date_type IS NULL;



-- -----------------------------------  last_update_submitted_date -------------------------------

-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE last_update_submitted_date IS NULL;
-- -- no null values


-- SELECT MAX(last_update_submitted_date), MIN(last_update_submitted_date)
-- FROM bronze.studies;
-- -- no outliers


-- -----------------------------------  last_update_submitted_qc_date -------------------------------

-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE last_update_submitted_qc_date IS NULL;
-- -- no null values


-- SELECT MAX(last_update_submitted_qc_date), MIN(last_update_submitted_qc_date)
-- FROM bronze.studies;
-- -- no outliers



-- -----------------------------------  last_update_posted_date -------------------------------

-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE last_update_posted_date IS NULL;
-- -- no null values


-- SELECT MAX(last_update_posted_date), MIN(last_update_posted_date)
-- FROM bronze.studies;
-- -- no outliers

-- -----------------------------------  last_update_posted_date_type -------------------------------
-- SELECT last_update_posted_date_type, COUNT(*)
-- FROM bronze.studies
-- GROUP BY last_update_posted_date_type
-- -- all good





-- -----------------------------------  primary_completion_date -------------------------------

-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE overall_status NOT IN ('COMPLETED', 'APPROVED_FOR_MARKETING')AND (primary_completion_date IS NOT NULL);

-- -- handle outliers, this is cleanand reasonable
-- SELECT MAX(primary_completion_date), MIN(primary_completion_date)
-- FROM bronze.studies;


-- SELECT overall_status,  primary_completion_date, completion_date
-- FROM bronze.studies
-- WHERE overall_status IN ('COMPLETED', 'APPROVED_FOR_MARKETING')AND (primary_completion_date IS NULL);

-- WITH primary_completion_date_cleaned AS (
--   SELECT
--     overall_status,
--     last_update_submitted_date,
--     completion_date,
--     primary_completion_date,
--     CASE
-- 	  WHEN primary_completion_date IS NOT NULL AND
-- 	       (primary_completion_date < '1999-09-17' 
-- 	       OR primary_completion_date > CURRENT_DATE) THEN
-- 	    last_update_submitted_date -- handle outliers
-- 	  WHEN overall_status IN ('COMPLETED', 'APPROVED_FOR_MARKETING') THEN
-- 	    COALESCE(primary_completion_date,last_update_submitted_date)
-- 	  ELSE primary_completion_date
-- 	END AS cleaned_primary_completion_date
--   FROM bronze.studies
-- )
-- SELECT *
-- FROM primary_completion_date_cleaned
-- WHERE cleaned_primary_completion_date IS NULL AND overall_status IN ('COMPLETED', 'APPROVED_FOR_MARKETING');




-- -----------------------------------  primary_completion_date_type -------------------------------

-- WITH primary_completion_date_type_cleaned AS (
--   SELECT
--     overall_status,
--     last_update_submitted_date,
--     completion_date,
--     primary_completion_date,
--     CASE
-- 	  WHEN primary_completion_date_type IS NOT NULL AND
-- 	  	   (primary_completion_date < '1999-09-17' 
-- 	       OR primary_completion_date > CURRENT_DATE) THEN 'Imputed' -- handle outliers
--       WHEN (overall_status IN  ('COMPLETED', 'APPROVED_FOR_MARKETING')) AND (primary_completion_date IS NULL OR completion_date IS NULL  OR last_update_submitted_date IS NULL)
-- 	  THEN 'Imputed'-- flag the imputed primary_completion_date
-- 	  WHEN primary_completion_date_type IS NULL  THEN 'ESTIMATED'
--       ELSE primary_completion_date_type
--     END AS primary_completion_date_type_cleaned
--   FROM bronze.studies
-- )
-- SELECT*
-- FROM primary_completion_date_type_cleaned
-- WHERE  primary_completion_date_type_cleaned IS NULL;


-- SELECT primary_completion_date_type_cleaned, COUNT(*)
-- FROM primary_completion_date_type_cleaned
-- GROUP BY primary_completion_date_type_cleaned;



-- -----------------------------------  completion_date -------------------------------


-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE overall_status NOT IN ('COMPLETED', 'APPROVED_FOR_MARKETING')AND (completion_date IS NOT NULL);

-- -- handle outliers, this is cleanand reasonable
-- SELECT MAX(primary_completion_date), MIN(primary_completion_date)
-- FROM bronze.studies;

-- SELECT overall_status,  primary_completion_date, completion_date
-- FROM bronze.studies
-- WHERE overall_status IN ('COMPLETED', 'APPROVED_FOR_MARKETING')AND (primary_completion_date IS NULL);

-- WITH completion_date_cleaned AS (
--   SELECT
--     overall_status,
--     last_update_submitted_date,
--     completion_date,
--     primary_completion_date,
--     CASE
-- 	  WHEN completion_date IS NOT NULL AND
-- 	       (completion_date < '1999-09-17' 
-- 	       OR completion_date > CURRENT_DATE) THEN
-- 	    last_update_submitted_date -- handle outliers
-- 	  WHEN overall_status IN ('COMPLETED', 'APPROVED_FOR_MARKETING') THEN
-- 	    COALESCE(completion_date,last_update_submitted_date) -- Impute missing completed trials
-- 	  ELSE completion_date
-- 	END AS cleaned_completion_date
--   FROM bronze.studies
-- )
-- SELECT *
-- FROM completion_date_cleaned
-- WHERE cleaned_completion_date IS NULL AND overall_status IN ('COMPLETED', 'APPROVED_FOR_MARKETING');


-- -----------------------------------  completion_date_type -------------------------------

-- WITH completion_date_type_cleaned AS (
--   SELECT
--     overall_status,
--     last_update_submitted_date,
--     completion_date,
--     primary_completion_date,
--     CASE
-- 	  WHEN completion_date_type IS NOT NULL AND
-- 	  	   (completion_date < '1999-09-17' 
-- 	       OR completion_date > CURRENT_DATE) THEN 'Imputed' -- flag imputed outliers
--       WHEN (overall_status IN  ('COMPLETED', 'APPROVED_FOR_MARKETING')) AND (primary_completion_date IS NULL OR completion_date IS NULL  OR last_update_submitted_date IS NULL)
-- 	  THEN 'Imputed'-- flag the imputed primary_completion_date
-- 	  WHEN completion_date_type IS NULL  THEN 'ESTIMATED'
--       ELSE completion_date_type
--     END AS completion_date_type_cleaned
--   FROM bronze.studies
-- )

-- SELECT completion_date_type_cleaned, COUNT(*)
-- FROM completion_date_type_cleaned
-- GROUP BY completion_date_type_cleaned;



-- -----------------------------------  results_first_submitted_date -------------------------------

-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE results_first_submitted_date IS NULL;

-- SELECT MAX(results_first_submitted_date), MIN(results_first_submitted_date)
-- FROM bronze.studies;
-- -- leave as is and we will create a flag for these dates results_first_submitted_date, results_first_posted_date, results_first_posted_date


-- -----------------------------------  results_first_posted_date -------------------------------


-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE results_first_posted_date IS NULL;

-- SELECT MAX(results_first_posted_date), MIN(results_first_posted_date)
-- FROM bronze.studies;
-- -- leave as is and we will create a flag for these dates results_first_submitted_date, results_first_posted_date, results_first_posted_date


-- -----------------------------------  results_first_posted_date -------------------------------


-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE results_first_submitted_qc_date IS NULL;

-- SELECT MAX(results_first_submitted_qc_date), MIN(results_first_submitted_qc_date)
-- FROM bronze.studies;
-- -- leave as is and we will create a flag for these dates results_first_submitted_date, results_first_posted_date, results_first_posted_date

-- -----------------------------------  disposition_first_submitted_date -------------------------------


-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE disposition_first_submitted_date IS NULL;

-- -- so many null, 535909, but lets leave it as we will not use it for the gold layer

-- SELECT MAX(disposition_first_submitted_date), MIN(disposition_first_submitted_date)
-- FROM bronze.studies;
-- -- no outliers




-- -----------------------------------  disposition_first_submitted_qc_date -------------------------------


-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE disposition_first_submitted_qc_date IS NULL;

-- -- so many null, 535909, but lets leave it as we will not use it for the gold layer

-- SELECT MAX(disposition_first_submitted_qc_date), MIN(disposition_first_submitted_qc_date)
-- FROM bronze.studies;
-- -- no outliers

-- -----------------------------------  disposition_first_posted_date -------------------------------


-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE disposition_first_posted_date IS NULL;

-- -- so many null, 535909, but lets leave it as we will not use it for the gold layer

-- SELECT MAX(disposition_first_posted_date), MIN(disposition_first_posted_date)
-- FROM bronze.studies;
-- -- no outliers
-- SELECT COUNT(*)
-- FROM bronze.studies


-- -----------------------------------  verification_date -------------------------------
-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE verification_date IS NULL;
-- -- have 920 nulls


-- SELECT overall_status, COUNT(*)
-- FROM bronze.studies
-- WHERE verification_date IS NULL
-- GROUP BY overall_status;

-- -- outliers belongs to those who are withheld


-- SELECT MAX(verification_date), MIN(verification_date)
-- FROM bronze.studies;
-- -- no outliers

-- SELECT overall_status, study_first_submitted_date, start_date, verification_date
-- FROM bronze.studies
-- ORDER BY verification_date
-- LIMIT 10;
-- -- there are some inconsistencies most probably it is system issue, i will just use flagging to those illogical verification date

-- -----------------------------------  verification_type -------------------------------

-- WITH verification_date_cleaned AS
-- (
-- SELECT overall_status, study_first_submitted_date, start_date,
-- 	CASE
-- 	  WHEN verification_date IS NULL THEN 'Information Witheld'
-- 	  WHEN verification_date  < study_first_submitted_date THEN 'Invalid  - Verification too early'
-- 	  WHEN verification_date > CURRENT_DATE THEN 'Invalid - In the future'
-- 	  ELSE 'Valid'
-- 	END AS verification_date_type
-- FROM bronze.studies
-- )
-- SELECT verification_date_type, COUNT(*)
-- FROM verification_date_cleaned
-- GROUP BY verification_date_type

-- -- instead of using the verification date, we will use this as quality check

-- -----------------------------------  created_at -------------------------------

-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE created_at IS NULL;

-- -- no nulls

-- SELECT MAX(created_at), MIN(created_at)
-- FROM bronze.studies;
-- -- no outliers




-- -----------------------------------  updated_at -------------------------------

-- SELECT COUNT(*)
-- FROM bronze.studies
-- WHERE updated_at IS NULL;

-- -- no nulls

-- SELECT MAX(updated_at), MIN(updated_at)
-- FROM bronze.studies;
-- -- no outliers


-- /*

-- For this 4 columns, we dont have to do anything as they can be derived anytime from our cleaned silver layers
-- start_month_year
-- verification_month_year
-- completion_month_year
-- primary_completion_month_year

-- */





-- --=========================================================================================
-- -- 2. Data Cleaning, Standardization, Normalization and Data Enrichment for silver.sponsors
-- --=========================================================================================




-- -----------------------------------  id -------------------------------

-- SELECT id, COUNT(*)
-- FROM bronze.sponsors
-- GROUP BY id
-- ORDER BY COUNT(*) DESC
-- LIMIT 10;

-- -- no nulls

-- SELECT MAX(created_at), MIN(created_at)
-- FROM bronze.studies;
-- -- no outliers


-- SELECT *
-- FROM bronze.sponsors
-- LIMIT 10;



-- -----------------------------------  NAME -------------------------------

-- SELECT *
-- FROM bronze.sponsors
-- WHERE name ILIKE '%pfizer';



-- SELECT name, COUNT(*)
-- FROM bronze.sponsors
-- GROUP BY  name
-- ORDER BY COUNT(*) DESC
-- LIMIT 100;
-- -- we will make a mapping table to clean all the top sponsor

-- ----------------------------------- CREATED COLUMN sponsor_category -------------------------------


-- SELECT sponsor_category, COUNT(*)
-- FROM silver.sponsors
-- GROUP BY  sponsor_category
-- ORDER BY COUNT(*) DESC
-- ;




-- ----------------------------------- CREATED COLUMN lead_or_collaborator -------------------------------


-- SELECT lead_or_collaborator, COUNT(*)
-- FROM silver.sponsors
-- GROUP BY  lead_or_collaborator
-- ORDER BY COUNT(*) DESC
-- ;




-- --========================================================================================
-- -- 3. Data Cleaning, Standardization, Normalization and Data Enrichment for silver.conditions
-- --========================================================================================



-- -- -----------------------------------  id -------------------------------
-- SELECT id, COUNT(*)
-- FROM bronze.conditions
-- GROUP BY id
-- ORDER BY COUNT(*) DESC
-- LIMIT 10;
-- -- no nulls

-- -- -----------------------------------  name -------------------------------
-- SELECT name, COUNT(*)
-- FROM bronze.conditions
-- GROUP BY name
-- ORDER BY COUNT(*) DESC
-- ;
-- -- no nulls


-- -- -----------------------------------  nct_id -------------------------------
-- SELECT nct_id, COUNT(name)
-- FROM bronze.conditions
-- GROUP BY nct_id
-- HAVING COUNT(name) > 1
-- ORDER BY COUNT(*) DESC
-- ;

-- SELECT  COUNT(*)
-- FROM bronze.conditions
-- WHERE nct_id IS NULL;
-- -- no nulls
-- -- -----------------------------------  downcase_name -------------------------------
-- SELECT downcase_name, COUNT(name)
-- FROM bronze.conditions
-- GROUP BY downcase_name
-- HAVING COUNT(name) > 1
-- ORDER BY COUNT(*) DESC
-- ;

-- SELECT name, COUNT(name)
-- FROM silver.conditions
-- GROUP BY name
-- HAVING COUNT(name) > 1
-- ORDER BY COUNT(*) DESC
-- looks good and standardized than name, better to use this one as name 




-- --==============================================================================================
-- -- 4. Data Cleaning, Standardization, Normalization and Data Enrichment for silver.interventions
-- --==============================================================================================
-- -- -----------------------------------  id -------------------------------
-- SELECT id, COUNT(*)
-- FROM bronze.interventions
-- GROUP BY id
-- ORDER BY COUNT(*) DESC
-- LIMIT 10;
-- -- no nulls and no dupilcates


-- -- -----------------------------------  nct_id -------------------------------
-- SELECT nct_id, COUNT(*)
-- FROM bronze.interventions
-- GROUP BY nct_id
-- ORDER BY COUNT(*) DESC
-- ;
-- -- no nulls


-- -- -----------------------------------  intervention_type -------------------------------
-- SELECT intervention_type, COUNT(*)
-- FROM bronze.interventions
-- GROUP BY intervention_type
-- ORDER BY COUNT(*) DESC;
-- -- no nulls

-- -- -----------------------------------  name -------------------------------
-- SELECT name, COUNT(*)
-- FROM bronze.interventions
-- GROUP BY name
-- ORDER BY COUNT(*) DESC;
-- -- no nulls
	
-- SELECT name, COUNT(*)
-- FROM silver.interventions
-- GROUP BY name
-- ORDER BY COUNT(*) DESC;
-- -- no nulls


-- SELECT*
-- FROM bronze.interventions
-- WHERE name IS NULL;

-- -- no nulls



-- --===========================================================================================
-- -- 5. Data Cleaning, Standardization, Normalization and Data Enrichment for silver.outcomes
-- --=========================================================================================

-- -- -----------------------------------  outcome_type -------------------------------

-- SELECT id, COUNT(*)
-- FROM bronze.outcomes
-- GROUP BY id
-- ORDER BY COUNT(*) DESC;
-- -- no nulls and no dupilcates

-- -- -----------------------------------  nct_id -------------------------------
-- SELECT COUNT(*)
-- FROM bronze.outcomes
-- WHERE nct_id IS NULL;
-- -- no nulls 

-- -- -----------------------------------  outcome_type -------------------------------
-- SELECT outcome_type, COUNT(*)
-- FROM bronze.outcomes
-- GROUP BY outcome_type
-- ORDER BY COUNT(*) DESC;
-- -- no nulls and no dupilcates

-- -- -----------------------------------  title -------------------------------
-- SELECT title, COUNT(*)
-- FROM bronze.outcomes
-- GROUP BY title
-- ORDER BY COUNT(*) DESC;
-- -- no nulls and no dupilcates


-- SELECT COUNT(*)
-- FROM bronze.outcomes
-- WHERE title IS NULL;
-- -- no nulls 


-- -- -----------------------------------  time_frame -------------------------------
-- SELECT COUNT(*), time_frame 
-- FROM bronze.outcomes
-- GROUP BY time_frame
-- ORDER BY COUNT(*) DESC;



-- SELECT COUNT(*)
-- FROM bronze.outcomes
-- WHERE time_frame IS NULL;
-- --  59 nulls, need to change it to "Not speecified during loading"

-- -- -----------------------------------  population -------------------------------
-- SELECT COUNT(*), population 
-- FROM bronze.outcomes
-- GROUP BY population
-- ORDER BY COUNT(*) DESC;
-- -- 145896 nullswe will replace this with not specified



















