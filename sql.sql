--From insurance_data
SELECT *
FROM insurance_data
--from employee_data
SELECT *
FROM employee_data

-- Implementation
SELECT TOP (1000) 
      txn_date_time,
      transaction_id,
      customer_id,
      policy_number,
      policy_eff_dt,
      loss_dt,
      report_dt,
      insurance_type,
      premium_amount,
      claim_amount,
      age,
      marital_status,
      employment_status,
      no_of_family_members,
      risk_segmentation,
      claim_status,
      incident_severity,
      authority_contacted,
      any_injury,
      police_report_available,
      incident_state,
      incident_city,
      incident_hour_of_the_day,
      agent_id
  FROM insurance_data;
  --Eployee data
  
SELECT TOP (1000)
      agent_id,
      agent_name,
      date_of_joining,
      city,
	  state
  FROM employee_data;

  -- Data Cleaning
  
-- Check for missing values in `insurance_data` table
SELECT * 
FROM insurance_data 
WHERE txn_date_time IS NULL OR
transaction_id IS NULL OR
customer_id IS NULL OR
customer_name IS NULL OR
policy_number IS NULL OR 
insurance_type IS NULL OR
claim_amount IS NULL OR
city IS NULL OR
state IS NULL OR
incident_state IS NULL OR
incident_city IS NULL OR
agent_id IS NULL

SELECT * 
FROM employee_data
WHERE agent_id IS NULL OR
agent_name IS NULL OR 
city IS NULL OR
state IS NULL


-- Remove null values from insurance_data
SELECT txn_date_time,
transaction_id,
customer_id,
customer_name,
policy_number ,
insurance_type,
claim_amount,
city,
state,
incident_state,
incident_city,
agent_id
FROM insurance_data
WHERE city IS NOT NULL 

SELECT agent_id,
agent_name,
city,
state
FROM employee_data
WHERE city IS NOT NULL



-- Transform txn_date_time to standard date and time formats.
ALTER TABLE insurance_data
ADD txn_date DATE;

ALTER TABLE insurance_data
ADD txn_year INT;

ALTER TABLE insurance_data
ADD txn_month INT;

-- Updating new columns with transformed data
UPDATE insurance_data
SET txn_date = CAST(txn_date_time AS DATE),
    txn_year = YEAR(txn_date_time),
    txn_month = MONTH(txn_date_time)
	
	--Statistics for claim amount.
SELECT 
    AVG(claim_amount) AS avg_claim_amount,
    MIN(claim_amount) AS min_claim_amount,
    MAX(claim_amount) AS max_claim_amount,
    COUNT(*) AS total_claims
FROM insurance_data;

-- Count of claims by insurance type
SELECT 
insurance_type,
COUNT(*) AS claim_count
FROM insurance_data
GROUP BY insurance_type;

--Average claim amount by insurance type
SELECT 
insurance_type, 
AVG(claim_amount) AS avg_claim_amount
FROM insurance_data
GROUP BY insurance_type;

--Claims by Month and Year
SELECT
txn_year,
txn_month,
COUNT(*) AS claim_count
FROM insurance_data
GROUP BY txn_year, txn_month
ORDER BY txn_year, txn_month;

--Claims by state 
SELECT
state,
COUNT(*) AS claim_count
FROM insurance_data
GROUP BY state;

--High claim amounts(potemcial fraud)

SELECT 
    transaction_id, 
    customer_id, 
    claim_amount
FROM insurance_data
WHERE claim_amount > (SELECT AVG(claim_amount) + 3 * STDEV(claim_amount) FROM insurance_data)
ORDER BY claim_amount DESC;

--claim by agent

SELECT 
    e.agent_id,
    e.agent_name,
    COUNT(*) AS claim_count,
    AVG(i.claim_amount) AS avg_claim_amount
FROM insurance_data i
JOIN employee_data e ON i.agent_id = e.agent_id
GROUP BY e.agent_id, e.agent_name;

-- Claims by insurance State and City 
SELECT 
incident_state,
incident_city,
COUNT (*) AS claim_count
FROM insurance_data 
GROUP BY incident_state, incident_city;

--High claim amount by agent ID and city 

SELECT 
    agent_id, 
    city, 
    transaction_id, 
    customer_id, 
    policy_number,
    insurance_type,
    claim_amount
FROM insurance_data
WHERE claim_amount > (SELECT AVG(claim_amount) + 3 * STDEV(claim_amount) FROM insurance_data)
ORDER BY claim_amount DESC, agent_id, city;

--claims by insurance type and incident severity
SELECT 
    insurance_type, 
    incident_severity, 
    COUNT(*) AS claim_count
FROM insurance_data
GROUP BY insurance_type, incident_severity
ORDER BY claim_count DESC;

--claims by coustomer ID with high claim amounts
SELECT 
    customer_id, 
    COUNT(*) AS claim_count, 
    SUM(claim_amount) AS total_claim_amount
FROM insurance_data
WHERE claim_amount > (SELECT AVG(claim_amount) + 3 * STDEV(claim_amount) FROM insurance_data)
GROUP BY customer_id
ORDER BY total_claim_amount DESC;


-- Create a new table to store summary data
CREATE TABLE fraud_summary (
    txn_date DATE,
    txn_year INT,
    txn_month INT,
    transaction_id NVARCHAR(50),
    customer_id NVARCHAR(50),
    policy_number VARCHAR(255),
    insurance_type VARCHAR(255),
    claim_amount DECIMAL(18, 2),
    city VARCHAR(255),
    state VARCHAR(255),
    agent_id NVARCHAR(50),
    agent_name VARCHAR(255),
    days_to_report INT
);

--insert data into the table

INSERT INTO fraud_summary (
    txn_date,
    txn_year,
    txn_month,
    transaction_id,
    customer_id,
    policy_number,
    insurance_type,
    claim_amount,
    city,
    state,
    agent_id,
    agent_name,
    days_to_report
)
SELECT 
    CAST(i.txn_date_time AS DATE) AS txn_date,
    YEAR(i.txn_date_time) AS txn_year,
    MONTH(i.txn_date_time) AS txn_month,
    i.transaction_id,
    i.customer_id,
    i.policy_number,
    i.insurance_type,
    i.claim_amount,
    i.city,
    i.state,
    i.agent_id,
    e.agent_name,
    DATEDIFF(day, i.policy_eff_dt, i.report_dt) AS days_to_report
FROM insurance_data i
JOIN employee_data e ON i.agent_id = e.agent_id
WHERE i.city IS NOT NULL;

--Create indexes for faster querying
CREATE INDEX idx_fraud_summary_date ON fraud_summary (txn_date);
CREATE INDEX idx_fraud_summary_year_month ON fraud_summary (txn_year, txn_month);
CREATE INDEX idx_fraud_summary_insurance_type ON fraud_summary (insurance_type);
CREATE INDEX idx_fraud_summary_state ON fraud_summary (state);
