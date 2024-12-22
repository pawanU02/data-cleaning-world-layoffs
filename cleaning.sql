-- DATA CLEANING

/*
Acces database from terminal: sqlite3 world_layoffs.db
Show tables: .tables
Run sql file from terminal: .read data.sql
Descibe the table: .schema layoffs
Clear shell: .shell clear
*/

-- STEPS:
-- 1. Check for DUPLICATES and remove them
-- 2. STANDARDIZE the data
-- 3. Handle NULL or BLANK vaklues
-- 4. Remove (columns) when required (for irrelevant columns)


SELECT * FROM layoffs LIMIT 10;

SELECT COUNT(*) FROM layoffs;


-- Creating an empty staging table different from raw data
CREATE TABLE layoffs_staging AS SELECT * FROM layoffs WHERE 1=0;

SELECT * FROM layoffs_staging;

-- Inserting values in layoffs_staging
INSERT INTO layoffs_staging
SELECT * FROM layoffs;

-- 1. Check for DUPLICATES and remove them

-- Giving row number unique to data to check for duplicates
SELECT *, ROW_NUMBER() OVER(
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num FROM layoffs_staging;

-- Checking for duplicates
WITH duplicate_cte AS
(
    SELECT *, ROW_NUMBER() OVER(
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num FROM layoffs_staging
)
SELECT * FROM duplicate_cte WHERE row_num > 1;

-- To remove the duplicate, we create staging2 table with row_num and delete the duplicate there.
CREATE TABLE layoffs_staging2 (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT DEFAULT NULL,
    percentage_laid_off TEXT,
    "date" TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT DEFAULT NULL,
    row_num INT
);

INSERT INTO layoffs_staging2
SELECT *, ROW_NUMBER() OVER(
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num FROM layoffs_staging;

SELECT * FROM layoffs_staging2;

SELECT * FROM layoffs_staging2
WHERE row_num > 1;

-- Delete the duplicates
DELETE FROM layoffs_staging2
WHERE row_num > 1;

-- Now we have deleted the duplicates

-- 2. STANDARDIZE the data

-- company
SELECT 
    company, TRIM(company)
FROM layoffs_staging2;

UPDATE
    layoffs_staging2
SET company = TRIM(company);

-- industry
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

/* Crypto
Crypto Currency
CryptoCurrency -- could be same */

SELECT * FROM 
layoffs_staging2
WHERE industry LIKE 'Crypto%'

-- update them to Crypto
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- location
SELECT DISTINCT `location` FROM
layoffs_staging2 ORDER BY 1;

-- country
SELECT DISTINCT `country` FROM
layoffs_staging2 ORDER BY 1;

-- we got an error in United States --> United States.
SELECT DISTINCT `country` FROM
layoffs_staging2
WHERE `country` LIKE 'United States%';

-- Update
UPDATE layoffs_staging2
SET country = TRIM(country, '.')
WHERE country LIKE 'United States%';


-- date
SELECT
    "date",
    CASE
        -- Case 1: mm/dd/yyyy (e.g., 12/16/2022)
        WHEN "date" LIKE '__/__/____' THEN
            strftime('%Y-%m-%d',
                substr("date", 7, 4) || '-' || substr("date", 1, 2) || '-' || substr("date", 4, 2)
            )
        -- Case 2: m/dd/yyyy (e.g., 7/25/2022)
        WHEN "date" LIKE '_/__/____' THEN
            strftime('%Y-%m-%d',
                substr("date", 6, 4) || '-0' || substr("date", 1, 1) || '-' || substr("date", 3, 2)
            )
        -- Case 3: mm/d/yyyy (e.g., 12/4/2022)
        WHEN "date" LIKE '__/_/____' THEN
            strftime('%Y-%m-%d',
                substr("date", 6, 4) || '-' || substr("date", 1, 2) || '-0' || substr("date", 4, 1)
            )
        -- Case 4: m/d/yyyy (e.g., 7/4/2022)
        WHEN "date" LIKE '_/_/____' THEN
            strftime('%Y-%m-%d',
                substr("date", 5, 4) || '-0' || substr("date", 1, 1) || '-0' || substr("date", 3, 1)
            )
        ELSE NULL
    END AS formatted_date
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` =
        CASE
        -- Case 1: mm/dd/yyyy (e.g., 12/16/2022)
        WHEN "date" LIKE '__/__/____' THEN
            strftime('%Y-%m-%d',
                substr("date", 7, 4) || '-' || substr("date", 1, 2) || '-' || substr("date", 4, 2)
            )
        -- Case 2: m/dd/yyyy (e.g., 7/25/2022)
        WHEN "date" LIKE '_/__/____' THEN
            strftime('%Y-%m-%d',
                substr("date", 6, 4) || '-0' || substr("date", 1, 1) || '-' || substr("date", 3, 2)
            )
        -- Case 3: mm/d/yyyy (e.g., 12/4/2022)
        WHEN "date" LIKE '__/_/____' THEN
            strftime('%Y-%m-%d',
                substr("date", 6, 4) || '-' || substr("date", 1, 2) || '-0' || substr("date", 4, 1)
            )
        -- Case 4: m/d/yyyy (e.g., 7/4/2022)
        WHEN "date" LIKE '_/_/____' THEN
            strftime('%Y-%m-%d',
                substr("date", 5, 4) || '-0' || substr("date", 1, 1) || '-0' || substr("date", 3, 1)
            )
        ELSE NULL
    END;

-- To DATE type
-- Create new staging table
 
CREATE TABLE layoffs_staging3 (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT DEFAULT NULL,
    percentage_laid_off TEXT,
    "date" DATE,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT DEFAULT NULL,
    row_num INT
);

INSERT INTO layoffs_staging3
SELECT * FROM layoffs_staging2;


-- 3. MISSING Values
-- Tota_laid_off
SELECT * FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- industry
SELECT * FROM layoffs_staging3
WHERE industry IS NULL OR industry = '';


-- company
SELECT * FROM layoffs_staging3
WHERE company = 'Airbnb';   -- we know that airbnb is travel industry

SELECT *
FROM layoffs_staging3 t1
JOIN layoffs_staging3 t2
    ON t1.company = t2.company
-- AND t1.location = t2.location
WHERE  (t1.industry IS NULL or t1.industry = '')
AND t2.industry IS NOT NULL;

-- updating the industry
UPDATE layoffs_staging3
SET industry = (
  SELECT industry
  FROM layoffs_staging3 AS subquery
  WHERE layoffs_staging3.company = subquery.company
  AND subquery.industry IS NOT NULL
)
WHERE industry IS NULL OR industry = '';

SELECT * FROM layoffs_staging3
WHERE company = "Bally's Interactive"; 


-- deleting rows where total and percentage laid off is both null since we cant trust that
DELETE FROM layoffs_staging3
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_staging3;

-- dropping the row_num now
ALTER TABLE layoffs_staging3
DROP COLUMN row_num;