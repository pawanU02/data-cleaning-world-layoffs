# Data Cleaning Process

This document outlines the steps taken to clean the data in the `world_layoffs.db` database. The process involves creating staging tables, removing duplicates, standardizing data, handling NULL values, and reformatting dates.

## Steps

1. **Create Initial Staging Table**
    - Create a staging table `layoffs_staging` from the raw data.
    - This table will be used for initial data manipulation and cleaning.

    ```sql
    CREATE TABLE layoffs_staging AS SELECT * FROM layoffs WHERE 1=0;
    INSERT INTO layoffs_staging SELECT * FROM layoffs;
    ```

2. **Check for Duplicates and Remove Them**
    - Assign a unique row number to each row to identify duplicates.
    - Remove duplicate rows based on the combination of columns: `company`, `location`, `industry`, `total_laid_off`, `percentage_laid_off`, `date`, `stage`, `country`, `funds_raised_millions`.

    ```sql
    SELECT *, ROW_NUMBER() OVER(
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_num FROM layoffs_staging;

    DELETE FROM layoffs_staging2 WHERE row_num > 1;
    ```

3. **Standardize the Data**
    - Trim whitespace from the `company` column.
    - Standardize the `industry` column values (e.g., update variations of "Crypto" (e.g. CryptoCurrency, Crypto Currency, etc) to "Crypto").
    - Correct country names (e.g., remove trailing periods from "United States.").

    ```sql
    UPDATE layoffs_staging2 SET company = TRIM(company);
    UPDATE layoffs_staging2 SET industry = 'Crypto' WHERE industry LIKE 'Crypto%';
    UPDATE layoffs_staging2 SET country = TRIM(country, '.') WHERE country LIKE 'United States%';
    ```

4. **Handle NULL or Blank Values**
    - Delete rows where both `total_laid_off` and `percentage_laid_off` are NULL.
    - Update the `industry` column based on known values for the same company.

    ```sql
    DELETE FROM layoffs_staging3 WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;
    UPDATE layoffs_staging3 SET industry = (
      SELECT industry FROM layoffs_staging3 AS subquery WHERE layoffs_staging3.company = subquery.company AND subquery.industry IS NOT NULL
    ) WHERE industry IS NULL OR industry = '';
    ```

5. **Reformat Dates**
    - Reformat the `date` column to `YYYY-MM-DD` format.
    - Handle different date formats (e.g., `mm/dd/yyyy`, `m/dd/yyyy`, `mm/d/yyyy`, `m/d/yyyy`).

    ```sql
    UPDATE layoffs_staging2 SET `date` = CASE
        WHEN "date" LIKE '__/__/____' THEN strftime('%Y-%m-%d', substr("date", 7, 4) || '-' || substr("date", 1, 2) || '-' || substr("date", 4, 2))
        WHEN "date" LIKE '_/__/____' THEN strftime('%Y-%m-%d', substr("date", 6, 4) || '-0' || substr("date", 1, 1) || '-' || substr("date", 3, 2))
        WHEN "date" LIKE '__/_/____' THEN strftime('%Y-%m-%d', substr("date", 6, 4) || '-' || substr("date", 1, 2) || '-0' || substr("date", 4, 1))
        WHEN "date" LIKE '_/_/____' THEN strftime('%Y-%m-%d', substr("date", 5, 4) || '-0' || substr("date", 1, 1) || '-0' || substr("date", 3, 1))
        ELSE NULL
    END;
    ```

6. **Create Final Staging Table**
    - Create a new staging table `layoffs_staging3` with the cleaned data.
    - Insert cleaned data into `layoffs_staging3`.

    ```sql
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

    INSERT INTO layoffs_staging3 SELECT * FROM layoffs_staging2;
    ```

7. **Select Cleaned Data**
    - Retrieve the cleaned data from the final staging table.

    ```sql
    SELECT * FROM layoffs_staging3;
    ```

## Running the SQL Script

To run the SQL script and clean the data, use the following commands in the SQLite terminal:

```sh
sqlite3 world_layoffs.db
.read cleaning.sql
```