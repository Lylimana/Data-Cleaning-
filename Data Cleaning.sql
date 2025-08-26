# Data Cleaning 
# Fixing data issues so that it is in a useable format ready for data visualisations etc.

SELECT * 
FROM layoffs;

# 1. Remove Duplicates 
# 2. Standardize Data 
# 3. Null/Blank Values
# 4. Remove Any Columns/rows

Create table layoffs_staging # Create new table to store data
like layoffs;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging # Insert data from layoffs to layoffs_staging to ensure data is backed up
SELECT *
FROM layoffs;

# 1. Remove Duplicates 
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY 
company, 
industry, 
total_laid_off, 
percentage_laid_off, 
'date' ) as row_num
FROM layoffs_staging;

WITH duplicate_cte as 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY 
company, 
location,
industry, 
total_laid_off, 
percentage_laid_off, 
date,
stage,
country,
funds_raised_millions
) as row_num
FROM layoffs_staging
) 
SELECT * 
FROM duplicate_cte 
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging
WHERE company = 'Cazoo';

WITH duplicate_cte as 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY 
company, 
location,
industry, 
total_laid_off, 
percentage_laid_off, 
date,
stage,
country,
funds_raised_millions
) as row_num
FROM layoffs_staging
) 
SELECT * 
FROM duplicate_cte 
WHERE row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY 
company, 
location,
industry, 
total_laid_off, 
percentage_laid_off, 
date,
stage,
country,
funds_raised_millions
) as row_num
FROM layoffs;

SELECT * 
FROM layoffs_staging2
WHERE row_num >1
;

DELETE 
FROM layoffs_staging2
WHERE row_num >1
;

# 2. Standardize Data 

SELECT company, TRIM(Company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT(company)
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT(industry)
FROM layoffs_staging2
WHERE industry like 'Crypto%'
;

SELECT * 
FROM layoffs_staging2
WHERE industry like 'Crypto%'
;

UPDATE layoffs_staging2
SET industry = 'Crypto' 
WHERE industry LIKE 'Crypto%'; # Updating columns with 'crypto' like values all to have just crypto 

SELECT DISTINCT(industry)
FROM layoffs_staging2
WHERE industry like 'Crypto%'
;

SELECT * 
FROM layoffs_staging2;

SELECT DISTINCT(location)
FROM layoffs_staging2
ORDER BY 1
;

UPDATE layoffs_staging2 
SET location = 'Malmo'
WHERE location like 'Mal%';

UPDATE layoffs_staging2 
SET location = 'Dusseldorf'
WHERE location like '%sseldorf';

SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1
;

UPDATE layoffs_staging2 
SET country = 'United States'
WHERE country like 'United States%';

SELECT DISTINCT country, TRIM(Trailing '.' FROM country) # Removes period rather than white space
FROM layoffs_staging2
ORDER BY 1; 

UPDATE layoffs_staging2 
SET country = TRIM(Trailing '.' FROM country)
WHERE country like 'United States%';

SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1
;

SELECT *
FROM layoffs_staging2
;

SELECT `date`,
STR_TO_DATE(`date`, '%d/%m/%Y') # (`Set column`, 'date format') Changing date from str to date format
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT *
FROM layoffs_staging2
;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE; # Changing column data type to date

SELECT *
FROM layoffs_staging2
;

# 3. Null/Blank Values
SELECT *
FROM layoffs_staging2
WHERE total_laid_off is Null
;

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off is Null
AND percentage_laid_off is Null
; # Both Null 

SELECT distinct(industry)
FROM layoffs_staging2
;

SELECT *
FROM layoffs_staging2
WHERE industry is NULL 
OR industry = ''; 
;

UPDATE layoffs_staging2
SET industry = 'Travel'
Where company = 'Airbnb';

SELECT *
FROM layoffs_staging2
WHERE company = 'Carvana'
;

UPDATE layoffs_staging2
SET industry = 'Transportation'
Where company = 'Carvana';

SELECT *
FROM layoffs_staging2
WHERE company = 'Juul'
;

UPDATE layoffs_staging2
SET industry = 'Consumer'
Where company = 'Juul';

SELECT *
FROM layoffs_staging2 t1 
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
WHERE (t1.industry IS NULL OR  t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 
SET industry = Null
Where industry = '';

UPDATE layoffs_staging2 t1 
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
SET t1.industry = t2.industry
-- WHERE (t1.industry IS NULL OR  t1.industry = '')
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off is Null
AND percentage_laid_off is Null
; # Both Null 

DELETE # Deleting Rows that have no usefull information
FROM layoffs_staging2
WHERE total_laid_off is Null
AND percentage_laid_off is Null
;

SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2 # Dropping Columns
DROP COLUMN row_num; 

# 1. Remove Duplicates 
# 2. Standardize Data 
# 3. Null/Blank Values
# 4. Remove Any Columns/rows
