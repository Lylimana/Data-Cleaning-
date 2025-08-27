# Exploring Data 

SELECT * 
FROM layoffs_staging2; 

SELECT MAX(total_laid_off), AVG(total_laid_off), AVG(percentage_laid_off) 
FROM layoffs_staging2; # Upon initial data exploration we know the max percentage laid off is 1 which means 100% of the company 

# Finding all the companies that laid off all its employees
SELECT company, `date`, stage, funds_raised_millions
FROM layoffs_staging2
WHERE percentage_laid_off = 1; 

# Sorting companies that went under by the number of employees laid off
SELECT company, total_laid_off, `date`, stage, funds_raised_millions
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC; 

# Sorting companies that went under by money raised
SELECT company, total_laid_off, `date`, stage, funds_raised_millions
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC; 

# Sorting companies based off the number of employees laid off
SELECT company, SUM(total_laid_off) sum_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY sum_laid_off DESC; 

# Taking first and last date of rows within the dataset 
SELECT MIN(`date`) , MAX(`date`)
FROM layoffs_staging2;

# Sorting industries based off the number of employees laid off
SELECT industry, SUM(total_laid_off) sum_laid_off
FROM layoffs_staging2
GROUP BY industry
ORDER BY sum_laid_off DESC; 

# Sorting countries based off the number of employees laid off
SELECT country, SUM(total_laid_off) sum_laid_off
FROM layoffs_staging2
GROUP BY country
ORDER BY sum_laid_off DESC; 

# Number of employees laid off based on year
SELECT YEAR(`date`), SUM(total_laid_off) sum_laid_off
FROM layoffs_staging2
GROUP BY YEAR(`date`) 
ORDER BY 1 DESC; # Order by first column in the table

# Number of employees laid off based on company stage 
SELECT stage, SUM(total_laid_off) sum_laid_off
FROM layoffs_staging2
GROUP BY stage 
ORDER BY 1 DESC; 

# Number of employees laid off every month
SELECT SUBSTRING(`date`, 1,7) AS `month`,
SUM(total_laid_off)
FROM layoffs_staging2 
WHERE `date` is not null
GROUP BY `month`
ORDER BY 1 ASC; 

# Rolling total of number of employees laid off every month
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1,7) AS `month`, sum(total_laid_off) AS sum_laid_off
FROM layoffs_staging2 
WHERE `date` is not null
GROUP BY `month`
ORDER BY 1 ASC # Partitioned already, does not need to be added to OVER()
) 
SELECT `month`, sum_laid_off, SUM(sum_laid_off) OVER(ORDER BY `month`) as rolling_total
FROM Rolling_Total;

# Rolling total of number of employees laid off every year by companies
SELECT company, YEAR(`date`) as `year`, SUM(total_laid_off) sum_laid_off
FROM layoffs_staging2
WHERE `date` is not null 
GROUP BY company ,`year`
ORDER BY company ASC;

# Ranking Companies by most laid off each year
WITH company_year (company, years, total_laid_off) AS 
(
	SELECT company, YEAR(`date`) as `year`, SUM(total_laid_off) sum_laid_off
	FROM layoffs_staging2
	WHERE `date` is not null 
	GROUP BY company ,`year`
)
SELECT *, DENSE_RANK() OVER(partition by years ORDER BY total_laid_off DESC) AS most_laid_off 
FROM company_year
WHERE total_laid_off IS NOT null
ORDER BY most_laid_off ASC;

WITH company_year (company, years, total_laid_off) AS 
(
	SELECT company, YEAR(`date`) as `year`, SUM(total_laid_off) sum_laid_off
	FROM layoffs_staging2
	WHERE `date` is not null 
	GROUP BY company ,`year`
), company_year_rank AS
(
SELECT *, DENSE_RANK() OVER(partition by years ORDER BY total_laid_off DESC) AS most_laid_off 
FROM company_year
WHERE total_laid_off IS NOT null
)
SELECT *
FROM company_year_rank
WHERE most_laid_off <= 5
ORDER BY most_laid_off
;