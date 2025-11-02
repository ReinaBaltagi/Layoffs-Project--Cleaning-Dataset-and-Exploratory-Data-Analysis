#data cleaning - Project 
#Fixing raw issues with the data 

#We'll start by importing a dataset, then cleaning it

#We start by discovering the data itself 
SELECT*
FROM layoffs;

#What we'll be doing in order:
# 1 - Remove duplicates 
# 2 - standardize the data 
# 3 - Null values or blank values
# 4 - Remove columns and rows that aren't necessary 

CREATE TABLE layoff_staging
LIKE layoffs;

SELECT *
FROM layoff_staging;

INSERT layoff_staging
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
FROM layoff_staging;

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,funds_raised_millions
) as row_num
FROM layoff_staging
)
SELECT*
FROM duplicate_cte
WHERE row_num> 1; #they are duplicate

SELECT *
FROM layoff_staging
WHERE company = 'Casper';


WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,funds_raised_millions
) as row_num
FROM layoff_staging
)
DELETE
FROM duplicate_cte
WHERE row_num> 1;

CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL, 
  `row_num`INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoff_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,funds_raised_millions
) as row_num
FROM layoff_staging;

SELECT *
FROM layoff_staging2
WHERE row_num > 1;
#Now that we found our duplicates, we have to delete them 

DELETE 
FROM layoff_staging2
WHERE row_num > 1;

SELECT * FROM layoff_staging2;

#standirdizing data --> removing details that would make things complicated 

SELECT company, Trim(company)
FROM layoff_staging2 ;

UPDATE layoff_staging2
SET company = TRIM(company);

SELECT company 
from layoff_staging2;

SELECT DISTINCT industry 
from layoff_staging2
ORDER BY 1; # we see that crypto and then crypto currency, cryptocurrency, so we're saying same industries under several names, we checked everythign else
#we need to fix this issue 

SELECT *
FROM layoff_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoff_staging2 
SET industry ='Crypto'
WHERE industry = 'Crypto%';

SELECT industry
FROM layoff_staging2
ORDER BY 1;

SELECT DISTINCT country 
from layoff_staging2
ORDER BY 1;

SELECT country
FROM layoff_staging2
WHERE country LIKE 'United States%';

UPDATE layoff_staging2
SET country = 'United States'
WHERE country = 'United States%';

#OR 
SELECT DISTINCT country, TRIM(Trailing '.' FROM country)
FROM layoff_staging2
ORDER BY 1;

UPDATE layoff_staging2
SET country = TRIM(Trailing '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`,
str_to_date(`date`, '%m/%d/%Y') #go from a string to a date according to the standard date format in SQL
FROM layoff_staging2;

UPDATE layoff_staging2
SET `date`= str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoff_staging2;

ALTER TABLE layoff_staging2 #never do this on the actual table only on a copied one 
MODIFY COLUMN `date`DATE;

SELECT *
FROM layoff_staging2;

#working with NULL and blank Values

SELECT *
FROM layoff_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

UPDATE layoff_staging2
SET industry = NULL 
WHERE industry = '';

SELECT *
FROM layoff_staging2
WHERE industry IS NULL 
OR industry = '' ;

SELECT *
FROM layoff_staging2
WHERE company = 'Airbnb'; #we new that the industry is travel


SELECT * 
FROM layoff_staging2 as t1
JOIN layoff_staging2 as t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry ='')
AND t2.industry IS NOT NULL;

UPDATE layoff_staging2 as t1
JOIN layoff_staging2 as t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

#Bally's interactive didn't work because it's not doubled, so it didn't exist before 

#If the original total before laid_off existed it would have worked to calculate the total laid off, having the percentage laid off through doing a calculation 

SELECT *
FROM layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; # we propably don't have these elements 

#We can delete them, we need to make sure that it isn't doable before though (to find them) 

SELECT *
FROM layoffs_staging2; 


DELETE
FROM layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; 

ALTER TABLE layoff_staging2
DROP COLUMN row_num;

SELECT *
FROM layoff_staging2; 
#Finalized cleaned data 










