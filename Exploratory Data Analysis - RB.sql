#Exploratpry Data Analysis 

SELECT *
FROM layoff_staging2;

#What are we looking for? what's the agenda? 

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoff_staging2; #we will be working on the total laid_off adn percentage laid off first

SELECT *
FROM layoff_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions;

SELECT company, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT  MIN(`date`), MAX(`date`)
FROM layoff_staging2;

SELECT industry, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR (`date`), SUM(total_laid_off)
FROM layoff_staging2
GROUP BY YEAR (`date`)
ORDER BY 2 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT company, SUM(percentage_laid_off)
FROM layoff_staging2
GROUP BY company
ORDER BY 2 DESC; #percent of the company so it's not relevant because we don't know how big each company is

SELECT company, AVG(percentage_laid_off)
FROM layoff_staging2
GROUP BY company
ORDER BY 2 DESC;

#progression of layoffs --> rolling sum 

SELECT SUBSTRING(`date`,1,7) AS `MONTH`,SUM(total_laid_off)
FROM layoff_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
;

WITH rolling_total AS (
SELECT SUBSTRING(`date`,1,7) AS `MONTH`,SUM(total_laid_off) AS total_off
FROM layoff_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off, SUM(total_off) OVER (ORDER BY `MONTH`) as rolling_total
FROM rolling_total;



SELECT company,YEAR (`date`),SUM(total_laid_off)
FROM layoff_staging2
GROUP BY company,YEAR(`date`)
ORDER BY 3 DESC;

WITH company_year (company, years, total_laid_off) as
(
SELECT company,YEAR (`date`),SUM(total_laid_off)
FROM layoff_staging2
GROUP BY company,YEAR(`date`)
)
SELECT *, DENSE_RANK () OVER  (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM company_Year
WHERE years IS NOT NULL
ORDER BY ranking ASC;


WITH company_year (company, years, total_laid_off) as
(
SELECT company,YEAR (`date`),SUM(total_laid_off)
FROM layoff_staging2
GROUP BY company,YEAR(`date`)
), company_year_rank as #we gave our first CTE a rank so we would filter on that rank --> hence the second CTE
(SELECT *, DENSE_RANK () OVER  (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM company_Year
WHERE years IS NOT NULL)
SELECT*
FROM company_year_rank
WHERE ranking<= 5 #queeried off the final CTE
;


