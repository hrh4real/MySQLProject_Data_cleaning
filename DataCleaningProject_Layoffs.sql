select * from layoffs;


LOAD DATA INFILE 'C:/Users/himanshu/documents/python/sql/layoffs.csv'
INTO TABLE layoff_data
FIELDS TERMINATED BY ','  
OPTIONALLY ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'  
IGNORE 1 ROWS;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove any unnecessary column

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

-- checking for duplicates 
WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER()OVER(partition by company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage,country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Creating another staging table for deleting duplicates
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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;
 -- INSTERING DATA INTO layoffs_staging2
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER()OVER(partition by company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage,country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Removing duplicates
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Rechecking 
SELECT *
FROM layoffs_staging2;

-- Standardizing Data

-- Trimming Company Titles

select distinct(company) 
from layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);


-- Grouping Similar Industry Sections

select distinct industry
from layoffs_staging2
order by 1; -- alternativly you can write `order by industry`;


select *
from layoffs_staging2
where industry like 'crypto';

update layoffs_staging2
SET industry = 'Crypto'
Where industry like 'Crypto%';


-- Searching for another columns which needs alteration 
select *
from layoffs_staging2;

select distinct location
from layoffs_staging2
order by 1;

select distinct country
from layoffs_staging2
order by 1;

select distinct country, TRIM(TRAILING '.' FROM country)
from layoffs_staging2
order by 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
where country LIKE 'united states%';

select distinct country
from layoffs_staging2;

-- Changing `date` column from text to date

SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

/*
SELECT `date`
FROM layoffs_staging2;
*/

-- modifying column 
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- dealing with NULL and BLANK Values

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

select DISTINCT industry, company
FROM layoffs_staging2
WHERE 
industry IS NULL OR 
industry = ' ';

SELECT * 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = ' ')
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;


SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off = 1;

SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off IS NULL
AND total_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- final cleaned data -- 

SELECT *
FROM layoffs_staging2;





