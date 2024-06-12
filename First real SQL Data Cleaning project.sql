 -- DATA CLEANING 
 
 select *
 from layoffs;
 
 create table layoffs_staging
 like layoffs;
 
 select *
 from layoffs_staging;
 
 insert into layoffs_staging
 select *
 from layoffs;
 
 select *,
 row_number() over(partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
 from layoffs_staging;
 
 
 with duplicate_cte as
 (select *,
 row_number() over
 (partition by company, industry, total_laid_off, percentage_laid_off, `date`, location, stage, funds_raised_millions) as row_num
 from layoffs_staging)
 select *
 from duplicate_cte
 where row_num > 1;
 
 -- Always confirm your duplicate data to be sure that they are indeed duplicate and to do that,
 
 select *
 from layoffs_staging
 where company like 'Cazoo';
 
 -- To delete duplicates
 
 with duplicate_cte as
 (select *,
 row_number() over
 (partition by company, industry, total_laid_off, percentage_laid_off, `date`, location, stage, funds_raised_millions) as row_num
 from layoffs_staging)
 select *
 from duplicate_cte
 where row_num > 1;
 Delete
 from duplicate_cte
 Where row_num > 1;
 
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

select *
 from layoffs_staging2;
 
 
  INSERT INTO layoffs_staging2
  select *,
 row_number() over
 (partition by company, industry, total_laid_off, percentage_laid_off, `date`, location, stage, funds_raised_millions) as row_num
 from layoffs_staging;
   -- Then u select from that table the duplicates and then easily delete them 
 select *
 from layoffs_staging2
 WHERE row_num > 1;
 
 -- THIS DELETES THE DUPLICATE 
 DELETE
 from layoffs_staging2
 Where row_num > 1;
 
 -- AND THIS CONFIRMS THAT THE DUPLICATES ARE GONE
 select *
 from layoffs_staging2
 Where row_num > 1;
 
 
 -- STANDARDIZING DATA
 
 Select distinct(TRIM(company))
 from layoffs_staging2;
 
 Select Company, TRIM(company)
 from layoffs_staging2;
 
 -- NOW U NEED TO UPDATE YOUR TABLE WITH THE CORRECTED VALUES
 
 UPDATE layoffs_staging2
 SET Company = TRIM(company);
 
 -- NOW U RE-CONFIRM IT WAS PROPERLY UPDATED
 
 Select Company, TRIM(company)
 from layoffs_staging2;
 
 SELECT DISTINCT(industry)
 from layoffs_staging2
 ORDER BY 1;
 
 SELECT *
 from layoffs_staging2
 WHERE industry LIKE 'crypto%';
 
 UPDATE layoffs_staging2
 SET Industry = 'Crypto'
 where industry like 'crypto%';
 
 select distinct(location)
 from layoffs_staging2
 ORDER BY 1;
 -- NOTHING WAS DONE TO THIS COLUMN BECUS ITS DATA WERE ALL CORRECT AND DIDNT NEED AND CORRECTION SO WE MOVED TO THE NEXT COLUMN
 -- NOTE WHEN EVER U SELECT A COLUMN, MAKE SURE TO SCAN THRU THE WHOLE DATA TO BE SURE IT HAS NO ERROR OR ISSUE AND ALSO EXPAND THE COLUMN FOR BETTER VIEW
 
 select distinct(Country)
 from layoffs_staging2
 ORDER BY 1;
 
 select *
 from layoffs_staging2
 where country like 'United States%';

select distinct Country, TRIM(TRAILING '.' FROM country)
from layoffs_staging2
ORDER BY 1;
 
UPDATE layoffs_staging2
SET Country = TRIM(TRAILING '.' FROM country)
where Country like 'United states%';

select distinct company
 from layoffs_staging2;
 
 -- NOTE THIS IS TO FORMAT THE DATE FROM TEXT INTO TIME SERIES WHICH IS YEAR, MONTH AND DAY RESPECTIVELY WHICH IS MYSQL STANDARD DATE FORMAT.
 
select `date`,
STR_TO_DATE(`Date`,'%m/%d/%Y')
from layoffs_staging2;
 
 Update layoffs_staging2
 SET `Date` = STR_TO_DATE(`Date`,'%m/%d/%Y');
 
 SELECT `Date`
 from layoffs_staging2;
 
 ALTER TABLE layoffs_staging2
 MODIFY column `Date` DATE;
 
 -- STEP 3 : REMOVING NULLS FROM A COLUMN TABLE AND U NEED TO CHECK INDIVIDUAL COLUMNS FIRST, IF NO DATA IS NULL,THEN MOVE TO THE NEXT COLUMN 
 
 Select *
 from layoffs_staging2
 where industry = '';
 
  -- THIS UPDATES ALL BLANKS TO NULLS IN THE COLUMN 
 update layoffs_staging2
 set industry = null
 where industry = '';
 
 -- HERE WE MAKE A SELF JOIN TO REMOVE THE NULLS
 SELECT *
 FROM layoffs_staging2 t1
 join layoffs_staging2 t2
 on t1.company = t2.company
 where t1.industry is null
 and t2.industry is not null;
 
 -- AND THEN U UPDATE THE NEW DATA TO THE TABLE
 
UPDATE layoffs_staging2 t1
join layoffs_staging2 t2
SET t1.company= t2.company
Where t1.industry is null
and t2.industry is not null;

select company, industry 
from layoffs_staging2
where company like 'E%';

UPDATE layoffs_staging2
SET industry = 'Transportation'
where industry is null ;
 
 select company, industry 
from layoffs_staging2
WHERE industry is null;

select *
from layoffs_staging2;

SELECT total_laid_off, percentage_laid_off
from layoffs_staging2
Where total_laid_off IS NULL
AND  percentage_laid_off IS NULL;

DELETE 
from layoffs_staging2
Where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

 -- STEP 4 ; TO REMOVE ANY UNNECESSARY ROW OR COLUMN IN OUR TABLE
 
 alter table layoffs_staging2
 DROP COLUMN row_num;
 
 select *
 from layoffs_staging2;
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 