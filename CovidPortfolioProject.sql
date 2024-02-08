-- Create Covid Deaths Table
CREATE TABLE covid_deaths (
	iso_code TEXT,
	continent TEXT,
	"location" TEXT,
	"date" DATE,
	population FLOAT,
	total_cases FLOAT,
	new_cases FLOAT,
	new_cases_smoothed FLOAT,
	total_deaths FLOAT,
	new_deaths FLOAT,
	new_deaths_smoothed FLOAT,
	total_cases_per_million FLOAT,
	new_cases_per_million FLOAT,
	new_cases_smoothed_per_million FLOAT,
	total_deaths_per_million FLOAT,
	new_deaths_per_million FLOAT,
	new_deaths_smoothed_per_million FLOAT,
	reproduction_rate FLOAT,
	icu_patients FLOAT,
	icu_patients_per_million FLOAT,
	hosp_patients FLOAT,
	hosp_patients_per_million FLOAT,
	weekly_icu_admissions FLOAT,
	weekly_icu_admissions_per_million FLOAT,
	weekly_hosp_admissions FLOAT,
	weekly_hosp_admissions_per_million FLOAT
);

-- Create Covid Vaccinations Table
CREATE TABLE covid_vaccinations (
	iso_code TEXT,
	continent TEXT,
	"location" TEXT,
	"date" DATE,
	total_test FLOAT,
	new_tests FLOAT,
	total_tests_per_thousand FLOAT,
	new_tests_per_thousand FLOAT,
	new_tests_smoothed FLOAT,
	new_tests_smoothed_per_thousand FLOAT,
	positive_rate FLOAT,
	tests_per_case FLOAT,
	tests_units TEXT,
	total_vaccinations FLOAT,
	people_vaccinated FLOAT,
	people_fully_vaccinated FLOAT,
	total_boosters FLOAT,
	new_vaccinations FLOAT,
	new_vaccinations_smoothed FLOAT,
	total_vaccinations_per_hundred FLOAT,
	people_vaccinated_per_hundred FLOAT,
	people_fully_vaccinated_per_hundred FLOAT,
	total_boosters_per_hundred FLOAT,
	new_vaccinations_smoothed_per_million FLOAT,
	new_people_vaccinated_smoothed FLOAT,
	new_people_vaccinated_smoothed_per_hundred FLOAT,
	stringency_index FLOAT,
	population_density FLOAT,
	median_age FLOAT,
	aged_65_older FLOAT,
	aged_70_older FLOAT,
	gdp_per_capita FLOAT,
	extreme_poverty FLOAT,
	cardiovasc_death_rate FLOAT,
	diabetes_prevalence FLOAT,
	female_smokers FLOAT,
	male_smokers FLOAT,
	handwashing_facilities FLOAT,
	hospital_beds_per_thousand FLOAT,
	life_expectancy FLOAT,
	human_development_index FLOAT,
	excess_mortality_cumulative_absolute FLOAT,
	excess_mortality_cumulative FLOAT,
	excess_mortality FLOAT,
	excess_mortality_cumulative_per_million FLOAT
);

-- Select tables info
SELECT * FROM covid_deaths;

-- Select data that we are going to use
SELECT 
	"location", 
	"date", 
	total_cases, 
	new_cases, 
	total_deaths, 
	population
FROM
	covid_deaths
ORDER BY
	1,2
	
-- Looking at the Total cases vs Total deaths in Colombia
-- This shows the likelihood of dying if you contract covid in Colombia
SELECT 
	"location", 
	"date", 
	total_cases, 
	total_deaths, 
	(total_deaths/total_cases)*100 as death_percentage
FROM
	covid_deaths
WHERE
	"location" like '%Colombia%'
ORDER BY
	1,2
	
-- Looking at the Total cases vs Population in Colombia
-- Shows what percentage of population got Covid
SELECT 
	"location", 
	"date", 
	total_cases, 
	population, 
	(total_cases/population)*100 as cases_percentage
FROM
	covid_deaths
WHERE
	"location" like '%Colombia%'
ORDER BY
	1,2
	
-- Looking at countries with highest infection rate compared to population
SELECT 
	"location", 
	MAX(total_cases) as highest_infection_count, 
	population, 
	MAX((total_cases/population))*100 as cases_percentage
FROM
	covid_deaths
GROUP BY
	"location", population
ORDER BY
	cases_percentage desc
	
-- Showing the countries with highest death count per population
SELECT 
	"location", 
	MAX(total_deaths) as highest_death_count
FROM
	covid_deaths
WHERE
	continent is not null
GROUP BY
	"location"
ORDER BY
	highest_death_count desc
	
-- Showing the highest death count per population in eah continent
SELECT 
	continent, 
	MAX(total_deaths) as highest_death_count
FROM
	covid_deaths
WHERE
	continent is not null
GROUP BY
	continent
ORDER BY
	highest_death_count desc
	
-- Looking at the percentage of death globally
SELECT
	SUM(new_cases) as total_cases,
	SUM(new_deaths) as total_deaths,
	(SUM(new_deaths)/SUM(new_cases))*100 as death_percentage
FROM
	covid_deaths
WHERE
	continent is not null
ORDER BY
	1,2
	
-- Join deaths and vaccinations tables
SELECT
	* 
FROM
	covid_deaths dea
JOIN
	covid_vaccinations vac
ON
	dea.location = vac.location
AND
	dea.date = vac.date
	
-- Looking at total population vs vaccinations
SELECT
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations,
	SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) as rolling_people_vaccinated
FROM
	covid_deaths dea
JOIN
	covid_vaccinations vac
ON
	dea.location = vac.location
AND
	dea.date = vac.date
WHERE
	dea.continent is not null
ORDER BY
	2,3
	
-- Use CTE
WITH
	PopvsVac (continent, "location", "date", population, new_vaccinations, rolling_people_vaccinated)
AS
(
SELECT
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations,
	SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) as rolling_people_vaccinated
FROM
	covid_deaths dea
JOIN
	covid_vaccinations vac
ON
	dea.location = vac.location
AND
	dea.date = vac.date
WHERE
	dea.continent is not null
)
SELECT *, (rolling_people_vaccinated/population)*100
FROM PopvsVac

-- Create new table with percent people vaccinated
DROP TABLE if exists PercentPopulationVaccinated;
CREATE TABLE PercentPopulationVaccinated (
	continent TEXT,
	"location" TEXT,
	"date" DATE,
	population FLOAT,
	new_vaccinations FLOAT,
	rolling_people_vaccinated FLOAT
);

INSERT INTO PercentPopulationVaccinated
SELECT
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations,
	SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) as rolling_people_vaccinated
FROM
	covid_deaths dea
JOIN
	covid_vaccinations vac
ON
	dea.location = vac.location
AND
	dea.date = vac.date
WHERE
	dea.continent is not null;

SELECT *, (rolling_people_vaccinated/population)*100
FROM PercentPopulationVaccinated

-- Creating view to store data for later visualizations
CREATE VIEW PercentPopulationVaccinated AS
SELECT
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations,
	SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) as rolling_people_vaccinated
FROM
	covid_deaths dea
JOIN
	covid_vaccinations vac
ON
	dea.location = vac.location
AND
	dea.date = vac.date
WHERE
	dea.continent is not null