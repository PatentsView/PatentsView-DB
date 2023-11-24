{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}


create table `{{reporting_db}}`.webtool_comparison_countryI
SELECT
    l.country
  , p.year
  , COUNT(DISTINCT inventor_id) AS invCount
FROM
    (SELECT location_id, IF(country = 'AN', 'CW', country) AS country FROM `{{reporting_db}}`.location) l
        LEFT JOIN `{{reporting_db}}`.patent_inventor pi ON l.location_id = pi.location_id
        LEFT JOIN `{{reporting_db}}`.patent p ON pi.patent_id = p.patent_id
WHERE
      p.year IS NOT NULL
  AND l.country IS NOT NULL
  AND l.country REGEXP '^[A-Z]{2}$'
  AND l.country NOT IN ('US', 'YU', 'SU')
GROUP BY
    l.country
  , p.year;

create table `{{reporting_db}}`.webtool_comparison_countryIsector
SELECT
    l.country
  , p.year
  , sector_title
  , count(DISTINCT (inventor_id)) AS invSubCount
FROM
    (SELECT location_id, IF(country = 'AN', 'CW', country) AS country FROM `{{reporting_db}}`.location) l
        LEFT JOIN `{{reporting_db}}`.patent_inventor pi ON l.location_id = pi.location_id
        LEFT JOIN `{{reporting_db}}`.wipo w ON w.patent_id = pi.patent_id
        LEFT JOIN `{{reporting_db}}`.wipo_field wf ON w.field_id = wf.id
        LEFT JOIN `{{reporting_db}}`.patent p ON pi.patent_id = p.patent_id
WHERE
      p.year IS NOT NULL
  AND sector_title IS NOT NULL
  AND l.country IS NOT NULL
  AND l.country REGEXP '^[A-Z]{2}$'
  AND l.country NOT IN ('US', 'YU', 'SU')
  AND w.sequence = 0
GROUP BY
    l.country
  , p.year
  , sector_title;

create table `{{reporting_db}}`.webtool_comparison_countryIA
SELECT
    l.country
  , p.year
  , COUNT(DISTINCT assignee_id) AS assiCount
FROM
    (SELECT location_id, IF(country = 'AN', 'CW', country) AS country FROM `{{reporting_db}}`.location) l
        LEFT JOIN `{{reporting_db}}`.patent_assignee pa ON l.location_id = pa.location_id
        LEFT JOIN `{{reporting_db}}`.patent p ON pa.patent_id = p.patent_id
WHERE
      p.year IS NOT NULL
  AND l.country IS NOT NULL
  AND l.country REGEXP '^[A-Z]{2}$'
  AND l.country NOT IN ('US', 'YU', 'SU')
GROUP BY
    l.country
  , p.year;

create table `{{reporting_db}}`.webtool_comparison_countryIAsector
SELECT
    l.country
  , p.year
  , sector_title
  , count(DISTINCT (assignee_id)) AS assiSubCount
FROM
    (SELECT location_id, IF(country = 'AN', 'CW', country) AS country FROM `{{reporting_db}}`.location) l
        LEFT JOIN `{{reporting_db}}`.patent_assignee pa ON l.location_id = pa.location_id
        LEFT JOIN `{{reporting_db}}`.wipo w ON w.patent_id = pa.patent_id
        LEFT JOIN `{{reporting_db}}`.wipo_field wf ON w.field_id = wf.id
        LEFT JOIN `{{reporting_db}}`.patent p ON pa.patent_id = p.patent_id
WHERE
      p.year IS NOT NULL
  AND sector_title IS NOT NULL
  AND l.country IS NOT NULL
  AND l.country REGEXP '^[A-Z]{2}$'
  AND l.country NOT IN ('US', 'YU', 'SU')
  AND w.sequence = 0
GROUP BY
    l.country
  , p.year
  , sector_title;

create table `{{reporting_db}}`.webtool_comparison_stateI
SELECT
    l.state
  , p.year
  , count(DISTINCT (inventor_id)) AS invCount
FROM
    `{{reporting_db}}`.location l
        LEFT JOIN `{{reporting_db}}`.patent_inventor pi ON l.location_id = pi.location_id
        LEFT JOIN `{{reporting_db}}`.patent p ON pi.patent_id = p.patent_id
WHERE
      l.country = 'US'
  AND p.year IS NOT NULL
  AND l.state IS NOT NULL
  AND l.state REGEXP '^[A-Z]{2}$'
  AND l.state NOT IN ('PR', 'VI', 'GU')
GROUP BY
    state
  , p.year;

create table `{{reporting_db}}`.webtool_comparison_stateIsector
SELECT
    l.state
  , p.year
  , sector_title
  , count(DISTINCT (inventor_id)) AS invSubCount
FROM
    `{{reporting_db}}`.location l
        LEFT JOIN `{{reporting_db}}`.patent_inventor pi ON l.location_id = pi.location_id
        LEFT JOIN `{{reporting_db}}`.wipo w ON pi.patent_id = w.patent_id
        LEFT JOIN `{{reporting_db}}`.wipo_field wf ON w.field_id = wf.id
        LEFT JOIN `{{reporting_db}}`.patent p ON pi.patent_id = p.patent_id
WHERE
      w.sequence = 0
  AND sector_title IS NOT NULL
  AND l.country = 'US'
  AND p.year IS NOT NULL
  AND l.state IS NOT NULL
  AND l.state REGEXP '^[A-Z]{2}$'
  AND l.state NOT IN ('PR', 'VI', 'GU')
GROUP BY
    state
  , p.year
  , sector_title;

create table `{{reporting_db}}`.webtool_comparison_stateA
SELECT
    l.state
  , p.year
  , count(DISTINCT (assignee_id)) AS assiCount
FROM
    `{{reporting_db}}`.location l
        LEFT JOIN `{{reporting_db}}`.patent_assignee pa ON l.location_id = pa.location_id
        LEFT JOIN `{{reporting_db}}`.patent p ON pa.patent_id = p.patent_id
WHERE
      l.country = 'US'
  AND p.year IS NOT NULL
  AND l.state IS NOT NULL
  AND l.state REGEXP '^[A-Z]{2}$'
  AND l.state NOT REGEXP 'PR|VI|GU'
GROUP BY
    state
  , p.year;

create table `{{reporting_db}}`.webtool_comparison_stateAsector
SELECT l.state, p.year, sector_title, count(DISTINCT(assignee_id)) AS assiSubCount
    FROM `{{reporting_db}}`.location l
    LEFT JOIN `{{reporting_db}}`.patent_assignee pa ON l.location_id = pa.location_id
    LEFT JOIN `{{reporting_db}}`.wipo w ON pa.patent_id = w.patent_id
    LEFT JOIN `{{reporting_db}}`.wipo_field wf ON w.field_id = wf.id
    LEFT JOIN `{{reporting_db}}`.patent p ON pa.patent_id = p.patent_id
WHERE w.sequence = 0
AND sector_title IS NOT NULL
AND p.year IS NOT NULL
AND l.country = 'US'
AND l.state IS NOT NULL
AND l.state REGEXP '^[A-Z]{2}$'
AND l.state NOT REGEXP 'PR|VI|GU'
GROUP BY `state`, p.year, sector_title;

create table `{{reporting_db}}`.webtool_comparison_stateIA
SELECT
    l.state
  , p.year
  , sector_title
  , count(DISTINCT (assignee_id)) AS assiSubCount
FROM
    `{{reporting_db}}`.location l
        LEFT JOIN `{{reporting_db}}`.patent_assignee pa ON l.location_id = pa.location_id
        LEFT JOIN `{{reporting_db}}`.wipo w ON pa.patent_id = w.patent_id
        LEFT JOIN `{{reporting_db}}`.wipo_field wf ON w.field_id = wf.id
        LEFT JOIN `{{reporting_db}}`.patent p ON pa.patent_id = p.patent_id
WHERE
      w.sequence = 0
  AND sector_title IS NOT NULL
  AND p.year IS NOT NULL
  AND l.country = 'US'
  AND l.state IS NOT NULL
  AND l.state REGEXP '^[A-Z]{2}$'
  AND l.state NOT REGEXP 'PR|VI|GU'
GROUP BY
    state
  , p.year
  , sector_title;


create table `{{reporting_db}}`.webtool_comparison_wipoI
SELECT
    sector_title
  , p.year
  , field_title
  , COUNT(DISTINCT inventor_id) AS invSubCount
FROM
    `{{reporting_db}}`.patent_inventor pi
        LEFT JOIN `{{reporting_db}}`.wipo w ON pi.patent_id = w.patent_id
        LEFT JOIN `{{reporting_db}}`.wipo_field wf ON w.field_id = wf.id
        LEFT JOIN `{{reporting_db}}`.patent p ON pi.patent_id = p.patent_id
WHERE
      w.sequence = 0
  AND p.year IS NOT NULL
  AND sector_title IS NOT NULL
  AND field_title IS NOT NULL
GROUP BY
    sector_title
  , p.year
  , field_title;

create table `{{reporting_db}}`.webtool_comparison_wipoA
SELECT
    sector_title
  , p.year
  , field_title
  , COUNT(DISTINCT assignee_id) AS assiSubCount
FROM
    `{{reporting_db}}`.patent_assignee pa
        LEFT JOIN `{{reporting_db}}`.wipo w ON pa.patent_id = w.patent_id
        LEFT JOIN `{{reporting_db}}`.wipo_field wf ON w.field_id = wf.id
        LEFT JOIN `{{reporting_db}}`.patent p ON pa.patent_id = p.patent_id
WHERE
      w.sequence = 0
  AND p.year IS NOT NULL
  AND sector_title IS NOT NULL
  AND field_title IS NOT NULL
GROUP BY
    sector_title
  , p.year
  , field_title;

