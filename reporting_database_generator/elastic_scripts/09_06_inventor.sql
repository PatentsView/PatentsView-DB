{% set elastic_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set version_indicator = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`inventor_years`
(
    `inventor_id` varchar(256) COLLATE utf8mb4_unicode_ci NOT NULL,
    `patent_year` smallint(6)      NOT NULL,
    `num_patents` int(10) unsigned NOT NULL,
    KEY `ix_inventor_year_inventor_id` (`inventor_id`),
    KEY `ix_inventor_year_year` (`patent_year`),
    KEY `ix_inventor_year_num_patents` (`num_patents`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`inventors`
(
    `inventor_id`                      varchar(256) COLLATE utf8mb4_unicode_ci NOT NULL,
    `name_first`                       varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `name_last`                        varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `num_patents`                      int(10) unsigned                        NOT NULL,
    `num_assignees`                    int(10) unsigned                        NOT NULL,
    `lastknown_location_id`            varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `lastknown_persistent_location_id` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `lastknown_city`                   varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `lastknown_state`                  varchar(20) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `lastknown_country`                varchar(10) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `lastknown_latitude`               float                                   DEFAULT NULL,
    `lastknown_longitude`              float                                   DEFAULT NULL,
    `first_seen_date`                  date                                    DEFAULT NULL,
    `last_seen_date`                   date                                    DEFAULT NULL,
    `years_active`                     smallint(5) unsigned                    NOT NULL,
    `persistent_inventor_id`           varchar(256) COLLATE utf8mb4_unicode_ci NOT NULL,
    `gender_code`                      varchar(5) COLLATE utf8mb4_unicode_ci,
    PRIMARY KEY (`inventor_id`),
    KEY `ix_inventor_lastknown_location_id` (`lastknown_location_id`),
    KEY `ix_inventor_first_seen_date` (`first_seen_date`),
    KEY `ix_inventor_last_seen_date` (`last_seen_date`),
    KEY `ix_inventor_lastknown_persistent_location_id` (`lastknown_persistent_location_id`),
    KEY `ix_inventor_num_assignees` (`num_assignees`),
    KEY `ix_inventor_num_patents` (`num_patents`),
    KEY `ix_inventor_name_first` (`name_first`),
    KEY `ix_inventor_name_last` (`name_last`),
    KEY `ix_inventor_persistent_inventor_id` (`persistent_inventor_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.inventors;
INSERT INTO `{{elastic_db}}`.inventors( inventor_id, name_first, name_last, num_patents, num_assignees
                                        , lastknown_location_id, lastknown_persistent_location_id, lastknown_city
                                        , lastknown_state, lastknown_country, lastknown_latitude, lastknown_longitude
                                        , first_seen_date, last_seen_date, years_active, persistent_inventor_id
                                        , gender_code)
select distinct i.inventor_id
  , i.name_first
  , i.name_last
  , i.num_patents
  , i.num_assignees
  , timl.old_location_id
  , i.lastknown_persistent_location_id
  , i.lastknown_city
  , i.lastknown_state
  , i.lastknown_country
  , i.lastknown_latitude
  , i.lastknown_longitude
  , i.first_seen_date
  , i.last_seen_date
  , i.years_active
  , i.persistent_inventor_id
  , gender_flag
from
    `{{reporting_db}}`.`inventor` i
        left join `{{reporting_db}}`.`temp_id_mapping_location` timl on timl.new_location_id = i.lastknown_location_id
        left join `PatentsView_{{version_indicator}}`.`temp_id_mapping_inventor` timi on i.inventor_id = timi.new_inventor_id
        left join gender_attribution.inventor_gender_{{version_indicator}} ig on i.inventor_id=ig.inventor_id;;


TRUNCATE TABLE `{{elastic_db}}`.inventor_years;
INSERT INTO `{{elastic_db}}`.inventor_years(inventor_id, patent_year, num_patents)
select
    ay.inventor_id
  , patent_year
  , ay.num_patents
from
    `{{reporting_db}}`.`inventor_year` ay
        join `{{elastic_db}}`.inventors a
where
    a.inventor_id = ay.inventor_id;