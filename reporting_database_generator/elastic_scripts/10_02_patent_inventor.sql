{% set elastic_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`patent_inventor`
(
    `inventor_id`            int(10) unsigned                       NOT NULL,
    `persistent_inventor_id` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `patent_id`              varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL,
    `sequence`               int(11)                                NOT NULL,
    `name_first`             varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `name_last`              varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `gender_code`            varchar(1)  COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `city`                   varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `state`                  varchar(20) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `country`                varchar(10) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `location_id`            int(11)                                 DEFAULT NULL,
    `persistent_location_id` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    PRIMARY KEY (`patent_id`, `inventor_id`, `sequence`),
    KEY `ix_inventor_name_first` (`name_first`),
    KEY `ix_inventor_name_last` (`name_last`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.`patent_inventor`;

insert into `{{elastic_db}}`.patent_inventor ( inventor_id, patent_id, sequence, name_first, name_last, gender_code, city, state
                                                          , country, location_id, persistent_inventor_id, persistent_location_id)

select pi.inventor_id
     , pi.patent_id
     , pi.sequence
     , i.name_first
     , i.name_last
     , g.gender_flag
     , l.city
     , l.state
     , l.country
     , pi.location_id
     , timi.old_inventor_id
     , timl.old_location_id
from `{{reporting_db}}`.patent_inventor pi
         join `{{reporting_db}}`.inventor i on i.inventor_id = pi.inventor_id
         join `{{reporting_db}}`.temp_id_mapping_inventor timi on timi.new_inventor_id = i.inventor_id
         left join gender_attribution.inventor_gender{{version_indicator}} g on pi.inventor_id = g.inventor_id 
         left join `{{reporting_db}}`.location l on l.location_id = pi.location_id
         left join `{{reporting_db}}`.temp_id_mapping_location timl on timl.new_location_id = l.location_id
         join `{{elastic_db}}`.patents p on p.patent_id = pi.patent_id;

