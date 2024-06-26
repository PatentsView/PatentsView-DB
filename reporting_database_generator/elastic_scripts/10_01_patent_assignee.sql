{% set elastic_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`patent_assignee`
(
    `assignee_id`            int(10) unsigned NOT NULL,
    `persistent_assignee_id` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `type`                   varchar(10) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `name_first`             varchar(64) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `name_last`              varchar(64) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `organization`           varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `city`                   varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `state`                  varchar(20) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `country`                varchar(10) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `sequence`               int(11) DEFAULT NULL,
    `location_id`            int(11) DEFAULT NULL,
    `persistent_location_id` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `patent_id`              varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
    PRIMARY KEY (`patent_id`, `assignee_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.`patent_assignee`;
INSERT INTO `{{elastic_db}}`.`patent_assignee`( assignee_id, type, name_first, name_last, organization, city, state
                                              , country, sequence, location_id, patent_id, persistent_location_id
                                              , persistent_assignee_id)

select pa.assignee_id
     , a.type
     , a.name_first
     , a.name_last
     , a.organization
     , l.city
     , l.state
     , l.country
     , pa.sequence
     , l.location_id
     , pa.patent_id
     , timl.old_location_id
     , tima.old_assignee_id
from `{{reporting_db}}`.patent_assignee pa
         join `{{elastic_db}}`.patents p on p.patent_id = pa.patent_id
         join `{{reporting_db}}`.assignee a on a.assignee_id = pa.assignee_id
         join `{{reporting_db}}`.temp_id_mapping_assignee tima on tima.new_assignee_id = a.assignee_id
         left join `{{reporting_db}}`.location l on l.location_id = pa.location_id
         left join `{{reporting_db}}`.temp_id_mapping_location timl on timl.new_location_id = l.location_id;