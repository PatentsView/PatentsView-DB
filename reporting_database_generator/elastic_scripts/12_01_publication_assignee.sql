{% set elastic_db = "elastic_production_pgpub_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "pregrant_publications" %}
{% set version_indicator = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`publication_assignee`
(
    `assignee_id`            int(10) unsigned NOT NULL,
    `type`                   varchar(10) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `name_first`             varchar(64) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `name_last`              varchar(64) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `organization`           varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `city`                   varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `state`                  varchar(20) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `country`                varchar(10) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `sequence`               int(11) DEFAULT NULL,
    `location_id`            int(11) DEFAULT NULL,
    `document_number`              varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
    PRIMARY KEY (`document_number`, `assignee_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.`patent_assignee`;
INSERT INTO `{{elastic_db}}`.`patent_assignee`( assignee_id, type, name_first, name_last, organization, city, state
                                              , country, sequence, location_id, document_number)

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
     , pa.document_number
from `{{reporting_db}}`.publication_assignee pa
         join `{{elastic_db}}`.publication p on p.document_number = pa.document_number
         join patent.assignee_{{version_indicator}} a on a.id = pa.assignee_id
         left join patent.location_{{version_indicator}} l on l.location_id = pa.location_id