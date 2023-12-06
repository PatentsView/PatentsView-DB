{% set elastic_db = "elastic_production_pgpub_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "pregrant_publications" %}
{% set version_indicator = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`publication_inventor`
(
    `inventor_id`            int(10) unsigned                       NOT NULL,
    `document_number`        varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL,
    `sequence`               int(11)                                NOT NULL,
    `name_first`             varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `name_last`              varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `city`                   varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `state`                  varchar(20) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `country`                varchar(10) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `location_id`            int(11)                                 DEFAULT NULL,
    PRIMARY KEY (`document_number`, `inventor_id`, `sequence`),
    KEY `ix_inventor_name_first` (`name_first`),
    KEY `ix_inventor_name_last` (`name_last`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.`publication_inventor`;

insert into `{{elastic_db}}`.publication_inventor ( inventor_id, document_number, sequence, name_first, name_last
                                                          , city, state
                                                          , country, location_id)

select pi.inventor_id
     , pi.document_number
     , pi.sequence
     , i.name_first
     , i.name_last
     , l.city
     , l.state
     , l.country
     , pi.location_id
from `{{reporting_db}}`.publication_inventor pi
         join patent.inventor_{{version_indicator}} i on i.id = pi.inventor_id
         left join patent.location_{{version_indicator}} l on l.location_id = pi.location_id
         join `{{elastic_db}}`.publication p on p.document_number = pi.document_number;

