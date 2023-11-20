{% set elastic_db = "elastic_production_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "pregrant_publications" %}

use `{{elastic_db}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`us_parties`
(
    `id`                                                    varchar(128) NOT NULL,
    `document_number`                                       bigint(16) NOT NULL,
    `name_first`                                            varchar(256) DEFAULT NULL,
    `name_last`                                             varchar(256) DEFAULT NULL,
    `organization`                                          varchar(256) DEFAULT NULL,
    `type`                                                  varchar(64) DEFAULT NULL,
    `designation`                                           varchar(32) DEFAULT NULL,
    `sequence`                                              int(11) DEFAULT NULL,
    `rawlocation_id`                                        varchar(128) DEFAULT NULL,
    `city`                                                  varchar(256) DEFAULT NULL,
    `state`                                                 varchar(256) DEFAULT NULL,
    `country`                                               varchar(256) DEFAULT NULL,
    `version_indicator`                                     date DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY                                                     `document_number` (`document_number`),
    KEY                                                     `us_parties_version_indicator_index` (`version_indicator`),
    KEY                                                     `rawlocation_id` (`rawlocation_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.`us_parties`;

insert into `{{elastic_db}}`.us_parties ( id, document_number, name_first, name_last, organization, type, designation, sequence, rawlocation_id, city, state, country, version_indicator)
select id
     , document_number
     , name_first
     , name_last
     , organization
     , type
     , designation
     , sequence
     , rawlocation_id
     , city
     , state
     , country
     , version_indicator
from `{{reporting_db}}`.`us_parties`;
