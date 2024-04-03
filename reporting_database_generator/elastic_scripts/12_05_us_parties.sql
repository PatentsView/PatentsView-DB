{% set elastic_db = "elastic_production_pgpub_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
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
    `applicant_authority`                                   varchar(256) DEFAULT NULL,
    `version_indicator`                                     date DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY                                                     `document_number` (`document_number`),
    KEY                                                     `us_parties_version_indicator_index` (`version_indicator`),
    KEY                                                     `rawlocation_id` (`rawlocation_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.`us_parties`;

insert into `{{elastic_db}}`.us_parties ( id, document_number, name_first, name_last, organization, type, designation, sequence, rawlocation_id, city, state, country, applicant_authority, version_indicator)
select up.id
     , up.document_number
     , up.name_first
     , up.name_last
     , up.organization
     , up.type
     , up.designation
     , up.sequence
     , up.rawlocation_id
     , up.city
     , up.state
     , up.country
     , up.applicant_authority
     , up.version_indicator
from `{{reporting_db}}`.`us_parties` up
join `{{elastic_db}}`.publication p on p.document_number = up.document_number;

