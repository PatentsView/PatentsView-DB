{% set elastic_db = "elastic_production_pgpub_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "pregrant_publications" %}

use `{{elastic_db}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`publication`
(
    `id`                                                    varchar(128) NOT NULL,
    `document_number`                                       bigint NOT NULL,
    `type`                                                  varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `application_number`                                    varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
    `country`                                               varchar(20) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `date`                                                  date                                    DEFAULT NULL,
    `year`                                                  smallint(5) unsigned DEFAULT NULL,
    `abstract`                                              text COLLATE utf8mb4_unicode_ci         DEFAULT NULL,
    `title`                                                 text COLLATE utf8mb4_unicode_ci         DEFAULT NULL,
    `kind`                                                  varchar(10) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `series_code`                                           varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
    `rule_47_flag`                                          varchar(5) NOT NULL,
    PRIMARY KEY (`id`),
    KEY                                                     `ix_publication_number` (`document_number`),
    KEY                                                     `ix_publication_application_number` (`application_number`),
    KEY                                                     `ix_publication_title` (`title`(128)),
    KEY                                                     `ix_publication_type` (`type`),
    KEY                                                     `ix_publication_year` (`year`),
    KEY                                                     `ix_publication_date` (`date`),
    KEY                                                     `ix_publication_country` (`country`),
    FULLTEXT KEY `fti_publication_title` (`title`),
    FULLTEXT KEY `fti_publication_abstract` (`abstract`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.`publication`;

insert into `{{elastic_db}}`.publication ( id, document_number, type, application_number, country, date, year, abstract, title, kind, series_code, rule_47_flag)
select p.id
     , p.document_number
     , a.type
     , a.application_number
     , p.country
     , p.date
     , YEAR(p.date) as year
     , a.invention_abstract
     , a.invention_title
     , p.kind
     , a.series_code
     , a.rule_47_flag
from `{{reporting_db}}`.publication p
    left join `{{reporting_db}}`.application a
on a.document_number = p.document_number;

