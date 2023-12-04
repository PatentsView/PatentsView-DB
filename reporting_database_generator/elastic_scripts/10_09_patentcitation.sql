{% set elastic_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`us_patent_citations`
(
    `uuid`               varchar(36) COLLATE utf8mb4_unicode_ci NOT NULL,
    `patent_id`          varchar(20) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `citation_id`        varchar(20) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `date`               date                                    DEFAULT NULL,
    `name`               varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `kind`               varchar(10) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `category`           varchar(64) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `sequence`           bigint(22)                             NOT NULL,
    `patent_zero_prefix` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    PRIMARY KEY (`uuid`),
    KEY `patent_id` (`patent_id`),
    KEY `citation_id` (`citation_id`),
    KEY `patent_id_2` (`patent_id`, `sequence`),
    KEY `date` (`date`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.us_patent_citations;
INSERT INTO `{{elastic_db}}`.us_patent_citations( uuid, patent_id, citation_id, date, name, kind, category
                                               , sequence, patent_zero_prefix)

select
    u2.uuid
  , u2.patent_id
  , u2.citation_id
  , p2.date
  , u2.name
  , u2.kind
  , u2.category
  , u2.sequence
  , p.patent_zero_prefix

from
    patent.uspatentcitation u2
        join `{{elastic_db}}`.patents p on p.patent_id = u2.patent_id
        join patent.patent p2 on p2.id = u2.citation_id;
