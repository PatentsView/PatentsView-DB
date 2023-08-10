{% set elastic_target_database = params.elastic_database_prefix + params.version_indicator.replace("-","") %}
{% set reporting_database = params.reporting_database %}
use `{{elastic_target_database}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_target_database}}`.`us_patent_citations`
(
    `uuid`               varchar(36) COLLATE utf8mb4_unicode_ci NOT NULL,
    `patent_id`          varchar(20) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `citation_id`        varchar(20) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `date`               date                                    DEFAULT NULL,
    `name`               varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `kind`               varchar(10) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `category`           varchar(64) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `sequence`           bigint(22)                             NOT NULL,
    `patent_zero_prefix` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
    PRIMARY KEY (`uuid`),
    KEY `patent_id` (`patent_id`),
    KEY `citation_id` (`citation_id`),
    KEY `patent_id_2` (`patent_id`, `sequence`),
    KEY `date` (`date`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_target_database}}`.us_patent_citations;
INSERT INTO `{{elastic_target_database}}`.us_patent_citations( uuid, patent_id, citation_id, date, name, kind, category
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
        join `{{elastic_target_database}}`.patents p on p.patent_id = u2.patent_id
        join patent.patent p2 on p2.id = u2.citation_id;
