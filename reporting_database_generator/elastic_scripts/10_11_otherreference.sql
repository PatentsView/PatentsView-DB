{% set elastic_target_database = params.elastic_database_prefix + params.version_indicator.replace("-","") %}
{% set reporting_database = params.reporting_database %}
use `{{elastic_target_database}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_target_database}}`.`other_reference`
(
    `uuid`               varchar(36) COLLATE utf8mb4_unicode_ci NOT NULL,
    `patent_id`          varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `text`               text COLLATE utf8mb4_unicode_ci        DEFAULT NULL,
    `sequence`           int(11)                                DEFAULT NULL,
    `patent_zero_prefix` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
    PRIMARY KEY (`uuid`),
    KEY `patent_id` (`patent_id`, `sequence`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;


TRUNCATE TABLE `{{elastic_target_database}}`.other_reference;
INSERT INTO `{{elastic_target_database}}`.other_reference(uuid, patent_id, text, sequence, patent_zero_prefix)
select
    o.uuid
  , o.patent_id
  , o.text
  , o.sequence
  , p.patent_zero_prefix
from
    patent.otherreference o
        join `{{elastic_target_database}}`.patents p on o.patent_id = p.patent_id;