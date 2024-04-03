{% set elastic_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`other_reference`
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


TRUNCATE TABLE `{{elastic_db}}`.other_reference;
INSERT INTO `{{elastic_db}}`.other_reference(uuid, patent_id, text, sequence, patent_zero_prefix)
select
    o.uuid
  , o.patent_id
  , o.text
  , o.sequence
  , p.patent_zero_prefix
from
    patent.otherreference o
        join `{{elastic_db}}`.patents p on o.patent_id = p.patent_id;