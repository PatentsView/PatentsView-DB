{% set elastic_db = "elastic_production_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`us_application_citations`
(
    `uuid`               varchar(36) COLLATE utf8mb4_unicode_ci NOT NULL,
    `patent_id`          varchar(20) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `document_number`    varchar(32) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `date`               date                                    DEFAULT NULL,
    `name`               varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `kind`               varchar(10) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `category`           varchar(64) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `sequence`           int(11)                                 DEFAULT NULL,
    `patent_zero_prefix` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
    PRIMARY KEY (`uuid`),
    KEY `patent_id` (`patent_id`, `sequence`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.us_application_citations;
INSERT INTO `{{elastic_db}}`.us_application_citations ( uuid, patent_id, document_number, date, name, kind, category
                                                     , sequence, patent_zero_prefix)
 select
    uuid
  , u.patent_id
  , u.number_transformed
  , u.date
  , name
  , u.kind
  , category
  , sequence
  , patent_zero_prefix
from
    patent.usapplicationcitation u
        join `{{elastic_db}}`.patents p on p.patent_id = u.patent_id;