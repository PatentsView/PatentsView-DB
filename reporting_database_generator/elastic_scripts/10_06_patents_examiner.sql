{% set elastic_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`patent_examiner`
(
    `patent_id`              varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
    `examiner_id`            int(10) unsigned NOT NULL,
    `persistent_examiner_id` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `name_first`             varchar(64) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `name_last`              varchar(64) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `role`                   varchar(20) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `group`                  varchar(20) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    PRIMARY KEY (`patent_id`, `examiner_id`),
    KEY                      `ix_examiner_name_first` (`name_first`),
    KEY                      `ix_examiner_name_last` (`name_last`),
    KEY                      `ix_examiner_role` (`role`),
    KEY                      `ix_examiner_group` (`group`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;



TRUNCATE TABLE `{{elastic_db}}`.`patent_examiner`;

insert into `{{elastic_db}}`.patent_examiner( patent_id, examiner_id, name_first, name_last, role, `group`
                                              , persistent_examiner_id)
select
    pe.patent_id
  , pe.examiner_id
  , e.name_first
  , e.name_last
  , e.role
  , e.`group`
  , `time`.old_examiner_id
from
    `{{reporting_db}}`.patent_examiner pe
        join `{{reporting_db}}`.examiner e on pe.examiner_id = e.examiner_id
        join `{{reporting_db}}`.temp_id_mapping_examiner `time` on `time`.new_examiner_id = e.examiner_id
        join `{{elastic_db}}`.patents p on p.patent_id = pe.patent_id;

