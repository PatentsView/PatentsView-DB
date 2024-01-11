{% set elastic_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`patent_attorneys`
(
    `patent_id`            varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
    `lawyer_id`            int(10) unsigned NOT NULL,
    `persistent_lawyer_id` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `sequence`             int(11) NOT NULL,
    `name_first`           varchar(64) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `name_last`            varchar(64) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `organization`         varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    PRIMARY KEY (`patent_id`, `sequence`),
    KEY                    `ix_lawyer_name_last` (`name_last`),
    KEY                    `ix_lawyer_organization` (`organization`),
    KEY                    `ix_lawyer_name_first` (`name_first`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.`patent_attorneys`;

insert into `{{elastic_db}}`.patent_attorneys( patent_id, lawyer_id, sequence, name_first, name_last, organization
                                               , persistent_lawyer_id)
select
    pl.patent_id
  , pl.lawyer_id
  , pl.sequence
  , l.name_first
  , l.name_last
  , l.organization
  , timl.old_lawyer_id
from
    `{{reporting_db}}`.patent_lawyer pl
        join `{{reporting_db}}`.lawyer l on pl.lawyer_id = l.lawyer_id
        join `{{reporting_db}}`.temp_id_mapping_lawyer timl on timl.new_lawyer_id = l.lawyer_id
        join `{{elastic_db}}`.patents p on pl.patent_id = p.patent_id;

