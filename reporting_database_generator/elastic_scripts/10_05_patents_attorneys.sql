use `elastic_production_{{ dag_run.logical_date | ds_nodash }}`;

CREATE TABLE IF NOT EXISTS `elastic_production_{{ dag_run.logical_date | ds_nodash }}`.`patent_attorneys`
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

TRUNCATE TABLE `elastic_production_{{ dag_run.logical_date | ds_nodash }}`.`patent_attorneys`;

insert into `elastic_production_{{ dag_run.logical_date | ds_nodash }}`.patent_attorneys( patent_id, lawyer_id, sequence, name_first, name_last, organization
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
    `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.patent_lawyer pl
        join `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.lawyer l on pl.lawyer_id = l.lawyer_id
        join `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.temp_id_mapping_lawyer timl on timl.new_lawyer_id = l.lawyer_id
        join `elastic_production_{{ dag_run.logical_date | ds_nodash }}`.patents p on pl.patent_id = p.patent_id;

