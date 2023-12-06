{% set elastic_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`attorneys`
(
    `lawyer_id`            int(10) unsigned                       NOT NULL,
    `name_first`           varchar(64) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `name_last`            varchar(64) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `organization`         varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `num_patents`          int(10) unsigned                       NOT NULL,
    `num_assignees`        int(10) unsigned                       NOT NULL,
    `num_inventors`        int(10) unsigned                       NOT NULL,
    `first_seen_date`      date                                    DEFAULT NULL,
    `last_seen_date`       date                                    DEFAULT NULL,
    `years_active`         smallint(5) unsigned                   NOT NULL,
    `persistent_lawyer_id` varchar(36) COLLATE utf8mb4_unicode_ci NOT NULL,
    PRIMARY KEY (`lawyer_id`),
    KEY `ix_lawyer_name_last` (`name_last`),
    KEY `ix_lawyer_organization` (`organization`),
    KEY `ix_lawyer_num_patents` (`num_patents`),
    KEY `ix_lawyer_name_first` (`name_first`),
    KEY `ix_lawyer_num_assignees` (`num_assignees`),
    KEY `ix_lawyer_num_inventors` (`num_inventors`),
    KEY `ix_lawyer_first_seen_date` (`first_seen_date`),
    KEY `ix_lawyer_last_seen_date` (`last_seen_date`),
    KEY `ix_lawyer_persistent_lawyer_id` (`persistent_lawyer_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.attorneys;
INSERT INTO `{{elastic_db}}`.attorneys ( lawyer_id, name_first, name_last, organization, num_patents, num_assignees
                                         , num_inventors, first_seen_date, last_seen_date, years_active
                                         , persistent_lawyer_id)
select distinct
    l.*

from
    `{{reporting_db}}`.`lawyer` l
        join `{{reporting_db}}`.`patent_lawyer` pl on pl.lawyer_id = l.lawyer_id;
