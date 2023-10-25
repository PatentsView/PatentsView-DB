{% set elastic_db = "elastic_production_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`patent_cpc_at_issue`
(
    `patent_id`    varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
    `sequence`     int(10) unsigned NOT NULL,
    `cpc_section`  varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `cpc_class`    varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `cpc_subclass` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `cpc_group`    varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `cpc_type`     varchar(36) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    PRIMARY KEY (`patent_id`, `sequence`),
    KEY            `ix_cpc_current_subsection_id` (`cpc_class`),
    KEY            `ix_cpc_current_group_id` (`cpc_subclass`),
    KEY            `ix_cpc_current_subgroup_id` (`cpc_group`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;


TRUNCATE TABLE `{{elastic_db}}`.patent_cpc_at_issue;

insert into `{{elastic_db}}`.patent_cpc_at_issue
    (patent_id, sequence, cpc_section, cpc_class, cpc_subclass, cpc_group, cpc_type)
SELECT 
    c.patent_id, c.sequence, c.section_id, c.subsection_id, c.group_id, c.subgroup_id, c.category
FROM patent.cpc c
JOIN `{{elastic_db}}`.patents p on p.patent_id = c.patent_id
