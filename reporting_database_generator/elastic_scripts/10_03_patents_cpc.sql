{% set elastic_db = "elastic_production_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

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

insert into `{{elastic_db}}`.patent_cpc_at_issue( patent_id, sequence, cpc_section, cpc_class, cpc_subclass
                                                             , cpc_group
                                                             , cpc_type)
select *
from (select x.patent_id
           , row_number() over (partition by x.patent_id order by x.source desc,x.sequence) - 1
  , x.section_id
           , x.subsection_id
           , x.group_id
           , x.subgroup_id
           , x.category
      from (SELECT c.patent_id
                 , c.sequence
                 , section as section_id
                 , concat(
                section
                 , class) as subsection_id
                 , concat(
                section
                 , class
                 , subclass) as group_id
                 , concat(
                section
                 , class
                 , subclass
                 , main_group
                 , '/'
                 , subgroup) as subgroup_id
                 , case when c.value = 'I' then 'inventional' else 'additional' end as category
                 , 'main' as source
            from
                patent.main_cpc c
                join `{{elastic_db}}`.patents p
            on p.patent_id = c.patent_id
            union
            SELECT
                c.patent_id
                    , c.sequence
                    , section as section_id
                    , concat(
                section
                    , class) as subsection_id
                    , concat(
                section
                    , class
                    , subclass) as group_id
                    , concat(
                section
                    , class
                    , subclass
                    , main_group
                    , '/'
                    , subgroup) as subgroup_id
                    , case when c.value = 'I' then 'inventional' else 'additional' end as category
                    , 'further' as source
            from
                patent.further_cpc c
                join `{{elastic_db}}`.patents p
            on p.patent_id = c.patent_id) x) y;

