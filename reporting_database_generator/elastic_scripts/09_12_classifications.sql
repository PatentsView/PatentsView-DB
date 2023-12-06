{% set elastic_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`wipo_field`
(
    `id`           varchar(3) COLLATE utf8mb4_unicode_ci NOT NULL,
    `sector_title` varchar(60) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `field_title`  varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `ix_wipo_field_sector_title` (`sector_title`),
    KEY `ix_wipo_field_field_title` (`field_title`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE  `{{elastic_db}}`.wipo_field;
insert into `{{elastic_db}}`.wipo_field
select *
from
    `{{reporting_db}}`.`wipo_field`;


CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`cpc_class`
(
    `id`              varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
    `title`           varchar(512) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `num_patents`     int(10) unsigned                        DEFAULT NULL,
    `num_inventors`   int(10) unsigned                        DEFAULT NULL,
    `num_assignees`   int(10) unsigned                        DEFAULT NULL,
    `first_seen_date` date                                    DEFAULT NULL,
    `last_seen_date`  date                                    DEFAULT NULL,
    `years_active`    smallint(5) unsigned                    DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `ix_cpc_subsection_num_inventors` (`num_inventors`),
    KEY `ix_cpc_subsection_num_assignees` (`num_assignees`),
    KEY `ix_cpc_subsection_num_patents` (`num_patents`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;
TRUNCATE TABLE `{{elastic_db}}`.cpc_class;
INSERT INTO `{{elastic_db}}`.cpc_class
select *
from
    `{{reporting_db}}`.`cpc_subsection`;
CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`cpc_subclass`
(
    `id`              varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
    `title`           varchar(512) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `num_patents`     int(10) unsigned                        DEFAULT NULL,
    `num_inventors`   int(10) unsigned                        DEFAULT NULL,
    `num_assignees`   int(10) unsigned                        DEFAULT NULL,
    `first_seen_date` date                                    DEFAULT NULL,
    `last_seen_date`  date                                    DEFAULT NULL,
    `years_active`    smallint(5) unsigned                    DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `ix_cpc_group_num_inventors` (`num_inventors`),
    KEY `ix_cpc_group_num_assignees` (`num_assignees`),
    KEY `ix_cpc_group_num_patents` (`num_patents`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.cpc_subclass;
INSERT INTO `{{elastic_db}}`.cpc_subclass
select *
from
    `{{reporting_db}}`.`cpc_group`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`cpc_group`
(
    `id`    varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
    `title` varchar(2048) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.cpc_group;
insert into `{{elastic_db}}`.cpc_group
select *
from
    `{{reporting_db}}`.`cpc_subgroup`;



CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`uspc_mainclass`
(
    `id`              varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
    `title`           varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `num_patents`     int(10) unsigned                        DEFAULT NULL,
    `num_inventors`   int(10) unsigned                        DEFAULT NULL,
    `num_assignees`   int(10) unsigned                        DEFAULT NULL,
    `first_seen_date` date                                    DEFAULT NULL,
    `last_seen_date`  date                                    DEFAULT NULL,
    `years_active`    smallint(5) unsigned                    DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `ix_uspc_mainclass_num_patents` (`num_patents`),
    KEY `ix_uspc_mainclass_num_inventors` (`num_inventors`),
    KEY `ix_uspc_mainclass_num_assignees` (`num_assignees`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.uspc_mainclass;
insert into `{{elastic_db}}`.uspc_mainclass
select *
from
    `{{reporting_db}}`.`uspc_mainclass`;


CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`uspc_subclass`
(
    `id`    varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
    `title` varchar(512) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.uspc_subclass;
insert into `{{elastic_db}}`.uspc_subclass
select *
from
    `{{reporting_db}}`.`uspc_subclass`;

