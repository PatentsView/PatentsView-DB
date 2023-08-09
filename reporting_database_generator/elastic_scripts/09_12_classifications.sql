{% set elastic_target_database = params.elastic_database_prefix + params.version_indicator.replace("-","") %}
{% set reporting_database = params.reporting_database %}
use `{{elastic_target_database}}`;






CREATE TABLE IF NOT EXISTS `{{elastic_target_database}}`.`wipo_field`
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

TRUNCATE TABLE  `{{elastic_target_database}}`.wipo_field;
insert into `{{elastic_target_database}}`.wipo_field
select *
from
    `{{reporting_database}}`.`wipo_field`;


CREATE TABLE IF NOT EXISTS `{{elastic_target_database}}`.`cpc_class`
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
TRUNCATE TABLE `{{elastic_target_database}}`.cpc_class;
INSERT INTO `{{elastic_target_database}}`.cpc_class
select *
from
    `{{reporting_database}}`.`cpc_subsection`;
CREATE TABLE IF NOT EXISTS `{{elastic_target_database}}`.`cpc_subclass`
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

TRUNCATE TABLE `{{elastic_target_database}}`.cpc_subclass;
INSERT INTO `{{elastic_target_database}}`.cpc_subclass
select *
from
    `{{reporting_database}}`.`cpc_group`;

CREATE TABLE IF NOT EXISTS `{{elastic_target_database}}`.`cpc_group`
(
    `id`    varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
    `title` varchar(2048) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_target_database}}`.cpc_group;
insert into `{{elastic_target_database}}`.cpc_group
select *
from
    `{{reporting_database}}`.`cpc_subgroup`;



CREATE TABLE IF NOT EXISTS `{{elastic_target_database}}`.`uspc_mainclass`
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

TRUNCATE TABLE `{{elastic_target_database}}`.uspc_mainclass;
insert into `{{elastic_target_database}}`.uspc_mainclass
select *
from
    `{{reporting_database}}`.`uspc_mainclass`;


CREATE TABLE IF NOT EXISTS `{{elastic_target_database}}`.`uspc_subclass`
(
    `id`    varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
    `title` varchar(512) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_target_database}}`.uspc_subclass;
insert into `{{elastic_target_database}}`.uspc_subclass
select *
from
    `{{reporting_database}}`.`uspc_subclass`;

