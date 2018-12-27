# BEGIN cpc_current

###########################################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_cpc_current_subsection_aggregate_counts`;

CREATE TABLE `PatentsView_20181127`.`temp_cpc_current_subsection_aggregate_counts`
(
   `subsection_id`          VARCHAR(20) NOT NULL,
   `num_assignees`          INT UNSIGNED NOT NULL,
   `num_inventors`          INT UNSIGNED NOT NULL,
   `num_patents`            INT UNSIGNED NOT NULL,
   `first_seen_date`        DATE NULL,
   `last_seen_date`         DATE NULL,
   `actual_years_active`    SMALLINT UNSIGNED NOT NULL,
   PRIMARY KEY(`subsection_id`)
)
ENGINE = INNODB;


# 29:37

INSERT INTO `PatentsView_20181127`.`temp_cpc_current_subsection_aggregate_counts`(
               `subsection_id`,
               `num_assignees`,
               `num_inventors`,
               `num_patents`,
               `first_seen_date`,
               `last_seen_date`,
               `actual_years_active`)
     SELECT c.`subsection_id`,
            count(DISTINCT pa.`assignee_id`)
               num_assignees,
            count(DISTINCT pii.`inventor_id`)
               num_inventors,
            count(DISTINCT c.`patent_id`)
               num_patents,
            min(p.`date`),
            max(p.`date`),
            ifnull(
               round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365),
               0)
       FROM `patent_20181127`.`cpc_current` c
            LEFT OUTER JOIN `patent_20181127`.`patent_assignee` pa
               ON pa.`patent_id` = c.`patent_id`
            LEFT OUTER JOIN `patent_20181127`.`patent_inventor` pii
               ON pii.`patent_id` = c.`patent_id`
            LEFT OUTER JOIN `PatentsView_20181127`.`patent` p
               ON p.`patent_id` = c.`patent_id`
   GROUP BY c.`subsection_id`;

DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_cpc_subsection_title`;

CREATE TABLE `PatentsView_20181127`.`temp_cpc_subsection_title`
(
   `id`       VARCHAR(20) NOT NULL,
   `title`    VARCHAR(256) NULL,
   PRIMARY KEY(`id`)
)
ENGINE = INNODB;


# 0.125 sec

INSERT INTO `PatentsView_20181127`.`temp_cpc_subsection_title`(`id`, `title`)
   SELECT `id`,
          CASE
             WHEN BINARY replace(`title`, 'e.g.', 'E.G.') =
                  BINARY ucase(`title`)
             THEN
                concat(ucase(substring(trim(`title`), 1, 1)),
                       lcase(substring(trim(nullif(`title`, '')), 2)))
             ELSE
                `title`
          END
     FROM `patent_20181127`.`cpc_subsection`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_cpc_current_group_aggregate_counts`;

CREATE TABLE `PatentsView_20181127`.`temp_cpc_current_group_aggregate_counts`
(
   `group_id`               VARCHAR(20) NOT NULL,
   `num_assignees`          INT UNSIGNED NOT NULL,
   `num_inventors`          INT UNSIGNED NOT NULL,
   `num_patents`            INT UNSIGNED NOT NULL,
   `first_seen_date`        DATE NULL,
   `last_seen_date`         DATE NULL,
   `actual_years_active`    SMALLINT UNSIGNED NOT NULL,
   PRIMARY KEY(`group_id`)
)
ENGINE = INNODB;


# 29:37

INSERT INTO `PatentsView_20181127`.`temp_cpc_current_group_aggregate_counts`(
               `group_id`,
               `num_assignees`,
               `num_inventors`,
               `num_patents`,
               `first_seen_date`,
               `last_seen_date`,
               `actual_years_active`)
     SELECT c.`group_id`,
            count(DISTINCT pa.`assignee_id`)
               num_assignees,
            count(DISTINCT pii.`inventor_id`)
               num_inventors,
            count(DISTINCT c.`patent_id`)
               num_patents,
            min(p.`date`),
            max(p.`date`),
            ifnull(
               round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365),
               0)
       FROM `patent_20181127`.`cpc_current` c
            LEFT OUTER JOIN `patent_20181127`.`patent_assignee` pa
               ON pa.`patent_id` = c.`patent_id`
            LEFT OUTER JOIN `patent_20181127`.`patent_inventor` pii
               ON pii.`patent_id` = c.`patent_id`
            LEFT OUTER JOIN `PatentsView_20181127`.`patent` p
               ON p.`patent_id` = c.`patent_id`
   GROUP BY c.`group_id`;



DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_cpc_group_title`;

CREATE TABLE `PatentsView_20181127`.`temp_cpc_group_title`
(
   `id`       VARCHAR(20) NOT NULL,
   `title`    VARCHAR(256) NULL,
   PRIMARY KEY(`id`)
)
ENGINE = INNODB;


# 0.156

INSERT INTO `PatentsView_20181127`.`temp_cpc_group_title`(`id`, `title`)
   SELECT `id`,
          CASE
             WHEN BINARY replace(`title`, 'e.g.', 'E.G.') =
                  BINARY ucase(`title`)
             THEN
                concat(ucase(substring(trim(`title`), 1, 1)),
                       lcase(substring(trim(nullif(`title`, '')), 2)))
             ELSE
                `title`
          END
     FROM `patent_20181127`.`cpc_group`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_cpc_subgroup_title`;

CREATE TABLE `PatentsView_20181127`.`temp_cpc_subgroup_title`
(
   `id`       VARCHAR(20) NOT NULL,
   `title`    VARCHAR(512) NULL,
   PRIMARY KEY(`id`)
)
ENGINE = INNODB;


# 0:07

INSERT INTO `PatentsView_20181127`.`temp_cpc_subgroup_title`(`id`, `title`)
   SELECT `id`,
          CASE
             WHEN BINARY replace(`title`, 'e.g.', 'E.G.') =
                  BINARY ucase(`title`)
             THEN
                concat(ucase(substring(trim(`title`), 1, 1)),
                       lcase(substring(trim(nullif(`title`, '')), 2)))
             ELSE
                `title`
          END
     FROM `patent_20181127`.`cpc_subgroup`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`cpc_current`;

CREATE TABLE `PatentsView_20181127`.`cpc_current`
(
   `patent_id`                VARCHAR(20) NOT NULL,
   `sequence`                 INT UNSIGNED NOT NULL,
   `section_id`               VARCHAR(10) NULL,
   `subsection_id`            VARCHAR(20) NULL,
   `subsection_title`         VARCHAR(512) NULL,
   `group_id`                 VARCHAR(20) NULL,
   `group_title`              VARCHAR(256) NULL,
   `subgroup_id`              VARCHAR(20) NULL,
   `subgroup_title`           VARCHAR(512) NULL,
   `category`                 VARCHAR(36) NULL,
   `num_assignees`            INT UNSIGNED NULL,
   `num_inventors`            INT UNSIGNED NULL,
   `num_patents`              INT UNSIGNED NULL,
   `first_seen_date`          DATE NULL,
   `last_seen_date`           DATE NULL,
   `years_active`             SMALLINT UNSIGNED NULL,
   `num_assignees_group`      INT UNSIGNED NULL,
   `num_inventors_group`      INT UNSIGNED NULL,
   `num_patents_group`        INT UNSIGNED NULL,
   `first_seen_date_group`    DATE NULL,
   `last_seen_date_group`     DATE NULL,
   `years_active_group`       SMALLINT UNSIGNED NULL,
   PRIMARY KEY(`patent_id`, `sequence`)
)
ENGINE = INNODB;


# 23,151,381 @ 1:29:48
# 23,151,381 @ 36:32

INSERT INTO `PatentsView_20181127`.`cpc_current`(`patent_id`,
                                                 `sequence`,
                                                 `section_id`,
                                                 `subsection_id`,
                                                 `subsection_title`,
                                                 `group_id`,
                                                 `group_title`,
                                                 `subgroup_id`,
                                                 `subgroup_title`,
                                                 `category`,
                                                 `num_assignees`,
                                                 `num_inventors`,
                                                 `num_patents`,
                                                 `first_seen_date`,
                                                 `last_seen_date`,
                                                 `years_active`,
                                                 `num_assignees_group`,
                                                 `num_inventors_group`,
                                                 `num_patents_group`,
                                                 `first_seen_date_group`,
                                                 `last_seen_date_group`,
                                                 `years_active_group`)
   SELECT p.`patent_id`,
          c.`sequence`,
          nullif(trim(c.`section_id`), ''),
          nullif(trim(c.`subsection_id`), ''),
          nullif(trim(s.`title`), ''),
          nullif(trim(c.`group_id`), ''),
          nullif(trim(g.`title`), ''),
          nullif(trim(c.`subgroup_id`), ''),
          nullif(trim(sg.`title`), ''),
          c.`category`,
          tccsac.`num_assignees`,
          tccsac.`num_inventors`,
          tccsac.`num_patents`,
          tccsac.`first_seen_date`,
          tccsac.`last_seen_date`,
          CASE
             WHEN tccsac.`actual_years_active` < 1 THEN 1
             ELSE tccsac.`actual_years_active`
          END,
          tccgac.`num_assignees`,
          tccgac.`num_inventors`,
          tccgac.`num_patents`,
          tccgac.`first_seen_date`,
          tccgac.`last_seen_date`,
          CASE
             WHEN tccgac.`actual_years_active` < 1 THEN 1
             ELSE tccgac.`actual_years_active`
          END
     FROM `PatentsView_20181127`.`patent` p
          INNER JOIN `patent_20181127`.`cpc_current` c
             ON p.`patent_id` = c.`patent_id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_cpc_subsection_title` s
             ON s.`id` = c.`subsection_id`
          LEFT OUTER JOIN `PatentsView_20181127`.`temp_cpc_group_title` g
             ON g.`id` = c.`group_id`
          LEFT OUTER JOIN `PatentsView_20181127`.`temp_cpc_subgroup_title` sg
             ON sg.`id` = c.`subgroup_id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_cpc_current_subsection_aggregate_counts`
          tccsac
             ON tccsac.`subsection_id` = c.`subsection_id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_cpc_current_group_aggregate_counts`
          tccgac
             ON tccgac.`group_id` = c.`group_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`cpc_current_subsection`;

CREATE TABLE `PatentsView_20181127`.`cpc_current_subsection`
(
   `patent_id`           VARCHAR(20) NOT NULL,
   `section_id`          VARCHAR(10) NULL,
   `subsection_id`       VARCHAR(20) NULL,
   `subsection_title`    VARCHAR(512) NULL,
   `num_assignees`       INT UNSIGNED NULL,
   `num_inventors`       INT UNSIGNED NULL,
   `num_patents`         INT UNSIGNED NULL,
   `first_seen_date`     DATE NULL,
   `last_seen_date`      DATE NULL,
   `years_active`        SMALLINT UNSIGNED NULL,
   PRIMARY KEY(`patent_id`, `subsection_id`)
)
ENGINE = INNODB;


# 7,240,381 @ 19:00

INSERT INTO `PatentsView_20181127`.`cpc_current_subsection`(
               `patent_id`,
               `section_id`,
               `subsection_id`,
               `subsection_title`,
               `num_assignees`,
               `num_inventors`,
               `num_patents`,
               `first_seen_date`,
               `last_seen_date`,
               `years_active`)
   SELECT c.`patent_id`,
          c.`section_id`,
          c.`subsection_id`,
          nullif(trim(s.`title`), ''),
          tccsac.`num_assignees`,
          tccsac.`num_inventors`,
          tccsac.`num_patents`,
          tccsac.`first_seen_date`,
          tccsac.`last_seen_date`,
          CASE
             WHEN tccsac.`actual_years_active` < 1 THEN 1
             ELSE tccsac.`actual_years_active`
          END
     FROM (SELECT DISTINCT `patent_id`, `section_id`, `subsection_id`
             FROM `PatentsView_20181127`.`cpc_current`) c
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_cpc_subsection_title` s
             ON s.`id` = c.`subsection_id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_cpc_current_subsection_aggregate_counts`
          tccsac
             ON tccsac.`subsection_id` = c.`subsection_id`;



DROP TABLE IF EXISTS `PatentsView_20181127`.`cpc_current_group`;

CREATE TABLE `PatentsView_20181127`.`cpc_current_group`
(
   `patent_id`          VARCHAR(20) NOT NULL,
   `section_id`         VARCHAR(10) NULL,
   `group_id`           VARCHAR(20) NULL,
   `group_title`        VARCHAR(512) NULL,
   `num_assignees`      INT UNSIGNED NULL,
   `num_inventors`      INT UNSIGNED NULL,
   `num_patents`        INT UNSIGNED NULL,
   `first_seen_date`    DATE NULL,
   `last_seen_date`     DATE NULL,
   `years_active`       SMALLINT UNSIGNED NULL,
   PRIMARY KEY(`patent_id`, `group_id`)
)
ENGINE = INNODB;


# 7,240,381 @ 19:00

INSERT INTO `PatentsView_20181127`.`cpc_current_group`(`patent_id`,
                                                       `section_id`,
                                                       `group_id`,
                                                       `group_title`,
                                                       `num_assignees`,
                                                       `num_inventors`,
                                                       `num_patents`,
                                                       `first_seen_date`,
                                                       `last_seen_date`,
                                                       `years_active`)
   SELECT c.`patent_id`,
          c.`section_id`,
          c.`group_id`,
          nullif(trim(s.`title`), ''),
          tccgac.`num_assignees`,
          tccgac.`num_inventors`,
          tccgac.`num_patents`,
          tccgac.`first_seen_date`,
          tccgac.`last_seen_date`,
          CASE
             WHEN tccgac.`actual_years_active` < 1 THEN 1
             ELSE tccgac.`actual_years_active`
          END
     FROM (SELECT DISTINCT `patent_id`, `section_id`, `group_id`
             FROM `PatentsView_20181127`.`cpc_current`) c
          LEFT OUTER JOIN `PatentsView_20181127`.`temp_cpc_group_title` s
             ON s.`id` = c.`group_id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_cpc_current_group_aggregate_counts`
          tccgac
             ON tccgac.`group_id` = c.`group_id`;



# END cpc_current

#############################################################################################################################################


# BEGIN cpc_current_subsection_patent_year

####################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`cpc_current_subsection_patent_year`;

CREATE TABLE `PatentsView_20181127`.`cpc_current_subsection_patent_year`
(
   `subsection_id`    VARCHAR(20) NOT NULL,
   `patent_year`      SMALLINT UNSIGNED NOT NULL,
   `num_patents`      INT UNSIGNED NOT NULL,
   PRIMARY KEY(`subsection_id`, `patent_year`)
)
ENGINE = INNODB;


# 13:24

INSERT INTO `PatentsView_20181127`.`cpc_current_subsection_patent_year`(
               `subsection_id`,
               `patent_year`,
               `num_patents`)
     SELECT c.`subsection_id`, year(p.`date`), count(DISTINCT c.`patent_id`)
       FROM `patent_20181127`.`cpc_current` c
            INNER JOIN `PatentsView_20181127`.`patent` p
               ON p.`patent_id` = c.`patent_id` AND p.`date` IS NOT NULL
      WHERE c.`subsection_id` IS NOT NULL AND c.`subsection_id` != ''
   GROUP BY c.`subsection_id`, year(p.`date`);


# END cpc_current_subsection_patent_year

######################################################################################################################

# BEGIN cpc_current_group_patent_year

####################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`cpc_current_group_patent_year`;

CREATE TABLE `PatentsView_20181127`.`cpc_current_group_patent_year`
(
   `group_id`       VARCHAR(20) NOT NULL,
   `patent_year`    SMALLINT UNSIGNED NOT NULL,
   `num_patents`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`group_id`, `patent_year`)
)
ENGINE = INNODB;


# 13:24

INSERT INTO `PatentsView_20181127`.`cpc_current_group_patent_year`(
               `group_id`,
               `patent_year`,
               `num_patents`)
     SELECT c.`group_id`, year(p.`date`), count(DISTINCT c.`patent_id`)
       FROM `patent_20181127`.`cpc_current` c
            INNER JOIN `PatentsView_20181127`.`patent` p
               ON p.`patent_id` = c.`patent_id` AND p.`date` IS NOT NULL
      WHERE c.`group_id` IS NOT NULL AND c.`group_id` != ''
   GROUP BY c.`group_id`, year(p.`date`);


# END cpc_current_group_patent_year

######################################################################################################################
