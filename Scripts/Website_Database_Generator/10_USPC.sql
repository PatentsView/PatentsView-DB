# BEGIN uspc_current

##########################################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_mainclass_current_aggregate_counts`;

CREATE TABLE `PatentsView_20181127`.`temp_mainclass_current_aggregate_counts`
(
   `mainclass_id`           VARCHAR(20) NOT NULL,
   `num_assignees`          INT UNSIGNED NOT NULL,
   `num_inventors`          INT UNSIGNED NOT NULL,
   `num_patents`            INT UNSIGNED NOT NULL,
   `first_seen_date`        DATE NULL,
   `last_seen_date`         DATE NULL,
   `actual_years_active`    SMALLINT UNSIGNED NOT NULL,
   PRIMARY KEY(`mainclass_id`)
)
ENGINE = INNODB;


# 24:52

INSERT INTO `PatentsView_20181127`.`temp_mainclass_current_aggregate_counts`(
               `mainclass_id`,
               `num_assignees`,
               `num_inventors`,
               `num_patents`,
               `first_seen_date`,
               `last_seen_date`,
               `actual_years_active`)
     SELECT u.`mainclass_id`,
            count(DISTINCT pa.`assignee_id`),
            count(DISTINCT pii.`inventor_id`),
            count(DISTINCT u.`patent_id`),
            min(p.`date`),
            max(p.`date`),
            ifnull(
               round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365),
               0)
       FROM `patent_20181127`.`uspc_current` u
            LEFT OUTER JOIN `patent_20181127`.`patent_assignee` pa
               ON pa.`patent_id` = u.`patent_id`
            LEFT OUTER JOIN `patent_20181127`.`patent_inventor` pii
               ON pii.`patent_id` = u.`patent_id`
            LEFT OUTER JOIN `PatentsView_20181127`.`patent` p
               ON p.`patent_id` = u.`patent_id` AND p.`date` IS NOT NULL
      WHERE u.`mainclass_id` IS NOT NULL AND u.`mainclass_id` != ''
   GROUP BY u.`mainclass_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_mainclass_current_title`;

CREATE TABLE `PatentsView_20181127`.`temp_mainclass_current_title`
(
   `id`       VARCHAR(20) NOT NULL,
   `title`    VARCHAR(512) NULL,
   PRIMARY KEY(`id`)
)
ENGINE = INNODB;


# "Fix" casing where necessary.
# 0.125 sec

INSERT INTO `PatentsView_20181127`.`temp_mainclass_current_title`(`id`,
                                                                  `title`)
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
     FROM `patent_20181127`.`mainclass_current`;


# Fix casing of subclass_current.
DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_subclass_current_title`;

CREATE TABLE `PatentsView_20181127`.`temp_subclass_current_title`
(
   `id`       VARCHAR(20) NOT NULL,
   `title`    VARCHAR(512) NULL,
   PRIMARY KEY(`id`)
)
ENGINE = INNODB;


# "Fix" casing where necessary.
# 1.719 sec

INSERT INTO `PatentsView_20181127`.`temp_subclass_current_title`(`id`,
                                                                 `title`)
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
     FROM `patent_20181127`.`subclass_current`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`uspc_current`;

CREATE TABLE `PatentsView_20181127`.`uspc_current`
(
   `patent_id`          VARCHAR(20) NOT NULL,
   `sequence`           INT UNSIGNED NOT NULL,
   `mainclass_id`       VARCHAR(20) NULL,
   `mainclass_title`    VARCHAR(256) NULL,
   `subclass_id`        VARCHAR(20) NULL,
   `subclass_title`     VARCHAR(512) NULL,
   `num_assignees`      INT UNSIGNED NULL,
   `num_inventors`      INT UNSIGNED NULL,
   `num_patents`        INT UNSIGNED NULL,
   `first_seen_date`    DATE NULL,
   `last_seen_date`     DATE NULL,
   `years_active`       SMALLINT UNSIGNED NULL,
   PRIMARY KEY(`patent_id`, `sequence`)
)
ENGINE = INNODB;


# 21,191,230 @ 16:54
# 21,175,812 @ 1:02:06
# 21,175,812 @ 11:36

INSERT INTO `PatentsView_20181127`.`uspc_current`(`patent_id`,
                                                  `sequence`,
                                                  `mainclass_id`,
                                                  `mainclass_title`,
                                                  `subclass_id`,
                                                  `subclass_title`,
                                                  `num_assignees`,
                                                  `num_inventors`,
                                                  `num_patents`,
                                                  `first_seen_date`,
                                                  `last_seen_date`,
                                                  `years_active`)
   SELECT p.`patent_id`,
          u.`sequence`,
          nullif(trim(u.`mainclass_id`), ''),
          nullif(trim(m.`title`), ''),
          nullif(trim(u.`subclass_id`), ''),
          nullif(trim(s.`title`), ''),
          tmcac.`num_assignees`,
          tmcac.`num_inventors`,
          tmcac.`num_patents`,
          tmcac.`first_seen_date`,
          tmcac.`last_seen_date`,
          ifnull(
             CASE
                WHEN tmcac.`actual_years_active` < 1 THEN 1
                ELSE tmcac.`actual_years_active`
             END,
             0)
     FROM `PatentsView_20181127`.`patent` p
          INNER JOIN `patent_20181127`.`uspc_current` u
             ON u.`patent_id` = p.`patent_id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_mainclass_current_title` m
             ON m.`id` = u.`mainclass_id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_subclass_current_title` s
             ON s.`id` = u.`subclass_id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_mainclass_current_aggregate_counts`
          tmcac
             ON tmcac.`mainclass_id` = u.`mainclass_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`uspc_current_mainclass`;

CREATE TABLE `PatentsView_20181127`.`uspc_current_mainclass`
(
   `patent_id`          VARCHAR(20) NOT NULL,
   `mainclass_id`       VARCHAR(20) NULL,
   `mainclass_title`    VARCHAR(256) NULL,
   `num_assignees`      INT UNSIGNED NULL,
   `num_inventors`      INT UNSIGNED NULL,
   `num_patents`        INT UNSIGNED NULL,
   `first_seen_date`    DATE NULL,
   `last_seen_date`     DATE NULL,
   `years_active`       SMALLINT UNSIGNED NULL,
   PRIMARY KEY(`patent_id`, `mainclass_id`)
)
ENGINE = INNODB;


# 9,054,003 @ 9:27

INSERT INTO `PatentsView_20181127`.`uspc_current_mainclass`(
               `patent_id`,
               `mainclass_id`,
               `mainclass_title`,
               `num_assignees`,
               `num_inventors`,
               `num_patents`,
               `first_seen_date`,
               `last_seen_date`,
               `years_active`)
   SELECT u.`patent_id`,
          u.`mainclass_id`,
          nullif(trim(m.`title`), ''),
          tmcac.`num_assignees`,
          tmcac.`num_inventors`,
          tmcac.`num_patents`,
          tmcac.`first_seen_date`,
          tmcac.`last_seen_date`,
          ifnull(
             CASE
                WHEN tmcac.`actual_years_active` < 1 THEN 1
                ELSE tmcac.`actual_years_active`
             END,
             0)
     FROM (SELECT DISTINCT `patent_id`, `mainclass_id`
             FROM `PatentsView_20181127`.`uspc_current`) u
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_mainclass_current_title` m
             ON m.`id` = u.`mainclass_id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_mainclass_current_aggregate_counts`
          tmcac
             ON tmcac.`mainclass_id` = u.`mainclass_id`;


# END uspc_current

############################################################################################################################################


# BEGIN uspc_current_mainclass_application_year

###############################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`uspc_current_mainclass_application_year`;

CREATE TABLE `PatentsView_20181127`.`uspc_current_mainclass_application_year`
(
   `mainclass_id`                      VARCHAR(20) NOT NULL,
   `application_year`                  SMALLINT UNSIGNED NOT NULL,
   `sample_size`                       INT UNSIGNED NOT NULL,
   `average_patent_processing_days`    INT UNSIGNED NULL,
   PRIMARY KEY(`mainclass_id`, `application_year`)
)
ENGINE = INNODB;


# 20,241 @ 0:56

INSERT INTO `PatentsView_20181127`.`uspc_current_mainclass_application_year`(
               `mainclass_id`,
               `application_year`,
               `sample_size`,
               `average_patent_processing_days`)
     SELECT u.`mainclass_id`,
            year(p.`earliest_application_date`),
            count(*),
            round(avg(p.`patent_processing_days`))
       FROM `PatentsView_20181127`.`patent` p
            INNER JOIN `PatentsView_20181127`.`uspc_current` u
               ON u.`patent_id` = p.`patent_id`
      WHERE p.`patent_processing_days` IS NOT NULL AND u.`sequence` = 0
   GROUP BY u.`mainclass_id`, year(p.`earliest_application_date`);


# 5,406,673 @ 32:45
# Update the patent with the average mainclass processing days.

UPDATE `PatentsView_20181127`.`patent` p
       INNER JOIN `PatentsView_20181127`.`uspc_current` u
          ON u.`patent_id` = p.`patent_id` AND u.`sequence` = 0
       INNER JOIN
       `PatentsView_20181127`.`uspc_current_mainclass_application_year` c
          ON     c.`mainclass_id` = u.`mainclass_id`
             AND c.`application_year` = year(p.`earliest_application_date`)
   SET p.`uspc_current_mainclass_average_patent_processing_days` =
          c.`average_patent_processing_days`;


# END uspc_current_mainclass_application_year

#################################################################################################################


# BEGIN cpc_current_group_application_year

###############################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`cpc_current_group_application_year`;

CREATE TABLE `PatentsView_20181127`.`cpc_current_group_application_year`
(
   `group_id`                          VARCHAR(20) NOT NULL,
   `application_year`                  SMALLINT UNSIGNED NOT NULL,
   `sample_size`                       INT UNSIGNED NOT NULL,
   `average_patent_processing_days`    INT UNSIGNED NULL,
   PRIMARY KEY(`group_id`, `application_year`)
)
ENGINE = INNODB;


# 20,241 @ 0:56

INSERT INTO `PatentsView_20181127`.`cpc_current_group_application_year`(
               `group_id`,
               `application_year`,
               `sample_size`,
               `average_patent_processing_days`)
     SELECT u.`group_id`,
            year(p.`earliest_application_date`),
            count(*),
            round(avg(p.`patent_processing_days`))
       FROM `PatentsView_20181127`.`patent` p
            INNER JOIN `PatentsView_20181127`.`cpc_current` u
               ON u.`patent_id` = p.`patent_id`
      WHERE p.`patent_processing_days` IS NOT NULL AND u.`sequence` = 0
   GROUP BY u.`group_id`, year(p.`earliest_application_date`);


# 5,406,673 @ 32:45
# Update the patent with the average mainclass processing days.

UPDATE `PatentsView_20181127`.`patent` p
       INNER JOIN `PatentsView_20181127`.`cpc_current` u
          ON u.`patent_id` = p.`patent_id` AND u.`sequence` = 0
       INNER JOIN
       `PatentsView_20181127`.`cpc_current_group_application_year` c
          ON     c.`group_id` = u.`group_id`
             AND c.`application_year` = year(p.`earliest_application_date`)
   SET p.`cpc_current_group_average_patent_processing_days` =
          c.`average_patent_processing_days`;


# END cpc_current_group_application_year

#################################################################################################################



# BEGIN uspc_current_mainclass_patent_year

####################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`uspc_current_mainclass_patent_year`;

CREATE TABLE `PatentsView_20181127`.`uspc_current_mainclass_patent_year`
(
   `mainclass_id`    VARCHAR(20) NOT NULL,
   `patent_year`     SMALLINT UNSIGNED NOT NULL,
   `num_patents`     INT UNSIGNED NOT NULL,
   PRIMARY KEY(`mainclass_id`, `patent_year`)
)
ENGINE = INNODB;


# 18,316 @ 12:56

INSERT INTO `PatentsView_20181127`.`uspc_current_mainclass_patent_year`(
               `mainclass_id`,
               `patent_year`,
               `num_patents`)
     SELECT u.`mainclass_id`, year(p.`date`), count(DISTINCT u.`patent_id`)
       FROM `patent_20181127`.`uspc_current` u
            INNER JOIN `PatentsView_20181127`.`patent` p
               ON p.`patent_id` = u.`patent_id` AND p.`date` IS NOT NULL
      WHERE u.`mainclass_id` IS NOT NULL AND u.`mainclass_id` != ''
   GROUP BY u.`mainclass_id`, year(p.`date`);


# END uspc_current_mainclass_patent_year

######################################################################################################################
