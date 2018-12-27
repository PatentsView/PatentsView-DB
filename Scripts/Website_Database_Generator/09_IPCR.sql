# BEGIN ipcr

################################################################################################################################################

##


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_ipcr_aggregations`;

CREATE TABLE `PatentsView_20181127`.`temp_ipcr_aggregations`
(
   `section`          VARCHAR(20) NULL,
   `ipc_class`        VARCHAR(20) NULL,
   `subclass`         VARCHAR(20) NULL,
   `num_assignees`    INT UNSIGNED NOT NULL,
   `num_inventors`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`section`, `ipc_class`, `subclass`)
)
ENGINE = INNODB;


# 11:53

INSERT INTO `PatentsView_20181127`.`temp_ipcr_aggregations`(`section`,
                                                            `ipc_class`,
                                                            `subclass`,
                                                            `num_assignees`,
                                                            `num_inventors`)
     SELECT i.`section`,
            i.`ipc_class`,
            i.`subclass`,
            count(DISTINCT pa.`assignee_id`),
            count(DISTINCT pii.`inventor_id`)
       FROM `patent_20181127`.`ipcr` i
            LEFT OUTER JOIN `patent_20181127`.`patent_assignee` pa
               ON pa.`patent_id` = i.`patent_id`
            LEFT OUTER JOIN `patent_20181127`.`patent_inventor` pii
               ON pii.`patent_id` = i.`patent_id`
   GROUP BY i.`section`, i.`ipc_class`, i.`subclass`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_ipcr_years_active`;

CREATE TABLE `PatentsView_20181127`.`temp_ipcr_years_active`
(
   `section`                VARCHAR(20) NULL,
   `ipc_class`              VARCHAR(20) NULL,
   `subclass`               VARCHAR(20) NULL,
   `first_seen_date`        DATE NULL,
   `last_seen_date`         DATE NULL,
   `actual_years_active`    SMALLINT UNSIGNED NOT NULL,
   PRIMARY KEY(`section`, `ipc_class`, `subclass`)
)
ENGINE = INNODB;


# 2:17

INSERT INTO `PatentsView_20181127`.`temp_ipcr_years_active`(
               `section`,
               `ipc_class`,
               `subclass`,
               `first_seen_date`,
               `last_seen_date`,
               `actual_years_active`)
     SELECT i.`section`,
            i.`ipc_class`,
            i.`subclass`,
            min(p.`date`),
            max(p.`date`),
            ifnull(
               round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365),
               0)
       FROM `patent_20181127`.`ipcr` i
            INNER JOIN `PatentsView_20181127`.`patent` p
               ON p.`patent_id` = i.`patent_id`
      WHERE p.`date` IS NOT NULL
   GROUP BY i.`section`, i.`ipc_class`, i.`subclass`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`ipcr`;

CREATE TABLE `PatentsView_20181127`.`ipcr`
(
   `patent_id`                     VARCHAR(20) NOT NULL,
   `sequence`                      INT NOT NULL,
   `section`                       VARCHAR(20) NULL,
   `ipc_class`                     VARCHAR(20) NULL,
   `subclass`                      VARCHAR(20) NULL,
   `main_group`                    VARCHAR(20) NULL,
   `subgroup`                      VARCHAR(20) NULL,
   `symbol_position`               VARCHAR(20) NULL,
   `classification_value`          VARCHAR(20) NULL,
   `classification_data_source`    VARCHAR(20) NULL,
   `action_date`                   DATE NULL,
   `ipc_version_indicator`         DATE NULL,
   `num_assignees`                 INT UNSIGNED NULL,
   `num_inventors`                 INT UNSIGNED NULL,
   `first_seen_date`               DATE NULL,
   `last_seen_date`                DATE NULL,
   `years_active`                  SMALLINT UNSIGNED NULL,
   PRIMARY KEY(`patent_id`, `sequence`)
)
ENGINE = INNODB;


# 7,702,885 @ 6:38

INSERT INTO `PatentsView_20181127`.`ipcr`(`patent_id`,
                                          `sequence`,
                                          `section`,
                                          `ipc_class`,
                                          `subclass`,
                                          `main_group`,
                                          `subgroup`,
                                          `symbol_position`,
                                          `classification_value`,
                                          `classification_data_source`,
                                          `action_date`,
                                          `ipc_version_indicator`,
                                          `num_assignees`,
                                          `num_inventors`,
                                          `first_seen_date`,
                                          `last_seen_date`,
                                          `years_active`)
   SELECT p.`patent_id`,
          i.`sequence`,
          nullif(trim(i.`section`), ''),
          nullif(trim(i.`ipc_class`), ''),
          nullif(trim(i.`subclass`), ''),
          nullif(trim(i.`main_group`), ''),
          nullif(trim(i.`subgroup`), ''),
          nullif(trim(i.`symbol_position`), ''),
          nullif(trim(i.`classification_value`), ''),
          nullif(trim(i.`classification_data_source`), ''),
          CASE
             WHEN     `action_date` > date('1899-12-31')
                  AND `action_date` <
                      date_add(current_date, INTERVAL 10 YEAR)
             THEN
                `action_date`
             ELSE
                NULL
          END,
          CASE
             WHEN     `ipc_version_indicator` > date('1899-12-31')
                  AND `ipc_version_indicator` <
                      date_add(current_date, INTERVAL 10 YEAR)
             THEN
                `ipc_version_indicator`
             ELSE
                NULL
          END,
          tia.`num_assignees`,
          tia.`num_inventors`,
          tiya.`first_seen_date`,
          tiya.`last_seen_date`,
          ifnull(
             CASE
                WHEN tiya.`actual_years_active` < 1 THEN 1
                ELSE tiya.`actual_years_active`
             END,
             0)
     FROM `PatentsView_20181127`.`patent` p
          INNER JOIN `patent_20181127`.`ipcr` i
             ON i.`patent_id` = p.`patent_id`
          LEFT OUTER JOIN `PatentsView_20181127`.`temp_ipcr_aggregations` tia
             ON     tia.`section` = i.`section`
                AND tia.`ipc_class` = i.`ipc_class`
                AND tia.`subclass` = i.`subclass`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_ipcr_years_active` tiya
             ON     tiya.`section` = i.`section`
                AND tiya.`ipc_class` = i.`ipc_class`
                AND tiya.`subclass` = i.`subclass`;


# END ipcr

################################################################################################################################################

####


# BEGIN nber

################################################################################################################################################

##


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_nber_subcategory_aggregate_counts`;

CREATE TABLE `PatentsView_20181127`.`temp_nber_subcategory_aggregate_counts`
(
   `subcategory_id`         VARCHAR(20) NOT NULL,
   `num_assignees`          INT UNSIGNED NOT NULL,
   `num_inventors`          INT UNSIGNED NOT NULL,
   `num_patents`            INT UNSIGNED NOT NULL,
   `first_seen_date`        DATE NULL,
   `last_seen_date`         DATE NULL,
   `actual_years_active`    SMALLINT UNSIGNED NOT NULL,
   PRIMARY KEY(`subcategory_id`)
)
ENGINE = INNODB;


# 38 @ 4:45

INSERT INTO `PatentsView_20181127`.`temp_nber_subcategory_aggregate_counts`(
               `subcategory_id`,
               `num_assignees`,
               `num_inventors`,
               `num_patents`,
               `first_seen_date`,
               `last_seen_date`,
               `actual_years_active`)
     SELECT n.`subcategory_id`,
            count(DISTINCT pa.`assignee_id`)
               num_assignees,
            count(DISTINCT pii.`inventor_id`)
               num_inventors,
            count(DISTINCT n.`patent_id`)
               num_patents,
            min(p.`date`),
            max(p.`date`),
            ifnull(
               round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365),
               0)
       FROM `patent_20181127`.`nber` n
            LEFT OUTER JOIN `patent_20181127`.`patent_assignee` pa
               ON pa.`patent_id` = n.`patent_id`
            LEFT OUTER JOIN `patent_20181127`.`patent_inventor` pii
               ON pii.`patent_id` = n.`patent_id`
            LEFT OUTER JOIN `PatentsView_20181127`.`patent` p
               ON p.`patent_id` = n.`patent_id`
   GROUP BY n.`subcategory_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`nber`;

CREATE TABLE `PatentsView_20181127`.`nber`
(
   `patent_id`            VARCHAR(20) NOT NULL,
   `category_id`          VARCHAR(20) NULL,
   `category_title`       VARCHAR(512) NULL,
   `subcategory_id`       VARCHAR(20) NULL,
   `subcategory_title`    VARCHAR(512) NULL,
   `num_assignees`        INT UNSIGNED NULL,
   `num_inventors`        INT UNSIGNED NULL,
   `num_patents`          INT UNSIGNED NULL,
   `first_seen_date`      DATE NULL,
   `last_seen_date`       DATE NULL,
   `years_active`         SMALLINT UNSIGNED NULL,
   PRIMARY KEY(`patent_id`)
)
ENGINE = INNODB;


# 4,927,287 @ 1:47

INSERT INTO `PatentsView_20181127`.`nber`(`patent_id`,
                                          `category_id`,
                                          `category_title`,
                                          `subcategory_id`,
                                          `subcategory_title`,
                                          `num_assignees`,
                                          `num_inventors`,
                                          `num_patents`,
                                          `first_seen_date`,
                                          `last_seen_date`,
                                          `years_active`)
   SELECT p.`patent_id`,
          nullif(trim(n.`category_id`), ''),
          nullif(trim(c.`title`), ''),
          nullif(trim(n.`subcategory_id`), ''),
          nullif(trim(s.`title`), ''),
          tnsac.`num_assignees`,
          tnsac.`num_inventors`,
          tnsac.`num_patents`,
          tnsac.`first_seen_date`,
          tnsac.`last_seen_date`,
          CASE
             WHEN tnsac.`actual_years_active` < 1 THEN 1
             ELSE tnsac.`actual_years_active`
          END
     FROM `PatentsView_20181127`.`patent` p
          INNER JOIN `patent_20181127`.`nber` n
             ON p.`patent_id` = n.`patent_id`
          LEFT OUTER JOIN `patent_20181127`.`nber_category` c
             ON c.`id` = n.`category_id`
          LEFT OUTER JOIN `patent_20181127`.`nber_subcategory` s
             ON s.`id` = n.`subcategory_id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_nber_subcategory_aggregate_counts`
          tnsac
             ON tnsac.`subcategory_id` = n.`subcategory_id`;


# END nber

################################################################################################################################################

####


# BEGIN nber_subcategory_patent_year

##########################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`nber_subcategory_patent_year`;

CREATE TABLE `PatentsView_20181127`.`nber_subcategory_patent_year`
(
   `subcategory_id`    VARCHAR(20) NOT NULL,
   `patent_year`       SMALLINT UNSIGNED NOT NULL,
   `num_patents`       INT UNSIGNED NOT NULL,
   PRIMARY KEY(`subcategory_id`, `patent_year`)
)
ENGINE = INNODB;


# 1,483 @ 1:01

INSERT INTO `PatentsView_20181127`.`nber_subcategory_patent_year`(
               `subcategory_id`,
               `patent_year`,
               `num_patents`)
     SELECT n.`subcategory_id`, year(p.`date`), count(DISTINCT n.`patent_id`)
       FROM `patent_20181127`.`nber` n
            INNER JOIN `PatentsView_20181127`.`patent` p
               ON p.`patent_id` = n.`patent_id` AND p.`date` IS NOT NULL
      WHERE n.`subcategory_id` IS NOT NULL AND n.`subcategory_id` != ''
   GROUP BY n.`subcategory_id`, year(p.`date`);


# END nber_subcategory_patent_year

############################################################################################################################
