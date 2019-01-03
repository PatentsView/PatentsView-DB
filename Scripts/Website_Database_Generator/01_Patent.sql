# BEGIN patent

################################################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_patent_firstnamed_assignee`;

CREATE TABLE `PatentsView_20181127`.`temp_patent_firstnamed_assignee`
(
   `patent_id`                     VARCHAR(20) NOT NULL,
   `assignee_id`                   INT UNSIGNED NULL,
   `persistent_assignee_id`        VARCHAR(36) NULL,
   `location_id`                   INT UNSIGNED NULL,
   `persistent_location_id`        VARCHAR(128) NULL,
   `city`                          VARCHAR(128) NULL,
   `state`                         VARCHAR(20) NULL,
   `country`                       VARCHAR(10) NULL,
   `latitude`                      FLOAT NULL,
   `longitude`                     FLOAT NULL,
   `ra_assignee_id`                VARCHAR(250) NULL,
   `rl_location_id`                VARCHAR(250) NULL,
   `rl_location_id_transformed`    VARCHAR(250) NULL,
   PRIMARY KEY(`patent_id`)
)
ENGINE = INNODB;

# 6,819,362 3:31

INSERT INTO `PatentsView_20181127`.`temp_patent_firstnamed_assignee`(
               patent_id,
               ra_assignee_id,
               rl_location_id,
               rl_location_id_transformed)
   SELECT p.`id`,
          ra.`assignee_id`,
          rl.`location_id`,
          rl.`location_id_transformed`
     FROM `patent_20181127`.`patent` p
          LEFT OUTER JOIN `patent_20181127`.`rawassignee` ra
             ON ra.`patent_id` = p.`id` AND ra.`sequence` = 0
          LEFT OUTER JOIN `patent_20181127`.`rawlocation` rl
             ON rl.`id` = ra.`rawlocation_id`;

ALTER TABLE `PatentsView_20181127`.`temp_patent_firstnamed_assignee` ADD INDEX (`rl_location_id`);
ALTER TABLE `PatentsView_20181127`.`temp_patent_firstnamed_assignee` ADD INDEX (`ra_assignee_id`);
ALTER TABLE `PatentsView_20181127`.`temp_patent_firstnamed_assignee` ADD INDEX (`rl_location_id_transformed`);

# 5,873,455 6:30
UPDATE PatentsView_20181127.`temp_patent_firstnamed_assignee`
        JOIN
    `PatentsView_20181127`.`temp_id_mapping_assignee` ta ON ta.`old_assignee_id` = ra_assignee_id 
SET 
    assignee_id = ta.`new_assignee_id`,
    persistent_assignee_id = ta.`old_assignee_id`;
	
# 5,865,836 5:30

UPDATE PatentsView_20181127.`temp_patent_firstnamed_assignee`
       JOIN `PatentsView_20181127`.`temp_id_mapping_location_transformed` tl
          ON tl.`old_location_id` = rl_location_id
   SET location_id = tl.`new_location_id`,
       persistent_location_id = tl.`old_location_id_transformed`;

# 5,865,832 2:50

UPDATE PatentsView_20181127.`temp_patent_firstnamed_assignee`
       JOIN `patent_20181127`.`location` l ON l.`id` = rl_location_id
   SET temp_patent_firstnamed_assignee.city = IF(l.city = '', NULL, l.city),
       temp_patent_firstnamed_assignee.`state` =
          IF(l.`state` = '', NULL, l.`state`),
       temp_patent_firstnamed_assignee.`country` =
          IF(l.`state` = '', NULL, l.`state`),
       temp_patent_firstnamed_assignee.`latitude` = l.`latitude`,
       temp_patent_firstnamed_assignee.`longitude` = l.`longitude`;

DELETE FROM `PatentsView_20181127`.`temp_patent_firstnamed_assignee`
      WHERE assignee_id IS NULL AND location_id IS NULL;


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_patent_firstnamed_inventor`;



CREATE TABLE `PatentsView_20181127`.`temp_patent_firstnamed_inventor`
(
   `patent_id`                     VARCHAR(20) NOT NULL,
   `inventor_id`                   INT UNSIGNED NULL,
   `persistent_inventor_id`        VARCHAR(36) NULL,
   `location_id`                   INT UNSIGNED NULL,
   `persistent_location_id`        VARCHAR(128) NULL,
   `city`                          VARCHAR(128) NULL,
   `state`                         VARCHAR(20) NULL,
   `country`                       VARCHAR(10) NULL,
   `latitude`                      FLOAT NULL,
   `longitude`                     FLOAT NULL,
   `ri_inventor_id`                VARCHAR(250) NULL,
   `rl_location_id`                VARCHAR(250) NULL,
   `rl_location_id_transformed`    VARCHAR(250) NULL,
   PRIMARY KEY(`patent_id`)
)
ENGINE = INNODB;

# 6,819,362 3:31

INSERT INTO `PatentsView_20181127`.`temp_patent_firstnamed_inventor`(
               patent_id,
               ri_inventor_id,
               rl_location_id,
               rl_location_id_transformed)
   SELECT p.`id`,
          ri.`inventor_id`,
          rl.`location_id`,
          rl.`location_id_transformed`
     FROM `patent_20181127`.`patent` p
          LEFT OUTER JOIN `patent_20181127`.`rawinventor` ri
             ON ri.`patent_id` = p.`id` AND ri.`sequence` = 0
          LEFT OUTER JOIN `patent_20181127`.`rawlocation` rl
             ON rl.`id` = ri.`rawlocation_id`;

ALTER TABLE `PatentsView_20181127`.`temp_patent_firstnamed_inventor`
   ADD INDEX (`rl_location_id`);

ALTER TABLE `PatentsView_20181127`.`temp_patent_firstnamed_inventor`
   ADD INDEX (`ri_inventor_id`);

ALTER TABLE `PatentsView_20181127`.`temp_patent_firstnamed_inventor`
   ADD INDEX (`rl_location_id_transformed`);

# 5,873,455 6:30

UPDATE PatentsView_20181127.`temp_patent_firstnamed_inventor`
       JOIN `PatentsView_20181127`.`temp_id_mapping_inventor` ta
          ON ta.`old_inventor_id` = ri_inventor_id
   SET inventor_id = ta.`new_inventor_id`,
       persistent_inventor_id = ta.`old_inventor_id`;

#5,865,836 5:30

UPDATE PatentsView_20181127.`temp_patent_firstnamed_inventor`
       JOIN `PatentsView_20181127`.`temp_id_mapping_location_transformed` tl
          ON tl.`old_location_id` = rl_location_id
   SET location_id = tl.`new_location_id`,
       persistent_location_id = tl.`old_location_id_transformed`;

# 5,865,832 2:50

UPDATE PatentsView_20181127.`temp_patent_firstnamed_inventor`
       JOIN `patent_20181127`.`location` l ON l.`id` = rl_location_id
   SET temp_patent_firstnamed_inventor.city = IF(l.city = '', NULL, l.city),
       temp_patent_firstnamed_inventor.`state` =
          IF(l.`state` = '', NULL, l.`state`),
       temp_patent_firstnamed_inventor.`country` =
          IF(l.`state` = '', NULL, l.`state`),
       temp_patent_firstnamed_inventor.`latitude` = l.`latitude`,
       temp_patent_firstnamed_inventor.`longitude` = l.`longitude`;

DELETE FROM temp_patent_firstnamed_inventor
      WHERE inventor_id IS NULL AND location_id IS NULL;



DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_num_foreign_documents_cited`;

CREATE TABLE `PatentsView_20181127`.`temp_num_foreign_documents_cited`
(
   `patent_id`                      VARCHAR(20) NOT NULL,
   `num_foreign_documents_cited`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`patent_id`)
)
ENGINE = INNODB;


# The number of foreign documents cited.
# 2,751,072 @ 1:52

INSERT INTO `PatentsView_20181127`.`temp_num_foreign_documents_cited`(
               `patent_id`,
               `num_foreign_documents_cited`)
     SELECT `patent_id`, count(*)
       FROM `patent_20181127`.`foreigncitation`
   GROUP BY `patent_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_num_us_applications_cited`;

CREATE TABLE `PatentsView_20181127`.`temp_num_us_applications_cited`
(
   `patent_id`                    VARCHAR(20) NOT NULL,
   `num_us_applications_cited`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`patent_id`)
)
ENGINE = INNODB;


# The number of U.S. patent applications cited.
# 1,534,484 @ 0:21

INSERT INTO `PatentsView_20181127`.`temp_num_us_applications_cited`(
               `patent_id`,
               `num_us_applications_cited`)
     SELECT `patent_id`, count(*)
       FROM `patent_20181127`.`usapplicationcitation`
   GROUP BY `patent_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_num_us_patents_cited`;

CREATE TABLE `PatentsView_20181127`.`temp_num_us_patents_cited`
(
   `patent_id`               VARCHAR(20) NOT NULL,
   `num_us_patents_cited`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`patent_id`)
)
ENGINE = INNODB;


# The number of U.S. patents cited.
# 5,231,893 @ 7:17

INSERT INTO `PatentsView_20181127`.`temp_num_us_patents_cited`(
               `patent_id`,
               `num_us_patents_cited`)
     SELECT `patent_id`, count(*)
       FROM `patent_20181127`.`uspatentcitation`
   GROUP BY `patent_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_num_times_cited_by_us_patents`;

CREATE TABLE `PatentsView_20181127`.`temp_num_times_cited_by_us_patents`
(
   `patent_id`                        VARCHAR(20) NOT NULL,
   `num_times_cited_by_us_patents`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`patent_id`)
)
ENGINE = INNODB;


# The number of times a U.S. patent was cited.
# 6,333,277 @ 7:27

INSERT INTO `PatentsView_20181127`.`temp_num_times_cited_by_us_patents`(
               `patent_id`,
               `num_times_cited_by_us_patents`)
     SELECT `citation_id`, count(*)
       FROM `patent_20181127`.`uspatentcitation`
      WHERE `citation_id` IS NOT NULL AND `citation_id` != ''
   GROUP BY `citation_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_patent_aggregations`;

CREATE TABLE `PatentsView_20181127`.`temp_patent_aggregations`
(
   `patent_id`                        VARCHAR(20) NOT NULL,
   `num_foreign_documents_cited`      INT UNSIGNED NOT NULL,
   `num_us_applications_cited`        INT UNSIGNED NOT NULL,
   `num_us_patents_cited`             INT UNSIGNED NOT NULL,
   `num_total_documents_cited`        INT UNSIGNED NOT NULL,
   `num_times_cited_by_us_patents`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`patent_id`)
)
ENGINE = INNODB;


# Combine all of our patent aggregations.
# 5,425,879 @ 2:14

INSERT INTO `PatentsView_20181127`.`temp_patent_aggregations`(
               `patent_id`,
               `num_foreign_documents_cited`,
               `num_us_applications_cited`,
               `num_us_patents_cited`,
               `num_total_documents_cited`,
               `num_times_cited_by_us_patents`)
   SELECT p.`id`,
          ifnull(t1.num_foreign_documents_cited, 0),
          ifnull(t2.num_us_applications_cited, 0),
          ifnull(t3.num_us_patents_cited, 0),
            ifnull(t1.num_foreign_documents_cited, 0)
          + ifnull(t2.num_us_applications_cited, 0)
          + ifnull(t3.num_us_patents_cited, 0),
          ifnull(t4.num_times_cited_by_us_patents, 0)
     FROM `patent_20181127`.`patent` p
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_num_foreign_documents_cited` t1
             ON t1.`patent_id` = p.`id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_num_us_applications_cited` t2
             ON t2.`patent_id` = p.`id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_num_us_patents_cited` t3
             ON t3.`patent_id` = p.`id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_num_times_cited_by_us_patents` t4
             ON t4.`patent_id` = p.`id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_patent_earliest_application_date`;

CREATE TABLE `PatentsView_20181127`.`temp_patent_earliest_application_date`
(
   `patent_id`                    VARCHAR(20) NOT NULL,
   `earliest_application_date`    DATE NOT NULL,
   PRIMARY KEY(`patent_id`)
)
ENGINE = INNODB;


# Find the earliest application date for each patent.
# 5,425,837 @ 1:35

INSERT INTO `PatentsView_20181127`.`temp_patent_earliest_application_date`(
               `patent_id`,
               `earliest_application_date`)
     SELECT a.`patent_id`, min(a.`date`)
       FROM `patent_20181127`.`application` a
      WHERE     a.`date` IS NOT NULL
            AND a.`date` > date('1899-12-31')
            AND a.`date` < date_add(current_date, INTERVAL 10 YEAR)
   GROUP BY a.`patent_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_patent_date`;

CREATE TABLE `PatentsView_20181127`.`temp_patent_date`
(
   `patent_id`    VARCHAR(20) NOT NULL,
   `date`         DATE NULL,
   PRIMARY KEY(`patent_id`)
)
ENGINE = INNODB;


# Eliminate obviously bad patent dates.
# 5,425,875 @ 0:37

INSERT INTO `PatentsView_20181127`.`temp_patent_date`(`patent_id`, `date`)
   SELECT p.`id`, p.`date`
     FROM `patent_20181127`.`patent` p
    WHERE     p.`date` IS NOT NULL
          AND p.`date` > date('1899-12-31')
          AND p.`date` < date_add(current_date, INTERVAL 10 YEAR);


DROP TABLE IF EXISTS `PatentsView_20181127`.`patent`;

CREATE TABLE `PatentsView_20181127`.`patent`
(
   `patent_id`                                                VARCHAR(20) NOT NULL,
   `type`                                                     VARCHAR(100) NULL,
   `number`                                                   VARCHAR(64) NOT NULL,
   `country`                                                  VARCHAR(20) NULL,
   `date`                                                     DATE NULL,
   `year`                                                     SMALLINT UNSIGNED NULL,
   `abstract`                                                 TEXT NULL,
   `title`                                                    TEXT NULL,
   `kind`                                                     VARCHAR(10) NULL,
   `num_claims`                                               SMALLINT UNSIGNED NULL,
   `firstnamed_assignee_id`                                   INT UNSIGNED NULL,
   `firstnamed_assignee_persistent_id`                        VARCHAR(36) NULL,
   `firstnamed_assignee_location_id`                          INT UNSIGNED NULL,
   `firstnamed_assignee_persistent_location_id`               VARCHAR(128) NULL,
   `firstnamed_assignee_city`                                 VARCHAR(128) NULL,
   `firstnamed_assignee_state`                                VARCHAR(20) NULL,
   `firstnamed_assignee_country`                              VARCHAR(10) NULL,
   `firstnamed_assignee_latitude`                             FLOAT NULL,
   `firstnamed_assignee_longitude`                            FLOAT NULL,
   `firstnamed_inventor_id`                                   INT UNSIGNED NULL,
   `firstnamed_inventor_persistent_id`                        VARCHAR(36) NULL,
   `firstnamed_inventor_location_id`                          INT UNSIGNED NULL,
   `firstnamed_inventor_persistent_location_id`               VARCHAR(128) NULL,
   `firstnamed_inventor_city`                                 VARCHAR(128) NULL,
   `firstnamed_inventor_state`                                VARCHAR(20) NULL,
   `firstnamed_inventor_country`                              VARCHAR(10) NULL,
   `firstnamed_inventor_latitude`                             FLOAT NULL,
   `firstnamed_inventor_longitude`                            FLOAT NULL,
   `num_foreign_documents_cited`                              INT UNSIGNED NOT NULL,
   `num_us_applications_cited`                                INT UNSIGNED NOT NULL,
   `num_us_patents_cited`                                     INT UNSIGNED NOT NULL,
   `num_total_documents_cited`                                INT UNSIGNED NOT NULL,
   `num_times_cited_by_us_patents`                            INT UNSIGNED NOT NULL,
   `earliest_application_date`                                DATE NULL,
   `patent_processing_days`                                   INT UNSIGNED NULL,
   `uspc_current_mainclass_average_patent_processing_days`    INT UNSIGNED
                                                                NULL,
   `cpc_current_group_average_patent_processing_days`         INT UNSIGNED
                                                                NULL,
   `term_extension`                                           INT UNSIGNED
                                                                NULL,
   PRIMARY KEY(`patent_id`)
)
ENGINE = INNODB;


# 5,425,879 @ 6:45

INSERT INTO `PatentsView_20181127`.`patent`(
               `patent_id`,
               `type`,
               `number`,
               `country`,
               `date`,
               `year`,
               `abstract`,
               `title`,
               `kind`,
               `num_claims`,
               `firstnamed_assignee_id`,
               `firstnamed_assignee_persistent_id`,
               `firstnamed_assignee_location_id`,
               `firstnamed_assignee_persistent_location_id`,
               `firstnamed_assignee_city`,
               `firstnamed_assignee_state`,
               `firstnamed_assignee_country`,
               `firstnamed_assignee_latitude`,
               `firstnamed_assignee_longitude`,
               `firstnamed_inventor_id`,
               `firstnamed_inventor_persistent_id`,
               `firstnamed_inventor_location_id`,
               `firstnamed_inventor_persistent_location_id`,
               `firstnamed_inventor_city`,
               `firstnamed_inventor_state`,
               `firstnamed_inventor_country`,
               `firstnamed_inventor_latitude`,
               `firstnamed_inventor_longitude`,
               `num_foreign_documents_cited`,
               `num_us_applications_cited`,
               `num_us_patents_cited`,
               `num_total_documents_cited`,
               `num_times_cited_by_us_patents`,
               `earliest_application_date`,
               `patent_processing_days`,
               `term_extension`)
   SELECT p.`id`,
          CASE
             WHEN ifnull(p.`type`, '') = 'sir'
             THEN
                'statutory invention registration'
             ELSE
                nullif(trim(p.`type`), '')
          END,
          `number`,
          nullif(trim(p.`country`), ''),
          tpd.`date`,
          year(tpd.`date`),
          nullif(trim(p.`abstract`), ''),
          nullif(trim(p.`title`), ''),
          nullif(trim(p.`kind`), ''),
          p.`num_claims`,
          tpfna.`assignee_id`,
          tpfna.`persistent_assignee_id`,
          tpfna.`location_id`,
          tpfna.`persistent_location_id`,
          tpfna.`city`,
          tpfna.`state`,
          tpfna.`country`,
          tpfna.`latitude`,
          tpfna.`longitude`,
          tpfni.`inventor_id`,
          tpfni.`persistent_inventor_id`,
          tpfni.`location_id`,
          tpfni.`persistent_location_id`,
          tpfni.`city`,
          tpfni.`state`,
          tpfni.`country`,
          tpfni.`latitude`,
          tpfni.`longitude`,
          tpa.`num_foreign_documents_cited`,
          tpa.`num_us_applications_cited`,
          tpa.`num_us_patents_cited`,
          tpa.`num_total_documents_cited`,
          tpa.`num_times_cited_by_us_patents`,
          tpead.`earliest_application_date`,
          CASE
             WHEN tpead.`earliest_application_date` <= p.`date`
             THEN
                timestampdiff(day,
                              tpead.`earliest_application_date`,
                              tpd.`date`)
             ELSE
                NULL
          END,
          ustog.`term_extension`
     FROM `patent_20181127`.`patent` p
          LEFT OUTER JOIN `PatentsView_20181127`.`temp_patent_date` tpd
             ON tpd.`patent_id` = p.`id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_patent_firstnamed_assignee` tpfna
             ON tpfna.`patent_id` = p.`id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_patent_firstnamed_inventor` tpfni
             ON tpfni.`patent_id` = p.`id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_patent_aggregations` tpa
             ON tpa.`patent_id` = p.`id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_patent_earliest_application_date`
          tpead
             ON tpead.`patent_id` = p.`id`
          LEFT OUTER JOIN `patent_20181127`.`us_term_of_grant` ustog
             ON ustog.`patent_id` = p.`id`;

# END patent

################################################################################################################################################
