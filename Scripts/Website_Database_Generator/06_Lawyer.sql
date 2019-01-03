# BEGIN lawyer

##############################################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_lawyer_num_patents`;

CREATE TABLE `PatentsView_20181127`.`temp_lawyer_num_patents`
(
   `lawyer_id`      VARCHAR(36) NOT NULL,
   `num_patents`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`lawyer_id`)
)
ENGINE = INNODB;


# 2:06

INSERT INTO `PatentsView_20181127`.`temp_lawyer_num_patents`(`lawyer_id`,
                                                             `num_patents`)
     SELECT `lawyer_id`, count(DISTINCT `patent_id`)
       FROM `patent_20181127`.`patent_lawyer`  where lawyer_id != ''
   GROUP BY `lawyer_id`;

DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_lawyer_num_assignees`;

CREATE TABLE `PatentsView_20181127`.`temp_lawyer_num_assignees`
(
   `lawyer_id`        VARCHAR(36) NOT NULL,
   `num_assignees`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`lawyer_id`)
)
ENGINE = INNODB;


# 0:15

INSERT INTO `PatentsView_20181127`.`temp_lawyer_num_assignees`(
               `lawyer_id`,
               `num_assignees`)
     SELECT ii.`lawyer_id`, count(DISTINCT aa.`assignee_id`)
       FROM `patent_20181127`.`patent_lawyer` ii
            JOIN `patent_20181127`.`patent_assignee` aa
               ON aa.`patent_id` = ii.`patent_id` where  ii.`lawyer_id` != ''
   GROUP BY ii.`lawyer_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_lawyer_num_inventors`;

CREATE TABLE `PatentsView_20181127`.`temp_lawyer_num_inventors`
(
   `lawyer_id`        VARCHAR(36) NOT NULL,
   `num_inventors`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`lawyer_id`)
)
ENGINE = INNODB;

# 0:15

INSERT INTO `PatentsView_20181127`.`temp_lawyer_num_inventors`(
               `lawyer_id`,
               `num_inventors`)
     SELECT aa.`lawyer_id`, count(DISTINCT ii.`inventor_id`)
       FROM `patent_20181127`.`patent_lawyer` aa
            JOIN `patent_20181127`.`patent_inventor` ii
               ON ii.patent_id = aa.patent_id where aa.`lawyer_id` != ''
   GROUP BY aa.`lawyer_id`;



DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_lawyer_years_active`;

CREATE TABLE `PatentsView_20181127`.`temp_lawyer_years_active`
(
   `lawyer_id`              VARCHAR(36) NOT NULL,
   `first_seen_date`        DATE NULL,
   `last_seen_date`         DATE NULL,
   `actual_years_active`    SMALLINT UNSIGNED NOT NULL,
   PRIMARY KEY(`lawyer_id`)
)
ENGINE = INNODB;


# 5:42

INSERT INTO `PatentsView_20181127`.`temp_lawyer_years_active`(
               `lawyer_id`,
               `first_seen_date`,
               `last_seen_date`,
               `actual_years_active`)
     SELECT pa.`lawyer_id`,
            min(p.`date`),
            max(p.`date`),
            ifnull(
               round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365),
               0)
       FROM `patent_20181127`.`patent_lawyer` pa
            INNER JOIN `PatentsView_20181127`.`patent` p
               ON p.`patent_id` = pa.`patent_id`
      WHERE p.`date` IS NOT NULL and pa.`lawyer_id` != ''
   GROUP BY pa.`lawyer_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`patent_lawyer`;

CREATE TABLE `PatentsView_20181127`.`patent_lawyer`
(
   `patent_id`    VARCHAR(20) NOT NULL,
   `lawyer_id`    INT UNSIGNED NOT NULL,
   `sequence`     SMALLINT UNSIGNED NOT NULL,
   PRIMARY KEY(`patent_id`, `lawyer_id`),
   UNIQUE INDEX ak_patent_lawyer(`lawyer_id`, `patent_id`)
)
ENGINE = INNODB;


# 12,389,559 @ 29:50

INSERT INTO `PatentsView_20181127`.`patent_lawyer`(`patent_id`,
                                                   `lawyer_id`,
                                                   `sequence`)
   SELECT DISTINCT pii.`patent_id`, tmpl.`new_lawyer_id`, ri.`sequence`
     FROM `patent_20181127`.`patent_lawyer` pii
          INNER JOIN `PatentsView_20181127`.`temp_id_mapping_lawyer` tmpl
             ON tmpl.`old_lawyer_id` = pii.`lawyer_id`
          LEFT OUTER JOIN
          (  SELECT patent_id, lawyer_id, min(sequence) sequence
               FROM `patent_20181127`.`rawlawyer`
           GROUP BY patent_id, lawyer_id) t
             ON     t.`patent_id` = pii.`patent_id`
                AND t.`lawyer_id` = pii.`lawyer_id`
          LEFT OUTER JOIN `patent_20181127`.`rawlawyer` ri
             ON     ri.`patent_id` = t.`patent_id`
                AND ri.`lawyer_id` = t.`lawyer_id`
                AND ri.`sequence` = t.`sequence`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`lawyer`;

CREATE TABLE `PatentsView_20181127`.`lawyer`
(
   `lawyer_id`               INT UNSIGNED NOT NULL,
   `name_first`              VARCHAR(64) NULL,
   `name_last`               VARCHAR(64) NULL,
   `organization`            VARCHAR(256) NULL,
   `num_patents`             INT UNSIGNED NOT NULL,
   `num_assignees`           INT UNSIGNED NOT NULL,
   `num_inventors`           INT UNSIGNED NOT NULL,
   `first_seen_date`         DATE NULL,
   `last_seen_date`          DATE NULL,
   `years_active`            SMALLINT UNSIGNED NOT NULL,
   `persistent_lawyer_id`    VARCHAR(36) NOT NULL,
   PRIMARY KEY(`lawyer_id`)
)
ENGINE = INNODB;


# 3,572,763 @ 1:57

INSERT INTO `PatentsView_20181127`.`lawyer`(`lawyer_id`,
                                            `name_first`,
                                            `name_last`,
                                            `organization`,
                                            `num_patents`,
                                            `num_assignees`,
                                            `num_inventors`,
                                            `first_seen_date`,
                                            `last_seen_date`,
                                            `years_active`,
                                            `persistent_lawyer_id`)
   SELECT t.`new_lawyer_id`,
          nullif(trim(i.`name_first`), ''),
          nullif(trim(i.`name_last`), ''),
          nullif(trim(i.`organization`), ''),
          tinp.`num_patents`,
          ifnull(tina.`num_assignees`, 0),
          ifnull(tini.`num_inventors`, 0),
          tifls.`first_seen_date`,
          tifls.`last_seen_date`,
          ifnull(
             CASE
                WHEN tifls.`actual_years_active` < 1 THEN 1
                ELSE tifls.`actual_years_active`
             END,
             0),
          i.`id`
     FROM `patent_20181127`.`lawyer` i
          INNER JOIN `PatentsView_20181127`.`temp_id_mapping_lawyer` t
             ON t.`old_lawyer_id` = i.`id`
          INNER JOIN `PatentsView_20181127`.`temp_lawyer_num_patents` tinp
             ON tinp.`lawyer_id` = i.`id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_lawyer_years_active` tifls
             ON tifls.`lawyer_id` = i.`id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_lawyer_num_assignees` tina
             ON tina.`lawyer_id` = i.`id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_lawyer_num_inventors` tini
             ON tini.`lawyer_id` = i.`id`;


# END lawyer

################################################################################################################################################


# BEGIN examiner

##############################################################################################################################################

DROP TABLE IF EXISTS `PatentsView_20181127`.`examiner`;

CREATE TABLE `PatentsView_20181127`.`examiner`
(
   `examiner_id`               INT UNSIGNED NOT NULL,
   `name_first`                VARCHAR(64) NULL,
   `name_last`                 VARCHAR(64) NULL,
   `role`                      VARCHAR(20) NULL,
   `group`                     VARCHAR(20) NULL,
   `persistent_examiner_id`    VARCHAR(36) NOT NULL,
   PRIMARY KEY(`examiner_id`)
)
ENGINE = INNODB;


# 3,572,763 @ 1:57

INSERT INTO `PatentsView_20181127`.`examiner`(`examiner_id`,
                                              `name_first`,
                                              `name_last`,
                                              `role`,
                                              `group`,
                                              `persistent_examiner_id`)
   SELECT t.`new_examiner_id`,
          nullif(trim(i.`name_first`), ''),
          nullif(trim(i.`name_last`), ''),
          nullif(trim(i.`role`), ''),
          nullif(trim(i.`group`), ''),
          i.`uuid`
     FROM `patent_20181127`.`rawexaminer` i
          INNER JOIN `PatentsView_20181127`.`temp_id_mapping_examiner` t
             ON t.`old_examiner_id` = i.`uuid`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`patent_examiner`;

CREATE TABLE `PatentsView_20181127`.`patent_examiner`
(
   `patent_id`      VARCHAR(20) NOT NULL,
   `examiner_id`    INT UNSIGNED NOT NULL,
   `role`           VARCHAR(20) NOT NULL,
   PRIMARY KEY(`patent_id`, `examiner_id`),
   UNIQUE INDEX ak_patent_examiner(`examiner_id`, `patent_id`)
)
ENGINE = INNODB;


# 12,389,559 @ 29:50

INSERT INTO `PatentsView_20181127`.`patent_examiner`(`patent_id`,
                                                     `examiner_id`,
                                                     `role`)
   SELECT DISTINCT ri.`patent_id`, t.`new_examiner_id`, ri.`role`
     FROM `patent_20181127`.`rawexaminer` ri
          INNER JOIN `PatentsView_20181127`.`temp_id_mapping_examiner` t
             ON t.`old_examiner_id` = ri.`uuid`;

# END examiner

################################################################################################################################################


# BEGIN foreignpriority

#################################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`foreignpriority`;

CREATE TABLE `PatentsView_20181127`.`foreignpriority`
(
   `patent_id`             VARCHAR(20) NOT NULL,
   `sequence`              INT NOT NULL,
   `foreign_doc_number`    VARCHAR(20) NULL,
   `date`                  DATE NULL,
   `country`               VARCHAR(64) NULL,
   `kind`                  VARCHAR(10) NULL,
   PRIMARY KEY(`patent_id`, `sequence`)
)
ENGINE = INNODB;


# 13,617,656 @ 8:22

INSERT INTO `PatentsView_20181127`.`foreignpriority`(`patent_id`,
                                                     `sequence`,
                                                     `foreign_doc_number`,
                                                     `date`,
                                                     `country`,
                                                     `kind`)
   SELECT ac.`patent_id`,
          ac.`sequence`,
          ac.`number`,
          CASE
             WHEN     ac.`date` > date('1899-12-31')
                  AND ac.`date` < date_add(current_date, INTERVAL 10 YEAR)
             THEN
                ac.`date`
             ELSE
                NULL
          END,
          nullif(trim(ac.`country_transformed`), ''),
          nullif(trim(ac.`kind`), '')
     FROM `PatentsView_20181127`.`patent` p
          INNER JOIN `patent_20181127`.`foreign_priority` ac
             ON ac.`patent_id` = p.`patent_id`;


# END foreignpriority

###################################################################################################################################


# BEGIN pctdata

#################################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`pctdata`;

CREATE TABLE `PatentsView_20181127`.`pctdata`
(
   `patent_id`     VARCHAR(20) NOT NULL,
   `doc_type`      VARCHAR(20) NOT NULL,
   `kind`          VARCHAR(2) NOT NULL,
   `doc_number`    VARCHAR(20) NULL,
   `date`          DATE NULL,
   `102_date`      DATE NULL,
   `371_date`      DATE NULL,
   PRIMARY KEY(`patent_id`, `kind`)
)
ENGINE = INNODB;


# 13,617,656 @ 8:22

INSERT INTO `PatentsView_20181127`.`pctdata`(`patent_id`,
                                             `doc_type`,
                                             `kind`,
                                             `doc_number`,
                                             `date`,
                                             `102_date`,
                                             `371_date`)
   SELECT ac.`patent_id`,
          ac.`doc_type`,
          ac.`kind`,
          ac.`rel_id`,
          CASE
             WHEN     ac.`date` > date('1899-12-31')
                  AND ac.`date` < date_add(current_date, INTERVAL 10 YEAR)
             THEN
                ac.`date`
             ELSE
                NULL
          END,
          CASE
             WHEN     ac.`102_date` > date('1899-12-31')
                  AND ac.`102_date` <
                      date_add(current_date, INTERVAL 10 YEAR)
             THEN
                ac.`102_date`
             ELSE
                NULL
          END,
          CASE
             WHEN     ac.`371_date` > date('1899-12-31')
                  AND ac.`371_date` <
                      date_add(current_date, INTERVAL 10 YEAR)
             THEN
                ac.`371_date`
             ELSE
                NULL
          END
     FROM `PatentsView_20181127`.`patent` p
          INNER JOIN `patent_20181127`.`pct_data` ac
             ON ac.`patent_id` = p.`patent_id`;


# END pctdata

###################################################################################################################################
