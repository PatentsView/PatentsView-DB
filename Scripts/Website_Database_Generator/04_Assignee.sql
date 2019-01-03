# BEGIN assignee

##############################################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_assignee_lastknown_location`;

CREATE TABLE `PatentsView_20181127`.`temp_assignee_lastknown_location`
(
   `assignee_id`               VARCHAR(36) NOT NULL,
   `location_id`               INT UNSIGNED NULL,
   `persistent_location_id`    VARCHAR(128) NULL,
   `city`                      VARCHAR(128) NULL,
   `state`                     VARCHAR(20) NULL,
   `country`                   VARCHAR(10) NULL,
   `latitude`                  FLOAT NULL,
   `longitude`                 FLOAT NULL,
   PRIMARY KEY(`assignee_id`)
)
ENGINE = INNODB;


# Populate temp_assignee_lastknown_location table.  The goal here is to grab the location associated
# with the most recent patent associated with the assignee.  It is possible for a patent/assignee
# combination not to have a location, so we will grab the most recent KNOWN location.
# 320,156 @ 3:51

INSERT INTO `PatentsView_20181127`.`temp_assignee_lastknown_location`(
               `assignee_id`,
               `location_id`,
               `persistent_location_id`,
               `city`,
               `state`,
               `country`,
               `latitude`,
               `longitude`)
   SELECT t.`assignee_id`,
          tl.`new_location_id`,
          tl.`old_location_id_transformed`,
          nullif(trim(l.`city`), ''),
          nullif(trim(l.`state`), ''),
          nullif(trim(l.`country`), ''),
          l.`latitude`,
          l.`longitude`
     FROM (SELECT t.`assignee_id`, t.`location_id`
             FROM (SELECT @rownum :=
                             CASE
                                WHEN @assignee_id = t.`assignee_id`
                                THEN
                                   @rownum + 1
                                ELSE
                                   1
                             END `rownum`,
                          @assignee_id := t.`assignee_id` `assignee_id`,
                          t.`location_id`
                     FROM (  SELECT ra.`assignee_id`, rl.`location_id`
                               FROM `patent_20181127`.`rawassignee` ra
                                    INNER JOIN `patent_20181127`.`patent` p
                                       ON p.`id` = ra.`patent_id`
                                    INNER JOIN
                                    `patent_20181127`.`rawlocation` rl
                                       ON rl.`id` = ra.`rawlocation_id`
                              WHERE     rl.`location_id` IS NOT NULL
                                    AND ra.`assignee_id` IS NOT NULL
                           ORDER BY ra.`assignee_id`,
                                    p.`date` DESC,
                                    p.`id` DESC) t,
                          (SELECT @rownum := 0, @assignee_id := '') r) t
            WHERE t.`rownum` < 2) t
          LEFT OUTER JOIN `patent_20181127`.`location` l
             ON l.`id` = t.`location_id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_id_mapping_location_transformed` tl
             ON tl.`old_location_id` = t.`location_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_assignee_num_patents`;

CREATE TABLE `PatentsView_20181127`.`temp_assignee_num_patents`
(
   `assignee_id`    VARCHAR(36) NOT NULL,
   `num_patents`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`assignee_id`)
)
ENGINE = INNODB;


#

INSERT INTO `PatentsView_20181127`.`temp_assignee_num_patents`(`assignee_id`,
                                                               `num_patents`)
     SELECT `assignee_id`, count(DISTINCT `patent_id`)
       FROM `patent_20181127`.`patent_assignee`
   GROUP BY `assignee_id`;

DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_assignee_num_inventors`;

CREATE TABLE `PatentsView_20181127`.`temp_assignee_num_inventors`
(
   `assignee_id`      VARCHAR(36) NOT NULL,
   `num_inventors`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`assignee_id`)
)
ENGINE = INNODB;

# 0:15

INSERT INTO `PatentsView_20181127`.`temp_assignee_num_inventors`(
               `assignee_id`,
               `num_inventors`)
     SELECT aa.`assignee_id`, count(DISTINCT ii.`inventor_id`)
       FROM `patent_20181127`.`patent_assignee` aa
            JOIN `patent_20181127`.`patent_inventor` ii
               ON ii.patent_id = aa.patent_id
   GROUP BY aa.`assignee_id`;

DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_assignee_years_active`;

CREATE TABLE `PatentsView_20181127`.`temp_assignee_years_active`
(
   `assignee_id`            VARCHAR(36) NOT NULL,
   `first_seen_date`        DATE NULL,
   `last_seen_date`         DATE NULL,
   `actual_years_active`    SMALLINT UNSIGNED NOT NULL,
   PRIMARY KEY(`assignee_id`)
)
ENGINE = INNODB;


# Years active is essentially the number of years difference between first associated patent and last.
# 1:15

INSERT INTO `PatentsView_20181127`.`temp_assignee_years_active`(
               `assignee_id`,
               `first_seen_date`,
               `last_seen_date`,
               `actual_years_active`)
     SELECT pa.`assignee_id`,
            min(p.`date`),
            max(p.`date`),
            ifnull(
               round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365),
               0)
       FROM `patent_20181127`.`patent_assignee` pa
            INNER JOIN `PatentsView_20181127`.`patent` p
               ON p.`patent_id` = pa.`patent_id`
      WHERE p.`date` IS NOT NULL
   GROUP BY pa.`assignee_id`;

DROP TABLE IF EXISTS `PatentsView_20181127`.`patent_assignee`;

CREATE TABLE `PatentsView_20181127`.`patent_assignee`
(
   `patent_id`      VARCHAR(20) NOT NULL,
   `assignee_id`    INT UNSIGNED NOT NULL,
   `location_id`    INT UNSIGNED NULL,
   `sequence`       SMALLINT UNSIGNED NOT NULL,
   PRIMARY KEY(`patent_id`, `assignee_id`),
   UNIQUE INDEX ak_patent_assignee(`assignee_id`, `patent_id`)
)
ENGINE = INNODB;


# 4,825,748 @ 7:20


insert into `PatentsView_20181127`.`patent_assignee`
(
  `patent_id`, `assignee_id`, `location_id`, `sequence`
)
select distinct
  pa.`patent_id`, t.`new_assignee_id`, tl.`new_location_id`, ra.`sequence`
from
  `patent_20181127`.`patent_assignee` pa
  inner join `PatentsView_20181127`.`temp_id_mapping_assignee` t on t.`old_assignee_id` = pa.`assignee_id`
  left join (select patent_id, assignee_id, min(sequence) sequence from `patent_20181127`.`rawassignee` group by patent_id, assignee_id) t 

on t.`patent_id` = pa.`patent_id` and t.`assignee_id` = pa.`assignee_id`
  left join `patent_20181127`.`rawassignee` ra on ra.`patent_id` = t.`patent_id` and ra.`assignee_id` = t.`assignee_id` and ra.`sequence` 

= t.`sequence`
  left join `patent_20181127`.`rawlocation` rl on rl.`id` = ra.`rawlocation_id`
  left join `PatentsView_20181127`.`temp_id_mapping_location` tl on tl.`old_location_id` = rl.`location_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`location_assignee`;

CREATE TABLE `PatentsView_20181127`.`location_assignee`
(
   `location_id`    INT UNSIGNED NOT NULL,
   `assignee_id`    INT UNSIGNED NOT NULL,
   `num_patents`    INT UNSIGNED,
   PRIMARY KEY(`location_id`, `assignee_id`)
)
ENGINE = INNODB;


# 438,452 @ 0:07

INSERT INTO `PatentsView_20181127`.`location_assignee`(`location_id`,
                                                       `assignee_id`,
                                                       `num_patents`)
   SELECT DISTINCT timl.`new_location_id`, tima.`new_assignee_id`, NULL
     FROM `patent_20181127`.`location_assignee` la
          INNER JOIN
          `PatentsView_20181127`.`temp_id_mapping_location_transformed` timl
             ON timl.`old_location_id_transformed` = la.`location_id`
          INNER JOIN `PatentsView_20181127`.`temp_id_mapping_assignee` tima
             ON tima.`old_assignee_id` = la.`assignee_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`assignee`;

CREATE TABLE `PatentsView_20181127`.`assignee`
(
   `assignee_id`                         INT UNSIGNED NOT NULL,
   `type`                                VARCHAR(10) NULL,
   `name_first`                          VARCHAR(64) NULL,
   `name_last`                           VARCHAR(64) NULL,
   `organization`                        VARCHAR(256) NULL,
   `num_patents`                         INT UNSIGNED NOT NULL,
   `num_inventors`                       INT UNSIGNED NOT NULL,
   `lastknown_location_id`               INT UNSIGNED NULL,
   `lastknown_persistent_location_id`    VARCHAR(128) NULL,
   `lastknown_city`                      VARCHAR(128) NULL,
   `lastknown_state`                     VARCHAR(20) NULL,
   `lastknown_country`                   VARCHAR(10) NULL,
   `lastknown_latitude`                  FLOAT NULL,
   `lastknown_longitude`                 FLOAT NULL,
   `first_seen_date`                     DATE NULL,
   `last_seen_date`                      DATE NULL,
   `years_active`                        SMALLINT UNSIGNED NOT NULL,
   `persistent_assignee_id`              VARCHAR(36) NOT NULL,
   PRIMARY KEY(`assignee_id`)
)
ENGINE = INNODB;


# 345,185 @ 0:15

INSERT INTO `PatentsView_20181127`.`assignee`(
               `assignee_id`,
               `type`,
               `name_first`,
               `name_last`,
               `organization`,
               `num_patents`,
               `num_inventors`,
               `lastknown_location_id`,
               `lastknown_persistent_location_id`,
               `lastknown_city`,
               `lastknown_state`,
               `lastknown_country`,
               `lastknown_latitude`,
               `lastknown_longitude`,
               `first_seen_date`,
               `last_seen_date`,
               `years_active`,
               `persistent_assignee_id`)
   SELECT t.`new_assignee_id`,
          trim(LEADING '0' FROM nullif(trim(a.`type`), '')),
          nullif(trim(a.`name_first`), ''),
          nullif(trim(a.`name_last`), ''),
          nullif(trim(a.`organization`), ''),
          tanp.`num_patents`,
          ifnull(tani.`num_inventors`, 0),
          talkl.`location_id`,
          talkl.`persistent_location_id`,
          talkl.`city`,
          talkl.`state`,
          talkl.`country`,
          talkl.`latitude`,
          talkl.`longitude`,
          tafls.`first_seen_date`,
          tafls.`last_seen_date`,
          ifnull(
             CASE
                WHEN tafls.`actual_years_active` < 1 THEN 1
                ELSE tafls.`actual_years_active`
             END,
             0),
          a.`id`
     FROM `patent_20181127`.`assignee` a
          INNER JOIN `PatentsView_20181127`.`temp_id_mapping_assignee` t
             ON t.`old_assignee_id` = a.`id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_assignee_lastknown_location` talkl
             ON talkl.`assignee_id` = a.`id`
          INNER JOIN `PatentsView_20181127`.`temp_assignee_num_patents` tanp
             ON tanp.`assignee_id` = a.`id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_assignee_years_active` tafls
             ON tafls.`assignee_id` = a.`id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_assignee_num_inventors` tani
             ON tani.`assignee_id` = a.`id`;


# END assignee

################################################################################################################################################
