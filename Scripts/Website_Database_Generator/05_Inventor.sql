# BEGIN inventor

##############################################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_inventor_lastknown_location`;

CREATE TABLE `PatentsView_20181127`.`temp_inventor_lastknown_location`
(
   `inventor_id`               VARCHAR(36) NOT NULL,
   `location_id`               INT UNSIGNED NULL,
   `persistent_location_id`    VARCHAR(128) NULL,
   `city`                      VARCHAR(128) NULL,
   `state`                     VARCHAR(20) NULL,
   `country`                   VARCHAR(10) NULL,
   `latitude`                  FLOAT NULL,
   `longitude`                 FLOAT NULL,
   PRIMARY KEY(`inventor_id`)
)
ENGINE = INNODB;


# Populate temp_inventor_lastknown_location table.  The goal here is to grab the location associated
# with the most recent patent associated with the inventor.  It is possible for a patent/inventor
# combination not to have a location, so we will grab the most recent KNOWN location.
# 3,437,668 @ 22:05


insert into `PatentsView_20181127`.`temp_inventor_lastknown_location`
(
  `inventor_id`, `location_id`, `persistent_location_id`, `city`, `state`, `country`, `latitude`, `longitude`
)
select
  t.`inventor_id`,
  tl.`new_location_id`,
  tl.`old_location_id_transformed`,
  nullif(trim(l.`city`), ''),
  nullif(trim(l.`state`), ''),
  nullif(trim(l.`country`), ''),
  l.`latitude`,
  l.`longitude`
from
  (
    select
      t.`inventor_id`,
      t.`location_id`
    from
      (
        select
          @rownum := case when @inventor_id = t.`inventor_id` then @rownum + 1 else 1 end `rownum`,
          @inventor_id := t.`inventor_id` `inventor_id`,
          t.`location_id`
        from
          (
            select
              ri.`inventor_id`,
              rl.`location_id`
            from
              `patent_20181127`.`rawinventor` ri
              inner join `patent_20181127`.`patent` p on p.`id` = ri.`patent_id`
              inner join `patent_20181127`.`rawlocation` rl on rl.`id` = ri.`rawlocation_id`
            where
              ri.`inventor_id` is not null and
              rl.`location_id` is not null
            order by
              ri.`inventor_id`,
              p.`date` desc,
              p.`id` desc
          ) t,
          (select @rownum := 0, @inventor_id := '') r
      ) t
    where
      t.`rownum` < 2
  ) t
  left outer join `patent_20181127`.`location` l on l.`id` = t.`location_id`
  left outer join `PatentsView_20181127`.`temp_id_mapping_location_transformed` tl on tl.`old_location_id` = 

t.`location_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_inventor_num_patents`;

CREATE TABLE `PatentsView_20181127`.`temp_inventor_num_patents`
(
   `inventor_id`    VARCHAR(36) NOT NULL,
   `num_patents`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`inventor_id`)
)
ENGINE = INNODB;


# 2:06

INSERT INTO `PatentsView_20181127`.`temp_inventor_num_patents`(`inventor_id`,
                                                               `num_patents`)
     SELECT `inventor_id`, count(DISTINCT `patent_id`)
       FROM `patent_20181127`.`patent_inventor`
   GROUP BY `inventor_id`;

DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_inventor_num_assignees`;

CREATE TABLE `PatentsView_20181127`.`temp_inventor_num_assignees`
(
   `inventor_id`      VARCHAR(36) NOT NULL,
   `num_assignees`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`inventor_id`)
)
ENGINE = INNODB;


# 0:15

INSERT INTO `PatentsView_20181127`.`temp_inventor_num_assignees`(
               `inventor_id`,
               `num_assignees`)
     SELECT ii.`inventor_id`, count(DISTINCT aa.`assignee_id`)
       FROM `patent_20181127`.`patent_inventor` ii
            JOIN `patent_20181127`.`patent_assignee` aa
               ON aa.`patent_id` = ii.`patent_id`
   GROUP BY ii.`inventor_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_inventor_years_active`;

CREATE TABLE `PatentsView_20181127`.`temp_inventor_years_active`
(
   `inventor_id`            VARCHAR(36) NOT NULL,
   `first_seen_date`        DATE NULL,
   `last_seen_date`         DATE NULL,
   `actual_years_active`    SMALLINT UNSIGNED NOT NULL,
   PRIMARY KEY(`inventor_id`)
)
ENGINE = INNODB;


# 5:42

INSERT INTO `PatentsView_20181127`.`temp_inventor_years_active`(
               `inventor_id`,
               `first_seen_date`,
               `last_seen_date`,
               `actual_years_active`)
     SELECT pa.`inventor_id`,
            min(p.`date`),
            max(p.`date`),
            ifnull(
               round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365),
               0)
       FROM `patent_20181127`.`patent_inventor` pa
            INNER JOIN `PatentsView_20181127`.`patent` p
               ON p.`patent_id` = pa.`patent_id`
      WHERE p.`date` IS NOT NULL
   GROUP BY pa.`inventor_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`patent_inventor`;

CREATE TABLE `PatentsView_20181127`.`patent_inventor`
(
   `patent_id`      VARCHAR(20) NOT NULL,
   `inventor_id`    INT UNSIGNED NOT NULL,
   `location_id`    INT UNSIGNED NULL,
   `sequence`       SMALLINT UNSIGNED NOT NULL,
   PRIMARY KEY(`patent_id`, `inventor_id`),
   UNIQUE INDEX ak_patent_inventor(`inventor_id`, `patent_id`)
)
ENGINE = INNODB;
ALTER TABLE `patent_20181127`.`rawinventor` ADD INDEX  join_idx (`patent_id`,`inventor_id`,`sequence`);

# 12,389,559 @ 29:50


insert into `PatentsView_20181127`.`patent_inventor`
(
  `patent_id`, `inventor_id`, `location_id`, `sequence`
)
SELECT DISTINCT pii.`patent_id`,
                t1.`new_inventor_id`,
                tl.`new_location_id`,
                ri.`sequence`
  FROM `patent_20181127`.`patent_inventor` pii
       INNER JOIN `PatentsView_20181127`.`temp_id_mapping_inventor` t1
          ON t1.`old_inventor_id` = pii.`inventor_id`
       LEFT OUTER JOIN (  SELECT patent_id, inventor_id, min(sequence) sequence
                            FROM `patent_20181127`.`rawinventor`
                        GROUP BY patent_id, inventor_id) t
          ON     t.`patent_id` = pii.`patent_id`
             AND t.`inventor_id` = pii.`inventor_id`
       LEFT OUTER JOIN `patent_20181127`.`rawinventor` ri
          ON     ri.`patent_id` = t.`patent_id`
             AND ri.`inventor_id` = t.`inventor_id`
             AND ri.`sequence` = t.`sequence`
       LEFT OUTER JOIN `patent_20181127`.`rawlocation` rl
          ON rl.`id` = ri.`rawlocation_id`
       LEFT OUTER JOIN `PatentsView_20181127`.`temp_id_mapping_location` tl
          ON tl.`old_location_id` = rl.`location_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`location_inventor`;

CREATE TABLE `PatentsView_20181127`.`location_inventor`
(
   `location_id`    INT UNSIGNED NOT NULL,
   `inventor_id`    INT UNSIGNED NOT NULL,
   `num_patents`    INT UNSIGNED,
   PRIMARY KEY(`location_id`, `inventor_id`)
)
ENGINE = INNODB;


# 4,188,507 @ 0:50

INSERT INTO `PatentsView_20181127`.`location_inventor`(`location_id`,
                                                       `inventor_id`,
                                                       `num_patents`)
   SELECT DISTINCT timl.`new_location_id`, timi.`new_inventor_id`, NULL
     FROM `patent_20181127`.`location_inventor` la
          INNER JOIN `PatentsView_20181127`.`temp_id_mapping_location` timl
             ON timl.`old_location_id_transformed` = la.`location_id`
          INNER JOIN `PatentsView_20181127`.`temp_id_mapping_inventor` timi
             ON timi.`old_inventor_id` = la.`inventor_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`inventor`;

CREATE TABLE `PatentsView_20181127`.`inventor`
(
   `inventor_id`                         INT UNSIGNED NOT NULL,
   `name_first`                          VARCHAR(64) NULL,
   `name_last`                           VARCHAR(64) NULL,
   `num_patents`                         INT UNSIGNED NOT NULL,
   `num_assignees`                       INT UNSIGNED NOT NULL,
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
   `persistent_inventor_id`              VARCHAR(36) NOT NULL,
   PRIMARY KEY(`inventor_id`)
)
ENGINE = INNODB;


# 3,572,763 @ 1:57

INSERT INTO `PatentsView_20181127`.`inventor`(
               `inventor_id`,
               `name_first`,
               `name_last`,
               `num_patents`,
               `num_assignees`,
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
               `persistent_inventor_id`)
   SELECT t.`new_inventor_id`,
          nullif(trim(i.`name_first`), ''),
          nullif(trim(i.`name_last`), ''),
          tinp.`num_patents`,
          ifnull(tina.`num_assignees`, 0),
          tilkl.`location_id`,
          tilkl.`persistent_location_id`,
          tilkl.`city`,
          tilkl.`state`,
          tilkl.`country`,
          tilkl.`latitude`,
          tilkl.`longitude`,
          tifls.`first_seen_date`,
          tifls.`last_seen_date`,
          ifnull(
             CASE
                WHEN tifls.`actual_years_active` < 1 THEN 1
                ELSE tifls.`actual_years_active`
             END,
             0),
          i.`id`
     FROM `patent_20181127`.`inventor` i
          INNER JOIN `PatentsView_20181127`.`temp_id_mapping_inventor` t
             ON t.`old_inventor_id` = i.`id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_inventor_lastknown_location` tilkl
             ON tilkl.`inventor_id` = i.`id`
          INNER JOIN `PatentsView_20181127`.`temp_inventor_num_patents` tinp
             ON tinp.`inventor_id` = i.`id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_inventor_years_active` tifls
             ON tifls.`inventor_id` = i.`id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_inventor_num_assignees` tina
             ON tina.`inventor_id` = i.`id`;


# END inventor

################################################################################################################################################
