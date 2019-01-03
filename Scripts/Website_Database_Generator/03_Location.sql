# BEGIN location

##############################################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_location_num_assignees`;

CREATE TABLE `PatentsView_20181127`.`temp_location_num_assignees`
(
   `location_id`      INT UNSIGNED NOT NULL,
   `num_assignees`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`location_id`)
)
ENGINE = INNODB;


# 34,018 @ 0:02

INSERT INTO `PatentsView_20181127`.`temp_location_num_assignees`(
               `location_id`,
               `num_assignees`)
     SELECT timl.`new_location_id`, count(DISTINCT la.`assignee_id`)
       FROM `PatentsView_20181127`.`temp_id_mapping_location_transformed` timl
            INNER JOIN `patent_20181127`.`location_assignee` la
               ON la.`location_id` = timl.`old_location_id_transformed`
   GROUP BY timl.`new_location_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_location_num_inventors`;

CREATE TABLE `PatentsView_20181127`.`temp_location_num_inventors`
(
   `location_id`      INT UNSIGNED NOT NULL,
   `num_inventors`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`location_id`)
)
ENGINE = INNODB;


# 94,350 @ 0:50

INSERT INTO `PatentsView_20181127`.`temp_location_num_inventors`(
               `location_id`,
               `num_inventors`)
     SELECT timl.`new_location_id`, count(DISTINCT li.`inventor_id`)
       FROM `PatentsView_20181127`.`temp_id_mapping_location` timl
            INNER JOIN `patent_20181127`.`location_inventor` li
               ON li.`location_id` = timl.`old_location_id`
   GROUP BY timl.`new_location_id`;


/*
  So after many, many attempts, the fastest way I found to calculate patents per location was the following:
    1) Remap IDs to integers
    2) Insert location_id and patent_id in a temp table with no primary key
    3) Build a non-unique index on this new table
    4) Run the calculation

  The total run time of this method is in the neighborhood of 18 minutes.  The original "straightforward"
  calculation whereby I ran the query directly against the source data using a "union all" between inventor
  and assignee locations ran well over 2 hours.
*/


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_location_patent`;

CREATE TABLE `PatentsView_20181127`.`temp_location_patent`
(
   `location_id`    INT UNSIGNED NOT NULL,
   `patent_id`      VARCHAR(20) NOT NULL
)
ENGINE = INNODB;


# 11,867,513 @ 3:41

INSERT INTO `PatentsView_20181127`.`temp_location_patent`(`location_id`,
                                                          `patent_id`)
   SELECT timl.`new_location_id`, ri.`patent_id`
     FROM `PatentsView_20181127`.`temp_id_mapping_location` timl
          INNER JOIN `patent_20181127`.`rawlocation` rl
             ON rl.`location_id` = timl.`old_location_id`
          INNER JOIN `patent_20181127`.`rawinventor` ri
             ON ri.`rawlocation_id` = rl.`id`;


# 4,457,955 @ 2:54

INSERT INTO `PatentsView_20181127`.`temp_location_patent`(`location_id`,
                                                          `patent_id`)
   SELECT timl.`new_location_id`, ra.`patent_id`
     FROM `PatentsView_20181127`.`temp_id_mapping_location` timl
          INNER JOIN `patent_20181127`.`rawlocation` rl
             ON rl.`location_id` = timl.`old_location_id`
          INNER JOIN `patent_20181127`.`rawassignee` ra
             ON ra.`rawlocation_id` = rl.`id`;


# 15:00

ALTER TABLE `PatentsView_20181127`.`temp_location_patent`
   ADD INDEX (`location_id`, `patent_id`);

ALTER TABLE `PatentsView_20181127`.`temp_location_patent`
   ADD INDEX (`patent_id`, `location_id`);


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_location_num_patents`;

CREATE TABLE `PatentsView_20181127`.`temp_location_num_patents`
(
   `location_id`    INT UNSIGNED NOT NULL,
   `num_patents`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`location_id`)
)
ENGINE = INNODB;


# 121,475 @ 1:10

INSERT INTO `PatentsView_20181127`.`temp_location_num_patents`(`location_id`,
                                                               `num_patents`)
     SELECT `location_id`, count(DISTINCT patent_id)
       FROM `PatentsView_20181127`.`temp_location_patent`
   GROUP BY `location_id`;


DROP TABLE IF EXISTS `PatentsView_20181127`.`location`;

CREATE TABLE `PatentsView_20181127`.`location`
(
   `location_id`               INT UNSIGNED NOT NULL,
   `city`                      VARCHAR(128) NULL,
   `state`                     VARCHAR(20) NULL,
   `country`                   VARCHAR(10) NULL,
   `county`                    VARCHAR(60) NULL,
   `state_fips`                VARCHAR(2) NULL,
   `county_fips`               VARCHAR(3) NULL,
   `latitude`                  FLOAT NULL,
   `longitude`                 FLOAT NULL,
   `num_assignees`             INT UNSIGNED NOT NULL,
   `num_inventors`             INT UNSIGNED NOT NULL,
   `num_patents`               INT UNSIGNED NOT NULL,
   `persistent_location_id`    VARCHAR(128) NOT NULL,
   PRIMARY KEY(`location_id`)
)
ENGINE = INNODB;


# 121,477 @ 0:02

INSERT INTO `PatentsView_20181127`.`location`(`location_id`,
                                              `city`,
                                              `state`,
                                              `country`,
                                              `county`,
                                              `state_fips`,
                                              `county_fips`,
                                              `latitude`,
                                              `longitude`,
                                              `num_assignees`,
                                              `num_inventors`,
                                              `num_patents`,
                                              `persistent_location_id`)
   SELECT timl.`new_location_id`,
          nullif(trim(l.`city`), ''),
          nullif(trim(l.`state`), ''),
          nullif(trim(l.`country`), ''),
          nullif(trim(l.`county`), ''),
          nullif(trim(l.`state_fips`), ''),
          nullif(trim(l.`county_fips`), ''),
          l.`latitude`,
          l.`longitude`,
          ifnull(tlna.`num_assignees`, 0),
          ifnull(tlni.`num_inventors`, 0),
          ifnull(tlnp.`num_patents`, 0),
          timlt.`old_location_id_transformed`
     FROM `patent_20181127`.`location` l
          INNER JOIN `PatentsView_20181127`.`temp_id_mapping_location` timl
             ON timl.`old_location_id` = l.`id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_id_mapping_location_transformed` timlt
             ON timlt.`new_location_id` = timl.`new_location_id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_location_num_assignees` tlna
             ON tlna.`location_id` = timl.`new_location_id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_location_num_inventors` tlni
             ON tlni.`location_id` = timl.`new_location_id`
          LEFT OUTER JOIN
          `PatentsView_20181127`.`temp_location_num_patents` tlnp
             ON tlnp.`location_id` = timl.`new_location_id`;


# END location

################################################################################################################################################
