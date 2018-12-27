#drop database if exists `PatentsView_20181127`;

CREATE DATABASE IF NOT EXISTS `PatentsView_20181127`
   DEFAULT CHARACTER SET = utf8mb4
   DEFAULT COLLATE = utf8mb4_unicode_ci;

#add indexes

#ALTER TABLE `patent_20181127`.`rawassignee` ADD INDEX `patent_id_ix` (`patent_id`);
#ALTER TABLE `patent_20181127`.`rawassignee` ADD INDEX `assignee_id` (`assignee_id`);

#ALTER TABLE `patent_20181127`.`assignee` ADD INDEX `assignee_id` (`id`);

#ALTER TABLE `patent_20181127`.`patent_assignee` ADD INDEX `patent_id` (`patent_id` );

#ALTER TABLE `patent_20181127`.`patent_assignee` ADD INDEX `assignee_id` (`assignee_id` );

ALTER TABLE `patent_20181127`.`cpc_current`
   ADD INDEX `section_id` (`section_id`);


ALTER TABLE `patent_20181127`.`rawexaminer`
   ADD INDEX `patent_id` (`patent_id`);



ALTER TABLE `patent_20181127`.`usreldoc`
   ADD INDEX `patent_id` (`patent_id`);

ALTER TABLE `patent_20181127`.`rel_app_text`
   ADD INDEX `patent_id` (`patent_id`);

ALTER TABLE `patent_20181127`.`wipo`
   ADD INDEX `patent_id` (`patent_id`);

ALTER TABLE `patent_20181127`.`wipo`
   ADD INDEX `field_id` (`field_id`);


# BEGIN assignee id mapping

###################################################################################################################################


# We need this early for firstnamed stuff.
DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_id_mapping_assignee`;

CREATE TABLE `PatentsView_20181127`.`temp_id_mapping_assignee`
(
   `old_assignee_id`    VARCHAR(36) NOT NULL,
   `new_assignee_id`    INT UNSIGNED NOT NULL AUTO_INCREMENT,
   PRIMARY KEY(`old_assignee_id`),
   UNIQUE INDEX `ak_temp_id_mapping_assignee`(`new_assignee_id`)
)
ENGINE = INNODB;


# There are assignees in the raw data that are not linked to anything so we will take our
# assignee ids from the patent_assignee table to ensure we don't copy any unused assignees over.
# 345,185 @ 0:23

INSERT INTO `PatentsView_20181127`.`temp_id_mapping_assignee`(
               `old_assignee_id`)
   SELECT DISTINCT pa.`assignee_id`
     FROM `patent_20181127`.`patent_assignee` pa;


# END assignee id mapping

#####################################################################################################################################


# BEGIN inventor id mapping

###################################################################################################################################


# We need this early for firstnamed stuff.
DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_id_mapping_inventor`;

CREATE TABLE `PatentsView_20181127`.`temp_id_mapping_inventor`
(
   `old_inventor_id`    VARCHAR(36) NOT NULL,
   `new_inventor_id`    INT UNSIGNED NOT NULL AUTO_INCREMENT,
   PRIMARY KEY(`old_inventor_id`),
   UNIQUE INDEX `ak_temp_id_mapping_inventor`(`new_inventor_id`)
)
ENGINE = INNODB;


# There are inventors in the raw data that are not linked to anything so we will take our
# inventor ids from the patent_inventor table to ensure we don't copy any unused inventors over.
# 3,572,763 @ 1:08

INSERT INTO `PatentsView_20181127`.`temp_id_mapping_inventor`(
               `old_inventor_id`)
   SELECT DISTINCT `inventor_id`
     FROM `patent_20181127`.`patent_inventor`;


# END inventor id mapping

#####################################################################################################################################


# BEGIN lawyer id mapping

###################################################################################################################################


# We need this early for firstnamed stuff.
DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_id_mapping_lawyer`;

CREATE TABLE `PatentsView_20181127`.`temp_id_mapping_lawyer`
(
   `old_lawyer_id`    VARCHAR(36) NOT NULL,
   `new_lawyer_id`    INT UNSIGNED NOT NULL AUTO_INCREMENT,
   PRIMARY KEY(`old_lawyer_id`),
   UNIQUE INDEX `ak_temp_id_mapping_lawyer`(`new_lawyer_id`)
)
ENGINE = INNODB;


# There are inventors in the raw data that are not linked to anything so we will take our
# lawyer ids from the patent_lawyer table to ensure we don't copy any unused lawyers over.
# 3,572,763 @ 1:08

INSERT INTO `PatentsView_20181127`.`temp_id_mapping_lawyer`(`old_lawyer_id`)
   SELECT DISTINCT `lawyer_id`
     FROM `patent_20181127`.`patent_lawyer`
    WHERE lawyer_id IS NOT NULL;


# END lawyer id mapping

#####################################################################################################################################


# BEGIN examiner id mapping

###################################################################################################################################


# We need this early for firstnamed stuff.
DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_id_mapping_examiner`;

CREATE TABLE `PatentsView_20181127`.`temp_id_mapping_examiner`
(
   `old_examiner_id`    VARCHAR(36) NOT NULL,
   `new_examiner_id`    INT UNSIGNED NOT NULL AUTO_INCREMENT,
   PRIMARY KEY(`old_examiner_id`),
   UNIQUE INDEX `ak_temp_id_mapping_examiner`(`new_examiner_id`)
)
ENGINE = INNODB;


# There are inventors in the raw data that are not linked to anything so we will take our
# lawyer ids from the patent_lawyer table to ensure we don't copy any unused lawyers over.
# 3,572,763 @ 1:08
# 9,344,749 @ 2:05

INSERT INTO `PatentsView_20181127`.`temp_id_mapping_examiner`(
               `old_examiner_id`)
   SELECT DISTINCT `uuid`
     FROM `patent_20181127`.`rawexaminer`;


# END examiner id mapping

#####################################################################################################################################


# BEGIN location id mapping

###################################################################################################################################


# This bit has changed.  Prior to February 2015, there were many locations that were the same but had
# slightly different lat-longs.  As of February, locations that shared a city, state, and country have
# been forced to use the same lag-long.  The algorithm used to determine which lat-long is outside of
# the scope of this discussion.
#
# So how does this affect PatentsView?  Well, for starters, we need to use the new location_update
# table instead of the old location table.  Additionally, we need to use the new
# rawlocation.location_id_transformed column instead of the old rawlocation.location_id column.  The
# problem, though, is that we have denormalized so much location data that these changes affect many,
# many tables.  In an effort to minimize changes to this script and, HOPEFULLY, minimize impact to
# performance, we are going to map new_location_id to location_id_transformed and then map location_id
# to location_id_transformed which will give us a direct pathway from location_id to new_location_id
# rather than having to drag rawlocation into all queries.


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_id_mapping_location_transformed`;

drop table if exists `PatentsView_20181127`.`temp_id_mapping_location_transformed`;
create table `PatentsView_20181127`.`temp_id_mapping_location_transformed`
(
  `old_location_id` varchar(128) not null,
  `old_location_id_transformed` varchar(128) not null,
  `new_location_id` int unsigned not null auto_increment,
  primary key (`old_location_id`),
  unique index `ak_temp_id_mapping_location_transformed` (`new_location_id`),
  unique index `ak_old_id_mapping_location_transformed` (`old_location_id`),
  index(old_location_id_transformed)
)
engine=InnoDB;


# 97,725 @ 0:02
# 141,189 @ 0:31

INSERT INTO `PatentsView_20181127`.`temp_id_mapping_location_transformed`(
               `old_location_id`,
               `old_location_id_transformed`)
   SELECT DISTINCT `location_id`, `location_id_transformed`
     FROM `patent_20181127`.`rawlocation`
    WHERE `location_id` IS NOT NULL AND `location_id` != '';


DROP TABLE IF EXISTS `PatentsView_20181127`.`temp_id_mapping_location`;

CREATE TABLE `PatentsView_20181127`.`temp_id_mapping_location`
(
   `old_location_id`    VARCHAR(128) NOT NULL,
   `new_location_id`    INT UNSIGNED NOT NULL,
   PRIMARY KEY(`old_location_id`),
   INDEX `ak_temp_id_mapping_location` (`new_location_id`),
   INDEX `ak_old_id_mapping_location` (`old_location_id`)
)
ENGINE = INNODB;


# 120,449 @ 3:27
# 473,585 @ 3:15

INSERT INTO `PatentsView_20181127`.`temp_id_mapping_location`(
               `old_location_id`,
               `new_location_id`)
   SELECT DISTINCT rl.`location_id`, t.`new_location_id`
     FROM (SELECT DISTINCT location_id
             FROM `patent_20181127`.`rawlocation`
            WHERE location_id != '' AND location_id IS NOT NULL) rl
          INNER JOIN
          `PatentsView_20181127`.`temp_id_mapping_location_transformed` t
             ON t.`old_location_id` = rl.`location_id`;


# END location id mapping

#####################################################################################################################################
