
# BEGIN WIPO fields tables 

###############################################################################################################################


DROP TABLE IF EXISTS `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`wipo`;
CREATE TABLE  `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`wipo` (
   `patent_id` varchar(20) NOT NULL,
   `field_id` varchar(3) DEFAULT NULL,
   `sequence` int(10) unsigned NOT NULL,
   PRIMARY KEY (`patent_id`,`sequence`),
   KEY `ix_wipo_field_id` (`field_id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
 DROP TABLE IF EXISTS `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`wipo_field`;
CREATE TABLE `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`wipo_field` (
   `id` varchar(3) NOT NULL,
   `sector_title` varchar(60) DEFAULT NULL,
   `field_title` varchar(255) DEFAULT NULL,
   PRIMARY KEY (`id`),
   KEY `ix_wipo_field_sector_title` (`sector_title`),
   KEY `ix_wipo_field_field_title` (`field_title`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`wipo` SELECT `patent_id`, `field_id`, `sequence` FROM `patent`.`wipo`;
INSERT INTO `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`wipo_field` SELECT `id`, `sector_title`, `field_title` FROM `patent`.`wipo_field`;

# END WIPO fields tables

###############################################################################################################################