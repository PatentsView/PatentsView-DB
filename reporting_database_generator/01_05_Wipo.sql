{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

# BEGIN WIPO fields tables 

###############################################################################################################################


DROP TABLE IF EXISTS `{{reporting_db}}`.`wipo`;
CREATE TABLE  `{{reporting_db}}`.`wipo` (
   `patent_id` varchar(20) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
   `field_id` int(11) DEFAULT NULL,
   `sequence` int(11) unsigned NOT NULL,
   PRIMARY KEY (`patent_id`,`sequence`),
   KEY `ix_wipo_field_id` (`field_id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
 DROP TABLE IF EXISTS `{{reporting_db}}`.`wipo_field`;
CREATE TABLE `{{reporting_db}}`.`wipo_field` (
   `id` varchar(3) NOT NULL DEFAULT '',
   `sector_title` varchar(60) DEFAULT NULL,
   `field_title` varchar(255) DEFAULT NULL,
   PRIMARY KEY (`id`),
   KEY `ix_wipo_field_sector_title` (`sector_title`),
   KEY `ix_wipo_field_field_title` (`field_title`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `{{reporting_db}}`.`wipo` SELECT `patent_id`, `field_id`, `sequence` FROM `patent`.`wipo`;
INSERT INTO `{{reporting_db}}`.`wipo_field` SELECT `id`, `sector_title`, `field_title` FROM `patent`.`wipo_field` where `id` not like 'D%';

# END WIPO fields tables

###############################################################################################################################