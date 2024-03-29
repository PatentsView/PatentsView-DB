{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set version_indicator = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

# BEGIN Government interest tables

###############################################################################################################################
drop table if exists `{{reporting_db}}`.`government_interest`;
CREATE TABLE `{{reporting_db}}`.`government_interest` (
   `patent_id` varchar(24) NOT NULL,
   `gi_statement` text,
   PRIMARY KEY (`patent_id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

drop table if exists `{{reporting_db}}`.`government_organization`;
CREATE TABLE IF NOT EXISTS `{{reporting_db}}`.`government_organization` (
   `organization_id` int(11) NOT NULL AUTO_INCREMENT,
   `name` varchar(255) DEFAULT NULL,
   `level_one` varchar(255) DEFAULT NULL,
   `level_two` varchar(255) DEFAULT NULL,
   `level_three` varchar(255) DEFAULT NULL,
   PRIMARY KEY (`organization_id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci AUTO_INCREMENT=137;

drop table if exists `{{reporting_db}}`.`patent_contractawardnumber`;
CREATE TABLE `{{reporting_db}}`.`patent_contractawardnumber` (
   `patent_id` varchar(24) NOT NULL,
   `contract_award_number` varchar(64) Null,
   PRIMARY KEY (`patent_id`,`contract_award_number`),
   CONSTRAINT `patent_contractawardnumber_ibfk_1` FOREIGN KEY (`patent_id`) REFERENCES `{{reporting_db}}`.`government_interest` (`patent_id`) ON DELETE CASCADE
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



drop table if exists `{{reporting_db}}`.`patent_govintorg`;
CREATE TABLE `{{reporting_db}}`.`patent_govintorg` (
   `patent_id` varchar(24) NOT NULL,
   `organization_id` int(11) NOT NULL,
   PRIMARY KEY (`patent_id`,`organization_id`),
   KEY `organization_id` (`organization_id`),
   CONSTRAINT `patent_govintorg_ibfk_2` FOREIGN KEY (`organization_id`) REFERENCES `{{reporting_db}}`.`government_organization` (`organization_id`) ON DELETE CASCADE
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;




INSERT INTO `{{reporting_db}}`.`government_interest` SELECT `patent_id`, `gi_statement` FROM `patent`.`government_interest` where version_indicator<='{{version_indicator}}';
INSERT INTO `{{reporting_db}}`.`government_organization` SELECT `organization_id`, `name`, `level_one`, `level_two`, `level_three` FROM `patent`.`government_organization`;
INSERT INTO `{{reporting_db}}`.`patent_contractawardnumber` SELECT `patent_id`, `contract_award_number` FROM `patent`.`patent_contractawardnumber`  where version_indicator<='{{version_indicator}}';
INSERT INTO `{{reporting_db}}`.`patent_govintorg` SELECT `patent_id`, `organization_id` FROM `patent`.`patent_govintorg`  where version_indicator<='{{version_indicator}}';

ALTER TABLE `{{reporting_db}}`.`government_organization` ADD INDEX `ix_government_organization_name`(`name`);
ALTER TABLE `{{reporting_db}}`.`government_organization` ADD INDEX `ix_government_organization_level_one`(`level_one`);
ALTER TABLE `{{reporting_db}}`.`government_organization` ADD INDEX `ix_government_organization_level_two`(`level_two`);
ALTER TABLE `{{reporting_db}}`.`government_organization` ADD INDEX `ix_government_organization_level_three`(`level_three`);
ALTER TABLE `{{reporting_db}}`.`government_interest` ADD FULLTEXT INDEX `fti_government_interest_gi_statement` (`gi_statement`);


# END Government interest tables
