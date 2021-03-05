
# BEGIN Government interest tables 

###############################################################################################################################
drop table if exists `{{params.reporting_database}}`.`government_interest`;
CREATE TABLE `{{params.reporting_database}}`.`government_interest` (
   `patent_id` varchar(24) NOT NULL,
   `gi_statement` text,
   PRIMARY KEY (`patent_id`)
 ) ENGINE=InnoDB;

drop table if exists `{{params.reporting_database}}`.`government_organization`;
CREATE TABLE IF NOT EXISTS `{{params.reporting_database}}`.`government_organization` (
   `organization_id` int(11) NOT NULL AUTO_INCREMENT,
   `name` varchar(255) DEFAULT NULL,
   `level_one` varchar(255) DEFAULT NULL,
   `level_two` varchar(255) DEFAULT NULL,
   `level_three` varchar(255) DEFAULT NULL,
   PRIMARY KEY (`organization_id`)
 ) ENGINE=InnoDB AUTO_INCREMENT=137;
drop table if exists `{{params.reporting_database}}`.`patent_contractawardnumber`;
CREATE TABLE `{{params.reporting_database}}`.`patent_contractawardnumber` (
   `patent_id` varchar(24) NOT NULL,
   `contract_award_number` varchar(64) Null,
   PRIMARY KEY (`patent_id`,`contract_award_number`),
   CONSTRAINT `patent_contractawardnumber_ibfk_1` FOREIGN KEY (`patent_id`) REFERENCES `{{params.reporting_database}}`.`government_interest` (`patent_id`) ON DELETE CASCADE
 ) ENGINE=InnoDB;



drop table if exists `{{params.reporting_database}}`.`patent_govintorg`;
 
CREATE TABLE `{{params.reporting_database}}`.`patent_govintorg` (
   `patent_id` varchar(24) NOT NULL,
   `organization_id` int(11) NOT NULL,
   PRIMARY KEY (`patent_id`,`organization_id`),
   KEY `organization_id` (`organization_id`),
   CONSTRAINT `patent_govintorg_ibfk_2` FOREIGN KEY (`organization_id`) REFERENCES `{{params.reporting_database}}`.`government_organization` (`organization_id`) ON DELETE CASCADE
 ) ENGINE=InnoDB;
 
 
 

INSERT INTO `{{params.reporting_database}}`.`government_interest` SELECT `patent_id`, `gi_statement` FROM `{{params.raw_database}}`.`government_interest`;
INSERT INTO `{{params.reporting_database}}`.`government_organization` SELECT `organization_id`, `name`, `level_one`, `level_two`, `level_three` FROM `{{params.raw_database}}`.`government_organization`;
INSERT INTO `{{params.reporting_database}}`.`patent_contractawardnumber` SELECT `patent_id`, `contract_award_number` FROM `{{params.raw_database}}`.`patent_contractawardnumber`;

INSERT INTO `{{params.reporting_database}}`.`patent_govintorg` SELECT `patent_id`, `organization_id` FROM `{{params.raw_database}}`.`patent_govintorg`;

ALTER TABLE `{{params.reporting_database}}`.`government_organization` ADD INDEX `ix_government_organization_name`(`name`);
ALTER TABLE `{{params.reporting_database}}`.`government_organization` ADD INDEX `ix_government_organization_level_one`(`level_one`);
ALTER TABLE `{{params.reporting_database}}`.`government_organization` ADD INDEX `ix_government_organization_level_two`(`level_two`);
ALTER TABLE `{{params.reporting_database}}`.`government_organization` ADD INDEX `ix_government_organization_level_three`(`level_three`);

# END Government interest tables
