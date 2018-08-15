-- Create patents schema
-- Updated 08.13.18 by Sarah Kelley;



DROP TABLE IF EXISTS `application`;

CREATE TABLE `application` (
  `id` varchar(36) NOT NULL,
  `patent_id` varchar(20) NOT NULL,
  `type` varchar(20) DEFAULT NULL,
  `number` varchar(64) DEFAULT NULL,
  `country` varchar(20) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `id_transformed` varchar(36) DEFAULT NULL,
  `number_transformed` varchar(64) DEFAULT NULL,
  `series_code_transformed_from_type` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`,`patent_id`),
  KEY `patent_id` (`patent_id`),
  KEY `app_idx1` (`type`,`number`),
  KEY `app_idx2` (`date`),
  KEY `app_idx3` (`number_transformed`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `rawexaminer`;
CREATE TABLE `rawexaminer` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) NOT NULL,
  `name_first` varchar(64) DEFAULT NULL,
   `name_last` varchar(64) DEFAULT NULL,
  `role` varchar(20) DEFAULT NULL,
  `group` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`uuid`,`patent_id`),
  KEY `patent_id` (`patent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `assignee`;

CREATE TABLE `assignee` (
  `id` varchar(36) NOT NULL,
  `type` varchar(10) DEFAULT NULL,
  `name_first` varchar(64) DEFAULT NULL,
  `name_last` varchar(64) DEFAULT NULL,
  `organization` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `claim`;
CREATE TABLE `claim` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `text` mediumtext,
  `dependent` varchar(40) DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `exemplary` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`),
  KEY `ix_claim_sequence` (`sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `cpc_current`;
CREATE TABLE `cpc_current` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `section_id` varchar(10) DEFAULT NULL,
  `subsection_id` varchar(20) DEFAULT NULL,
  `group_id` varchar(20) DEFAULT NULL,
  `subgroup_id` varchar(20) DEFAULT NULL,
  `category` varchar(36) DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`),
  KEY `subsection_id` (`subsection_id`),
  KEY `group_id` (`group_id`),
  KEY `subgroup_id` (`subgroup_id`),
  KEY `ix_cpc_current_sequence` (`sequence`),
  KEY `ix_cpc_current_category` (`category`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `cpc_group`;

CREATE TABLE `cpc_group` (
  `id` varchar(20) NOT NULL,
  `title` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `cpc_subgroup`;

CREATE TABLE `cpc_subgroup` (
  `id` varchar(20) NOT NULL,
  `title` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `cpc_subsection`;

CREATE TABLE `cpc_subsection` (
  `id` varchar(20) NOT NULL,
  `title` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;




DROP TABLE IF EXISTS `foreigncitation`;

CREATE TABLE `foreigncitation` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `number` varchar(64) DEFAULT NULL,
  `country` varchar(10) DEFAULT NULL,
  `category` varchar(20) DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `government_interest`;

CREATE TABLE `government_interest` (
  `patent_id` varchar(255) NOT NULL,
  `gi_statement` mediumtext,
  PRIMARY KEY (`patent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `government_organization`;

CREATE TABLE `government_organization` (
  `organization_id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `level_one` varchar(255) DEFAULT NULL,
  `level_two` varchar(255) DEFAULT NULL,
  `level_three` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`organization_id`)
) ENGINE=InnoDB AUTO_INCREMENT=137 DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;




DROP TABLE IF EXISTS `inventor`;

CREATE TABLE `inventor` (
  `id` varchar(36) NOT NULL,
  `name_first` varchar(64) DEFAULT NULL,
  `name_last` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `ipcr`;

CREATE TABLE `ipcr` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `classification_level` varchar(20) DEFAULT NULL,
  `section` varchar(20) DEFAULT NULL,
  `ipc_class` varchar(20) DEFAULT NULL,
  `subclass` varchar(20) DEFAULT NULL,
  `main_group` varchar(20) DEFAULT NULL,
  `subgroup` varchar(20) DEFAULT NULL,
  `symbol_position` varchar(20) DEFAULT NULL,
  `classification_value` varchar(20) DEFAULT NULL,
  `classification_status` varchar(20) DEFAULT NULL,
  `classification_data_source` varchar(20) DEFAULT NULL,
  `action_date` date DEFAULT NULL,
  `ipc_version_indicator` date DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`),
  KEY `ix_ipcr_sequence` (`sequence`),
  KEY `ix_ipcr_action_date` (`action_date`),
  KEY `ix_ipcr_ipc_version_indicator` (`ipc_version_indicator`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `lawyer`;
CREATE TABLE `lawyer` (
  `id` varchar(36) NOT NULL,
  `name_first` varchar(64) DEFAULT NULL,
  `name_last` varchar(64) DEFAULT NULL,
  `organization` varchar(64) DEFAULT NULL,
  `country` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `location`;

CREATE TABLE `location` (
  `id` varchar(128) NOT NULL,
  `city` varchar(128) DEFAULT NULL,
  `state` varchar(20) DEFAULT NULL,
  `country` varchar(10) DEFAULT NULL,
  `latitude` float DEFAULT NULL,
  `longitude` float DEFAULT NULL,
  `county` varchar(128) DEFAULT NULL,
  `state_fips` int DEFAULT NULL,
  `county_fips` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_location_country` (`country`),
  KEY `dloc_idx2` (`city`,`state`,`country`),
  KEY `dloc_idx1` (`latitude`,`longitude`),
  KEY `ix_location_state` (`state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `location_assignee`;

CREATE TABLE `location_assignee` (
  `location_id` varchar(128) DEFAULT NULL,
  `assignee_id` varchar(36) DEFAULT NULL,
  KEY `location_id` (`location_id`),
  KEY `assignee_id` (`assignee_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;




DROP TABLE IF EXISTS `location_inventor`;

CREATE TABLE `location_inventor` (
  `location_id` varchar(128) DEFAULT NULL,
  `inventor_id` varchar(36) DEFAULT NULL,
  KEY `location_id` (`location_id`),
  KEY `inventor_id` (`inventor_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `mainclass`;

CREATE TABLE `mainclass` (
  `id` varchar(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `mainclass_current`;

CREATE TABLE `mainclass_current` (
  `id` varchar(20) NOT NULL,
  `title` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;




DROP TABLE IF EXISTS `nber`;

CREATE TABLE `nber` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `category_id` varchar(20) DEFAULT NULL,
  `subcategory_id` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`),
  KEY `category_id` (`category_id`),
  KEY `subcategory_id` (`subcategory_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `nber_category`;

CREATE TABLE `nber_category` (
  `id` varchar(20) NOT NULL,
  `title` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `nber_subcategory`;

CREATE TABLE `nber_subcategory` (
  `id` varchar(20) NOT NULL,
  `title` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;




DROP TABLE IF EXISTS `otherreference`;

CREATE TABLE `otherreference` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `text` text,
  `sequence` int(11) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`),
  FULLTEXT KEY `fti_text` (`text`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `patent`;

CREATE TABLE `patent` (
  `id` varchar(20) NOT NULL,
  `type` varchar(100) DEFAULT NULL,
  `number` varchar(64) DEFAULT NULL,
  `country` varchar(20) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `abstract` mediumtext,
  `title` mediumtext,
  `kind` varchar(10) DEFAULT NULL,
  `num_claims` int(11) DEFAULT NULL,
  `filename` varchar(120) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `pat_idx1` (`type`,`number`),
  KEY `pat_idx2` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `patent_assignee`;

CREATE TABLE `patent_assignee` (
  `patent_id` varchar(20) DEFAULT NULL,
  `assignee_id` varchar(36) DEFAULT NULL,
  KEY `ix_patent_assignee_patent_id` (`patent_id`),
  KEY `ix_patent_assignee_assignee_id` (`assignee_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `patent_contractawardnumber`;

CREATE TABLE `patent_contractawardnumber` (
  `patent_id` varchar(255) NOT NULL,
  `contract_award_number` varchar(255) NOT NULL,
  PRIMARY KEY (`patent_id`,`contract_award_number`),
  CONSTRAINT `patent_contractawardnumber_ibfk_1` FOREIGN KEY (`patent_id`) REFERENCES `government_interest` (`patent_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `patent_govintorg`;

CREATE TABLE `patent_govintorg` (
  `patent_id` varchar(255) NOT NULL,
  `organization_id` int(11) NOT NULL,
  PRIMARY KEY (`patent_id`,`organization_id`),
  KEY `organization_id` (`organization_id`),
  CONSTRAINT `patent_govintorg_ibfk_1` FOREIGN KEY (`patent_id`) REFERENCES `government_interest` (`patent_id`) ON DELETE CASCADE,
  CONSTRAINT `patent_govintorg_ibfk_2` FOREIGN KEY (`organization_id`) REFERENCES `government_organization` (`organization_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `patent_inventor`;

CREATE TABLE `patent_inventor` (
  `patent_id` varchar(20) DEFAULT NULL,
  `inventor_id` varchar(36) DEFAULT NULL,
  KEY `patent_id` (`patent_id`),
  KEY `inventor_id` (`inventor_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `patent_lawyer`;

CREATE TABLE `patent_lawyer` (
  `patent_id` varchar(20) DEFAULT NULL,
  `lawyer_id` varchar(36) DEFAULT NULL,
  KEY `patent_id` (`patent_id`),
  KEY `lawyer_id` (`lawyer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `rawassignee`;

CREATE TABLE `rawassignee` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `assignee_id` varchar(36) DEFAULT NULL,
  `rawlocation_id` varchar(128) DEFAULT NULL,
  `type` varchar(10) DEFAULT NULL,
  `name_first` varchar(64) DEFAULT NULL,
  `name_last` varchar(64) DEFAULT NULL,
  `organization` varchar(256) DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`),
  KEY `assignee_id` (`assignee_id`),
  KEY `rawlocation_id` (`rawlocation_id`),
  KEY `ix_rawassignee_sequence` (`sequence`),
  KEY `ix_organization` (`organization`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `rawinventor`;

CREATE TABLE `rawinventor` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `inventor_id` varchar(36) DEFAULT NULL,
  `rawlocation_id` varchar(128) DEFAULT NULL,
  `name_first` varchar(64) DEFAULT NULL,
  `name_last` varchar(64) DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `rule_47` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`),
  KEY `inventor_id` (`inventor_id`),
  KEY `rawlocation_id` (`rawlocation_id`),
  KEY `ix_rawinventor_sequence` (`sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `rawlawyer`;

CREATE TABLE `rawlawyer` (
  `uuid` varchar(36) NOT NULL,
  `lawyer_id` varchar(36) DEFAULT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `name_first` varchar(64) DEFAULT NULL,
  `name_last` varchar(64) DEFAULT NULL,
  `organization` varchar(64) DEFAULT NULL,
  `country` varchar(10) DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `lawyer_id` (`lawyer_id`),
  KEY `patent_id` (`patent_id`),
  KEY `ix_rawlawyer_sequence` (`sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `rawlocation`;

CREATE TABLE `rawlocation` (
  `id` varchar(128) NOT NULL,
  `location_id` varchar(128) DEFAULT NULL,
  `city` varchar(128) DEFAULT NULL,
  `state` varchar(20) DEFAULT NULL,
  `country` varchar(10) DEFAULT NULL,
  `country_transformed` varchar(10) DEFAULT NULL,
  `location_id_transformed` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `location_id` (`location_id`),
  KEY `loc_idx1` (`city`,`state`,`country`),
  KEY `ix_rawlocation_state` (`state`),
  KEY `ix_rawlocation_country` (`country`)
  KEY `rawlocation_location_id_transformed` (`location_id_transformed`),
  add KEY `ix_rawlocation_country_transformed_state` (`country_transformed`,`state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;

DROP TABLE IF EXISTS `subclass`;

CREATE TABLE `subclass` (
  `id` varchar(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;

DROP TABLE IF EXISTS `subclass_current`;

CREATE TABLE `subclass_current` (
  `id` varchar(20) NOT NULL,
  `title` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `usapplicationcitation`;
CREATE TABLE `usapplicationcitation` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `application_id` varchar(20) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `name` varchar(64) DEFAULT NULL,
  `kind` varchar(10) DEFAULT NULL,
  `number` varchar(64) DEFAULT NULL,
  `country` varchar(10) DEFAULT NULL,
  `category` varchar(20) DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `application_id_transformed` varchar(36) DEFAULT NULL,
  `number_transformed` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`),
  KEY `ix_usapplicationcitation_application_id` (`application_id`),
  KEY `ix_number_transformed` (`number_transformed`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `uspatentcitation`;

CREATE TABLE `uspatentcitation` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `citation_id` varchar(20) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `name` varchar(64) DEFAULT NULL,
  `kind` varchar(10) DEFAULT NULL,
  `country` varchar(10) DEFAULT NULL,
  `category` varchar(20) DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`),
  KEY `ix_uspatentcitation_citation_id` (`citation_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `uspc`;

CREATE TABLE `uspc` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `mainclass_id` varchar(20) DEFAULT NULL,
  `subclass_id` varchar(20) DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`),
  KEY `mainclass_id` (`mainclass_id`),
  KEY `subclass_id` (`subclass_id`),
  KEY `ix_uspc_sequence` (`sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `uspc_current`;
CREATE TABLE `uspc_current` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `mainclass_id` varchar(20) DEFAULT NULL,
  `subclass_id` varchar(20) DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`),
  KEY `mainclass_id` (`mainclass_id`),
  KEY `subclass_id` (`subclass_id`),
  KEY `ix_uspc_current_sequence` (`sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `usreldoc`;

CREATE TABLE `usreldoc` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `doctype` varchar(64) DEFAULT NULL,
  `relkind` varchar(64) DEFAULT NULL,
  `reldocno` varchar(64) DEFAULT NULL,
  `country` varchar(20) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `status` varchar(20) DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `kind` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`),
  KEY `ix_usreldoc_country` (`country`),
  KEY `ix_usreldoc_doctype` (`doctype`),
  KEY `ix_usreldoc_date` (`date`),
  KEY `ix_usreldoc_reldocno` (`reldocno`),
  KEY `ix_usreldoc_sequence` (`sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `wipo`;

CREATE TABLE `wipo` (
  `patent_id` varchar(20) NOT NULL,
  `field_id` varchar(3) DEFAULT NULL,
  `sequence` int(10) unsigned NOT NULL,
  PRIMARY KEY (`patent_id`,`sequence`),
  KEY `ix_wipo_field_id` (`field_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `wipo_field`;

CREATE TABLE `wipo_field` (
  `id` varchar(3) NOT NULL DEFAULT '',
  `sector_title` varchar(60) DEFAULT NULL,
  `field_title` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_wipo_field_sector_title` (`sector_title`),
  KEY `ix_wipo_field_field_title` (`field_title`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `brf_sum_text`;

CREATE TABLE `brf_sum_text` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `text` mediumtext NOT NULL,
  PRIMARY KEY (`uuid`),
  KEY `ix_brfsumtext_patent_id` (`patent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `detail_desc_text`;

CREATE TABLE `detail_desc_text` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `text` mediumtext NOT NULL,
  `length` int(10) unsigned NOT NULL,
  PRIMARY KEY (`uuid`),
  KEY `ix__patent_id` (`patent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `draw_desc_text`;

CREATE TABLE `draw_desc_text` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `text` mediumtext NOT NULL,
  `sequence` int(10) unsigned NOT NULL,
  PRIMARY KEY (`uuid`),
  KEY `ix_drawdesctext_patent_id` (`patent_id`,`sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `rel_app_text`;

CREATE TABLE `rel_app_text` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `text` mediumtext NOT NULL,
  `sequence` int(10) unsigned NOT NULL,
  PRIMARY KEY (`uuid`),
  KEY `ix_relapptext_patent_id` (`patent_id`, `sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `figures`;

CREATE TABLE `figures` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) NOT NULL,
  `num_figures` int(11) DEFAULT NULL,
  `num_sheets` int(11) DEFAULT NULL, 
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `ix_figures_patentid` (`patent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `pct_data`;

CREATE TABLE `pct_data` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) NOT NULL,
  `rel_id` varchar(20) NOT NULL, 
  `date` date DEFAULT NULL, 
  `371_date` date DEFAULT NULL, 
  `country` varchar(20) DEFAULT NULL,  
  `kind` varchar(20) DEFAULT NULL,
  `doc_type` varchar(20) DEFAULT NULL, 
  `102_date` date DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `ix_pctdat_patentid` (`patent_id`, `rel_id`),
  KEY `ix_pctdat_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;



DROP TABLE IF EXISTS `us_term_of_grant`;


CREATE TABLE `us_term_of_grant` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) NOT NULL,
  `lapse_of_patent` varchar(20) DEFAULT NULL,
  `disclaimer_date` date DEFAULT NULL, 
  `term_disclaimer` VARCHAR(128) DEFAULT NULL,
  `term_grant` VARCHAR(128) DEFAULT NULL,
  `term_extension` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `ix_ustergra_patentid` (`patent_id`),
  KEY `ix_ustergra_date` (`disclaimer_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;

DROP TABLE IF EXISTS `foreign_priority`;
CREATE TABLE `foreign_priority` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) NOT NULL,
  `sequence` int(11) DEFAULT NULL,
  `kind` varchar(20) DEFAULT NULL,
  `number` varchar(70) DEFAULT NULL,
  `date` date DEFAULT NULL,  
  `country` varchar(20) DEFAULT NULL,
  `country_transformed` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `ix_forpri_patentid` (`patent_id`, `sequence`),
  KEY `ix_forpri_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `botanic`;

CREATE TABLE `botanic` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) NOT NULL,
  `latin_name` varchar(128) DEFAULT NULL,
  `variety` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `ix_botanic_patent_id` (`patent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;


DROP TABLE IF EXISTS `non_inventor_applicant`;

CREATE TABLE `non_inventor_applicant` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(36) NOT NULL,
  `rawlocation_id` varchar(36) NOT NULL,
  `lname` varchar(40) DEFAULT NULL,
  `fname` varchar(30) DEFAULT  NULL,
  `organization` varchar(128) DEFAULT NULL,
  `sequence` int(11) DEFAULT  NULL,
  `designation` varchar(20) DEFAULT NULL,
  `applicant_type` varchar(30) DEFAULT  NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`),
  KEY `rawlocation_id` (`rawlocation_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;
