DELIMITER ;

CREATE TABLE `publication` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
  `date` date DEFAULT NULL,
  `country` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `kind` varchar(8) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filing_type` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `document_number` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_publication BEFORE INSERT
ON publication
FOR EACH row
  SET new.id = uuid();


CREATE TABLE `application` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
  `type` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `application_number` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `date` date DEFAULT NULL,
  `country` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `series_code` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `invention_title` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `invention_abstract` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `rule_47_flag` varchar(8) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `application_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_application BEFORE INSERT
ON application
FOR EACH row
  SET new.id = uuid();


CREATE TABLE `brf_sum_text` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pgpub_id` bigint(16) DEFAULT NULL,
  `summary_text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `pgpub_id_key` (`pgpub_id`),
  CONSTRAINT `brf_sum_text_ibfk_1` FOREIGN KEY (`pgpub_id`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_brf_sum_text BEFORE INSERT
ON brf_sum_text
FOR EACH row
  SET new.id = uuid();


CREATE TABLE `claims` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pgpub_id` bigint(16) DEFAULT NULL,
  `claim_text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `claim_sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `claim_number` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY (`pgpub_id`,`claim_sequence`),
  CONSTRAINT `claims_ibfk_1` FOREIGN KEY (`pgpub_id`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_claims BEFORE INSERT
ON claims
FOR EACH row
  SET new.id = uuid();


CREATE TABLE `cpc` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
  `sequence` int(11) DEFAULT NULL,
  `version` date DEFAULT NULL,
  `section_id` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `subsection_id` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `group_id` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `subgroup_id` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `symbol_position` varchar(4) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `value` varchar(4) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `category` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `action_date` date DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `document_number` (`document_number`,`sequence`),
  CONSTRAINT `temp_cpc_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_cpc BEFORE INSERT
ON cpc
FOR EACH row
  SET new.id = uuid();


CREATE TABLE `detail_desc_text` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pgpub_id` bigint(16) DEFAULT NULL,
  `description_text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `description_length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `pgpub_id` (`pgpub_id`),
  CONSTRAINT `detail_desc_text_ibfk_1` FOREIGN KEY (`pgpub_id`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_detail_desc_text BEFORE INSERT
ON detail_desc_text
FOR EACH row
  SET new.id = uuid();

CREATE TABLE `draw_desc_text` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pgpub_id` bigint(16) DEFAULT NULL,
  `draw_desc_text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `draw_desc_sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY (`pgpub_id`,`draw_desc_sequence`),
  CONSTRAINT `draw_desc_text_ibfk_1` FOREIGN KEY (`pgpub_id`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_draw_desc_text BEFORE INSERT
ON draw_desc_text
FOR EACH row
  SET new.id = uuid();


CREATE TABLE `government_interest` (
  `document_number` bigint(16) DEFAULT NULL,
  `gi_statement` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `version_indicator` date DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  UNIQUE KEY (`document_number`),
  KEY `government_interest_version_indicator_index` (`version_indicator`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `publication_govintorg` (
  `document_number` bigint(16) DEFAULT NULL,
  `organization_id` int(11) NOT NULL,
  `version_indicator` date DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  UNIQUE KEY (`document_number`,`organization_id`),
  KEY `patent_govintorg_version_indicator_index` (`version_indicator`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `publication_contractawardnumber` (
  `document_number` bigint(16) DEFAULT NULL,
  `contract_award_number` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `version_indicator` date DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  UNIQUE KEY (`document_number`,`contract_award_number`),
  KEY `patent_contractawardnumber_version_indicator_index` (`version_indicator`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `foreign_priority` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
  `country` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `date` date DEFAULT NULL,
  `foreign_doc_number` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `kind` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `document_number` (`document_number`),
  CONSTRAINT `foreign_priority_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_foreign_priority BEFORE INSERT
ON foreign_priority
FOR EACH row
  SET new.id = uuid();


CREATE TABLE `further_cpc` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
  `sequence` int(11) DEFAULT NULL,
  `version` date DEFAULT NULL,
  `section` varchar(2) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `class` varchar(4) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `subclass` varchar(2) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `main_group` varchar(4) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `subgroup` varchar(8) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `symbol_position` varchar(4) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `value` varchar(4) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `action_date` date DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `document_number` (`document_number`),
  CONSTRAINT `further_cpc_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_further_cpc BEFORE INSERT
ON further_cpc
FOR EACH row
  SET new.id = uuid();


CREATE TABLE `ipcr` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
  `sequence` int(11) DEFAULT NULL,
  `version` date DEFAULT NULL,
  `class_level` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `section` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `class` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `subclass` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `main_group` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `subgroup` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `symbol_position` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `class_value` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `action_date` date DEFAULT NULL,
  `class_status` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `class_data_source` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `document_number` (`document_number`,`sequence`),
  CONSTRAINT `ipcr_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_ipcr BEFORE INSERT
ON ipcr
FOR EACH row
  SET new.id = uuid();


CREATE TABLE `main_cpc` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
  `sequence` int(11) DEFAULT NULL,
  `version` date DEFAULT NULL,
  `section` varchar(2) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `class` varchar(4) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `subclass` varchar(2) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `main_group` varchar(4) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `subgroup` varchar(8) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `symbol_position` varchar(4) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `value` varchar(4) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `action_date` date DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `document_number` (`document_number`),
  CONSTRAINT `main_cpc_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_main_cpc BEFORE INSERT
ON main_cpc
FOR EACH row
  SET new.id = uuid();


CREATE TABLE `pct_data` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
  `pct_doc_number` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `country` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `date` date DEFAULT NULL,
  `us_371c124_date` date DEFAULT NULL,
  `us_371c12_date` date DEFAULT NULL,
  `kind` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `doc_type` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `document_number` (`document_number`),
  CONSTRAINT `pct_data_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_pct_data BEFORE INSERT
ON pct_data
FOR EACH row
  SET new.id = uuid();


CREATE TABLE `rawassignee` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
`assignee_id` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `name_first` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `name_last` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `organization` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `type` int(11) DEFAULT NULL,
  `rawlocation_id` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `city` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `state` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `country` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `document_number` (`document_number`),
  CONSTRAINT `rawassignee_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_rawassignee BEFORE INSERT
ON rawassignee
FOR EACH row
  SET new.id = uuid();

CREATE TRIGGER before_insert_rawassignee_rawlocation BEFORE INSERT
ON rawassignee
FOR EACH row
  SET new.rawlocation_id = uuid();


CREATE TABLE `rawinventor` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
  `inventor_id` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `name_first` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `name_last` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `designation` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `deceased` varchar(8) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `rawlocation_id` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `city` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `state` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `country` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `document_number` (`document_number`),
  CONSTRAINT `rawinventor_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_rawinventor BEFORE INSERT
ON rawinventor
FOR EACH row
  SET new.id = uuid();

CREATE TRIGGER before_insert_rawinventor_rawlocation BEFORE INSERT
ON rawinventor
FOR EACH row
  SET new.rawlocation_id = uuid();


CREATE TABLE `rawlocation` (
  `id` varchar(512) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `location_id` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `city` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `state` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `country` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `country_transformed` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `latitude` float DEFAULT NULL,
  `longitude` float DEFAULT NULL,
  `qa_override` int(1) DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `rawuspc` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
  `classification` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `document_number` (`document_number`,`sequence`),
  CONSTRAINT `rawuspc_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_rawuspc BEFORE INSERT
ON rawuspc
FOR EACH row
  SET new.id = uuid();


CREATE TABLE `rel_app_text` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `rel_app_text_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_rel_app_text BEFORE INSERT
ON rel_app_text
FOR EACH row
  SET new.id = uuid();


CREATE TABLE `us_parties` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
  `name_first` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `name_last` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `organization` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `type` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `applicant_authority` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `designation` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `rawlocation_id` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `city` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `state` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `country` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `document_number` (`document_number`),
  CONSTRAINT `us_parties_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_us_parties BEFORE INSERT
ON us_parties
FOR EACH row
  SET new.id = uuid();

CREATE TRIGGER before_insert_us_parties_rawlocation BEFORE INSERT
ON us_parties
FOR EACH row
  SET new.rawlocation_id = uuid();


CREATE TABLE `uspc` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
  `mainclass_id` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `subclass_id` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `document_number` (`document_number`,`sequence`),
  CONSTRAINT `uspc_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_uspc BEFORE INSERT
ON uspc
FOR EACH row
  SET new.id = uuid();


CREATE TABLE `usreldoc_single` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
  `related_doc_number` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `country` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `doc_type` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `relkind` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `date` date DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `document_number` (`document_number`),
  CONSTRAINT `usreldoc_single_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_usreldoc_single BEFORE INSERT
ON usreldoc_single
FOR EACH row
  SET new.id = uuid();


CREATE TABLE `usreldoc_related` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
  `related_doc_number` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `country` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `doc_type` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `relkind` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `date` date DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `document_number` (`document_number`),
  CONSTRAINT `usreldoc_related_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_usreldoc_related BEFORE INSERT
ON usreldoc_related
FOR EACH row
  SET new.id = uuid();


CREATE TABLE `usreldoc_parent_child` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
  `related_doc_number` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `country` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `doc_type` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `relkind` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `date` date DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `document_number` (`document_number`),
  CONSTRAINT `usreldoc_parent_child_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_usreldoc_parent_child BEFORE INSERT
ON usreldoc_parent_child
FOR EACH row
  SET new.id = uuid();


CREATE TABLE `usreldoc` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
  `related_doc_number` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `country` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `doc_type` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `relkind` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `date` date DEFAULT NULL,
  `filename` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `document_number` (`document_number`),
  CONSTRAINT `usreldoc_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TRIGGER before_insert_usreldoc BEFORE INSERT
ON usreldoc
FOR EACH row
  SET new.id = uuid();
