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


DELIMITER ;;
DELIMITER ;


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


DELIMITER ;;
DELIMITER ;


CREATE TABLE `brf_sum_text` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text BEFORE INSERT
ON brf_sum_text
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2022` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2022_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2022 BEFORE INSERT
ON brf_sum_text_2022
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;


CREATE TABLE `brf_sum_text_2021` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2021_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2021 BEFORE INSERT
ON brf_sum_text_2021
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2020` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2020_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2020 BEFORE INSERT
ON brf_sum_text_2020
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2019` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2019_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2019 BEFORE INSERT
ON brf_sum_text_2019
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2018` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2018_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2018 BEFORE INSERT
ON brf_sum_text_2018
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2017` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2017_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2017 BEFORE INSERT
ON brf_sum_text_2017
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2016` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2016_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2016 BEFORE INSERT
ON brf_sum_text_2016
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2015` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2015_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2015 BEFORE INSERT
ON brf_sum_text_2015
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2014` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2014_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2014 BEFORE INSERT
ON brf_sum_text_2014
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2013` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2013_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2013 BEFORE INSERT
ON brf_sum_text_2013
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2012` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2012_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2012 BEFORE INSERT
ON brf_sum_text_2012
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2011` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2011_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2011 BEFORE INSERT
ON brf_sum_text_2011
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2010` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2010_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2010 BEFORE INSERT
ON brf_sum_text_2010
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2009` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2009_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2009 BEFORE INSERT
ON brf_sum_text_2009
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2008` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2008_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2008 BEFORE INSERT
ON brf_sum_text_2008
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2007` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2007_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2007 BEFORE INSERT
ON brf_sum_text_2007
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2006` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2006_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2006 BEFORE INSERT
ON brf_sum_text_2006
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2005` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2005_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2005 BEFORE INSERT
ON brf_sum_text_2005
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2004` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2004_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2004 BEFORE INSERT
ON brf_sum_text_2004
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2003` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2003_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2003 BEFORE INSERT
ON brf_sum_text_2003
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2002` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2002_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2002 BEFORE INSERT
ON brf_sum_text_2002
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;

CREATE TABLE `brf_sum_text_2001` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `brf_sum_text_2001_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_brf_sum_text_2001 BEFORE INSERT
ON brf_sum_text_2001
FOR EACH row
  SET new.id = uuid();

DELIMITER ;;
DELIMITER ;


CREATE TABLE `claim` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim BEFORE INSERT
ON claim
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2022` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2022_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2022 BEFORE INSERT
ON claim_2022
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2021` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2021_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2021 BEFORE INSERT
ON claim_2021
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2020` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2020_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2020 BEFORE INSERT
ON claim_2020
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2019` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2019_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2019 BEFORE INSERT
ON claim_2019
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2018` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2018_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2018 BEFORE INSERT
ON claim_2018
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2017` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2017_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2017 BEFORE INSERT
ON claim_2017
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2016` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2016_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2016 BEFORE INSERT
ON claim_2016
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2015` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2015_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2015 BEFORE INSERT
ON claim_2015
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2014` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2014_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2014 BEFORE INSERT
ON claim_2014
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2013` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2013_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2013 BEFORE INSERT
ON claim_2013
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2012` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2012_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2012 BEFORE INSERT
ON claim_2012
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;


CREATE TABLE `claim_2011` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2011_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2011 BEFORE INSERT
ON claim_2011
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2010` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2010_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2010 BEFORE INSERT
ON claim_2010
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2009` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2009_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2009 BEFORE INSERT
ON claim_2009
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2008` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2008_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2008 BEFORE INSERT
ON claim_2008
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2007` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2007_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2007 BEFORE INSERT
ON claim_2007
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2006` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2006_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2006 BEFORE INSERT
ON claim_2006
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2005` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2005_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2005 BEFORE INSERT
ON claim_2005
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2004` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2004_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2004 BEFORE INSERT
ON claim_2004
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2003` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2003_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2003 BEFORE INSERT
ON claim_2003
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2002` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2002_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2002 BEFORE INSERT
ON claim_2002
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `claim_2001` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `dependent` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  `num` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`,`num`),
  CONSTRAINT `claim_2001_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_claim_2001 BEFORE INSERT
ON claim_2001
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

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
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text BEFORE INSERT
ON detail_desc_text
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2022` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2022_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2022 BEFORE INSERT
ON detail_desc_text_2022
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2021` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2021_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2021 BEFORE INSERT
ON detail_desc_text_2021
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2020` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2020_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2020 BEFORE INSERT
ON detail_desc_text_2020
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2019` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2019_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2019 BEFORE INSERT
ON detail_desc_text_2019
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2018` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2018_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2018 BEFORE INSERT
ON detail_desc_text_2018
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2017` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2017_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2017 BEFORE INSERT
ON detail_desc_text_2017
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2016` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2016_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2016 BEFORE INSERT
ON detail_desc_text_2016
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2015` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2015_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2015 BEFORE INSERT
ON detail_desc_text_2015
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2014` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2014_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2014 BEFORE INSERT
ON detail_desc_text_2014
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2013` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2013_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2013 BEFORE INSERT
ON detail_desc_text_2013
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2012` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2012_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2012 BEFORE INSERT
ON detail_desc_text_2012
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2011` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2011_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2011 BEFORE INSERT
ON detail_desc_text_2011
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2010` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2010_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2010 BEFORE INSERT
ON detail_desc_text_2010
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2009` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2009_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2009 BEFORE INSERT
ON detail_desc_text_2009
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2008` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2008_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2008 BEFORE INSERT
ON detail_desc_text_2008
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2007` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2007_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2007 BEFORE INSERT
ON detail_desc_text_2007
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2006` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2006_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2006 BEFORE INSERT
ON detail_desc_text_2006
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;


CREATE TABLE `detail_desc_text_2005` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2005_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2005 BEFORE INSERT
ON detail_desc_text_2005
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2004` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2004_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2004 BEFORE INSERT
ON detail_desc_text_2004
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2003` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2003_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2003 BEFORE INSERT
ON detail_desc_text_2003
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2002` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2002_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2002 BEFORE INSERT
ON detail_desc_text_2002
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `detail_desc_text_2001` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length` bigint(16) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  UNIQUE KEY `document_number` (`document_number`),
  CONSTRAINT `detail_desc_text_2001_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_detail_desc_text_2001 BEFORE INSERT
ON detail_desc_text_2001
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;


CREATE TABLE `draw_desc_text` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text BEFORE INSERT
ON draw_desc_text
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2022` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2022_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2022 BEFORE INSERT
ON draw_desc_text_2022
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;


CREATE TABLE `draw_desc_text_2021` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2021_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2021 BEFORE INSERT
ON draw_desc_text_2021
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2020` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2020_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2020 BEFORE INSERT
ON draw_desc_text_2020
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2019` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2019_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2019 BEFORE INSERT
ON draw_desc_text_2019
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2018` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2018_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2018 BEFORE INSERT
ON draw_desc_text_2018
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2017` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2017_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2017 BEFORE INSERT
ON draw_desc_text_2017
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2016` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2016_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2016 BEFORE INSERT
ON draw_desc_text_2016
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2015` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2015_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2015 BEFORE INSERT
ON draw_desc_text_2015
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2014` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2014_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2014 BEFORE INSERT
ON draw_desc_text_2014
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2013` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2013_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2013 BEFORE INSERT
ON draw_desc_text_2013
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2012` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2012_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2012 BEFORE INSERT
ON draw_desc_text_2012
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2011` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2011_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2011 BEFORE INSERT
ON draw_desc_text_2011
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2010` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2010_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2010 BEFORE INSERT
ON draw_desc_text_2010
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2009` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2009_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2009 BEFORE INSERT
ON draw_desc_text_2009
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2008` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2008_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2008 BEFORE INSERT
ON draw_desc_text_2008
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2007` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2007_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2007 BEFORE INSERT
ON draw_desc_text_2007
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;


CREATE TABLE `draw_desc_text_2006` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2006_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2006 BEFORE INSERT
ON draw_desc_text_2006
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2005` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2005_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2005 BEFORE INSERT
ON draw_desc_text_2005
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2004` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2004_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2004 BEFORE INSERT
ON draw_desc_text_2004
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2003` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2003_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2003 BEFORE INSERT
ON draw_desc_text_2003
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2002` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2002_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2002 BEFORE INSERT
ON draw_desc_text_2002
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;

CREATE TABLE `draw_desc_text_2001` (
  `id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `document_number` bigint(16) DEFAULT NULL,
  `text` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `filename` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT current_timestamp(),
  `updated_date` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `version_indicator` date DEFAULT NULL,
  KEY `document_number` (`document_number`),
  CONSTRAINT `draw_desc_text_2001_ibfk_1` FOREIGN KEY (`document_number`) REFERENCES `publication` (`document_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_draw_desc_text_2001 BEFORE INSERT
ON draw_desc_text_2001
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;


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


DELIMITER ;;
DELIMITER ;

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


DELIMITER ;;
DELIMITER ;


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


DELIMITER ;;
DELIMITER ;


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


DELIMITER ;;
DELIMITER ;


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


DELIMITER ;;
DELIMITER ;


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


DELIMITER ;;
DELIMITER ;


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


DELIMITER ;;
DELIMITER ;


CREATE TABLE `rawlocation` (
  `id` varchar(512) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `location_id` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `city` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `state` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `country` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `country_transformed` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `latitude` float DEFAULT NULL,
  `longitude` float DEFAULT NULL,
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


DELIMITER ;;
DELIMITER ;


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
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TRIGGER before_insert_rel_app_text BEFORE INSERT
ON rel_app_text
FOR EACH row
  SET new.id = uuid();


DELIMITER ;;
DELIMITER ;


CREATE TABLE `us_parties` (
  `id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `document_number` bigint(16) NOT NULL,
  `name_first` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `name_last` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `organization` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `type` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
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


DELIMITER ;;
DELIMITER ;


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


DELIMITER ;;
DELIMITER ;

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


DELIMITER ;;
DELIMITER ;

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


DELIMITER ;;
DELIMITER ;

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


DELIMITER ;;
DELIMITER ;

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


DELIMITER ;;
DELIMITER ;
