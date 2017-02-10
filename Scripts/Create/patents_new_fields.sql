-- Create patents schema
-- Updated 2.2.17 by Sarah Kelley

/*REATE SCHEMA new_parser_qa; */
use new_parser_qa;

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `application`
--

DROP TABLE IF EXISTS `application`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

DROP TABLE IF EXISTS `examiner`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `examiner` (
  `id` varchar(36) NOT NULL,
  `patent_id` varchar(20) NOT NULL,
  `fname` varchar(30) DEFAULT NULL,
  `lname` varchar(40) DEFAULT NULL,
  `role` varchar(20) DEFAULT NULL,
  `group` varchar(20) DEFAULT NULL,
  `sequence` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`,`patent_id`),
  KEY `patent_id` (`patent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `assignee`
--

DROP TABLE IF EXISTS `assignee`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `assignee` (
  `id` varchar(36) NOT NULL,
  `type` varchar(10) DEFAULT NULL,
  `name_first` varchar(64) DEFAULT NULL,
  `name_last` varchar(64) DEFAULT NULL,
  `organization` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `assignee_preprocessed`
--

DROP TABLE IF EXISTS `assignee_preprocessed`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `assignee_preprocessed` (
  `id` varchar(36) NOT NULL,
  `type` varchar(10) DEFAULT NULL,
  `name_first` varchar(64) DEFAULT NULL,
  `name_last` varchar(64) DEFAULT NULL,
  `organization` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `claim`
--

DROP TABLE IF EXISTS `claim`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `claim` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `text` text,
  `dependent` int(11) DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `exemplary` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`),
  KEY `ix_claim_sequence` (`sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cpc_current`
--

DROP TABLE IF EXISTS `cpc_current`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cpc_group`
--

DROP TABLE IF EXISTS `cpc_group`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cpc_group` (
  `id` varchar(20) NOT NULL,
  `title` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cpc_subgroup`
--

DROP TABLE IF EXISTS `cpc_subgroup`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cpc_subgroup` (
  `id` varchar(20) NOT NULL,
  `title` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cpc_subsection`
--

DROP TABLE IF EXISTS `cpc_subsection`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cpc_subsection` (
  `id` varchar(20) NOT NULL,
  `title` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `foreigncitation`
--

DROP TABLE IF EXISTS `foreigncitation`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `government_interest`
--

DROP TABLE IF EXISTS `government_interest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `government_interest` (
  `patent_id` varchar(255) NOT NULL,
  `gi_statement` text,
  PRIMARY KEY (`patent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `government_organization`
--

DROP TABLE IF EXISTS `government_organization`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `government_organization` (
  `organization_id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `level_one` varchar(255) DEFAULT NULL,
  `level_two` varchar(255) DEFAULT NULL,
  `level_three` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`organization_id`)
) ENGINE=InnoDB AUTO_INCREMENT=137 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `inventor`
--

DROP TABLE IF EXISTS `inventor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `inventor` (
  `id` varchar(36) NOT NULL,
  `name_first` varchar(64) DEFAULT NULL,
  `name_last` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ipcr`
--

DROP TABLE IF EXISTS `ipcr`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `lawyer`
--

DROP TABLE IF EXISTS `lawyer`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `lawyer` (
  `id` varchar(36) NOT NULL,
  `name_first` varchar(64) DEFAULT NULL,
  `name_last` varchar(64) DEFAULT NULL,
  `organization` varchar(64) DEFAULT NULL,
  `country` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `location`
--

DROP TABLE IF EXISTS `location`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `location` (
  `id` varchar(128) NOT NULL,
  `city` varchar(128) DEFAULT NULL,
  `state` varchar(20) DEFAULT NULL,
  `country` varchar(10) DEFAULT NULL,
  `latitude` float DEFAULT NULL,
  `longitude` float DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_location_country` (`country`),
  KEY `dloc_idx2` (`city`,`state`,`country`),
  KEY `dloc_idx1` (`latitude`,`longitude`),
  KEY `ix_location_state` (`state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `location_assignee`
--

DROP TABLE IF EXISTS `location_assignee`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `location_assignee` (
  `location_id` varchar(128) DEFAULT NULL,
  `assignee_id` varchar(36) DEFAULT NULL,
  KEY `location_id` (`location_id`),
  KEY `assignee_id` (`assignee_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `location_inventor`
--

DROP TABLE IF EXISTS `location_inventor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `location_inventor` (
  `location_id` varchar(128) DEFAULT NULL,
  `inventor_id` varchar(36) DEFAULT NULL,
  KEY `location_id` (`location_id`),
  KEY `inventor_id` (`inventor_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `mainclass`
--

DROP TABLE IF EXISTS `mainclass`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `mainclass` (
  `id` varchar(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `mainclass_current`
--

DROP TABLE IF EXISTS `mainclass_current`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `mainclass_current` (
  `id` varchar(20) NOT NULL,
  `title` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `nber`
--

DROP TABLE IF EXISTS `nber`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `nber` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `category_id` varchar(20) DEFAULT NULL,
  `subcategory_id` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`),
  KEY `category_id` (`category_id`),
  KEY `subcategory_id` (`subcategory_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `nber_category`
--

DROP TABLE IF EXISTS `nber_category`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `nber_category` (
  `id` varchar(20) NOT NULL,
  `title` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `nber_subcategory`
--

DROP TABLE IF EXISTS `nber_subcategory`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `nber_subcategory` (
  `id` varchar(20) NOT NULL,
  `title` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `otherreference`
--

DROP TABLE IF EXISTS `otherreference`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `otherreference` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `text` text,
  `sequence` int(11) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`),
  FULLTEXT KEY `fti_text` (`text`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `patent`
--

DROP TABLE IF EXISTS `patent`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `patent` (
  `id` varchar(20) NOT NULL,
  `type` varchar(100) DEFAULT NULL,
  `number` varchar(64) DEFAULT NULL,
  `country` varchar(20) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `abstract` text,
  `title` text,
  `kind` varchar(10) DEFAULT NULL,
  `num_claims` int(11) DEFAULT NULL,
  `filename` varchar(120) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `pat_idx1` (`type`,`number`),
  KEY `pat_idx2` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `patent_assignee`
--

DROP TABLE IF EXISTS `patent_assignee`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `patent_assignee` (
  `patent_id` varchar(20) DEFAULT NULL,
  `assignee_id` varchar(36) DEFAULT NULL,
  KEY `ix_patent_assignee_patent_id` (`patent_id`),
  KEY `ix_patent_assignee_assignee_id` (`assignee_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `patent_contractawardnumber`
--

DROP TABLE IF EXISTS `patent_contractawardnumber`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `patent_contractawardnumber` (
  `patent_id` varchar(255) NOT NULL,
  `contract_award_number` varchar(255) NOT NULL,
  PRIMARY KEY (`patent_id`,`contract_award_number`),
  CONSTRAINT `patent_contractawardnumber_ibfk_1` FOREIGN KEY (`patent_id`) REFERENCES `government_interest` (`patent_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `patent_govintorg`
--

DROP TABLE IF EXISTS `patent_govintorg`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `patent_govintorg` (
  `patent_id` varchar(255) NOT NULL,
  `organization_id` int(11) NOT NULL,
  PRIMARY KEY (`patent_id`,`organization_id`),
  KEY `organization_id` (`organization_id`),
  CONSTRAINT `patent_govintorg_ibfk_1` FOREIGN KEY (`patent_id`) REFERENCES `government_interest` (`patent_id`) ON DELETE CASCADE,
  CONSTRAINT `patent_govintorg_ibfk_2` FOREIGN KEY (`organization_id`) REFERENCES `government_organization` (`organization_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `patent_inventor`
--

DROP TABLE IF EXISTS `patent_inventor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `patent_inventor` (
  `patent_id` varchar(20) DEFAULT NULL,
  `inventor_id` varchar(36) DEFAULT NULL,
  KEY `patent_id` (`patent_id`),
  KEY `inventor_id` (`inventor_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `patent_lawyer`
--

DROP TABLE IF EXISTS `patent_lawyer`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `patent_lawyer` (
  `patent_id` varchar(20) DEFAULT NULL,
  `lawyer_id` varchar(36) DEFAULT NULL,
  KEY `patent_id` (`patent_id`),
  KEY `lawyer_id` (`lawyer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `rawassignee`
--

DROP TABLE IF EXISTS `rawassignee`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `rawassignee_transformed`
--

DROP TABLE IF EXISTS `rawassignee_transformed`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `rawassignee_transformed` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `assignee_id` varchar(36) DEFAULT NULL,
  `rawlocation_id` varchar(128) DEFAULT NULL,
  `type` varchar(10) DEFAULT NULL,
  `name_first` varchar(64) DEFAULT NULL,
  `name_last` varchar(64) DEFAULT NULL,
  `organization` varchar(128) DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `patent_id` (`patent_id`),
  KEY `assignee_id` (`assignee_id`),
  KEY `rawlocation_id` (`rawlocation_id`),
  KEY `ix_rawassignee_sequence` (`sequence`),
  KEY `ix_organization` (`organization`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `rawinventor`
--

DROP TABLE IF EXISTS `rawinventor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `rawlawyer`
--

DROP TABLE IF EXISTS `rawlawyer`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `rawlawyer` (
  `uuid` varchar(36) NOT NULL,
  `lawyer_id` varchar(36) DEFAULT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `name_first` varchar(64) DEFAULT NULL,
  `name_last` varchar(64) DEFAULT NULL,
  `organization` varchar(64) DEFAULT NULL,
  `country` varchar(10) DEFAULT NULL,
  `sequence` int(11) DEFAULT NULL,
  `name_first_transformed` varchar(64) DEFAULT NULL,
  `name_last_transformed` varchar(64) DEFAULT NULL,
  `organization_transformed` varchar(64) DEFAULT NULL,
  `country_transformed` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `lawyer_id` (`lawyer_id`),
  KEY `patent_id` (`patent_id`),
  KEY `ix_rawlawyer_sequence` (`sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `rawlocation`
--

DROP TABLE IF EXISTS `rawlocation`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
  KEY `ix_rawlocation_country` (`country`),
  KEY `rawlocation_location_id_transformed` (`location_id_transformed`),
  KEY `ix_rawlocation_country_transformed_state` (`country_transformed`,`state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `subclass`
--

DROP TABLE IF EXISTS `subclass`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `subclass` (
  `id` varchar(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `subclass_current`
--

DROP TABLE IF EXISTS `subclass_current`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `subclass_current` (
  `id` varchar(20) NOT NULL,
  `title` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `temp`
--

DROP TABLE IF EXISTS `temp`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `temp` (
  `patent_id` varchar(20) NOT NULL,
  PRIMARY KEY (`patent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `temporary_update`
--

DROP TABLE IF EXISTS `temporary_update`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `temporary_update` (
  `pk` varchar(36) NOT NULL,
  `update` varchar(36) DEFAULT NULL,
  PRIMARY KEY (`pk`),
  KEY `ix_temporary_update_update` (`update`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `usapplicationcitation`
--

DROP TABLE IF EXISTS `usapplicationcitation`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `uspatentcitation`
--

DROP TABLE IF EXISTS `uspatentcitation`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `uspc`
--

DROP TABLE IF EXISTS `uspc`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `uspc_current`
--

DROP TABLE IF EXISTS `uspc_current`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `usreldoc`
--

DROP TABLE IF EXISTS `usreldoc`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `wipo`
--

DROP TABLE IF EXISTS `wipo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `wipo` (
  `patent_id` varchar(20) NOT NULL,
  `field_id` varchar(3) DEFAULT NULL,
  `sequence` int(10) unsigned NOT NULL,
  PRIMARY KEY (`patent_id`,`sequence`),
  KEY `ix_wipo_field_id` (`field_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `wipo_field`
--

DROP TABLE IF EXISTS `wipo_field`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `wipo_field` (
  `id` varchar(3) NOT NULL DEFAULT '',
  `sector_title` varchar(60) DEFAULT NULL,
  `field_title` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_wipo_field_sector_title` (`sector_title`),
  KEY `ix_wipo_field_field_title` (`field_title`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;



--
-- Table structure for table `brf_sum_text`
--

DROP TABLE IF EXISTS `brf_sum_text`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `brf_sum_text` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `text` text DEFAULT NULL,
  `sequence` int(10) unsigned NOT NULL,
  PRIMARY KEY (`uuid`),
  KEY `ix_brfsumtext_patent_id` (`patent_id`, `sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `detail_desc_text`
--

DROP TABLE IF EXISTS `detail_desc_text`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `detail_desc_text` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `text` text DEFAULT NULL,
  `sequence` int(10) unsigned NOT NULL,
  PRIMARY KEY (`uuid`),
  KEY `ix_detaildesctext_patent_id` (`patent_id`,`sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `draw_desc_text`
--

DROP TABLE IF EXISTS `draw_desc_text`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `draw_desc_text` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `text` text DEFAULT NULL,
  `sequence` int(10) unsigned NOT NULL,
  PRIMARY KEY (`uuid`),
  KEY `ix_detaildesctext_patent_id` (`patent_id`,`sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `rel_app_text`
--

DROP TABLE IF EXISTS `rel_app_text`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `rel_app_text` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) DEFAULT NULL,
  `text` text DEFAULT NULL,
  `sequence` int(10) unsigned NOT NULL,
  PRIMARY KEY (`uuid`),
  KEY `ix_relapptext_patent_id` (`patent_id`, `sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `raw_examiner`
--

DROP TABLE IF EXISTS `raw_examiner`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `raw_examiner` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(36) NOT NULL,
  `first_name` varchar(64) DEFAULT NULL,
  `last_name` varchar(64) DEFAULT NULL,
  `role` varchar(20) DEFAULT NULL,
  `department` smallint DEFAULT NULL,
  `sequence` int(10) unsigned NOT NULL,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `ix_rawexam_patidseq` (`patent_id`, `sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;



--
-- Table structure for table `drawing_spec`
-- 

DROP TABLE IF EXISTS `figures`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `figures` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) NOT NULL,
  `num_figures` int(11) DEFAULT NULL,
  `num_sheets` int(11) DEFAULT NULL, 
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `ix_figures_patentid` (`patent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `pct_regauth`
-- 


DROP TABLE IF EXISTS `pct_data`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `term_of_grant`
-- 

DROP TABLE IF EXISTS `us_term_of_grant`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `us_term_of_grant` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) NOT NULL,
  `lapse_of_patent` int(11) DEFAULT NULL,
  `disclaimer_date` date DEFAULT NULL, 
  `term_disclaimer` VARCHAR(128) DEFAULT NULL,
  `term_grant` VARCHAR(128) DEFAULT NULL, -- ska: confirm data type
  `term_extension` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `ix_ustergra_patentid` (`patent_id`),
  KEY `ix_ustergra_date` (`disclaimer_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `foreign_priority`
--

DROP TABLE IF EXISTS `foreign_priority`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `foreign_priority` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) NOT NULL,
  `sequence` int(11) DEFAULT NULL,
  `kind` varchar(10) DEFAULT NULL,
  `number` varchar(64) DEFAULT NULL,
  `date` date DEFAULT NULL,  
  `country` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `ix_forpri_patentid` (`patent_id`, `sequence`),
  KEY `ix_forpri_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;



--
-- Table structure for table `botanical`
--

DROP TABLE IF EXISTS `botanic`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `botanic` (
  `uuid` varchar(36) NOT NULL,
  `patent_id` varchar(20) NOT NULL,
  `latin_name` varchar(128) DEFAULT NULL,
  `variety` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `ix_botanic_patent_id` (`patent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

DROP TABLE IF EXISTS `non_inventor_applicant`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
  UNIQUE KEY `ix_noninvntappl_patentid` (`patent_id`),
  KEY `patent_id` (`patent_id`),
  KEY `rawlocation_id` (`rawlocation_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;





/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
