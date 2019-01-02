# BEGIN additional indexing

###################################################################################################################################


# 1:53:23

ALTER TABLE `PatentsView_20181127`.`application`
   ADD INDEX `ix_application_number` (`number`);

ALTER TABLE `PatentsView_20181127`.`application`
   ADD INDEX `ix_application_patent_id` (`patent_id`);

ALTER TABLE `PatentsView_20181127`.`assignee`
   ADD INDEX `ix_assignee_name_first` (`name_first`);

ALTER TABLE `PatentsView_20181127`.`assignee`
   ADD INDEX `ix_assignee_name_last` (`name_last`);

ALTER TABLE `PatentsView_20181127`.`assignee`
   ADD INDEX `ix_assignee_organization` (`organization`);

ALTER TABLE `PatentsView_20181127`.`assignee`
   ADD INDEX `ix_assignee_persistent_assignee_id` (`persistent_assignee_id`);

ALTER TABLE `PatentsView_20181127`.`assignee_cpc_subsection`
   ADD INDEX `ix_assignee_cpc_subsection_assignee_id` (`assignee_id`);

ALTER TABLE `PatentsView_20181127`.`assignee_cpc_subsection`
   ADD INDEX `ix_assignee_cpc_subsection_subsection_id` (`subsection_id`);

ALTER TABLE `PatentsView_20181127`.`assignee_cpc_group`
   ADD INDEX `ix_assignee_cpc_group_assignee_id` (`assignee_id`);

ALTER TABLE `PatentsView_20181127`.`assignee_cpc_group`
   ADD INDEX `ix_assignee_cpc_group_group_id` (`group_id`);

ALTER TABLE `PatentsView_20181127`.`assignee_inventor`
   ADD INDEX `ix_assignee_inventor_assignee_id` (`assignee_id`);

ALTER TABLE `PatentsView_20181127`.`assignee_inventor`
   ADD INDEX `ix_assignee_inventor_inventor_id` (`inventor_id`);

ALTER TABLE `PatentsView_20181127`.`assignee_nber_subcategory`
   ADD INDEX `ix_assignee_nber_subcategory_assignee_id` (`assignee_id`);

ALTER TABLE `PatentsView_20181127`.`assignee_nber_subcategory`
   ADD INDEX `ix_assignee_nber_subcategory_subcategory_id` (`subcategory_id`);

ALTER TABLE `PatentsView_20181127`.`assignee_uspc_mainclass`
   ADD INDEX `ix_assignee_uspc_mainclass_assignee_id` (`assignee_id`);

ALTER TABLE `PatentsView_20181127`.`assignee_uspc_mainclass`
   ADD INDEX `ix_assignee_uspc_mainclass_mainclass_id` (`mainclass_id`);

ALTER TABLE `PatentsView_20181127`.`assignee_year`
   ADD INDEX `ix_assignee_year_assignee_id` (`assignee_id`);

ALTER TABLE `PatentsView_20181127`.`assignee_year`
   ADD INDEX `ix_assignee_year_year` (`patent_year`);

ALTER TABLE `PatentsView_20181127`.`cpc_current_group`
   ADD INDEX `ix_cpc_current_group_group_id` (`group_id`);

ALTER TABLE `PatentsView_20181127`.`cpc_current_group`
   ADD INDEX `ix_cpc_current_group_title` (`group_title`);

ALTER TABLE `PatentsView_20181127`.`cpc_current_subsection`
   ADD INDEX `ix_cpc_current_subsection_subsection_id` (`subsection_id`);

ALTER TABLE `PatentsView_20181127`.`cpc_current_subsection`
   ADD INDEX `ix_cpc_current_subsection_title` (`subsection_title`);

ALTER TABLE `PatentsView_20181127`.`cpc_current`
   ADD INDEX `ix_cpc_current_group_id` (`group_id`);

ALTER TABLE `PatentsView_20181127`.`cpc_current`
   ADD INDEX `ix_cpc_current_subgroup_id` (`subgroup_id`);

ALTER TABLE `PatentsView_20181127`.`cpc_current`
   ADD INDEX `ix_cpc_current_subsection_id` (`subsection_id`);

ALTER TABLE `PatentsView_20181127`.`inventor`
   ADD INDEX `ix_inventor_name_first` (`name_first`);

ALTER TABLE `PatentsView_20181127`.`inventor`
   ADD INDEX `ix_inventor_name_last` (`name_last`);

ALTER TABLE `PatentsView_20181127`.`inventor`
   ADD INDEX `ix_inventor_persistent_inventor_id` (`persistent_inventor_id`);

ALTER TABLE `PatentsView_20181127`.`inventor_coinventor`
   ADD INDEX `ix_inventor_coinventor_inventor_id` (`inventor_id`);

ALTER TABLE `PatentsView_20181127`.`inventor_coinventor`
   ADD INDEX `ix_inventor_coinventor_coinventor_id` (`coinventor_id`);

ALTER TABLE `PatentsView_20181127`.`inventor_cpc_subsection`
   ADD INDEX `ix_inventor_cpc_subsection_inventor_id` (`inventor_id`);

ALTER TABLE `PatentsView_20181127`.`inventor_cpc_subsection`
   ADD INDEX `ix_inventor_cpc_subsection_subsection_id` (`subsection_id`);

ALTER TABLE `PatentsView_20181127`.`inventor_cpc_group`
   ADD INDEX `ix_inventor_cpc_group_inventor_id` (`inventor_id`);

ALTER TABLE `PatentsView_20181127`.`inventor_cpc_group`
   ADD INDEX `ix_inventor_cpc_group_group_id` (`group_id`);

ALTER TABLE `PatentsView_20181127`.`inventor_nber_subcategory`
   ADD INDEX `ix_inventor_nber_subcategory_inventor_id` (`inventor_id`);

ALTER TABLE `PatentsView_20181127`.`inventor_nber_subcategory`
   ADD INDEX `ix_inventor_nber_subcategory_subcategory_id` (`subcategory_id`);

ALTER TABLE `PatentsView_20181127`.`inventor_uspc_mainclass`
   ADD INDEX `ix_inventor_uspc_mainclass_inventor_id` (`inventor_id`);

ALTER TABLE `PatentsView_20181127`.`inventor_uspc_mainclass`
   ADD INDEX `ix_inventor_uspc_mainclass_mainclass_id` (`mainclass_id`);

ALTER TABLE `PatentsView_20181127`.`inventor_year`
   ADD INDEX `ix_inventor_year_inventor_id` (`inventor_id`);

ALTER TABLE `PatentsView_20181127`.`inventor_year`
   ADD INDEX `ix_inventor_year_year` (`patent_year`);

ALTER TABLE `PatentsView_20181127`.`ipcr`
   ADD INDEX `ix_ipcr_ipc_class` (`ipc_class`);

ALTER TABLE `PatentsView_20181127`.`location_assignee`
   ADD INDEX `ix_location_assignee_assignee_id` (`assignee_id`);

ALTER TABLE `PatentsView_20181127`.`location_inventor`
   ADD INDEX `ix_location_inventor_inventor_id` (`inventor_id`);

ALTER TABLE `PatentsView_20181127`.`location`
   ADD INDEX `ix_location_city` (`city`);

ALTER TABLE `PatentsView_20181127`.`location`
   ADD INDEX `ix_location_country` (`country`);

ALTER TABLE `PatentsView_20181127`.`location`
   ADD INDEX `ix_location_persistent_location_id` (`persistent_location_id`);

ALTER TABLE `PatentsView_20181127`.`location`
   ADD INDEX `ix_location_state` (`state`);

ALTER TABLE `PatentsView_20181127`.`location_cpc_subsection`
   ADD INDEX `ix_location_cpc_subsection_location_id` (`location_id`);

ALTER TABLE `PatentsView_20181127`.`location_cpc_subsection`
   ADD INDEX `ix_location_cpc_subsection_subsection_id` (`subsection_id`);

ALTER TABLE `PatentsView_20181127`.`location_cpc_group`
   ADD INDEX `ix_location_cpc_group_location_id` (`location_id`);

ALTER TABLE `PatentsView_20181127`.`location_cpc_group`
   ADD INDEX `ix_location_cpc_group_subsection_id` (`group_id`);

ALTER TABLE `PatentsView_20181127`.`location_nber_subcategory`
   ADD INDEX `ix_location_nber_subcategory_location_id` (`location_id`);

ALTER TABLE `PatentsView_20181127`.`location_nber_subcategory`
   ADD INDEX `ix_location_nber_subcategory_mainclass_id` (`subcategory_id`);

ALTER TABLE `PatentsView_20181127`.`location_uspc_mainclass`
   ADD INDEX `ix_location_uspc_mainclass_location_id` (`location_id`);

ALTER TABLE `PatentsView_20181127`.`location_uspc_mainclass`
   ADD INDEX `ix_location_uspc_mainclass_mainclass_id` (`mainclass_id`);

ALTER TABLE `PatentsView_20181127`.`location_year`
   ADD INDEX `ix_location_year_location_id` (`location_id`);

ALTER TABLE `PatentsView_20181127`.`location_year`
   ADD INDEX `ix_location_year_year` (`year`);

ALTER TABLE `PatentsView_20181127`.`nber`
   ADD INDEX `ix_nber_subcategory_id` (`subcategory_id`);

ALTER TABLE `PatentsView_20181127`.`nber`
   ADD INDEX `ix_nber_subcategory_title` (`subcategory_title`);

ALTER TABLE `PatentsView_20181127`.`patent_assignee`
   ADD INDEX `ix_patent_assignee_location_id` (`location_id`);

ALTER TABLE `PatentsView_20181127`.`patent_inventor`
   ADD INDEX `ix_patent_inventor_location_id` (`location_id`);

ALTER TABLE `PatentsView_20181127`.`patent`
   ADD INDEX `ix_patent_date` (`date`);

ALTER TABLE `PatentsView_20181127`.`patent`
   ADD INDEX `ix_patent_number` (`number`);

ALTER TABLE `PatentsView_20181127`.`patent`
   ADD INDEX `ix_patent_title` (`title` (128));

ALTER TABLE `PatentsView_20181127`.`patent`
   ADD INDEX `ix_patent_type` (`type`);

ALTER TABLE `PatentsView_20181127`.`patent`
   ADD INDEX `ix_patent_year` (`year`);

ALTER TABLE `PatentsView_20181127`.`usapplicationcitation`
   ADD INDEX `ix_usapplicationcitation_cited_application_id`
          (`cited_application_id`);

ALTER TABLE `PatentsView_20181127`.`uspatentcitation`
   ADD INDEX `ix_uspatentcitation_cited_patent_id` (`cited_patent_id`);

ALTER TABLE `PatentsView_20181127`.`uspc_current_mainclass`
   ADD INDEX `ix_uspc_current_mainclass_mainclass_id` (`mainclass_id`);

ALTER TABLE `PatentsView_20181127`.`uspc_current_mainclass`
   ADD INDEX `ix_uspc_current_mainclass_mainclass_title` (`mainclass_title`);

ALTER TABLE `PatentsView_20181127`.`uspc_current`
   ADD INDEX `ix_uspc_current_mainclass_id` (`mainclass_id`);

ALTER TABLE `PatentsView_20181127`.`uspc_current`
   ADD INDEX `ix_uspc_current_subclass_id` (`subclass_id`);

ALTER TABLE `PatentsView_20181127`.`uspc_current`
   ADD INDEX `ix_uspc_current_mainclass_title` (`mainclass_title`);

ALTER TABLE `PatentsView_20181127`.`uspc_current`
   ADD INDEX `ix_uspc_current_subclass_title` (`subclass_title`);

ALTER TABLE `PatentsView_20181127`.`assignee`
   ADD INDEX `ix_assignee_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`assignee`
   ADD INDEX `ix_assignee_num_inventors` (`num_inventors`);

ALTER TABLE `PatentsView_20181127`.`assignee_cpc_subsection`
   ADD INDEX `ix_assignee_cpc_subsection_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`assignee_cpc_group`
   ADD INDEX `ix_assignee_cpc_group_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`assignee_inventor`
   ADD INDEX `ix_assignee_inventor_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`assignee_nber_subcategory`
   ADD INDEX `ix_assignee_nber_subcategory_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`assignee_uspc_mainclass`
   ADD INDEX `ix_assignee_uspc_mainclass_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`assignee_year`
   ADD INDEX `ix_assignee_year_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`cpc_current`
   ADD INDEX `ix_cpc_current_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`cpc_current`
   ADD INDEX `ix_cpc_current_num_inventors` (`num_inventors`);

ALTER TABLE `PatentsView_20181127`.`cpc_current`
   ADD INDEX `ix_cpc_current_num_assignees` (`num_assignees`);

ALTER TABLE `PatentsView_20181127`.`cpc_current_subsection`
   ADD INDEX `ix_cpc_current_subsection_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`cpc_current_subsection`
   ADD INDEX `ix_cpc_current_subsection_num_inventors` (`num_inventors`);

ALTER TABLE `PatentsView_20181127`.`cpc_current_subsection`
   ADD INDEX `ix_cpc_current_subsection_num_assignees` (`num_assignees`);

ALTER TABLE `PatentsView_20181127`.`cpc_current_subsection_patent_year`
   ADD INDEX `ix_cpc_current_subsection_patent_year_num_patents`
          (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`cpc_current`
   ADD INDEX `ix_cpc_current_num_patents_group` (`num_patents_group`);

ALTER TABLE `PatentsView_20181127`.`cpc_current`
   ADD INDEX `ix_cpc_current_num_inventors_group` (`num_inventors_group`);

ALTER TABLE `PatentsView_20181127`.`cpc_current`
   ADD INDEX `ix_cpc_current_num_assignees_group` (`num_assignees_group`);

ALTER TABLE `PatentsView_20181127`.`cpc_current_group`
   ADD INDEX `ix_cpc_current_group_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`cpc_current_group`
   ADD INDEX `ix_cpc_current_group_num_inventors` (`num_inventors`);

ALTER TABLE `PatentsView_20181127`.`cpc_current_group`
   ADD INDEX `ix_cpc_current_group_num_assignees` (`num_assignees`);

ALTER TABLE `PatentsView_20181127`.`cpc_current_group_patent_year`
   ADD INDEX `ix_cpc_current_group_patent_year_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`inventor`
   ADD INDEX `ix_inventor_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`inventor`
   ADD INDEX `ix_inventor_num_assignees` (`num_assignees`);

ALTER TABLE `PatentsView_20181127`.`inventor_coinventor`
   ADD INDEX `ix_inventor_coinventor_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`inventor_cpc_subsection`
   ADD INDEX `ix_inventor_cpc_subsection_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`inventor_cpc_group`
   ADD INDEX `ix_inventor_cpc_group_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`inventor_nber_subcategory`
   ADD INDEX `ix_inventor_nber_subcategory_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`inventor_uspc_mainclass`
   ADD INDEX `ix_inventor_uspc_mainclass_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`inventor_year`
   ADD INDEX `ix_inventor_year_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`ipcr`
   ADD INDEX `ix_ipcr_num_inventors` (`num_inventors`);

ALTER TABLE `PatentsView_20181127`.`ipcr`
   ADD INDEX `ix_ipcr_num_assignees` (`num_assignees`);

ALTER TABLE `PatentsView_20181127`.`location`
   ADD INDEX `ix_location_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`location`
   ADD INDEX `ix_location_num_inventors` (`num_inventors`);

ALTER TABLE `PatentsView_20181127`.`location`
   ADD INDEX `ix_location_num_assignees` (`num_assignees`);

ALTER TABLE `PatentsView_20181127`.`location_assignee`
   ADD INDEX `ix_location_assignee_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`location_cpc_subsection`
   ADD INDEX `ix_location_cpc_subsection_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`location_cpc_group`
   ADD INDEX `ix_location_cpc_group_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`location_inventor`
   ADD INDEX `ix_location_inventor_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`location_nber_subcategory`
   ADD INDEX `ix_location_nber_subcategory_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`location_uspc_mainclass`
   ADD INDEX `ix_location_uspc_mainclass_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`location_year`
   ADD INDEX `ix_location_year_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`nber`
   ADD INDEX `ix_nber_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`nber`
   ADD INDEX `ix_nber_num_inventors` (`num_inventors`);

ALTER TABLE `PatentsView_20181127`.`nber`
   ADD INDEX `ix_nber_num_assignees` (`num_assignees`);

ALTER TABLE `PatentsView_20181127`.`nber_subcategory_patent_year`
   ADD INDEX `ix_nber_subcategory_patent_year_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`uspc_current`
   ADD INDEX `ix_uspc_current_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`uspc_current`
   ADD INDEX `ix_uspc_current_num_inventors` (`num_inventors`);

ALTER TABLE `PatentsView_20181127`.`uspc_current`
   ADD INDEX `ix_uspc_current_num_assignees` (`num_assignees`);

ALTER TABLE `PatentsView_20181127`.`uspc_current_mainclass`
   ADD INDEX `ix_uspc_current_mainclass_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`uspc_current_mainclass`
   ADD INDEX `ix_uspc_current_mainclass_num_inventors` (`num_inventors`);

ALTER TABLE `PatentsView_20181127`.`uspc_current_mainclass`
   ADD INDEX `ix_uspc_current_mainclass_num_assignees` (`num_assignees`);

ALTER TABLE `PatentsView_20181127`.`uspc_current_mainclass_patent_year`
   ADD INDEX `ix_uspc_current_mainclass_patent_year_num_patents`
          (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`inventor`
   ADD INDEX `ix_inventor_lastknown_location_id` (`lastknown_location_id`);

ALTER TABLE `PatentsView_20181127`.`inventor`
   ADD INDEX `ix_inventor_lastknown_persistent_location_id`
          (`lastknown_persistent_location_id`);

ALTER TABLE `PatentsView_20181127`.`inventor`
   ADD INDEX `ix_inventor_first_seen_date` (`first_seen_date`);

ALTER TABLE `PatentsView_20181127`.`inventor`
   ADD INDEX `ix_inventor_last_seen_date` (`last_seen_date`);

ALTER TABLE `PatentsView_20181127`.`nber`
   ADD INDEX `ix_nber_category_id` (`category_id`);

ALTER TABLE `PatentsView_20181127`.`assignee`
   ADD INDEX `ix_assignee_lastknown_location_id` (`lastknown_location_id`);

ALTER TABLE `PatentsView_20181127`.`assignee`
   ADD INDEX `ix_assignee_lastknown_persistent_location_id`
          (`lastknown_persistent_location_id`);

ALTER TABLE `PatentsView_20181127`.`assignee`
   ADD INDEX `ix_assignee_last_seen_date` (`last_seen_date`);

ALTER TABLE `PatentsView_20181127`.`assignee`
   ADD INDEX `ix_assignee_first_seen_date` (`first_seen_date`);

ALTER TABLE `PatentsView_20181127`.`patent`
   ADD INDEX `ix_patent_country` (`country`);

ALTER TABLE `PatentsView_20181127`.`patent`
   ADD INDEX `ix_patent_num_claims` (`num_claims`);

ALTER TABLE `PatentsView_20181127`.`patent`
   ADD INDEX `ix_patent_firstnamed_assignee_id` (`firstnamed_assignee_id`);

ALTER TABLE `PatentsView_20181127`.`patent`
   ADD INDEX `ix_patent_firstnamed_assignee_persistent_id`
          (`firstnamed_assignee_persistent_id`);

ALTER TABLE `PatentsView_20181127`.`patent`
   ADD INDEX `ix_patent_firstnamed_assignee_persistent_location_id`
          (`firstnamed_assignee_persistent_location_id`);

ALTER TABLE `PatentsView_20181127`.`patent`
   ADD INDEX `ix_patent_firstnamed_assignee_location_id`
          (`firstnamed_assignee_location_id`);

ALTER TABLE `PatentsView_20181127`.`patent`
   ADD INDEX `ix_patent_firstnamed_inventor_persistent_id`
          (`firstnamed_inventor_persistent_id`);

ALTER TABLE `PatentsView_20181127`.`patent`
   ADD INDEX `ix_patent_firstnamed_inventor_persistent_location_id`
          (`firstnamed_inventor_persistent_location_id`);

ALTER TABLE `PatentsView_20181127`.`patent`
   ADD INDEX `ix_patent_firstnamed_inventor_id` (`firstnamed_inventor_id`);

ALTER TABLE `PatentsView_20181127`.`patent`
   ADD INDEX `ix_patent_firstnamed_inventor_location_id`
          (`firstnamed_inventor_location_id`);

ALTER TABLE `PatentsView_20181127`.`lawyer`
   ADD INDEX `ix_lawyer_name_first` (`name_first`);

ALTER TABLE `PatentsView_20181127`.`lawyer`
   ADD INDEX `ix_lawyer_name_last` (`name_last`);

ALTER TABLE `PatentsView_20181127`.`lawyer`
   ADD INDEX `ix_lawyer_organization` (`organization`);

ALTER TABLE `PatentsView_20181127`.`lawyer`
   ADD INDEX `ix_lawyer_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`lawyer`
   ADD INDEX `ix_lawyer_num_assignees` (`num_assignees`);

ALTER TABLE `PatentsView_20181127`.`lawyer`
   ADD INDEX `ix_lawyer_num_inventors` (`num_inventors`);

ALTER TABLE `PatentsView_20181127`.`lawyer`
   ADD INDEX `ix_lawyer_first_seen_date` (`first_seen_date`);

ALTER TABLE `PatentsView_20181127`.`lawyer`
   ADD INDEX `ix_lawyer_last_seen_date` (`last_seen_date`);

ALTER TABLE `PatentsView_20181127`.`lawyer`
   ADD INDEX `ix_lawyer_persistent_lawyer_id` (`persistent_lawyer_id`);

ALTER TABLE `PatentsView_20181127`.`examiner`
   ADD INDEX `ix_examiner_name_first` (`name_first`);

ALTER TABLE `PatentsView_20181127`.`examiner`
   ADD INDEX `ix_examiner_name_last` (`name_last`);

ALTER TABLE `PatentsView_20181127`.`examiner`
   ADD INDEX `ix_examiner_role` (`role`);

ALTER TABLE `PatentsView_20181127`.`examiner`
   ADD INDEX `ix_examiner_group` (`group`);

ALTER TABLE `PatentsView_20181127`.`examiner`
   ADD INDEX `ix_examiner_persistent_examiner_id` (`persistent_examiner_id`);

ALTER TABLE `PatentsView_20181127`.`foreignpriority`
   ADD INDEX `ix_foreignpriority_foreign_doc_number` (`foreign_doc_number`);

ALTER TABLE `PatentsView_20181127`.`foreignpriority`
   ADD INDEX `ix_foreignpriority_date` (`date`);

ALTER TABLE `PatentsView_20181127`.`foreignpriority`
   ADD INDEX `ix_foreignpriority_country` (`country`);

ALTER TABLE `PatentsView_20181127`.`foreignpriority`
   ADD INDEX `ix_foreignpriority_kind` (`kind`);

ALTER TABLE `PatentsView_20181127`.`location`
   ADD INDEX `ix_location_county_fips` (`county_fips`);

ALTER TABLE `PatentsView_20181127`.`location`
   ADD INDEX `ix_location_state_fips` (`state_fips`);

ALTER TABLE `PatentsView_20181127`.`location`
   ADD INDEX `ix_location_county` (`county`);

ALTER TABLE `PatentsView_20181127`.`pctdata`
   ADD INDEX `ix_pctdata_doc_type` (`doc_type`);

ALTER TABLE `PatentsView_20181127`.`pctdata`
   ADD INDEX `ix_pctdata_doc_number` (`doc_number`);

ALTER TABLE `PatentsView_20181127`.`pctdata`
   ADD INDEX `ix_pctdata_date` (`date`);

ALTER TABLE `PatentsView_20181127`.`pctdata`
   ADD INDEX `ix_pctdata_102_date` (`102_date`);

ALTER TABLE `PatentsView_20181127`.`pctdata`
   ADD INDEX `ix_pctdata_371_date` (`371_date`);








# END additional indexing

#####################################################################################################################################

# BEGIN new class table creation
#####################################################################################################################################

CREATE TABLE IF NOT EXISTS `PatentsView_20181127`.`cpc_subsection`
(
   id                 VARCHAR(20) PRIMARY KEY,
   title              VARCHAR(256),
   num_patents        INT(10) UNSIGNED,
   num_inventors      INT(10) UNSIGNED,
   num_assignees      INT(10) UNSIGNED,
   first_seen_date    DATE,
   last_seen_date     DATE,
   years_active       SMALLINT(5) UNSIGNED
);

CREATE TABLE IF NOT EXISTS `PatentsView_20181127`.`cpc_subgroup`
(
   id       VARCHAR(20) PRIMARY KEY,
   title    VARCHAR(512)
);

CREATE TABLE IF NOT EXISTS `PatentsView_20181127`.`cpc_group`
(
   id                 VARCHAR(20) PRIMARY KEY,
   title              VARCHAR(256),
   num_patents        INT(10) UNSIGNED,
   num_inventors      INT(10) UNSIGNED,
   num_assignees      INT(10) UNSIGNED,
   first_seen_date    DATE,
   last_seen_date     DATE,
   years_active       SMALLINT(5) UNSIGNED
);

CREATE TABLE IF NOT EXISTS `PatentsView_20181127`.`nber_category`
(
   id       VARCHAR(20) PRIMARY KEY,
   title    VARCHAR(512)
);

CREATE TABLE IF NOT EXISTS `PatentsView_20181127`.nber_subcategory
(
   id                 VARCHAR(20) PRIMARY KEY,
   title              VARCHAR(512),
   num_patents        INT(10) UNSIGNED,
   num_inventors      INT(10) UNSIGNED,
   num_assignees      INT(10) UNSIGNED,
   first_seen_date    DATE,
   last_seen_date     DATE,
   years_active       SMALLINT(5) UNSIGNED
);

CREATE TABLE IF NOT EXISTS `PatentsView_20181127`.uspc_mainclass
(
   id                 VARCHAR(20) PRIMARY KEY,
   title              VARCHAR(256),
   num_patents        INT(10) UNSIGNED,
   num_inventors      INT(10) UNSIGNED,
   num_assignees      INT(10) UNSIGNED,
   first_seen_date    DATE,
   last_seen_date     DATE,
   years_active       SMALLINT(5) UNSIGNED
);

CREATE TABLE IF NOT EXISTS `PatentsView_20181127`.uspc_subclass
(
   id       VARCHAR(20) PRIMARY KEY,
   title    VARCHAR(512)
);

CREATE TABLE IF NOT EXISTS `PatentsView_20181127`.`nber_copy`
(
   `patent_id`         VARCHAR(20) NOT NULL,
   `category_id`       VARCHAR(20) DEFAULT NULL,
   `subcategory_id`    VARCHAR(20) DEFAULT NULL,
   PRIMARY KEY(`patent_id`),
   KEY `ix_nber_subcategory_id` (`subcategory_id`),
   KEY `ix_nber_category_id` (`category_id`)
);

CREATE TABLE IF NOT EXISTS `PatentsView_20181127`.`cpc_current_copy`
(
   `patent_id`        VARCHAR(20) NOT NULL,
   `sequence`         INT(10) UNSIGNED NOT NULL,
   `section_id`       VARCHAR(10) DEFAULT NULL,
   `subsection_id`    VARCHAR(20) DEFAULT NULL,
   `group_id`         VARCHAR(20) DEFAULT NULL,
   `subgroup_id`      VARCHAR(20) DEFAULT NULL,
   `category`         VARCHAR(36) DEFAULT NULL,
   PRIMARY KEY(`patent_id`, `sequence`),
   KEY `ix_cpc_current_group_id` (`group_id`),
   KEY `ix_cpc_current_subgroup_id` (`subgroup_id`),
   KEY `ix_cpc_current_subsection_id` (`subsection_id`),
   KEY `ix_cpc_current_section_id` (`section_id`),
   KEY `ix_cpc_current_sequence` (`sequence`)
);

CREATE TABLE IF NOT EXISTS `PatentsView_20181127`.`cpc_current_subsection_copy`
(
   `patent_id`        VARCHAR(20) NOT NULL,
   `section_id`       VARCHAR(10) DEFAULT NULL,
   `subsection_id`    VARCHAR(20) NOT NULL DEFAULT '',
   PRIMARY KEY(`patent_id`, `subsection_id`),
   KEY `ix_cpc_current_subsection_subsection_id` (`subsection_id`),
   KEY `ix_cpc_current_subsection_section_id` (`section_id`)
);

CREATE TABLE IF NOT EXISTS `PatentsView_20181127`.`cpc_current_group_copy`
(
   `patent_id`     VARCHAR(20) NOT NULL,
   `section_id`    VARCHAR(10) DEFAULT NULL,
   `group_id`      VARCHAR(20) NOT NULL DEFAULT '',
   PRIMARY KEY(`patent_id`, `group_id`),
   KEY `ix_cpc_current_group_group_id` (`group_id`),
   KEY `ix_cpc_current_group_section_id` (`section_id`)
);

CREATE TABLE IF NOT EXISTS `PatentsView_20181127`.`uspc_current_mainclass_copy`
(
   `patent_id`       VARCHAR(20) NOT NULL,
   `mainclass_id`    VARCHAR(20) NOT NULL DEFAULT '',
   PRIMARY KEY(`patent_id`, `mainclass_id`),
   KEY `ix_uspc_current_mainclass_mainclass_id` (`mainclass_id`)
);

CREATE TABLE IF NOT EXISTS `PatentsView_20181127`.`uspc_current_copy`
(
   `patent_id`       VARCHAR(20) NOT NULL,
   `sequence`        INT(10) UNSIGNED NOT NULL,
   `mainclass_id`    VARCHAR(20) DEFAULT NULL,
   `subclass_id`     VARCHAR(20) DEFAULT NULL,
   PRIMARY KEY(`patent_id`, `sequence`),
   KEY `ix_uspc_current_mainclass_id` (`mainclass_id`),
   KEY `ix_uspc_current_subclass_id` (`subclass_id`),
   KEY `ix_uspc_current_sequence` (`sequence`)
);

# END new class table creation
#####################################################################################################################################

# BEGIN new class table population
#####################################################################################################################################


INSERT INTO `PatentsView_20181127`.cpc_subsection
     SELECT subsection_id,
            subsection_title,
            num_patents,
            num_inventors,
            num_assignees,
            first_seen_date,
            last_seen_date,
            years_active
       FROM `PatentsView_20181127`.cpc_current
   GROUP BY subsection_id;

INSERT INTO `PatentsView_20181127`.cpc_group
     SELECT group_id,
            group_title,
            num_patents_group,
            num_inventors_group,
            num_assignees_group,
            first_seen_date_group,
            last_seen_date_group,
            years_active_group
       FROM `PatentsView_20181127`.cpc_current
   GROUP BY group_id;

INSERT INTO `PatentsView_20181127`.cpc_subgroup
     SELECT subgroup_id, subgroup_title
       FROM `PatentsView_20181127`.cpc_current
   GROUP BY subgroup_id;

INSERT INTO `PatentsView_20181127`.nber_category
     SELECT category_id, category_title
       FROM `PatentsView_20181127`.nber
   GROUP BY category_id;

INSERT INTO `PatentsView_20181127`.nber_subcategory
     SELECT subcategory_id,
            subcategory_title,
            num_patents,
            num_inventors,
            num_assignees,
            first_seen_date,
            last_seen_date,
            years_active
       FROM `PatentsView_20181127`.nber
   GROUP BY subcategory_id;

INSERT INTO `PatentsView_20181127`.uspc_mainclass
     SELECT mainclass_id,
            mainclass_title,
            num_patents,
            num_inventors,
            num_assignees,
            first_seen_date,
            last_seen_date,
            years_active
       FROM `PatentsView_20181127`.uspc_current
   GROUP BY mainclass_id;

INSERT INTO `PatentsView_20181127`.uspc_subclass
     SELECT subclass_id, subclass_title
       FROM `PatentsView_20181127`.uspc_current
   GROUP BY subclass_id;

INSERT INTO `PatentsView_20181127`.uspc_current_mainclass_copy
   SELECT DISTINCT patent_id, mainclass_id
     FROM `PatentsView_20181127`.uspc_current_mainclass;

INSERT INTO `PatentsView_20181127`.cpc_current_subsection_copy
   SELECT DISTINCT patent_id, section_id, subsection_id
     FROM `PatentsView_20181127`.cpc_current_subsection;

INSERT INTO `PatentsView_20181127`.cpc_current_group_copy
   SELECT DISTINCT patent_id, section_id, group_id
     FROM `PatentsView_20181127`.cpc_current_group;

INSERT INTO `PatentsView_20181127`.uspc_current_copy
   SELECT DISTINCT patent_id,
                   sequence,
                   mainclass_id,
                   subclass_id
     FROM `PatentsView_20181127`.uspc_current;

INSERT INTO `PatentsView_20181127`.cpc_current_copy
   SELECT DISTINCT patent_id,
                   sequence,
                   section_id,
                   subsection_id,
                   group_id,
                   subgroup_id,
                   category
     FROM `PatentsView_20181127`.cpc_current;

INSERT INTO `PatentsView_20181127`.nber_copy
   SELECT DISTINCT patent_id, category_id, subcategory_id
     FROM `PatentsView_20181127`.nber;

# END new class table population
#####################################################################################################################################


# BEGIN new class table indexing
#####################################################################################################################################


ALTER TABLE `PatentsView_20181127`.`uspc_mainclass`
   ADD INDEX `ix_uspc_mainclass_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`uspc_mainclass`
   ADD INDEX `ix_uspc_mainclass_num_inventors` (`num_inventors`);

ALTER TABLE `PatentsView_20181127`.`uspc_mainclass`
   ADD INDEX `ix_uspc_mainclass_num_assignees` (`num_assignees`);

ALTER TABLE `PatentsView_20181127`.`nber_subcategory`
   ADD INDEX `ix_nber_subcategory_num_inventors` (`num_inventors`);

ALTER TABLE `PatentsView_20181127`.`nber_subcategory`
   ADD INDEX `ix_nber_subcategory_num_assignees` (`num_assignees`);

ALTER TABLE `PatentsView_20181127`.`nber_subcategory`
   ADD INDEX `ix_nber_subcategory_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`cpc_subsection`
   ADD INDEX `ix_cpc_subsection_num_inventors` (`num_inventors`);

ALTER TABLE `PatentsView_20181127`.`cpc_subsection`
   ADD INDEX `ix_cpc_subsection_num_assignees` (`num_assignees`);

ALTER TABLE `PatentsView_20181127`.`cpc_subsection`
   ADD INDEX `ix_cpc_subsection_num_patents` (`num_patents`);

ALTER TABLE `PatentsView_20181127`.`cpc_group`
   ADD INDEX `ix_cpc_group_num_inventors` (`num_inventors`);

ALTER TABLE `PatentsView_20181127`.`cpc_group`
   ADD INDEX `ix_cpc_group_num_assignees` (`num_assignees`);

ALTER TABLE `PatentsView_20181127`.`cpc_group`
   ADD INDEX `ix_cpc_group_num_patents` (`num_patents`);


# END new class table indexing
#####################################################################################################################################

# BEGIN inventor_rawinventor alias

###############################################################################################################################

CREATE TABLE IF NOT EXISTS `PatentsView_20181127`.inventor_rawinventor
(
   uuid           INT(10) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
   name_first     VARCHAR(64),
   name_last      VARCHAR(64),
   patent_id      VARCHAR(20),
   inventor_id    INT(10) UNSIGNED
);

INSERT INTO `PatentsView_20181127`.`inventor_rawinventor`(name_first,
                                                          name_last,
                                                          patent_id,
                                                          inventor_id)
   SELECT DISTINCT ri.name_first,
                   ri.name_last,
                   ri.patent_id,
                   repi.inventor_id
     FROM `PatentsView_20181127`.`inventor` repi
          LEFT JOIN `patent_20181127`.`rawinventor` ri
             ON ri.inventor_id = repi.persistent_inventor_id;

ALTER TABLE `PatentsView_20181127`.`inventor_rawinventor`
   ADD INDEX `ix_inventor_rawinventor_name_first` (`name_first`);

ALTER TABLE `PatentsView_20181127`.`inventor_rawinventor`
   ADD INDEX `ix_inventor_rawinventor_name_last` (`name_last`);

ALTER TABLE `PatentsView_20181127`.`inventor_rawinventor`
   ADD INDEX `ix_inventor_rawinventor_inventor_id` (`inventor_id`);

ALTER TABLE `PatentsView_20181127`.`inventor_rawinventor`
   ADD INDEX `ix_inventor_rawinventor_patent_id` (`patent_id`);

# END inventor_rawinventor alias

###############################################################################################################################

# BEGIN WIPO fields tables

###############################################################################################################################


DROP TABLE IF EXISTS `PatentsView_20181127`.`wipo`;

CREATE TABLE `PatentsView_20181127`.`wipo`
(
   `patent_id`    VARCHAR(20) NOT NULL,
   `field_id`     VARCHAR(3) DEFAULT NULL,
   `sequence`     INT(10) UNSIGNED NOT NULL,
   PRIMARY KEY(`patent_id`, `sequence`),
   KEY `ix_wipo_field_id` (`field_id`)
)
ENGINE = INNODB
DEFAULT CHARSET = utf8;

DROP TABLE IF EXISTS `PatentsView_20181127`.`wipo_field`;

CREATE TABLE `PatentsView_20181127`.`wipo_field`
(
   `id`              VARCHAR(3) NOT NULL,
   `sector_title`    VARCHAR(60) DEFAULT NULL,
   `field_title`     VARCHAR(255) DEFAULT NULL,
   PRIMARY KEY(`id`),
   KEY `ix_wipo_field_sector_title` (`sector_title`),
   KEY `ix_wipo_field_field_title` (`field_title`)
)
ENGINE = INNODB
DEFAULT CHARSET = utf8;

INSERT INTO `PatentsView_20181127`.`wipo`
   SELECT * FROM `patent_20181127`.`wipo`;

INSERT INTO `PatentsView_20181127`.`wipo_field`
   SELECT * FROM `patent_20181127`.`wipo_field`;

# END WIPO fields tables

###############################################################################################################################

# BEGIN Government interest tables

###############################################################################################################################
DROP TABLE IF EXISTS `PatentsView_20181127`.`government_interest`;

CREATE TABLE `PatentsView_20181127`.`government_interest`
(
   `patent_id`       VARCHAR(255) NOT NULL,
   `gi_statement`    TEXT,
   PRIMARY KEY(`patent_id`)
)
ENGINE = INNODB
DEFAULT CHARSET = utf8;

DROP TABLE IF EXISTS `PatentsView_20181127`.`government_organization`;

CREATE TABLE IF NOT EXISTS `PatentsView_20181127`.`government_organization`
(
   `organization_id`    INT(11) NOT NULL AUTO_INCREMENT,
   `name`               VARCHAR(255) DEFAULT NULL,
   `level_one`          VARCHAR(255) DEFAULT NULL,
   `level_two`          VARCHAR(255) DEFAULT NULL,
   `level_three`        VARCHAR(255) DEFAULT NULL,
   PRIMARY KEY(`organization_id`)
)
ENGINE = INNODB
AUTO_INCREMENT = 137
DEFAULT CHARSET = utf8;

DROP TABLE IF EXISTS `PatentsView_20181127`.`patent_contractawardnumber`;

CREATE TABLE `PatentsView_20181127`.`patent_contractawardnumber`
(
   `patent_id`                VARCHAR(24) NOT NULL,
   `contract_award_number`    VARCHAR(255) NULL,
   PRIMARY KEY(`patent_id`, `contract_award_number`),
   CONSTRAINT `patent_contractawardnumber_ibfk_1` FOREIGN KEY(`patent_id`)
      REFERENCES `PatentsView_20181127`.`government_interest` (`patent_id`)
         ON DELETE CASCADE
)
ENGINE = INNODB
DEFAULT CHARSET = utf8;

DROP TABLE IF EXISTS `PatentsView_20181127`.`patent_govintorg`;
#CREATE TABLE IF NOT EXISTS `PatentsView_20181127`.`patent_govintorg` (
#   `patent_id` varchar(255) NOT NULL,
#   `organization_id` int(11) NOT NULL,
#   PRIMARY KEY (`patent_id`,`organization_id`),
#   KEY `organization_id` (`organization_id`),
#   CONSTRAINT `patent_govintorg_ibfk_1` FOREIGN KEY (`patent_id`) REFERENCES `PatentsView_20181127`.`government_interest` (`patent_id`) ON DELETE CASCADE,
#   CONSTRAINT `patent_govintorg_ibfk_2` FOREIGN KEY (`organization_id`) REFERENCES `PatentsView_20181127`.`government_organization` (`organization_id`) ON DELETE CASCADE
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `PatentsView_20181127`.`patent_govintorg`
(
   `patent_id`          VARCHAR(255) NOT NULL,
   `organization_id`    INT(11) NOT NULL,
   PRIMARY KEY(`patent_id`, `organization_id`),
   KEY `organization_id` (`organization_id`),
   CONSTRAINT `patent_govintorg_ibfk_2` FOREIGN KEY(`organization_id`)
      REFERENCES `PatentsView_20181127`.`government_organization`
         (`organization_id`)
         ON DELETE CASCADE
)
ENGINE = INNODB
DEFAULT CHARSET = utf8;



INSERT INTO `PatentsView_20181127`.`government_interest`
   SELECT * FROM `patent_20181127`.`government_interest`;

INSERT INTO `PatentsView_20181127`.`government_organization`
   SELECT * FROM `patent_20181127`.`government_organization`;

INSERT INTO `PatentsView_20181127`.`patent_contractawardnumber`
   SELECT * FROM `patent_20181127`.`patent_contractawardnumber`;

INSERT INTO `PatentsView_20181127`.`patent_govintorg`
   SELECT * FROM `patent_20181127`.`patent_govintorg`;

ALTER TABLE `PatentsView_20181127`.`government_organization`
   ADD INDEX `ix_government_organization_name` (`name`);

ALTER TABLE `PatentsView_20181127`.`government_organization`
   ADD INDEX `ix_government_organization_level_one` (`level_one`);

ALTER TABLE `PatentsView_20181127`.`government_organization`
   ADD INDEX `ix_government_organization_level_two` (`level_two`);

ALTER TABLE `PatentsView_20181127`.`government_organization`
   ADD INDEX `ix_government_organization_level_three` (`level_three`);

# END Government interest tables

###############################################################################################################################

# Make the claim table
#takes a very long time, idk why quite so long

CREATE TABLE `PatentsView_20181127`.`claim`
LIKE `patent_20181127`.`claim`;

INSERT INTO `PatentsView_20181127`.`claim`
   SELECT * FROM `patent_20181127`.`claim`;





# Add final (v.slow) indexes

# BEGIN full text indexing ####################################################################################################################################


# According to documentation, it is faster to load a table without FTI then add the
# FTI afterwards.  In SQL Server world, we also found this to be true of regular indexes, fwiw...
# 3:16:38

ALTER TABLE `PatentsView_20181127`.`patent`
   ADD FULLTEXT INDEX `fti_patent_abstract` (`abstract`);

ALTER TABLE `PatentsView_20181127`.`patent`
   ADD FULLTEXT INDEX `fti_patent_title` (`title`);

ALTER TABLE `PatentsView_20181127`.`government_interest`
   ADD FULLTEXT INDEX `fti_government_interest_gi_statement` (`gi_statement`);

ALTER TABLE `PatentsView_20181127`.`uspc_current`
   ADD FULLTEXT INDEX `fti_uspc_current_mainclass_title` (`mainclass_title`);

ALTER TABLE `PatentsView_20181127`.`uspc_current`
   ADD FULLTEXT INDEX `fti_uspc_current_subclass_title` (`subclass_title`);

ALTER TABLE `PatentsView_20181127`.`uspc_current_mainclass`
   ADD FULLTEXT INDEX `fti_uspc_current_mainclass_mainclass_title`
          (`mainclass_title`);


# END full text indexing ######################################################################################################################################
