{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

# BEGIN new class table creation
#####################################################################################################################################

create table if not exists `{{reporting_db}}`.`cpc_subsection` (id varchar(20) primary key,title varchar(512), num_patents int(10) unsigned, num_inventors int(10) unsigned, num_assignees int(10) unsigned,first_seen_date date,last_seen_date date,years_active smallint(5) unsigned);
create table if not exists `{{reporting_db}}`.`cpc_subgroup` (id varchar(20) primary key,title varchar(2048));
create table if not exists `{{reporting_db}}`.`cpc_group` (id varchar(20) primary key,title varchar(512), num_patents int(10) unsigned, num_inventors int(10) unsigned, num_assignees int(10) unsigned,first_seen_date date,last_seen_date date,years_active smallint(5) unsigned);
create table if not exists `{{reporting_db}}`.`nber_category` (id varchar(20) primary key,title varchar(512));
create table if not exists `{{reporting_db}}`.`nber_subcategory` (id varchar(20) primary key,title varchar(512), num_patents int(10) unsigned,
  num_inventors int(10) unsigned, num_assignees int(10) unsigned,first_seen_date date,last_seen_date date,years_active smallint(5) unsigned);
create table if not exists `{{reporting_db}}`.`uspc_mainclass` (id varchar(20) primary key,title varchar(256), num_patents int(10) unsigned,
  num_inventors int(10) unsigned, num_assignees int(10) unsigned,first_seen_date date,last_seen_date date,years_active smallint(5) unsigned);
create table if not exists `{{reporting_db}}`.`uspc_subclass` (id varchar(20) primary key,title varchar(512));
CREATE TABLE if not exists `{{reporting_db}}`.`nber_copy` ( `patent_id` varchar(20) NOT NULL,  `category_id` varchar(20) DEFAULT NULL,
  `subcategory_id` varchar(20) DEFAULT NULL,PRIMARY KEY (`patent_id`),KEY `ix_nber_subcategory_id` (`subcategory_id`),
  KEY `ix_nber_category_id`(`category_id`));
CREATE TABLE if not exists `{{reporting_db}}`.`cpc_current_copy` (  `patent_id` varchar(20) NOT NULL,  `sequence` int(10) unsigned NOT NULL,
  `section_id` varchar(10) DEFAULT NULL,  `subsection_id` varchar(20) DEFAULT NULL,  `group_id` varchar(20) DEFAULT NULL,  
  `subgroup_id` varchar(20) DEFAULT NULL,  `category` varchar(36) DEFAULT NULL,  PRIMARY KEY (`patent_id`,`sequence`),  
  KEY `ix_cpc_current_group_id` (`group_id`),  KEY `ix_cpc_current_subgroup_id` (`subgroup_id`),  
  KEY `ix_cpc_current_subsection_id` (`subsection_id`),  KEY `ix_cpc_current_section_id` (`section_id`),  
  KEY `ix_cpc_current_sequence` (`sequence`));
CREATE TABLE if not exists `{{reporting_db}}`.`cpc_current_subsection_copy` (  `patent_id` varchar(20) NOT NULL,
  `section_id` varchar(10) DEFAULT NULL,  `subsection_id` varchar(20) NOT NULL DEFAULT '',  PRIMARY KEY (`patent_id`,`subsection_id`),  
  KEY `ix_cpc_current_subsection_subsection_id` (`subsection_id`),  KEY `ix_cpc_current_subsection_section_id` (`section_id`));
CREATE TABLE if not exists `{{reporting_db}}`.`cpc_current_group_copy` (  `patent_id` varchar(20) NOT NULL,
  `section_id` varchar(10) DEFAULT NULL,  `group_id` varchar(20) NOT NULL DEFAULT '',  PRIMARY KEY (`patent_id`,`group_id`),  
  KEY `ix_cpc_current_group_group_id` (`group_id`),  KEY `ix_cpc_current_group_section_id` (`section_id`));
CREATE TABLE if not exists `{{reporting_db}}`.`uspc_current_mainclass_copy` (  `patent_id` varchar(20) NOT NULL,
  `mainclass_id` varchar(20) NOT NULL DEFAULT '',  PRIMARY KEY (`patent_id`,`mainclass_id`),  
  KEY `ix_uspc_current_mainclass_mainclass_id` (`mainclass_id`));
CREATE TABLE if not exists `{{reporting_db}}`.`uspc_current_copy` (  `patent_id` varchar(20) NOT NULL,  `sequence` int(10) unsigned NOT NULL,
  `mainclass_id` varchar(20) DEFAULT NULL,  `subclass_id` varchar(20) DEFAULT NULL,  PRIMARY KEY (`patent_id`,`sequence`),  
  KEY `ix_uspc_current_mainclass_id` (`mainclass_id`),  KEY `ix_uspc_current_subclass_id` (`subclass_id`),  KEY `ix_uspc_current_sequence`(`sequence`));

# END new class table creation
#####################################################################################################################################

# BEGIN new class table population
#####################################################################################################################################


insert into `{{reporting_db}}`.`cpc_subsection` select subsection_id,subsection_title,num_patents,num_inventors,num_assignees,first_seen_date,last_seen_date,years_active from `{{reporting_db}}`.`cpc_current` group by subsection_id;
insert into `{{reporting_db}}`.`cpc_group` select group_id,group_title,num_patents_group,num_inventors_group,num_assignees_group,first_seen_date_group,last_seen_date_group,years_active_group from `{{reporting_db}}`.`cpc_current` group by group_id;
insert into `{{reporting_db}}`.`cpc_subgroup` select subgroup_id,subgroup_title from `{{reporting_db}}`.`cpc_current` group by subgroup_id;
insert into `{{reporting_db}}`.`nber_category` select category_id,category_title from `{{reporting_db}}`.`nber` group by category_id;
insert into `{{reporting_db}}`.`nber_subcategory` select subcategory_id,subcategory_title,num_patents,num_inventors,num_assignees,first_seen_date,last_seen_date,years_active from `{{reporting_db}}`.`nber` group by subcategory_id;
insert into `{{reporting_db}}`.`uspc_mainclass` select mainclass_id,mainclass_title,num_patents, num_inventors, num_assignees,first_seen_date,last_seen_date,years_active from `{{reporting_db}}`.`uspc_current` group by mainclass_id;
insert into `{{reporting_db}}`.`uspc_subclass` select subclass_id,subclass_title from `{{reporting_db}}`.`uspc_current` group by subclass_id;
insert into `{{reporting_db}}`.`uspc_current_mainclass_copy` select distinct patent_id,mainclass_id from `{{reporting_db}}`.`uspc_current_mainclass`;
insert into `{{reporting_db}}`.`cpc_current_subsection_copy` select distinct patent_id,section_id,subsection_id from `{{reporting_db}}`.`cpc_current_subsection`;
insert into `{{reporting_db}}`.`cpc_current_group_copy` select distinct patent_id,section_id,group_id from `{{reporting_db}}`.`cpc_current_group`;
insert into `{{reporting_db}}`.`uspc_current_copy` select distinct patent_id,sequence,mainclass_id,subclass_id from `{{reporting_db}}`.`uspc_current`;
insert into `{{reporting_db}}`.`cpc_current_copy` select distinct patent_id,sequence,section_id,subsection_id,group_id,subgroup_id,category from `{{reporting_db}}`.`cpc_current`;
insert into `{{reporting_db}}`.`nber_copy` select distinct patent_id,category_id,subcategory_id from `{{reporting_db}}`.`nber`;

# END new class table population
#####################################################################################################################################


# BEGIN new class table indexing
#####################################################################################################################################


alter table `{{reporting_db}}`.`uspc_mainclass` add index `ix_uspc_mainclass_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`uspc_mainclass` add index `ix_uspc_mainclass_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`uspc_mainclass` add index `ix_uspc_mainclass_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`nber_subcategory` add index `ix_nber_subcategory_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`nber_subcategory` add index `ix_nber_subcategory_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`nber_subcategory` add index `ix_nber_subcategory_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`cpc_subsection` add index `ix_cpc_subsection_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`cpc_subsection` add index `ix_cpc_subsection_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`cpc_subsection` add index `ix_cpc_subsection_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`cpc_group` add index `ix_cpc_group_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`cpc_group` add index `ix_cpc_group_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`cpc_group` add index `ix_cpc_group_num_patents` (`num_patents`);


# END new class table indexing
#####################################################################################################################################