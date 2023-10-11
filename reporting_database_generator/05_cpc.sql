{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

create table if not exists `{{reporting_db}}`.`cpc_subsection` (id varchar(20) primary key,title varchar(512), num_patents int(10) unsigned, num_inventors int(10) unsigned, num_assignees int(10) unsigned,first_seen_date date,last_seen_date date,years_active smallint(5) unsigned);
create table if not exists `{{reporting_db}}`.`cpc_subgroup` (id varchar(20) primary key,title varchar(2048));
create table if not exists `{{reporting_db}}`.`cpc_group` (id varchar(20) primary key,title varchar(512), num_patents int(10) unsigned, num_inventors int(10) unsigned, num_assignees int(10) unsigned,first_seen_date date,last_seen_date date,years_active smallint(5) unsigned);

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

insert into `{{reporting_db}}`.`cpc_subsection` select subsection_id,subsection_title,num_patents,num_inventors,num_assignees,first_seen_date,last_seen_date,years_active from `{{reporting_db}}`.`cpc_current` group by subsection_id;
insert into `{{reporting_db}}`.`cpc_group` select group_id,group_title,num_patents_group,num_inventors_group,num_assignees_group,first_seen_date_group,last_seen_date_group,years_active_group from `{{reporting_db}}`.`cpc_current` group by group_id;
insert into `{{reporting_db}}`.`cpc_subgroup` select subgroup_id,subgroup_title from `{{reporting_db}}`.`cpc_current` group by subgroup_id;
insert into `{{reporting_db}}`.`cpc_current_subsection_copy` select distinct patent_id,section_id,subsection_id from `{{reporting_db}}`.`cpc_current_subsection`;
insert into `{{reporting_db}}`.`cpc_current_group_copy` select distinct patent_id,section_id,group_id from `{{reporting_db}}`.`cpc_current_group`;


alter table `{{reporting_db}}`.`cpc_subsection` add index `ix_cpc_subsection_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`cpc_subsection` add index `ix_cpc_subsection_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`cpc_subsection` add index `ix_cpc_subsection_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`cpc_group` add index `ix_cpc_group_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`cpc_group` add index `ix_cpc_group_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`cpc_group` add index `ix_cpc_group_num_patents` (`num_patents`);
