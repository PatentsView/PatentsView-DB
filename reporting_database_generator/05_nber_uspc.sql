{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

create table if not exists `{{reporting_db}}`.`nber_category` (id varchar(20) primary key,title varchar(512));
create table if not exists `{{reporting_db}}`.`nber_subcategory` (id varchar(20) primary key,title varchar(512), num_patents int(10) unsigned,
  num_inventors int(10) unsigned, num_assignees int(10) unsigned,first_seen_date date,last_seen_date date,years_active smallint(5) unsigned);
CREATE TABLE if not exists `{{reporting_db}}`.`nber_copy` ( `patent_id` varchar(20) NOT NULL,  `category_id` varchar(20) DEFAULT NULL,
  `subcategory_id` varchar(20) DEFAULT NULL,PRIMARY KEY (`patent_id`),KEY `ix_nber_subcategory_id` (`subcategory_id`),
  KEY `ix_nber_category_id`(`category_id`));

create table if not exists `{{reporting_db}}`.`uspc_mainclass` (id varchar(20) primary key,title varchar(256), num_patents int(10) unsigned,
  num_inventors int(10) unsigned, num_assignees int(10) unsigned,first_seen_date date,last_seen_date date,years_active smallint(5) unsigned);
create table if not exists `{{reporting_db}}`.`uspc_subclass` (id varchar(20) primary key,title varchar(512));
CREATE TABLE if not exists `{{reporting_db}}`.`uspc_current_mainclass_copy` (  `patent_id` varchar(20) NOT NULL,
  `mainclass_id` varchar(20) NOT NULL DEFAULT '',  PRIMARY KEY (`patent_id`,`mainclass_id`),
  KEY `ix_uspc_current_mainclass_mainclass_id` (`mainclass_id`));
CREATE TABLE if not exists `{{reporting_db}}`.`uspc_current_copy` (  `patent_id` varchar(20) NOT NULL,  `sequence` int(10) unsigned NOT NULL,
  `mainclass_id` varchar(20) DEFAULT NULL,  `subclass_id` varchar(20) DEFAULT NULL,  PRIMARY KEY (`patent_id`,`sequence`),
  KEY `ix_uspc_current_mainclass_id` (`mainclass_id`),  KEY `ix_uspc_current_subclass_id` (`subclass_id`),  KEY `ix_uspc_current_sequence`(`sequence`));

insert into `{{reporting_db}}`.`nber_category` select category_id,category_title from `{{reporting_db}}`.`nber` group by category_id;
insert into `{{reporting_db}}`.`nber_subcategory` select subcategory_id,subcategory_title,num_patents,num_inventors,num_assignees,first_seen_date,last_seen_date,years_active from `{{reporting_db}}`.`nber` group by subcategory_id;
insert into `{{reporting_db}}`.`uspc_mainclass` select mainclass_id,mainclass_title,num_patents, num_inventors, num_assignees,first_seen_date,last_seen_date,years_active from `{{reporting_db}}`.`uspc_current` group by mainclass_id;
insert into `{{reporting_db}}`.`uspc_subclass` select subclass_id,subclass_title from `{{reporting_db}}`.`uspc_current` group by subclass_id;
insert into `{{reporting_db}}`.`uspc_current_mainclass_copy` select distinct patent_id,mainclass_id from `{{reporting_db}}`.`uspc_current_mainclass`;
insert into `{{reporting_db}}`.`uspc_current_copy` select distinct patent_id,sequence,mainclass_id,subclass_id from `{{reporting_db}}`.`uspc_current`;
insert into `{{reporting_db}}`.`nber_copy` select distinct patent_id,category_id,subcategory_id from `{{reporting_db}}`.`nber`;


alter table `{{reporting_db}}`.`uspc_mainclass` add index `ix_uspc_mainclass_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`uspc_mainclass` add index `ix_uspc_mainclass_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`uspc_mainclass` add index `ix_uspc_mainclass_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`nber_subcategory` add index `ix_nber_subcategory_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`nber_subcategory` add index `ix_nber_subcategory_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`nber_subcategory` add index `ix_nber_subcategory_num_patents` (`num_patents`);