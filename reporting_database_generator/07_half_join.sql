-- Index on patent_id and inventor_key_id

drop table if exists `{{params.reporting_database}}`.`inventor_entity`;
create table `{{params.reporting_database}}`.`inventor_entity`
(`patent_id` varchar(20) not null,
   	`inventor_city` varchar(256) null,
    `inventor_country` varchar(10) null,
    `inventor_county` varchar(60) null,
    `inventor_county_fips` varchar(6) null,
    `inventor_first_name` varchar(128) null,
    `inventor_first_seen_date` date null,
    `inventor_id` varchar(256) null,
    `inventor_key_id` int(10) unsigned,
    `inventor_last_name` varchar(128) null,
   	`inventor_last_seen_date` date null,
    `inventor_lastknown_city` varchar(256) null,
    `inventor_lastknown_country` varchar(10) null,
    `inventor_lastknown_latitude` float null,
	`inventor_lastknown_location_id` varchar(128) null,
	`inventor_lastknown_longitude` float null,
	`inventor_lastknown_state` varchar(20) null,
	`inventor_latitude` float null,
 	`inventor_location_id` varchar(128),
   	`inventor_longitude` float null,
    `inventor_state` varchar(20) null,
    `inventor_state_fips` varchar(2) null,
    `inventor_total_num_assignees` int(10) unsigned,
    `inventor_total_num_patents` int(10) unsigned
)
engine=InnoDB;

insert into `{{params.reporting_database}}`.`inventor_entity`
(
  	`patent_id`, `inventor_city`, `inventor_country`,
    `inventor_county`, `inventor_county_fips`, `inventor_first_name`,
    `inventor_first_seen_date`, `inventor_id`, `inventor_key_id`,
    `inventor_last_name`, `inventor_last_seen_date`, `inventor_lastknown_city`,
    `inventor_lastknown_country`, `inventor_lastknown_latitude`, `inventor_lastknown_location_id`,
	`inventor_lastknown_longitude`, `inventor_lastknown_state`, `inventor_latitude`,
 	`inventor_location_id`, `inventor_longitude`,  `inventor_state`,
    `inventor_state_fips`, `inventor_total_num_assignees`, `inventor_total_num_patents`
)

select
  	p.`patent_id`,
  	locationI.`city`,
  	locationI.`country`,
    locationI.`county`,
    locationI.`county_fips`,
    i.`name_first`,
    i.`first_seen_date`,
    i.`persistent_inventor_id`,
    i.`inventor_id`,
    i.`name_last`,
    i.`last_seen_date`,
    i.`lastknown_city`,
    i.`lastknown_country`,
    i.`lastknown_latitude`,
    i.`lastknown_persistent_location_id`,
	i.`lastknown_longitude`,
	i.`lastknown_state`,
	locationI.`latitude`,
 	locationI.`persistent_location_id`,
 	locationI.`longitude`,
 	locationI.`state`,
    locationI.`state_fips`,
    i.`num_assignees`,
    i.`num_patents`
from
 	`{{params.reporting_database}}`.`patent_inventor` p
 	left outer join `{{params.reporting_database}}`.`inventor` i ON p.`inventor_id`=i.`inventor_id`
    left outer join `{{params.reporting_database}}`.`location` as `locationI` on p.`location_id`= locationI.`location_id`;

ALTER TABLE `{{params.reporting_database}}`.`inventor_entity` ADD INDEX `patent_id` (`patent_id` ASC);
ALTER TABLE `{{params.reporting_database}}`.`inventor_entity` ADD INDEX `inventor_key_id` (`inventor_key_id` ASC);

--  Index on patent_id and assignee_key_id

drop table if exists `{{params.reporting_database}}`.`assignee_entity`;
create table `{{params.reporting_database}}`.`assignee_entity`
(
	`patent_id` varchar(20) not null,
	`assignee_county` varchar(60) null,
    `assignee_county_fips` varchar(6) null,
    `assignee_state_fips` varchar(2) null,
	`assignee_city` varchar(256) null,
    `assignee_country` varchar(10) null,
    `assignee_first_name` varchar(64) null,
    `assignee_first_seen_date` date null,
    `assignee_id` varchar(64),
    `assignee_key_id` int(10) unsigned,
    `assignee_lastknown_city` varchar(128),
    `assignee_lastknown_country` varchar(10),
    `assignee_lastknown_latitude` float null,
    `assignee_lastknown_location_id` varchar(128),
    `assignee_lastknown_longitude` float null,
    `assignee_lastknown_state` varchar(20) null,
    `assignee_last_name` varchar(64) null,
    `assignee_last_seen_date` date null,
    `assignee_latitude`  float null,
    `assignee_location_id` varchar(128),
    `assignee_longitude` float null,
   	`assignee_organization` varchar(256) null,
    `assignee_sequence` smallint(5) unsigned,
    `assignee_state` varchar(20) null,
    `assignee_total_num_patents` int(10) unsigned,
    `assignee_total_num_inventors` int(10) unsigned,
    `assignee_type` varchar(10) null
)
engine=InnoDB;

insert into `{{params.reporting_database}}`.`assignee_entity`
(
      `patent_id`,`assignee_county`,`assignee_county_fips`,`assignee_state_fips`,`assignee_city`,
    `assignee_country`,`assignee_first_name`,`assignee_first_seen_date`,`assignee_id`,
    `assignee_key_id`,`assignee_lastknown_city`,`assignee_lastknown_country`,`assignee_lastknown_latitude`,
    `assignee_lastknown_location_id`,`assignee_lastknown_longitude`,`assignee_lastknown_state`,
    `assignee_last_name`,`assignee_last_seen_date`, `assignee_latitude`, `assignee_location_id`,
    `assignee_longitude`,`assignee_organization`,`assignee_sequence`,`assignee_state`,`assignee_total_num_patents`,
    `assignee_total_num_inventors`,`assignee_type`
)


select
	pa.`patent_id`,
	locationA.`county`,
    locationA.`county_fips`,
    locationA.`state_fips`,
    locationA.`city`,
    locationA.`country`,
    a.`name_first`,
    a.`first_seen_date`,
    a.`persistent_assignee_id`,
    a.`assignee_id`,
    a.`lastknown_city`,
    a.`lastknown_country`,
    a.`lastknown_latitude`,
    a.`lastknown_persistent_location_id`,
    a.`lastknown_longitude`,
    a.`lastknown_state`,
    a.`name_last`,
    a.`last_seen_date`,
    locationA.`latitude`,
    locationA.`persistent_location_id`,
    locationA.`longitude`,
    a.`organization`,
    pa.`sequence`,
    locationA.`state`,
    a.`num_patents`,
    a.`num_inventors`,
    a.`type`
from
	`{{params.reporting_database}}`.`patent_assignee` pa
	left outer join `{{params.reporting_database}}`.`assignee` a ON pa.`assignee_id`=a.`assignee_id`
    left outer join `{{params.reporting_database}}`.`location` as `locationA` on pa.`location_id`= locationA.`location_id`;


ALTER TABLE `{{params.reporting_database}}`.`assignee_entity` ADD INDEX `patent_id` (`patent_id` ASC);
ALTER TABLE `{{params.reporting_database}}`.`assignee_entity` ADD INDEX `assignee_key_id` (`assignee_key_id` ASC);


-- Index on patent_id
drop table if exists `{{params.reporting_database}}`.`uspc_entity`;
create table `{{params.reporting_database}}`.`uspc_entity`
(
	`patent_id` varchar(20) not null, 
   	`uspc_first_seen_date` date null, 
    `uspc_last_seen_date` date null, 
    `uspc_mainclass_id` varchar(20) not null, 
    `uspc_mainclass_title` varchar(256) null, 
    `uspc_sequence` int(10) unsigned, 
    `uspc_subclass_id`varchar(20) null, 
    `uspc_subclass_title` varchar(512) null, 
    `uspc_total_num_assignees` int(10) unsigned null, 
    `uspc_total_num_inventors` int(10) unsigned null, 
  	`uspc_total_num_patents` int(10) unsigned null 
)
engine=InnoDB;

insert into `{{params.reporting_database}}`.`uspc_entity`
(

	`patent_id`, `uspc_first_seen_date` , `uspc_last_seen_date` ,`uspc_mainclass_id` ,
    `uspc_mainclass_title`,`uspc_sequence`,`uspc_subclass_id`,`uspc_subclass_title` ,
    `uspc_total_num_assignees` ,`uspc_total_num_inventors` ,`uspc_total_num_patents`
)



select
	ucm.`patent_id`,
    um.`first_seen_date`, 
    um.`last_seen_date`, 
    um.`id`, 
    um.`title`, 
    uc.`sequence`, 
    uc.`subclass_id`, 
    us.`title`, 
    um.`num_assignees`, 
    um.`num_inventors`, 
  	um.`num_patents`
from
	`{{params.reporting_database}}`.`uspc_current_mainclass_copy` ucm
	left outer join `{{params.reporting_database}}`.`uspc_current_copy` uc on ucm.`patent_id`=uc.`patent_id`
		and ucm.`mainclass_id`=uc.`mainclass_id` 
	left outer join `{{params.reporting_database}}`.`uspc_mainclass` um on ucm.`mainclass_id`=um.`id` 
    left outer join `{{params.reporting_database}}`.`uspc_subclass` us on uc.`subclass_id`=us.`id`;

ALTER TABLE `{{params.reporting_database}}`.`uspc_entity` ADD INDEX `patent_id` (`patent_id` ASC);


-- Index on patent_id
drop table if exists `{{params.reporting_database}}`.`cpc_entity`;
create table `{{params.reporting_database}}`.`cpc_entity`
(
	`patent_id` varchar(20) not null, 
    `cpc_category` varchar(36) null, 
    `cpc_first_seen_date` date null, 
    `cpc_group_id` varchar(20) null, 
    `cpc_group_title` varchar(512) null, 
    `cpc_last_seen_date` date null, 
    `cpc_section_id` varchar(10) null, 
    `cpc_sequence` int(10) unsigned null,
    `cpc_subgroup_id` varchar(20) null, 
    `cpc_subgroup_title` varchar(2048) null, 
    `cpc_subsection_id` varchar(20) not null, 
    `cpc_subsection_title` varchar(512) null, 
    `cpc_total_num_inventors` int(10) unsigned null, 
    `cpc_total_num_patents` int(10) unsigned null, 
    `cpc_total_num_assignees` int(10) unsigned null 
)

engine=InnoDB;


insert into `{{params.reporting_database}}`.`cpc_entity`
(
  `patent_id` ,`cpc_category` ,`cpc_first_seen_date` ,
    `cpc_group_id` ,`cpc_group_title` ,`cpc_last_seen_date` ,
    `cpc_section_id` ,`cpc_sequence` ,`cpc_subgroup_id` ,
    `cpc_subgroup_title` ,`cpc_subsection_id` ,`cpc_subsection_title` ,
    `cpc_total_num_inventors` ,`cpc_total_num_patents` ,
    `cpc_total_num_assignees`


)

select
	cpc_current_subsection_copy.`patent_id`, 
    cpc_current_copy.`category`, 
    cpc_subsection.`first_seen_date`, 
    cpc_current_copy.`group_id`, 
    cpc_group.`title`, 
    cpc_subsection.`last_seen_date`, 
    cpc_current_subsection_copy.`section_id`, 
    cpc_current_copy.`sequence`, 
    cpc_current_copy.`subgroup_id`, 
    cpc_subgroup.`title`, 
    cpc_subsection.`id`, 
    cpc_subsection.`title`, 
    cpc_subsection.`num_inventors`, 
    cpc_subsection.`num_patents`, 
    cpc_subsection.`num_assignees`
FROM 
	`{{params.reporting_database}}`.cpc_current_subsection_copy left outer join `{{params.reporting_database}}`.cpc_subsection on cpc_current_subsection_copy.`subsection_id`=cpc_subsection.`id`
    left outer join `{{params.reporting_database}}`.cpc_current_copy on cpc_current_subsection_copy.`patent_id`=cpc_current_copy.`patent_id`
		and cpc_subsection.`id`=cpc_current_copy.`subsection_id`
	left outer join `{{params.reporting_database}}`.cpc_group on cpc_current_copy.`group_id`=cpc_group.`id` 
    left outer join `{{params.reporting_database}}`.cpc_subgroup on cpc_current_copy.`subgroup_id`=cpc_subgroup.`id`;
    
ALTER TABLE `{{params.reporting_database}}`.`cpc_entity` ADD INDEX `patent_id` (`patent_id` ASC);

-- Index on patent_id
drop table if exists `{{params.reporting_database}}`.`nber_entity`;
create table `{{params.reporting_database}}`.`nber_entity`
(
 	`patent_id` varchar(20) not null, 
   	`nber_category_id` varchar(20) null, 
    `nber_category_title` varchar(512) null, 
    `nber_first_seen_date` date null, 
    `nber_last_seen_date` date null, 
   	`nber_subcategory_id` varchar(20) null, 
    `nber_subcategory_title` varchar(512) null, 
    `nber_total_num_assignees` int(10) unsigned null, 
    `nber_total_num_inventors` int(10) unsigned null, 
    `nber_total_num_patents` int(10) unsigned null 
)

engine=InnoDB;

insert into `{{params.reporting_database}}`.`nber_entity`
(
   `patent_id`, `nber_category_id` ,`nber_category_title`, `nber_first_seen_date`,
    `nber_last_seen_date` ,	`nber_subcategory_id`,`nber_subcategory_title` ,
    `nber_total_num_assignees` , `nber_total_num_inventors` , `nber_total_num_patents`

)

select
 	nber_copy.`patent_id`, 
    nber_copy.`category_id`, 
    nber_category.`title`, 
    nber_subcategory.`first_seen_date`, 
    nber_subcategory.`last_seen_date`, 
    nber_copy.`subcategory_id`, 
    nber_subcategory.`title`, 
    nber_subcategory.`num_assignees`, 
    nber_subcategory.`num_inventors`, 
    nber_subcategory.`num_patents` 
    
FROM 
	`{{params.reporting_database}}`.nber_copy left outer join `{{params.reporting_database}}`.nber_category on nber_copy.`category_id`=nber_category.`id` 
    left outer join `{{params.reporting_database}}`.nber_subcategory on nber_copy.`subcategory_id`=nber_subcategory.`id`;
    
ALTER TABLE `{{params.reporting_database}}`.`nber_entity` ADD INDEX `patent_id` (`patent_id` ASC);


-- Index on patent_id
drop table if exists `{{params.reporting_database}}`.`wipo_entity`;
create table `{{params.reporting_database}}`.`wipo_entity`
(
	`patent_id` varchar(20) not null, 
   	`wipo_field_id` varchar(3) null, 
    `wipo_field_title` varchar(255) null, 
    `wipo_sector_title` varchar(60) null, 
    `wipo_sequence` int(10) unsigned not null
)


engine=InnoDB;

insert into `{{params.reporting_database}}`.`wipo_entity`
(
  `patent_id`,	`wipo_field_id` , `wipo_field_title` ,`wipo_sector_title` ,
    `wipo_sequence`

)


select
	wipo.`patent_id`, 
    wipo.`field_id`, 
    wipo_field.`field_title`, 
    wipo_field.`sector_title`,
    wipo.`sequence`
    
FROM `{{params.reporting_database}}`.wipo left outer join `{{params.reporting_database}}`.wipo_field on wipo.`field_id`=wipo_field.`id`;
    
ALTER TABLE `{{params.reporting_database}}`.`wipo_entity` ADD INDEX `patent_id` (`patent_id` ASC);

-- Index on patent_id  
drop table if exists `{{params.reporting_database}}`.`examiner_entity`;
create table `{{params.reporting_database}}`.`examiner_entity`
(
	`patent_id` varchar(20) not null, 
	`examiner_first_name` varchar(64) null, 
	`examiner_id` varchar(36) null,
	`examiner_key_id` int(10) unsigned null,
	`examiner_last_name` varchar(64) null, 
	`examiner_role` varchar(20) not null,
	`examiner_group` varchar(20) null
)

engine=InnoDB;

insert into `{{params.reporting_database}}`.`examiner_entity`
(
 `patent_id`,`examiner_first_name` ,`examiner_id` , `examiner_key_id`,
	`examiner_last_name` , `examiner_role` , `examiner_group`

)

select
	patent_examiner.`patent_id`, 
    examiner.`name_first`, 
    examiner.`persistent_examiner_id`,
    examiner.`examiner_id`,
    examiner.`name_last`,
    patent_examiner.`role`, 
    examiner.`group`
    
FROM `{{params.reporting_database}}`.patent_examiner left outer join `{{params.reporting_database}}`.examiner on examiner.`examiner_id`=patent_examiner.`examiner_id`;
    
ALTER TABLE `{{params.reporting_database}}`.`examiner_entity` ADD INDEX `patent_id` (`patent_id` ASC);

-- Index on patent_id
drop table if exists `{{params.reporting_database}}`.`lawyer_entity`;
create table `{{params.reporting_database}}`.`lawyer_entity`
(
	`patent_id` varchar(20) not null,
	`lawyer_first_name` varchar(64) null, 
	`lawyer_first_seen_date` date null, 
	`lawyer_id` varchar(36) null ,
	`lawyer_key_id` int(10) unsigned null,
	`lawyer_last_name` varchar(64) null, 
	`lawyer_last_seen_date` date null, 
	`lawyer_organization` varchar(256) null, 
	`lawyer_sequence` smallint(5) unsigned not null,
	`lawyer_total_num_patents` int(10) unsigned, 
	`lawyer_total_num_assignees` int(10) unsigned, 
	`lawyer_total_num_inventors` int(10) unsigned
)

engine=InnoDB;

insert into `{{params.reporting_database}}`.`lawyer_entity`
(
 `patent_id` , `lawyer_first_name`, `lawyer_first_seen_date`, `lawyer_id`  ,
	`lawyer_key_id` ,	`lawyer_last_name` ,`lawyer_last_seen_date` , `lawyer_organization`,
	`lawyer_sequence` ,`lawyer_total_num_patents`, `lawyer_total_num_assignees`, `lawyer_total_num_inventors`

)


select
   	patent_lawyer.`patent_id`,
   	lawyer.`name_first`,
    lawyer.`first_seen_date`,
    lawyer.`persistent_lawyer_id`, 
    lawyer.`lawyer_id`,
    lawyer.`name_last`,
    lawyer.`last_seen_date`,
    lawyer.`organization`,
    patent_lawyer.`sequence`, 
    lawyer.`num_patents`, 
    lawyer.`num_assignees`,
    lawyer.`num_inventors`
FROM `{{params.reporting_database}}`.patent_lawyer left outer join `{{params.reporting_database}}`.lawyer on lawyer.`lawyer_id`=patent_lawyer.`lawyer_id`;
    
ALTER TABLE `{{params.reporting_database}}`.`lawyer_entity` ADD INDEX `patent_id` (`patent_id` ASC);
