-- Index on patent_id and inventor_key_id
CREATE TABLE `{{params.reporting_database}}`.inventor_entity as 
	SELECT patent_inventor.patent_id as patent_id, 
    locationI.city as inventor_city, 
    locationI.country as inventor_country, 
    locationI.county as inventor_county, 
    locationI.county_fips as inventor_county_fips, 
    inventor.name_first as inventor_first_name, 
    inventor.first_seen_date as inventor_first_seen_date, 
    inventor.persistent_inventor_id as inventor_id, 
    inventor.inventor_id as inventor_key_id, 
    inventor.name_last as inventor_last_name, 
    inventor.last_seen_date as inventor_last_seen_date, 
    inventor.lastknown_city as inventor_lastknown_city, 
    inventor.lastknown_country as inventor_lastknown_country, 
    inventor.lastknown_latitude as inventor_lastknown_latitude, 
    inventor.lastknown_persistent_location_id as inventor_lastknown_location_id, 
    inventor.lastknown_longitude as inventor_lastknown_longitude, 
    inventor.lastknown_state as inventor_lastknown_state, 
    locationI.latitude as inventor_latitude, 
    locationI.persistent_location_id as inventor_location_id, 
    locationI.longitude as inventor_longitude,  
    locationI.state as inventor_state, 
    locationI.state_fips as inventor_state_fips, 
    inventor.num_assignees as inventor_total_num_assignees, 
    inventor.num_patents as inventor_total_num_patents 
    
    FROM `{{params.reporting_database}}`.patent_inventor left outer join `{{params.reporting_database}}`.inventor ON patent_inventor.inventor_id=inventor.inventor_id 
    left outer join `{{params.reporting_database}}`.location as locationI on patent_inventor.location_id=locationI.location_id;

ALTER TABLE `{{params.reporting_database}}`.`inventor_entity` ADD INDEX `patent_id` (`patent_id` ASC);
ALTER TABLE `{{params.reporting_database}}`.`inventor_entity` ADD INDEX `inventor_key_id` (`inventor_key_id` ASC);

-- Index on patent_id and assignee_key_id
CREATE TABLE `{{params.reporting_database}}`.assignee_entity as 
	SELECT patent_assignee.patent_id as patent_id, 
    locationA.county as assignee_county, 
    locationA.county_fips as assignee_county_fips, 
    locationA.state_fips as assignee_state_fips, 
    locationA.city as assignee_city, 
    locationA.country as assignee_country, 
    assignee.name_first as assignee_first_name, 
    assignee.first_seen_date as assignee_first_seen_date, 
    assignee.persistent_assignee_id as assignee_id, 
    assignee.assignee_id as assignee_key_id, 
    assignee.lastknown_city as assignee_lastknown_city, 
    assignee.lastknown_country as assignee_lastknown_country, 
    assignee.lastknown_latitude as assignee_lastknown_latitude, 
    assignee.lastknown_persistent_location_id as assignee_lastknown_location_id, 
    assignee.lastknown_longitude as assignee_lastknown_longitude, 
    assignee.lastknown_state as assignee_lastknown_state, 
    assignee.name_last as assignee_last_name, 
    assignee.last_seen_date as assignee_last_seen_date, 
    locationA.latitude as assignee_latitude, 
    locationA.persistent_location_id as assignee_location_id, 
    locationA.longitude as assignee_longitude, 
    assignee.organization as assignee_organization, 
    patent_assignee.sequence as assignee_sequence, 
    locationA.state as assignee_state, 
    assignee.num_patents as assignee_total_num_patents, 
    assignee.num_inventors as assignee_total_num_inventors, 
    assignee.type as assignee_type 
    
    FROM `{{params.reporting_database}}`.patent_assignee left outer join `{{params.reporting_database}}`.assignee ON patent_assignee.assignee_id=assignee.assignee_id 
    left outer join `{{params.reporting_database}}`.location as locationA on patent_assignee.location_id=locationA.location_id;

ALTER TABLE `{{params.reporting_database}}`.`assignee_entity` ADD INDEX `patent_id` (`patent_id` ASC);
ALTER TABLE `{{params.reporting_database}}`.`assignee_entity` ADD INDEX `assignee_key_id` (`assignee_key_id` ASC);

-- Index on patent_id
CREATE TABLE `{{params.reporting_database}}`.uspc_entity as 
	SELECT uspc_current_mainclass_copy.patent_id as patent_id, 
    uspc_mainclass.first_seen_date as uspc_first_seen_date, 
    uspc_mainclass.last_seen_date as uspc_last_seen_date, 
    uspc_mainclass.id as uspc_mainclass_id, 
    uspc_mainclass.title as uspc_mainclass_title, 
    uspc_current_copy.sequence as uspc_sequence, 
    uspc_current_copy.subclass_id as uspc_subclass_id, 
    uspc_subclass.title as uspc_subclass_title, 
    uspc_mainclass.num_assignees as uspc_total_num_assignees, 
    uspc_mainclass.num_inventors as uspc_total_num_inventors, 
    uspc_mainclass.num_patents as uspc_total_num_patents 
    
    FROM `{{params.reporting_database}}`.uspc_current_mainclass_copy left outer join `{{params.reporting_database}}`.uspc_current_copy on uspc_current_mainclass_copy.patent_id=uspc_current_copy.patent_id 
		and uspc_current_mainclass_copy.mainclass_id=uspc_current_copy.mainclass_id 
	left outer join `{{params.reporting_database}}`.uspc_mainclass on uspc_current_mainclass_copy.mainclass_id=uspc_mainclass.id 
    left outer join `{{params.reporting_database}}`.uspc_subclass on uspc_current_copy.subclass_id=uspc_subclass.id;

ALTER TABLE `{{params.reporting_database}}`.`uspc_entity` ADD INDEX `patent_id` (`patent_id` ASC);

-- Index on patent_id
CREATE TABLE `{{params.reporting_database}}`.cpc_entity as 
	SELECT cpc_current_subsection_copy.patent_id as patent_id, 
    cpc_current_copy.category as cpc_category, 
    cpc_subsection.first_seen_date as cpc_first_seen_date, 
    cpc_current_copy.group_id as cpc_group_id, 
    cpc_group.title as cpc_group_title, 
    cpc_subsection.last_seen_date as cpc_last_seen_date, 
    cpc_current_subsection_copy.section_id as cpc_section_id, 
    cpc_current_copy.sequence as cpc_sequence, 
    cpc_current_copy.subgroup_id as cpc_subgroup_id, 
    cpc_subgroup.title as cpc_subgroup_title, 
    cpc_subsection.id as cpc_subsection_id, 
    cpc_subsection.title as cpc_subsection_title, 
    cpc_subsection.num_inventors as cpc_total_num_inventors, 
    cpc_subsection.num_patents as cpc_total_num_patents, 
    cpc_subsection.num_assignees as cpc_total_num_assignees 
    
    FROM `{{params.reporting_database}}`.cpc_current_subsection_copy left outer join `{{params.reporting_database}}`.cpc_subsection on cpc_current_subsection_copy.subsection_id=cpc_subsection.id 
    left outer join `{{params.reporting_database}}`.cpc_current_copy on cpc_current_subsection_copy.patent_id=cpc_current_copy.patent_id 
		and cpc_subsection.id=cpc_current_copy.subsection_id 
	left outer join `{{params.reporting_database}}`.cpc_group on cpc_current_copy.group_id=cpc_group.id 
    left outer join `{{params.reporting_database}}`.cpc_subgroup on cpc_current_copy.subgroup_id=cpc_subgroup.id;
    
ALTER TABLE `{{params.reporting_database}}`.`cpc_entity` ADD INDEX `patent_id` (`patent_id` ASC);

-- Index on patent_id
CREATE TABLE `{{params.reporting_database}}`.nber_entity as 
	SELECT nber_copy.patent_id as patent_id, 
    nber_copy.category_id as nber_category_id, 
    nber_category.title as nber_category_title, 
    nber_subcategory.first_seen_date as nber_first_seen_date, 
    nber_subcategory.last_seen_date as nber_last_seen_date, 
    nber_copy.subcategory_id as nber_subcategory_id, 
    nber_subcategory.title as nber_subcategory_title, 
    nber_subcategory.num_assignees as nber_total_num_assignees, 
    nber_subcategory.num_inventors as nber_total_num_inventors, 
    nber_subcategory.num_patents as nber_total_num_patents 
    
    FROM `{{params.reporting_database}}`.nber_copy left outer join `{{params.reporting_database}}`.nber_category on nber_copy.category_id=nber_category.id 
    left outer join `{{params.reporting_database}}`.nber_subcategory on nber_copy.subcategory_id=nber_subcategory.id;
    
ALTER TABLE `{{params.reporting_database}}`.`nber_entity` ADD INDEX `patent_id` (`patent_id` ASC);

-- Index on patent_id
CREATE TABLE `{{params.reporting_database}}`.wipo_entity as 
	SELECT wipo.patent_id as patent_id, 
    wipo.field_id as wipo_field_id, 
    wipo_field.field_title as wipo_field_title, 
    wipo_field.sector_title as wipo_sector_title, 
    wipo.sequence as wipo_sequence 
    
    FROM `{{params.reporting_database}}`.wipo left outer join `{{params.reporting_database}}`.wipo_field on wipo.field_id=wipo_field.id;
    
ALTER TABLE `{{params.reporting_database}}`.`wipo_entity` ADD INDEX `patent_id` (`patent_id` ASC);

-- Index on patent_id
CREATE TABLE `{{params.reporting_database}}`.examiner_entity as 
	SELECT patent_examiner.patent_id as patent_id, 
    examiner.name_first as examiner_first_name, 
    examiner.persistent_examiner_id as examiner_id, 
    examiner.examiner_id as examiner_key_id, 
    examiner.name_last as examiner_last_name, 
    patent_examiner.role as examiner_role, 
    examiner.`group` as examiner_group 
    
    FROM `{{params.reporting_database}}`.patent_examiner left outer join `{{params.reporting_database}}`.examiner on examiner.examiner_id=patent_examiner.examiner_id;
    
ALTER TABLE `{{params.reporting_database}}`.`examiner_entity` ADD INDEX `patent_id` (`patent_id` ASC);

-- Index on patent_id
CREATE TABLE `{{params.reporting_database}}`.lawyer_entity as 
	SELECT patent_lawyer.patent_id as patent_id, 
    lawyer.name_first as lawyer_first_name, 
    lawyer.first_seen_date as lawyer_first_seen_date, 
    lawyer.persistent_lawyer_id as lawyer_id, 
    lawyer.lawyer_id as lawyer_key_id, 
    lawyer.name_last as lawyer_last_name, 
    lawyer.last_seen_date as lawyer_last_seen_date, 
    lawyer.organization as lawyer_organization, 
    patent_lawyer.sequence as lawyer_sequence, 
    lawyer.num_patents as lawyer_total_num_patents, 
    lawyer.num_assignees as lawyer_total_num_assignees, 
    lawyer.num_inventors as lawyer_total_num_inventors 
    
    FROM `{{params.reporting_database}}`.patent_lawyer left outer join `{{params.reporting_database}}`.lawyer on lawyer.lawyer_id=patent_lawyer.lawyer_id;
    
ALTER TABLE `{{params.reporting_database}}`.`lawyer_entity` ADD INDEX `patent_id` (`patent_id` ASC);
