

# BEGIN additional indexing 

###################################################################################################################################


# 1:53:23
alter table `{{params.reporting_database}}`.`application` add index `ix_application_number` (`number`);
alter table `{{params.reporting_database}}`.`application` add index `ix_application_patent_id` (`patent_id`);
alter table `{{params.reporting_database}}`.`assignee` add index `ix_assignee_name_first` (`name_first`);
alter table `{{params.reporting_database}}`.`assignee` add index `ix_assignee_name_last` (`name_last`);
alter table `{{params.reporting_database}}`.`assignee` add index `ix_assignee_organization` (`organization`);
alter table `{{params.reporting_database}}`.`assignee` add index `ix_assignee_persistent_assignee_id` (`persistent_assignee_id`);
alter table `{{params.reporting_database}}`.`assignee_cpc_subsection` add index `ix_assignee_cpc_subsection_assignee_id` (`assignee_id`);
alter table `{{params.reporting_database}}`.`assignee_cpc_subsection` add index `ix_assignee_cpc_subsection_subsection_id` (`subsection_id`);
alter table `{{params.reporting_database}}`.`assignee_cpc_group` add index `ix_assignee_cpc_group_assignee_id` (`assignee_id`);
alter table `{{params.reporting_database}}`.`assignee_cpc_group` add index `ix_assignee_cpc_group_group_id` (`group_id`);
alter table `{{params.reporting_database}}`.`assignee_inventor` add index `ix_assignee_inventor_assignee_id` (`assignee_id`);
alter table `{{params.reporting_database}}`.`assignee_inventor` add index `ix_assignee_inventor_inventor_id` (`inventor_id`);
alter table `{{params.reporting_database}}`.`assignee_nber_subcategory` add index `ix_assignee_nber_subcategory_assignee_id` (`assignee_id`);
alter table `{{params.reporting_database}}`.`assignee_nber_subcategory` add index `ix_assignee_nber_subcategory_subcategory_id` (`subcategory_id`);
alter table `{{params.reporting_database}}`.`assignee_uspc_mainclass` add index `ix_assignee_uspc_mainclass_assignee_id` (`assignee_id`);
alter table `{{params.reporting_database}}`.`assignee_uspc_mainclass` add index `ix_assignee_uspc_mainclass_mainclass_id` (`mainclass_id`);
alter table `{{params.reporting_database}}`.`assignee_year` add index `ix_assignee_year_assignee_id` (`assignee_id`);
alter table `{{params.reporting_database}}`.`assignee_year` add index `ix_assignee_year_year` (`patent_year`);
alter table `{{params.reporting_database}}`.`cpc_current_group` add index `ix_cpc_current_group_group_id` (`group_id`);
alter table `{{params.reporting_database}}`.`cpc_current_group` add index `ix_cpc_current_group_title` (`group_title`);
alter table `{{params.reporting_database}}`.`cpc_current_subsection` add index `ix_cpc_current_subsection_subsection_id` (`subsection_id`);
alter table `{{params.reporting_database}}`.`cpc_current_subsection` add index `ix_cpc_current_subsection_title` (`subsection_title`);
alter table `{{params.reporting_database}}`.`cpc_current` add index `ix_cpc_current_group_id` (`group_id`);
alter table `{{params.reporting_database}}`.`cpc_current` add index `ix_cpc_current_subgroup_id` (`subgroup_id`);
alter table `{{params.reporting_database}}`.`cpc_current` add index `ix_cpc_current_subsection_id` (`subsection_id`);
alter table `{{params.reporting_database}}`.`inventor` add index `ix_inventor_name_first` (`name_first`);
alter table `{{params.reporting_database}}`.`inventor` add index `ix_inventor_name_last` (`name_last`);
alter table `{{params.reporting_database}}`.`inventor` add index `ix_inventor_persistent_inventor_id` (`persistent_inventor_id`);
alter table `{{params.reporting_database}}`.`inventor_coinventor` add index `ix_inventor_coinventor_inventor_id` (`inventor_id`);
alter table `{{params.reporting_database}}`.`inventor_coinventor` add index `ix_inventor_coinventor_coinventor_id` (`coinventor_id`);
alter table `{{params.reporting_database}}`.`inventor_cpc_subsection` add index `ix_inventor_cpc_subsection_inventor_id` (`inventor_id`);
alter table `{{params.reporting_database}}`.`inventor_cpc_subsection` add index `ix_inventor_cpc_subsection_subsection_id` (`subsection_id`);
alter table `{{params.reporting_database}}`.`inventor_cpc_group` add index `ix_inventor_cpc_group_inventor_id` (`inventor_id`);
alter table `{{params.reporting_database}}`.`inventor_cpc_group` add index `ix_inventor_cpc_group_group_id` (`group_id`);
alter table `{{params.reporting_database}}`.`inventor_nber_subcategory` add index `ix_inventor_nber_subcategory_inventor_id` (`inventor_id`);
alter table `{{params.reporting_database}}`.`inventor_nber_subcategory` add index `ix_inventor_nber_subcategory_subcategory_id` (`subcategory_id`);
alter table `{{params.reporting_database}}`.`inventor_uspc_mainclass` add index `ix_inventor_uspc_mainclass_inventor_id` (`inventor_id`);
alter table `{{params.reporting_database}}`.`inventor_uspc_mainclass` add index `ix_inventor_uspc_mainclass_mainclass_id` (`mainclass_id`);
alter table `{{params.reporting_database}}`.`inventor_year` add index `ix_inventor_year_inventor_id` (`inventor_id`);
alter table `{{params.reporting_database}}`.`inventor_year` add index `ix_inventor_year_year` (`patent_year`);
alter table `{{params.reporting_database}}`.`ipcr` add index `ix_ipcr_ipc_class` (`ipc_class`);
alter table `{{params.reporting_database}}`.`location_assignee` add index `ix_location_assignee_assignee_id` (`assignee_id`);
alter table `{{params.reporting_database}}`.`location_inventor` add index `ix_location_inventor_inventor_id` (`inventor_id`);
alter table `{{params.reporting_database}}`.`location` add index `ix_location_city` (`city`);
alter table `{{params.reporting_database}}`.`location` add index `ix_location_country` (`country`);
alter table `{{params.reporting_database}}`.`location` add index `ix_location_persistent_location_id` (`persistent_location_id`);
alter table `{{params.reporting_database}}`.`location` add index `ix_location_state` (`state`);
alter table `{{params.reporting_database}}`.`location_cpc_subsection` add index `ix_location_cpc_subsection_location_id` (`location_id`);
alter table `{{params.reporting_database}}`.`location_cpc_subsection` add index `ix_location_cpc_subsection_subsection_id` (`subsection_id`);
alter table `{{params.reporting_database}}`.`location_cpc_group` add index `ix_location_cpc_group_location_id` (`location_id`);
alter table `{{params.reporting_database}}`.`location_cpc_group` add index `ix_location_cpc_group_subsection_id` (`group_id`);
alter table `{{params.reporting_database}}`.`location_nber_subcategory` add index `ix_location_nber_subcategory_location_id` (`location_id`);
alter table `{{params.reporting_database}}`.`location_nber_subcategory` add index `ix_location_nber_subcategory_mainclass_id` (`subcategory_id`);
alter table `{{params.reporting_database}}`.`location_uspc_mainclass` add index `ix_location_uspc_mainclass_location_id` (`location_id`);
alter table `{{params.reporting_database}}`.`location_uspc_mainclass` add index `ix_location_uspc_mainclass_mainclass_id` (`mainclass_id`);
alter table `{{params.reporting_database}}`.`location_year` add index `ix_location_year_location_id` (`location_id`);
alter table `{{params.reporting_database}}`.`location_year` add index `ix_location_year_year` (`year`);
alter table `{{params.reporting_database}}`.`nber` add index `ix_nber_subcategory_id` (`subcategory_id`);
alter table `{{params.reporting_database}}`.`nber` add index `ix_nber_subcategory_title` (`subcategory_title`);
alter table `{{params.reporting_database}}`.`patent_assignee` add index `ix_patent_assignee_location_id` (`location_id`);
alter table `{{params.reporting_database}}`.`patent_inventor` add index `ix_patent_inventor_location_id` (`location_id`);
alter table `{{params.reporting_database}}`.`patent` add index `ix_patent_date` (`date`);
alter table `{{params.reporting_database}}`.`patent` add index `ix_patent_number` (`number`);
alter table `{{params.reporting_database}}`.`patent` add index `ix_patent_title` (`title`(128));
alter table `{{params.reporting_database}}`.`patent` add index `ix_patent_type` (`type`);
alter table `{{params.reporting_database}}`.`patent` add index `ix_patent_year` (`year`);
alter table `{{params.reporting_database}}`.`usapplicationcitation` add index `ix_usapplicationcitation_cited_application_id` (`cited_application_id`);
alter table `{{params.reporting_database}}`.`uspatentcitation` add index `ix_uspatentcitation_cited_patent_id` (`cited_patent_id`);
alter table `{{params.reporting_database}}`.`uspc_current_mainclass` add index `ix_uspc_current_mainclass_mainclass_id` (`mainclass_id`);
alter table `{{params.reporting_database}}`.`uspc_current_mainclass` add index `ix_uspc_current_mainclass_mainclass_title` (`mainclass_title`);
alter table `{{params.reporting_database}}`.`uspc_current` add index `ix_uspc_current_mainclass_id` (`mainclass_id`);
alter table `{{params.reporting_database}}`.`uspc_current` add index `ix_uspc_current_subclass_id` (`subclass_id`);
alter table `{{params.reporting_database}}`.`uspc_current` add index `ix_uspc_current_mainclass_title` (`mainclass_title`);
alter table `{{params.reporting_database}}`.`uspc_current` add index `ix_uspc_current_subclass_title` (`subclass_title`);
alter table `{{params.reporting_database}}`.`assignee` add index `ix_assignee_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`assignee` add index `ix_assignee_num_inventors` (`num_inventors`);
alter table `{{params.reporting_database}}`.`assignee_cpc_subsection` add index `ix_assignee_cpc_subsection_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`assignee_cpc_group` add index `ix_assignee_cpc_group_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`assignee_inventor` add index `ix_assignee_inventor_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`assignee_nber_subcategory` add index `ix_assignee_nber_subcategory_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`assignee_uspc_mainclass` add index `ix_assignee_uspc_mainclass_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`assignee_year` add index `ix_assignee_year_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`cpc_current` add index `ix_cpc_current_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`cpc_current` add index `ix_cpc_current_num_inventors` (`num_inventors`);
alter table `{{params.reporting_database}}`.`cpc_current` add index `ix_cpc_current_num_assignees` (`num_assignees`);
alter table `{{params.reporting_database}}`.`cpc_current_subsection` add index `ix_cpc_current_subsection_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`cpc_current_subsection` add index `ix_cpc_current_subsection_num_inventors` (`num_inventors`);
alter table `{{params.reporting_database}}`.`cpc_current_subsection` add index `ix_cpc_current_subsection_num_assignees` (`num_assignees`);
alter table `{{params.reporting_database}}`.`cpc_current_subsection_patent_year` add index `ix_cpc_current_subsection_patent_year_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`cpc_current` add index `ix_cpc_current_num_patents_group` (`num_patents_group`);
alter table `{{params.reporting_database}}`.`cpc_current` add index `ix_cpc_current_num_inventors_group` (`num_inventors_group`);
alter table `{{params.reporting_database}}`.`cpc_current` add index `ix_cpc_current_num_assignees_group` (`num_assignees_group`);
alter table `{{params.reporting_database}}`.`cpc_current_group` add index `ix_cpc_current_group_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`cpc_current_group` add index `ix_cpc_current_group_num_inventors` (`num_inventors`);
alter table `{{params.reporting_database}}`.`cpc_current_group` add index `ix_cpc_current_group_num_assignees` (`num_assignees`);
alter table `{{params.reporting_database}}`.`cpc_current_group_patent_year` add index `ix_cpc_current_group_patent_year_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`inventor` add index `ix_inventor_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`inventor` add index `ix_inventor_num_assignees` (`num_assignees`);
alter table `{{params.reporting_database}}`.`inventor_coinventor` add index `ix_inventor_coinventor_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`inventor_cpc_subsection` add index `ix_inventor_cpc_subsection_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`inventor_cpc_group` add index `ix_inventor_cpc_group_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`inventor_nber_subcategory` add index `ix_inventor_nber_subcategory_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`inventor_uspc_mainclass` add index `ix_inventor_uspc_mainclass_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`inventor_year` add index `ix_inventor_year_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`ipcr` add index `ix_ipcr_num_inventors` (`num_inventors`);
alter table `{{params.reporting_database}}`.`ipcr` add index `ix_ipcr_num_assignees` (`num_assignees`);
alter table `{{params.reporting_database}}`.`location` add index `ix_location_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`location` add index `ix_location_num_inventors` (`num_inventors`);
alter table `{{params.reporting_database}}`.`location` add index `ix_location_num_assignees` (`num_assignees`);
alter table `{{params.reporting_database}}`.`location_assignee` add index `ix_location_assignee_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`location_cpc_subsection` add index `ix_location_cpc_subsection_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`location_cpc_group` add index `ix_location_cpc_group_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`location_inventor` add index `ix_location_inventor_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`location_nber_subcategory` add index `ix_location_nber_subcategory_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`location_uspc_mainclass` add index `ix_location_uspc_mainclass_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`location_year` add index `ix_location_year_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`nber` add index `ix_nber_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`nber` add index `ix_nber_num_inventors` (`num_inventors`);
alter table `{{params.reporting_database}}`.`nber` add index `ix_nber_num_assignees` (`num_assignees`);
alter table `{{params.reporting_database}}`.`nber_subcategory_patent_year` add index `ix_nber_subcategory_patent_year_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`uspc_current` add index `ix_uspc_current_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`uspc_current` add index `ix_uspc_current_num_inventors` (`num_inventors`);
alter table `{{params.reporting_database}}`.`uspc_current` add index `ix_uspc_current_num_assignees` (`num_assignees`);
alter table `{{params.reporting_database}}`.`uspc_current_mainclass` add index `ix_uspc_current_mainclass_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`uspc_current_mainclass` add index `ix_uspc_current_mainclass_num_inventors` (`num_inventors`);
alter table `{{params.reporting_database}}`.`uspc_current_mainclass` add index `ix_uspc_current_mainclass_num_assignees` (`num_assignees`);
alter table `{{params.reporting_database}}`.`uspc_current_mainclass_patent_year` add index `ix_uspc_current_mainclass_patent_year_num_patents`(`num_patents`);
alter table `{{params.reporting_database}}`.`inventor` add index `ix_inventor_lastknown_location_id` (`lastknown_location_id`);
alter table `{{params.reporting_database}}`.`inventor` add index `ix_inventor_lastknown_persistent_location_id` (`lastknown_persistent_location_id`);
alter table `{{params.reporting_database}}`.`inventor` add index `ix_inventor_first_seen_date` (`first_seen_date`);
alter table `{{params.reporting_database}}`.`inventor` add index `ix_inventor_last_seen_date` (`last_seen_date`);
alter table `{{params.reporting_database}}`.`nber` add index `ix_nber_category_id` (`category_id`);
alter table `{{params.reporting_database}}`.`assignee` add index `ix_assignee_lastknown_location_id` (`lastknown_location_id`);
alter table `{{params.reporting_database}}`.`assignee` add index `ix_assignee_lastknown_persistent_location_id` (`lastknown_persistent_location_id`);
alter table `{{params.reporting_database}}`.`assignee` add index `ix_assignee_last_seen_date` (`last_seen_date`);
alter table `{{params.reporting_database}}`.`assignee` add index `ix_assignee_first_seen_date` (`first_seen_date`);
alter table `{{params.reporting_database}}`.`patent` add index `ix_patent_country` (`country`);
alter table `{{params.reporting_database}}`.`patent` add index `ix_patent_num_claims` (`num_claims`);
alter table `{{params.reporting_database}}`.`patent` add index `ix_patent_firstnamed_assignee_id` (`firstnamed_assignee_id`);
alter table `{{params.reporting_database}}`.`patent` add index `ix_patent_firstnamed_assignee_persistent_id` (`firstnamed_assignee_persistent_id`);
alter table `{{params.reporting_database}}`.`patent` add index `ix_patent_firstnamed_assignee_persistent_location_id`(`firstnamed_assignee_persistent_location_id`);
alter table `{{params.reporting_database}}`.`patent` add index `ix_patent_firstnamed_assignee_location_id` (`firstnamed_assignee_location_id`);
alter table `{{params.reporting_database}}`.`patent` add index `ix_patent_firstnamed_inventor_persistent_id` (`firstnamed_inventor_persistent_id`);
alter table `{{params.reporting_database}}`.`patent` add index `ix_patent_firstnamed_inventor_persistent_location_id`(`firstnamed_inventor_persistent_location_id`);
alter table `{{params.reporting_database}}`.`patent` add index `ix_patent_firstnamed_inventor_id` (`firstnamed_inventor_id`);
alter table `{{params.reporting_database}}`.`patent` add index `ix_patent_firstnamed_inventor_location_id` (`firstnamed_inventor_location_id`);

alter table `{{params.reporting_database}}`.`lawyer` add index `ix_lawyer_name_first` (`name_first`);
alter table `{{params.reporting_database}}`.`lawyer` add index `ix_lawyer_name_last` (`name_last`);
alter table `{{params.reporting_database}}`.`lawyer` add index `ix_lawyer_organization` (`organization`);
alter table `{{params.reporting_database}}`.`lawyer` add index `ix_lawyer_num_patents` (`num_patents`);
alter table `{{params.reporting_database}}`.`lawyer` add index `ix_lawyer_num_assignees` (`num_assignees`);
alter table `{{params.reporting_database}}`.`lawyer` add index `ix_lawyer_num_inventors` (`num_inventors`);
alter table `{{params.reporting_database}}`.`lawyer` add index `ix_lawyer_first_seen_date` (`first_seen_date`);
alter table `{{params.reporting_database}}`.`lawyer` add index `ix_lawyer_last_seen_date` (`last_seen_date`);
alter table `{{params.reporting_database}}`.`lawyer` add index `ix_lawyer_persistent_lawyer_id` (`persistent_lawyer_id`);

alter table `{{params.reporting_database}}`.`examiner` add index `ix_examiner_name_first` (`name_first`);
alter table `{{params.reporting_database}}`.`examiner` add index `ix_examiner_name_last` (`name_last`);
alter table `{{params.reporting_database}}`.`examiner` add index `ix_examiner_role` (`role`);
alter table `{{params.reporting_database}}`.`examiner` add index `ix_examiner_group` (`group`);
alter table `{{params.reporting_database}}`.`examiner` add index `ix_examiner_persistent_examiner_id` (`persistent_examiner_id`);

alter table `{{params.reporting_database}}`.`foreignpriority` add index `ix_foreignpriority_foreign_doc_number` (`foreign_doc_number`);
alter table `{{params.reporting_database}}`.`foreignpriority` add index `ix_foreignpriority_date` (`date`);
alter table `{{params.reporting_database}}`.`foreignpriority` add index `ix_foreignpriority_country` (`country`);
alter table `{{params.reporting_database}}`.`foreignpriority` add index `ix_foreignpriority_kind` (`kind`);

alter table `{{params.reporting_database}}`.`location` add index `ix_location_county_fips` (`county_fips`);
alter table `{{params.reporting_database}}`.`location` add index `ix_location_state_fips` (`state_fips`);
alter table `{{params.reporting_database}}`.`location` add index `ix_location_county` (`county`);

alter table `{{params.reporting_database}}`.`pctdata` add index `ix_pctdata_doc_type` (`doc_type`);
alter table `{{params.reporting_database}}`.`pctdata` add index `ix_pctdata_doc_number` (`doc_number`);
alter table `{{params.reporting_database}}`.`pctdata` add index `ix_pctdata_date` (`date`);
alter table `{{params.reporting_database}}`.`pctdata` add index `ix_pctdata_102_date` (`102_date`);
alter table `{{params.reporting_database}}`.`pctdata` add index `ix_pctdata_371_date` (`371_date`);








# END additional indexing 
