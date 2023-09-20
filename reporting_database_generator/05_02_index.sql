{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

alter table `{{reporting_db}}`.`assignee_nber_subcategory` add index `ix_assignee_nber_subcategory_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`assignee_nber_subcategory` add index `ix_assignee_nber_subcategory_assignee_id` (`assignee_id`);
alter table `{{reporting_db}}`.`assignee_nber_subcategory` add index `ix_assignee_nber_subcategory_subcategory_id` (`subcategory_id`);
alter table `{{reporting_db}}`.`assignee_uspc_mainclass` add index `ix_assignee_uspc_mainclass_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`assignee_uspc_mainclass` add index `ix_assignee_uspc_mainclass_assignee_id` (`assignee_id`);
alter table `{{reporting_db}}`.`assignee_uspc_mainclass` add index `ix_assignee_uspc_mainclass_mainclass_id` (`mainclass_id`);
alter table `{{reporting_db}}`.`assignee_year` add index `ix_assignee_year_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`assignee_year` add index `ix_assignee_year_year` (`patent_year`);
alter table `{{reporting_db}}`.`assignee_year` add index `ix_assignee_year_assignee_id` (`assignee_id`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_first_seen_date` (`first_seen_date`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_last_seen_date` (`last_seen_date`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_lastknown_persistent_location_id` (`lastknown_persistent_location_id`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_lastknown_location_id` (`lastknown_location_id`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_name_first` (`name_first`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_name_last` (`name_last`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_organization` (`organization`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_persistent_assignee_id` (`persistent_assignee_id`);