{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

alter table `{{reporting_db}}`.`lawyer` add index `ix_lawyer_name_last` (`name_last`);
alter table `{{reporting_db}}`.`lawyer` add index `ix_lawyer_organization` (`organization`);
alter table `{{reporting_db}}`.`lawyer` add index `ix_lawyer_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`lawyer` add index `ix_lawyer_name_first` (`name_first`);
alter table `{{reporting_db}}`.`lawyer` add index `ix_lawyer_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`lawyer` add index `ix_lawyer_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`lawyer` add index `ix_lawyer_first_seen_date` (`first_seen_date`);
alter table `{{reporting_db}}`.`lawyer` add index `ix_lawyer_last_seen_date` (`last_seen_date`);
alter table `{{reporting_db}}`.`lawyer` add index `ix_lawyer_persistent_lawyer_id` (`persistent_lawyer_id`);
alter table `{{reporting_db}}`.`location_assignee` add index `ix_location_assignee_assignee_id` (`assignee_id`);
alter table `{{reporting_db}}`.`location_assignee` add index `ix_location_assignee_num_patents` (`num_patents`);