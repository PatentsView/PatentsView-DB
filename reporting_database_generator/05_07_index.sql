{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_lastknown_location_id` (`lastknown_location_id`);
alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_first_seen_date` (`first_seen_date`);
alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_last_seen_date` (`last_seen_date`);
alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_lastknown_persistent_location_id` (`lastknown_persistent_location_id`);
alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_name_first` (`name_first`);
alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_name_last` (`name_last`);
alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_persistent_inventor_id` (`persistent_inventor_id`);
alter table `{{reporting_db}}`.`ipcr` add index `ix_ipcr_ipc_class` (`ipc_class`);
alter table `{{reporting_db}}`.`ipcr` add index `ix_ipcr_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`ipcr` add index `ix_ipcr_num_inventors` (`num_inventors`);