{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

alter table `{{reporting_db}}`.`cpc_current` add index `ix_cpc_current_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`cpc_current` add index `ix_cpc_current_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`cpc_current` add index `ix_cpc_current_num_inventors_group` (`num_inventors_group`);
alter table `{{reporting_db}}`.`cpc_current` add index `ix_cpc_current_num_assignees_group` (`num_assignees_group`);
alter table `{{reporting_db}}`.`cpc_current` add index `ix_cpc_current_subsection_id` (`subsection_id`);
alter table `{{reporting_db}}`.`cpc_current` add index `ix_cpc_current_num_patents_group` (`num_patents_group`);
alter table `{{reporting_db}}`.`cpc_current` add index `ix_cpc_current_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`cpc_current` add index `ix_cpc_current_group_id` (`group_id`);
alter table `{{reporting_db}}`.`cpc_current` add index `ix_cpc_current_subgroup_id` (`subgroup_id`);
alter table `{{reporting_db}}`.`examiner` add index `ix_examiner_name_first` (`name_first`);
alter table `{{reporting_db}}`.`examiner` add index `ix_examiner_name_last` (`name_last`);
alter table `{{reporting_db}}`.`examiner` add index `ix_examiner_role` (`role`);
alter table `{{reporting_db}}`.`examiner` add index `ix_examiner_group` (`group`);
alter table `{{reporting_db}}`.`examiner` add index `ix_examiner_persistent_examiner_id` (`persistent_examiner_id`);