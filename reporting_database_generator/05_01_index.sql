{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

alter table `{{reporting_db}}`.`application` add index `ix_application_number` (`number`);
alter table `{{reporting_db}}`.`application` add index `ix_application_patent_id` (`patent_id`);
alter table `{{reporting_db}}`.`assignee_cpc_group` add index `ix_assignee_cpc_group_group_id` (`group_id`);
alter table `{{reporting_db}}`.`assignee_cpc_group` add index `ix_assignee_cpc_group_assignee_id` (`assignee_id`);
alter table `{{reporting_db}}`.`assignee_cpc_group` add index `ix_assignee_cpc_group_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`assignee_cpc_subsection` add index `ix_assignee_cpc_subsection_assignee_id` (`assignee_id`);
alter table `{{reporting_db}}`.`assignee_cpc_subsection` add index `ix_assignee_cpc_subsection_subsection_id` (`subsection_id`);
alter table `{{reporting_db}}`.`assignee_cpc_subsection` add index `ix_assignee_cpc_subsection_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`assignee_inventor` add index `ix_assignee_inventor_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`assignee_inventor` add index `ix_assignee_inventor_assignee_id` (`assignee_id`);
alter table `{{reporting_db}}`.`assignee_inventor` add index `ix_assignee_inventor_inventor_id` (`inventor_id`);