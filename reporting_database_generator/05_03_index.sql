{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

alter table `{{reporting_db}}`.`cpc_current_group_patent_year` add index `ix_cpc_current_group_patent_year_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`cpc_current_group` add index `ix_cpc_current_group_title` (`group_title`);
alter table `{{reporting_db}}`.`cpc_current_group` add index `ix_cpc_current_group_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`cpc_current_group` add index `ix_cpc_current_group_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`cpc_current_group` add index `ix_cpc_current_group_group_id` (`group_id`);
alter table `{{reporting_db}}`.`cpc_current_group` add index `ix_cpc_current_group_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`cpc_current_subsection_patent_year` add index `ix_cpc_current_subsection_patent_year_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`cpc_current_subsection` add index `ix_cpc_current_subsection_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`cpc_current_subsection` add index `ix_cpc_current_subsection_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`cpc_current_subsection` add index `ix_cpc_current_subsection_title` (`subsection_title`);
alter table `{{reporting_db}}`.`cpc_current_subsection` add index `ix_cpc_current_subsection_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`cpc_current_subsection` add index `ix_cpc_current_subsection_subsection_id` (`subsection_id`);