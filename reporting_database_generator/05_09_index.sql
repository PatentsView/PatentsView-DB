{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

alter table `{{reporting_db}}`.`location_cpc_group` add index `ix_location_cpc_group_location_id` (`location_id`);
alter table `{{reporting_db}}`.`location_cpc_group` add index `ix_location_cpc_group_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`location_cpc_group` add index `ix_location_cpc_group_subsection_id` (`group_id`);
alter table `{{reporting_db}}`.`location_cpc_subsection` add index `ix_location_cpc_subsection_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`location_cpc_subsection` add index `ix_location_cpc_subsection_subsection_id` (`subsection_id`);
alter table `{{reporting_db}}`.`location_cpc_subsection` add index `ix_location_cpc_subsection_location_id` (`location_id`);
alter table `{{reporting_db}}`.`location_inventor` add index `ix_location_inventor_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`location_inventor` add index `ix_location_inventor_inventor_id` (`inventor_id`);
alter table `{{reporting_db}}`.`location_nber_subcategory` add index `ix_location_nber_subcategory_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`location_nber_subcategory` add index `ix_location_nber_subcategory_mainclass_id` (`subcategory_id`);
alter table `{{reporting_db}}`.`location_nber_subcategory` add index `ix_location_nber_subcategory_location_id` (`location_id`);