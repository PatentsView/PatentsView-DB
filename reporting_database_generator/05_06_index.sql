{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

alter table `{{reporting_db}}`.`inventor_cpc_subsection` add index `ix_inventor_cpc_subsection_subsection_id` (`subsection_id`);
alter table `{{reporting_db}}`.`inventor_cpc_subsection` add index `ix_inventor_cpc_subsection_inventor_id` (`inventor_id`);
alter table `{{reporting_db}}`.`inventor_cpc_subsection` add index `ix_inventor_cpc_subsection_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`inventor_nber_subcategory` add index `ix_inventor_nber_subcategory_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`inventor_nber_subcategory` add index `ix_inventor_nber_subcategory_inventor_id` (`inventor_id`);
alter table `{{reporting_db}}`.`inventor_nber_subcategory` add index `ix_inventor_nber_subcategory_subcategory_id` (`subcategory_id`);
alter table `{{reporting_db}}`.`inventor_uspc_mainclass` add index `ix_inventor_uspc_mainclass_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`inventor_uspc_mainclass` add index `ix_inventor_uspc_mainclass_inventor_id` (`inventor_id`);
alter table `{{reporting_db}}`.`inventor_uspc_mainclass` add index `ix_inventor_uspc_mainclass_mainclass_id` (`mainclass_id`);
alter table `{{reporting_db}}`.`inventor_year` add index `ix_inventor_year_inventor_id` (`inventor_id`);
alter table `{{reporting_db}}`.`inventor_year` add index `ix_inventor_year_year` (`patent_year`);
alter table `{{reporting_db}}`.`inventor_year` add index `ix_inventor_year_num_patents` (`num_patents`);