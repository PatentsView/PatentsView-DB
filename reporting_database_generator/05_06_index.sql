alter table `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`inventor_cpc_subsection` add index `ix_inventor_cpc_subsection_subsection_id` (`subsection_id`);
alter table `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`inventor_cpc_subsection` add index `ix_inventor_cpc_subsection_inventor_id` (`inventor_id`);
alter table `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`inventor_cpc_subsection` add index `ix_inventor_cpc_subsection_num_patents` (`num_patents`);
alter table `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`inventor_nber_subcategory` add index `ix_inventor_nber_subcategory_num_patents` (`num_patents`);
alter table `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`inventor_nber_subcategory` add index `ix_inventor_nber_subcategory_inventor_id` (`inventor_id`);
alter table `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`inventor_nber_subcategory` add index `ix_inventor_nber_subcategory_subcategory_id` (`subcategory_id`);
alter table `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`inventor_uspc_mainclass` add index `ix_inventor_uspc_mainclass_num_patents` (`num_patents`);
alter table `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`inventor_uspc_mainclass` add index `ix_inventor_uspc_mainclass_inventor_id` (`inventor_id`);
alter table `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`inventor_uspc_mainclass` add index `ix_inventor_uspc_mainclass_mainclass_id` (`mainclass_id`);
alter table `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`inventor_year` add index `ix_inventor_year_inventor_id` (`inventor_id`);
alter table `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`inventor_year` add index `ix_inventor_year_year` (`patent_year`);
alter table `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`inventor_year` add index `ix_inventor_year_num_patents` (`num_patents`);