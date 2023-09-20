{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

alter table `{{reporting_db}}`.`pctdata` add index `ix_pctdata_102_date` (`102_date`);
alter table `{{reporting_db}}`.`pctdata` add index `ix_pctdata_date` (`date`);
alter table `{{reporting_db}}`.`pctdata` add index `ix_pctdata_doc_number` (`doc_number`);
alter table `{{reporting_db}}`.`pctdata` add index `ix_pctdata_doc_type` (`doc_type`);
alter table `{{reporting_db}}`.`pctdata` add index `ix_pctdata_371_date` (`371_date`);
alter table `{{reporting_db}}`.`usapplicationcitation` add index `ix_usapplicationcitation_cited_application_id` (`cited_application_id`);
alter table `{{reporting_db}}`.`uspatentcitation` add index `ix_uspatentcitation_cited_patent_id` (`cited_patent_id`);
alter table `{{reporting_db}}`.`uspc_current_mainclass_patent_year` add index `ix_uspc_current_mainclass_patent_year_num_patents`(`num_patents`);
alter table `{{reporting_db}}`.`uspc_current_mainclass` add index `ix_uspc_current_mainclass_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`uspc_current_mainclass` add fulltext index `fti_uspc_current_mainclass_mainclass_title` (`mainclass_title`);
alter table `{{reporting_db}}`.`uspc_current_mainclass` add index `ix_uspc_current_mainclass_mainclass_title` (`mainclass_title`);
alter table `{{reporting_db}}`.`uspc_current_mainclass` add index `ix_uspc_current_mainclass_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`uspc_current_mainclass` add index `ix_uspc_current_mainclass_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`uspc_current_mainclass` add index `ix_uspc_current_mainclass_mainclass_id` (`mainclass_id`);