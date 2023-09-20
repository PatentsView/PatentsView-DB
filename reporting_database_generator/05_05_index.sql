{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

alter table `{{reporting_db}}`.`foreignpriority` add index `ix_foreignpriority_foreign_doc_number` (`foreign_doc_number`);
alter table `{{reporting_db}}`.`foreignpriority` add index `ix_foreignpriority_date` (`date`);
alter table `{{reporting_db}}`.`foreignpriority` add index `ix_foreignpriority_kind` (`kind`);
alter table `{{reporting_db}}`.`foreignpriority` add index `ix_foreignpriority_country` (`country`);
ALTER TABLE `{{reporting_db}}`.`government_interest` ADD FULLTEXT INDEX `fti_government_interest_gi_statement` (`gi_statement`);
alter table `{{reporting_db}}`.`inventor_coinventor` add index `ix_inventor_coinventor_coinventor_id` (`coinventor_id`);
alter table `{{reporting_db}}`.`inventor_coinventor` add index `ix_inventor_coinventor_inventor_id` (`inventor_id`);
alter table `{{reporting_db}}`.`inventor_coinventor` add index `ix_inventor_coinventor_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`inventor_cpc_group` add index `ix_inventor_cpc_group_group_id` (`group_id`);
alter table `{{reporting_db}}`.`inventor_cpc_group` add index `ix_inventor_cpc_group_inventor_id` (`inventor_id`);
alter table `{{reporting_db}}`.`inventor_cpc_group` add index `ix_inventor_cpc_group_num_patents` (`num_patents`);