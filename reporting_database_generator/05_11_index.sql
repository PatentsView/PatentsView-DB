{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

alter table `{{reporting_db}}`.`nber_subcategory_patent_year` add index `ix_nber_subcategory_patent_year_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`nber` add index `ix_nber_subcategory_id` (`subcategory_id`);
alter table `{{reporting_db}}`.`nber` add index `ix_nber_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`nber` add index `ix_nber_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`nber` add index `ix_nber_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`nber` add index `ix_nber_category_id` (`category_id`);
alter table `{{reporting_db}}`.`nber` add index `ix_nber_subcategory_title` (`subcategory_title`);
alter table `{{reporting_db}}`.`patent_assignee` add index `ix_patent_assignee_location_id` (`location_id`);
alter table `{{reporting_db}}`.`patent_inventor` add index `ix_patent_inventor_location_id` (`location_id`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_firstnamed_inventor_location_id` (`firstnamed_inventor_location_id`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_number` (`number`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_title` (`title`(128));
alter table `{{reporting_db}}`.`patent` add index `ix_patent_type` (`type`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_year` (`year`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_date` (`date`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_firstnamed_inventor_persistent_location_id`(`firstnamed_inventor_persistent_location_id`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_firstnamed_inventor_persistent_id` (`firstnamed_inventor_persistent_id`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_firstnamed_assignee_location_id` (`firstnamed_assignee_location_id`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_firstnamed_assignee_persistent_location_id`(`firstnamed_assignee_persistent_location_id`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_firstnamed_assignee_persistent_id` (`firstnamed_assignee_persistent_id`);
alter table `{{reporting_db}}`.`patent` add fulltext index `fti_patent_title` (`title`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_firstnamed_inventor_id` (`firstnamed_inventor_id`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_firstnamed_assignee_id` (`firstnamed_assignee_id`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_num_claims` (`num_claims`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_country` (`country`);
alter table `{{reporting_db}}`.`patent` add fulltext index `fti_patent_abstract` (`abstract`);