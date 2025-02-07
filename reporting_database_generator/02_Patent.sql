{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set version_indicator = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

# BEGIN patent 

################################################################################################################################################

insert into `{{reporting_db}}`.`patent`
(
  `patent_id`, `type`, `number`, `country`, `date`, `year`,
  `abstract`, `title`, `kind`, `withdrawn`, `num_claims`,
  `firstnamed_assignee_id`, `firstnamed_assignee_persistent_id`,
  `firstnamed_assignee_location_id`, `firstnamed_assignee_persistent_location_id`,
  `firstnamed_assignee_city`, `firstnamed_assignee_state`,
  `firstnamed_assignee_country`, `firstnamed_assignee_latitude`,
  `firstnamed_assignee_longitude`, `firstnamed_inventor_id`,
  `firstnamed_inventor_persistent_id`, `firstnamed_inventor_location_id`,
  `firstnamed_inventor_persistent_location_id`, `firstnamed_inventor_city`,
  `firstnamed_inventor_state`, `firstnamed_inventor_country`,
  `firstnamed_inventor_latitude`, `firstnamed_inventor_longitude`,
  `num_foreign_documents_cited`, `num_us_applications_cited`,
  `num_us_patents_cited`,
  `num_total_documents_cited`,
  `num_times_cited_by_us_patents`,
  `earliest_application_date`,
  `patent_processing_days`,
  `uspc_current_mainclass_average_patent_processing_days`,
  `cpc_current_group_average_patent_processing_days`,
  `term_extension`, `detail_desc_length`
)
select
  p.`id`, case when ifnull(p.`type`, '') = 'sir' then 'statutory invention registration' else nullif(trim(p.`type`), '') end,
  `number`, nullif(trim(p.`country`), ''), tpd.`date`, year(tpd.`date`),
  nullif(trim(p.`abstract`), ''), nullif(trim(p.`title`), ''), nullif(trim(p.`kind`), ''), p.`withdrawn`, p.`num_claims`,
  tpfna.`assignee_id`, tpfna.`persistent_assignee_id`, tpfna.`location_id`,
  tpfna.`persistent_location_id`, tpfna.`city`,
  tpfna.`state`, tpfna.`country`, tpfna.`latitude`, tpfna.`longitude`,
  tpfni.`inventor_id`, tpfni.`persistent_inventor_id`, tpfni.`location_id`,
  tpfni.`persistent_location_id`, tpfni.`city`,
  tpfni.`state`, tpfni.`country`, tpfni.`latitude`, tpfni.`longitude`,
  tpa.`num_foreign_documents_cited`, tpa.`num_us_applications_cited`,
  tpa.`num_us_patents_cited`,
  tpa.`num_total_documents_cited`,
  tpa.`num_times_cited_by_us_patents`,
  tpead.`earliest_application_date`,
  case when tpead.`earliest_application_date` <= p.`date` then timestampdiff(day, tpead.`earliest_application_date`, tpd.`date`) else null end,
  null,
  null,
  ustog.`term_extension`, `detail_desc_length`
from
  `patent`.`patent` p
  left outer join `{{reporting_db}}`.`temp_patent_date` tpd on tpd.`patent_id` = p.`id`
  left outer join `{{reporting_db}}`.`temp_patent_firstnamed_assignee` tpfna on tpfna.`patent_id` = p.`id`
  left outer join `{{reporting_db}}`.`temp_patent_firstnamed_inventor` tpfni on tpfni.`patent_id` = p.`id`
  left outer join `{{reporting_db}}`.`temp_patent_aggregations` tpa on tpa.`patent_id` = p.`id`
  left outer join `{{reporting_db}}`.`temp_patent_earliest_application_date` tpead on tpead.`patent_id` = p.`id`
  left outer join `patent`.`us_term_of_grant` ustog on ustog.`patent_id`=p.`id`
  left outer join `patent`.`detail_desc_length` ddl on ddl.`patent_id` = p.`id`
 where  p.version_indicator<='{{version_indicator}}'
 ON DUPLICATE KEY UPDATE
        `type` = VALUES(`type`),
        `number` = VALUES(`number`),
        `country` = VALUES(`country`),
        `date` = VALUES(`date`),
        `year` = VALUES(`year`),
        `abstract` = VALUES(`abstract`),
        `title` = VALUES(`title`),
        `kind` = VALUES(`kind`),
        `withdrawn` = VALUES(`withdrawn`),
        `num_claims` = VALUES(`num_claims`),
        `firstnamed_assignee_id` = VALUES(`firstnamed_assignee_id`),
        `firstnamed_assignee_persistent_id` = VALUES(`firstnamed_assignee_persistent_id`),
        `firstnamed_assignee_location_id` = VALUES(`firstnamed_assignee_location_id`),
        `firstnamed_assignee_persistent_location_id` = VALUES(`firstnamed_assignee_persistent_location_id`),
        `firstnamed_assignee_city` = VALUES(`firstnamed_assignee_city`),
        `firstnamed_assignee_state` = VALUES(`firstnamed_assignee_state`),
        `firstnamed_assignee_country` = VALUES(`firstnamed_assignee_country`),
        `firstnamed_assignee_latitude` = VALUES(`firstnamed_assignee_latitude`),
        `firstnamed_assignee_longitude` = VALUES(`firstnamed_assignee_longitude`),
        `firstnamed_inventor_id` = VALUES(`firstnamed_inventor_id`),
        `firstnamed_inventor_persistent_id` = VALUES(`firstnamed_inventor_persistent_id`),
        `firstnamed_inventor_location_id` = VALUES(`firstnamed_inventor_location_id`),
        `firstnamed_inventor_persistent_location_id` = VALUES(`firstnamed_inventor_persistent_location_id`),
        `firstnamed_inventor_city` = VALUES(`firstnamed_inventor_city`),
        `firstnamed_inventor_state` = VALUES(`firstnamed_inventor_state`),
        `firstnamed_inventor_country` = VALUES(`firstnamed_inventor_country`),
        `firstnamed_inventor_latitude` = VALUES(`firstnamed_inventor_latitude`),
        `firstnamed_inventor_longitude` = VALUES(`firstnamed_inventor_longitude`),
        `num_foreign_documents_cited` = VALUES(`num_foreign_documents_cited`),
        `num_us_applications_cited` = VALUES(`num_us_applications_cited`),
        `num_us_patents_cited` = VALUES(`num_us_patents_cited`),
        `num_total_documents_cited` = VALUES(`num_total_documents_cited`),
        `num_times_cited_by_us_patents` = VALUES(`num_times_cited_by_us_patents`),
        `earliest_application_date` = VALUES(`earliest_application_date`),
        `patent_processing_days` = VALUES(`patent_processing_days`),
        `uspc_current_mainclass_average_patent_processing_days` = VALUES(`uspc_current_mainclass_average_patent_processing_days`),
        `cpc_current_group_average_patent_processing_days` = VALUES(`cpc_current_group_average_patent_processing_days`),
        `term_extension` = VALUES(`term_extension`),
        `detail_desc_length` = VALUES(`detail_desc_length`);


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

# END patent 

################################################################################################################################################