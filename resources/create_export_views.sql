CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_applicant_not_disambiguated` AS 
select `a`.`patent_id` AS `patent_id`,
`a`.`sequence` AS `applicant_sequence`,
`a`.`fname` AS `raw_applicant_name_first`,
`a`.`lname` AS `raw_applicant_name_last`,
`a`.`organization` AS `raw_applicant_organization`,
`a`.`applicant_type` AS `applicant_type`,
`a`.`designation` AS `applicant_designation`,
`a`.`applicant_authority` AS `applicant_authority`,
`a`.`rawlocation_id` AS `rawlocation_id` 
from `patent`.`non_inventor_applicant` `a` 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_application` AS 
select `a`.`number_transformed` AS `application_id`,
`a`.`patent_id` AS `patent_id`,
`a`.`type` AS `patent_application_type`,
`a`.`date` AS `filing_date`,
`a`.`series_code_transformed_from_type` AS `series_code`,
`b`.`rule_47_flag` AS `rule_47_flag` 
from (`patent`.`application` `a` 
left join (select `patent`.`rawinventor`.`patent_id` AS `patent_id`,
max(case when `patent`.`rawinventor`.`rule_47` in ('1','TRUE') then 1 else 0 end) AS `rule_47_flag` 
from `patent`.`rawinventor` 
group by `patent`.`rawinventor`.`patent_id`) `b` on(`a`.`patent_id` = `b`.`patent_id`)) 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_assignee_disambiguated` AS 
select `a`.`patent_id` AS `patent_id`,
`a`.`sequence` AS `assignee_sequence`,
`a`.`assignee_id` AS `assignee_id`,
`b`.`name_first` AS `disambig_assignee_individual_name_first`,
`b`.`name_last` AS `disambig_assignee_individual_name_last`,
`b`.`organization` AS `disambig_assignee_organization`,
`b`.`type` AS `assignee_type`,
`c`.`location_id` AS `location_id` 
from ((`patent`.`rawassignee` `a` 
left join `patent`.`assignee` `b` on(`a`.`assignee_id` = `b`.`id`)) 
left join `patent`.`rawlocation` `c` on(`a`.`rawlocation_id` = `c`.`id`)) 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_assignee_not_disambiguated` AS 
select `a`.`patent_id` AS `patent_id`,
`a`.`sequence` AS `assignee_sequence`,
`a`.`assignee_id` AS `assignee_id`,
`a`.`name_first` AS `raw_assignee_individual_name_first`,
`a`.`name_last` AS `raw_assignee_individual_name_last`,
`a`.`organization` AS `raw_assignee_organization`,
`a`.`type` AS `assignee_type`,
`a`.`rawlocation_id` AS `rawlocation_id` 
from `patent`.`rawassignee` `a` 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_attorney_disambiguated` AS 
select `a`.`patent_id` AS `patent_id`,
`a`.`sequence` AS `attorney_sequence`,
`a`.`lawyer_id` AS `attorney_id`,
`b`.`name_first` AS `disambig_attorney_name_first`,
`b`.`name_last` AS `disambig_attorney_name_last`,
`b`.`organization` AS `disambig_attorney_organization`,
`b`.`country` AS `attorney_country` 
from (`patent`.`rawlawyer` `a` 
join `patent`.`lawyer` `b` on(`a`.`lawyer_id` = `b`.`id`)) 
where `b`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_attorney_not_disambiguated` AS 
select `patent`.`rawlawyer`.`lawyer_id` AS `attorney_id`,
`patent`.`rawlawyer`.`sequence` AS `attorney_sequence`,
`patent`.`rawlawyer`.`patent_id` AS `patent_id`,
`patent`.`rawlawyer`.`name_first` AS `raw_attorney_name_first`,
`patent`.`rawlawyer`.`name_last` AS `raw_attorney_name_last`,
`patent`.`rawlawyer`.`organization` AS `raw_attorney_organization`,
`patent`.`rawlawyer`.`country` AS `attorney_country` 
from `patent`.`rawlawyer` 
where `patent`.`rawlawyer`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_botanic` AS 
select `patent`.`botanic`.`patent_id` AS `patent_id`,
`patent`.`botanic`.`latin_name` AS `latin_name`,
`patent`.`botanic`.`variety` AS `plant_variety` 
from `patent`.`botanic` 
where `patent`.`botanic`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_cpc_at_issue` AS 
select `patent`.`cpc`.`patent_id` AS `patent_id`,
`patent`.`cpc`.`sequence` AS `cpc_sequence`,
`patent`.`cpc`.`version` AS `cpc_version_indicator`,
`patent`.`cpc`.`section_id` AS `cpc_section`,
`patent`.`cpc`.`subsection_id` AS `cpc_class`,
`patent`.`cpc`.`group_id` AS `cpc_subclass`,
`patent`.`cpc`.`subgroup_id` AS `cpc_group`,
`patent`.`cpc`.`category` AS `cpc_type`,
`patent`.`cpc`.`action_date` AS `cpc_action_date` 
from `patent`.`cpc` 
where `patent`.`cpc`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_cpc_current` AS 
select `patent`.`cpc_current`.`patent_id` AS `patent_id`,
`patent`.`cpc_current`.`sequence` AS `cpc_sequence`,
`patent`.`cpc_current`.`section_id` AS `cpc_section`,
`patent`.`cpc_current`.`subsection_id` AS `cpc_class`,
`patent`.`cpc_current`.`group_id` AS `cpc_subclass`,
`patent`.`cpc_current`.`subgroup_id` AS `cpc_group`,
`patent`.`cpc_current`.`category` AS `cpc_type`
from `patent`.`cpc_current` 
where `patent`.`cpc_current`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_cpc_title` AS 
select `a`.`id` AS `cpc_subclass`,
`a`.`title` AS `cpc_subclass_title`,
`b`.`id` AS `cpc_group`,
`b`.`title` AS `cpc_group_title`,
`c`.`id` AS `cpc_class`,
`c`.`title` AS `cpc_class_title` 
from ((`patent`.`cpc_group` `a` 
left join `patent`.`cpc_subgroup` `b` on(`a`.`id` = left(`b`.`id`, 4))) 
left join `patent`.`cpc_subsection` `c` on(left(`b`.`id`, 3) = `c`.`id`));

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_examiner_not_disambiguated` AS 
select `patent`.`rawexaminer`.`patent_id` AS `patent_id`,
`patent`.`rawexaminer`.`name_first` AS `raw_examiner_name_first`,
`patent`.`rawexaminer`.`name_last` AS `raw_examiner_name_last`,
`patent`.`rawexaminer`.`role` AS `examiner_role`,
`patent`.`rawexaminer`.`group` AS `art_group` 
from `patent`.`rawexaminer` 
where `patent`.`rawexaminer`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_figures` AS 
select `patent`.`figures`.`patent_id` AS `patent_id`,
`patent`.`figures`.`num_figures` AS `num_figures`,
`patent`.`figures`.`num_sheets` AS `num_sheets` 
from `patent`.`figures` 
where `patent`.`figures`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_foreign_citation` AS 
select `patent`.`foreigncitation`.`patent_id` AS `patent_id`,
`patent`.`foreigncitation`.`sequence` AS `citation_sequence`,
`patent`.`foreigncitation`.`number` AS `citation_application_id`,
`patent`.`foreigncitation`.`date` AS `citation_date`,
`patent`.`foreigncitation`.`category` AS `citation_category`,
`patent`.`foreigncitation`.`country` AS `citation_country` 
from `patent`.`foreigncitation` 
where `patent`.`foreigncitation`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_foreign_priority` AS 
select `a`.`patent_id` AS `patent_id`,
`a`.`sequence` AS `priority_claim_sequence`,
`a`.`kind` AS `priority_claim_kind`,
`a`.`number` AS `foreign_application_id`,
`a`.`date` AS `filing_date`,
`a`.`country_transformed` AS `foreign_country_filed` 
from `patent`.`foreign_priority` `a` 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_gov_interest` AS 
select `patent`.`government_interest`.`patent_id` AS `patent_id`,
`patent`.`government_interest`.`gi_statement` AS `gi_statement` 
from `patent`.`government_interest` 
where `patent`.`government_interest`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_gov_interest_contracts` AS 
select `patent`.`patent_contractawardnumber`.`patent_id` AS `patent_id`,
`patent`.`patent_contractawardnumber`.`contract_award_number` AS `contract_award_number` 
from `patent`.`patent_contractawardnumber` 
where `patent`.`patent_contractawardnumber`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_gov_interest_org` AS 
select `a`.`patent_id` AS `patent_id`,
`a`.`organization_id` AS `gi_organization_id`,
`b`.`name` AS `fedagency_name`,
`b`.`level_one` AS `level_one`,
`b`.`level_two` AS `level_two`,
`b`.`level_three` AS `level_three` 
from (`patent`.`patent_govintorg` `a` 
left join `patent`.`government_organization` `b` on(`a`.`organization_id` = `b`.`organization_id`)) 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_inventor_disambiguated` AS 
select `a`.`patent_id` AS `patent_id`,
`a`.`sequence` AS `inventor_sequence`,
`a`.`inventor_id` AS `inventor_id`,
`b`.`name_first` AS `disambig_inventor_name_first`,
`b`.`name_last` AS `disambig_inventor_name_last`,
`c`.`gender_flag` AS `gender_code`,
`d`.`location_id` AS `location_id` 
from (`patent`.`rawinventor` `a` 
    LEFT JOIN `patent`.`inventor` `b` ON (`a`.`inventor_id` = `b`.`id`)
    LEFT JOIN `gender_attribution`.`inventor_gender_{{datestring}}` `c` ON (`a`.`inventor_id` = `c`.`inventor_id`)) 
    LEFT JOIN `patent`.`rawlocation` `d` ON (`a`.`rawlocation_id` = `d`.`id`)
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_inventor_not_disambiguated` AS 
select `patent`.`rawinventor`.`patent_id` AS `patent_id`,
`patent`.`rawinventor`.`sequence` AS `inventor_sequence`,
`patent`.`rawinventor`.`inventor_id` AS `inventor_id`,
`patent`.`rawinventor`.`name_first` AS `raw_inventor_name_first`,
`patent`.`rawinventor`.`name_last` AS `raw_inventor_name_last`,
`patent`.`rawinventor`.`deceased` AS `deceased_flag`,
`patent`.`rawinventor`.`rawlocation_id` AS `rawlocation_id` 
from `patent`.`rawinventor` 
where `patent`.`rawinventor`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_ipc_at_issue` AS 
select `patent`.`ipcr`.`patent_id` AS `patent_id`,
`patent`.`ipcr`.`sequence` AS `ipc_sequence`,
`patent`.`ipcr`.`classification_level` AS `classification_level`,
`patent`.`ipcr`.`section` AS `section`,
`patent`.`ipcr`.`ipc_class` AS `ipc_class`,
`patent`.`ipcr`.`subclass` AS `subclass`,
`patent`.`ipcr`.`main_group` AS `main_group`,
`patent`.`ipcr`.`subgroup` AS `subgroup`,
`patent`.`ipcr`.`classification_value` AS `classification_value`,
`patent`.`ipcr`.`classification_status` AS `classification_status`,
`patent`.`ipcr`.`classification_data_source` AS `classification_data_source`,
`patent`.`ipcr`.`action_date` AS `action_date`,
`patent`.`ipcr`.`ipc_version_indicator` AS `ipc_version_indicator` 
from `patent`.`ipcr` 
where `patent`.`ipcr`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_location_disambiguated` AS 
select `location`.`id` AS `location_id`,
`location`.`city` AS `disambig_city`,
`location`.`state` AS `disambig_state`,
`location`.`country` AS `disambig_country`,
`location`.`latitude` AS `latitude`,
`location`.`longitude` AS `longitude`,
`location`.`county` AS `county`,
`location`.`state_fips` AS `state_fips`,
RIGHT(`location`.`county_fips`, 3) AS `county_fips` 
from `patent`.`location` 
where `location`.`id` in (select distinct `patent`.`rawlocation`.`location_id` 
from `patent`.`rawlocation`
where `patent`.`rawlocation`.`version_indicator` <= '{{datestring}}');

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_location_not_disambiguated` AS 
select `a`.`id` AS `rawlocation_id`,
`a`.`location_id` AS `location_id`,
`a`.`city` AS `raw_city`,
`a`.`state` AS `raw_state`,
`a`.`country` AS `raw_country` 
from `patent`.`rawlocation` `a` 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_other_reference` AS 
select `patent`.`otherreference`.`patent_id` AS `patent_id`,
`patent`.`otherreference`.`sequence` AS `other_reference_sequence`,
`patent`.`otherreference`.`text` AS `other_reference_text` 
from `patent`.`otherreference` 
where `patent`.`otherreference`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_patent` AS 
select `patent`.`patent`.`id` AS `patent_id`,
`patent`.`patent`.`type` AS `patent_type`,
`patent`.`patent`.`date` AS `patent_date`,
`patent`.`patent`.`title` AS `patent_title`,
`patent`.`patent`.`abstract` AS `patent_abstract`,
`patent`.`patent`.`kind` AS `wipo_kind`,
`patent`.`patent`.`num_claims` AS `num_claims`,
`patent`.`patent`.`withdrawn` AS `withdrawn`,
`patent`.`patent`.`filename` AS `filename` 
from `patent`.`patent` 
where `patent`.`patent`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_pct_data` AS 
select `a`.`patent_id` AS `patent_id`,
`a`.`date` AS `published_or_filed_date`,
`a`.`371_date` AS `pct_371_date`,
`a`.`102_date` AS `pct_102_date`,
`a`.`country` AS `filed_country`,
`a`.`kind` AS `application_kind`,
`a`.`rel_id` AS `pct_doc_number`,
`a`.`doc_type` AS `pct_doc_type` 
from `patent`.`pct_data` `a` 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_persistent_assignee` AS 
select `patent`.`persistent_assignee_disambig`.`patent_id` AS `patent_id`,
`patent`.`persistent_assignee_disambig`.`sequence` AS `assignee_sequence`,
`patent`.`persistent_assignee_disambig`.`disamb_assignee_id_20181127` AS `disamb_assignee_id_20181127`,
`patent`.`persistent_assignee_disambig`.`disamb_assignee_id_20190312` AS `disamb_assignee_id_20190312`,
`patent`.`persistent_assignee_disambig`.`disamb_assignee_id_20190820` AS `disamb_assignee_id_20190820`,
`patent`.`persistent_assignee_disambig`.`disamb_assignee_id_20191008` AS `disamb_assignee_id_20191008`,
`patent`.`persistent_assignee_disambig`.`disamb_assignee_id_20191231` AS `disamb_assignee_id_20191231`,
`patent`.`persistent_assignee_disambig`.`disamb_assignee_id_20200331` AS `disamb_assignee_id_20200331`,
`patent`.`persistent_assignee_disambig`.`disamb_assignee_id_20200630` AS `disamb_assignee_id_20200630`,
`patent`.`persistent_assignee_disambig`.`disamb_assignee_id_20200929` AS `disamb_assignee_id_20200929`,
`patent`.`persistent_assignee_disambig`.`disamb_assignee_id_20201229` AS `disamb_assignee_id_20201229`,
`patent`.`persistent_assignee_disambig`.`disamb_assignee_id_20210330` AS `disamb_assignee_id_20210330`,
`patent`.`persistent_assignee_disambig`.`disamb_assignee_id_20210629` AS `disamb_assignee_id_20210629`,
`patent`.`persistent_assignee_disambig`.`disamb_assignee_id_20210930` AS `disamb_assignee_id_20210930`,
`patent`.`persistent_assignee_disambig`.`disamb_assignee_id_20211230` AS `disamb_assignee_id_20211230`,
`patent`.`persistent_assignee_disambig`.`disamb_assignee_id_20220630` AS `disamb_assignee_id_20220630`,
`patent`.`persistent_assignee_disambig`.`disamb_assignee_id_20220929` AS `disamb_assignee_id_20220929`,
`patent`.`persistent_assignee_disambig`.`disamb_assignee_id_20230330` AS `disamb_assignee_id_20230330`,
`patent`.`persistent_assignee_disambig`.`disamb_assignee_id_20230629` AS `disamb_assignee_id_20230629`
from `patent`.`persistent_assignee_disambig` 
where `patent`.`persistent_assignee_disambig`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_persistent_inventor` AS 
select `patent`.`persistent_inventor_disambig`.`patent_id` AS `patent_id`,
`patent`.`persistent_inventor_disambig`.`sequence` AS `inventor_sequence`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20170808` AS `disamb_inventor_id_20170808`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20171003` AS `disamb_inventor_id_20171003`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20171226` AS `disamb_inventor_id_20171226`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20180528` AS `disamb_inventor_id_20180528`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20181127` AS `disamb_inventor_id_20181127`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20190312` AS `disamb_inventor_id_20190312`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20190820` AS `disamb_inventor_id_20190820`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20191008` AS `disamb_inventor_id_20191008`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20191231` AS `disamb_inventor_id_20191231`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20200331` AS `disamb_inventor_id_20200331`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20200630` AS `disamb_inventor_id_20200630`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20200929` AS `disamb_inventor_id_20200929`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20201229` AS `disamb_inventor_id_20201229`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20211230` AS `disamb_inventor_id_20211230`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20220630` AS `disamb_inventor_id_20220630`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20220929` AS `disamb_inventor_id_20220929`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20230330` AS `disamb_inventor_id_20230330`,
`patent`.`persistent_inventor_disambig`.`disamb_inventor_id_20230629` AS `disamb_inventor_id_20230629` 
from `patent`.`persistent_inventor_disambig` 
where `patent`.`persistent_inventor_disambig`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_rel_app_text` AS 
select `patent`.`rel_app_text`.`patent_id` AS `patent_id`,
`patent`.`rel_app_text`.`text` AS `rel_app_text` 
from `patent`.`rel_app_text` 
where `patent`.`rel_app_text`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_us_application_citation` AS 
select `patent`.`usapplicationcitation`.`patent_id` AS `patent_id`,
`patent`.`usapplicationcitation`.`sequence` AS `citation_sequence`,
`patent`.`usapplicationcitation`.`number_transformed` AS `citation_document_number`,
`patent`.`usapplicationcitation`.`date` AS `citation_date`,
`patent`.`usapplicationcitation`.`name` AS `record_name`,
`patent`.`usapplicationcitation`.`kind` AS `wipo_kind`,
`patent`.`usapplicationcitation`.`category` AS `citation_category` 
from `patent`.`usapplicationcitation` 
where `patent`.`usapplicationcitation`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_us_citation` AS 
select `a`.`patent_id` AS `patent_id`,
`a`.`citation_type` AS `citation_type`,
`a`.`citation_patent_id` AS `citation_patent_id`,
`a`.`citation_document_number` AS `citation_document_number`,
`a`.`citation_date` AS `citation_date`,
`a`.`record_name` AS `record_name`,
`a`.`wipo_kind` AS `wipo_kind`,
`a`.`citation_category` AS `citation_category`,
`a`.`version_indicator` AS `version_indicator`,
`a`.`created_date` AS `created_date`,
`a`.`updated_date` AS `updated_date`,
row_number() over ( partition by `a`.`patent_id` order by `a`.`citation_type` desc,`a`.`subseq`) AS `citation_sequence` 
from (select `uspc`.`patent_id` AS `patent_id`,
'patent' AS `citation_type`,
`uspc`.`citation_id` AS `citation_patent_id`,
NULL AS `citation_document_number`,
`uspc`.`sequence` AS `subseq`,
`uspc`.`date` AS `citation_date`,
`uspc`.`name` AS `record_name`,
`uspc`.`kind` AS `wipo_kind`,
`uspc`.`category` AS `citation_category`,
`uspc`.`version_indicator` AS `version_indicator`,
`uspc`.`created_date` AS `created_date`,
`uspc`.`updated_date` AS `updated_date` 
from `patent`.`uspatentcitation` `uspc` 
union all 
select `usac`.`patent_id` AS `patent_id`,
'application' AS `citation_type`,
NULL AS `citation_patent_id`,
`usac`.`number` AS `citation_document_number`,
`usac`.`sequence` AS `subseq`,
`usac`.`date` AS `citation_date`,
`usac`.`name` AS `record_name`,
`usac`.`kind` AS `wipo_kind`,
`usac`.`category` AS `citation_category`,
`usac`.`version_indicator` AS `version_indicator`,
`usac`.`created_date` AS `created_date`,
`usac`.`updated_date` AS `updated_date` 
from `patent`.`usapplicationcitation` `usac`) `a` 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_us_patent_citation` AS 
select `patent`.`uspatentcitation`.`patent_id` AS `patent_id`,
`patent`.`uspatentcitation`.`sequence` AS `citation_sequence`,
`patent`.`uspatentcitation`.`citation_id` AS `citation_patent_id`,
`patent`.`uspatentcitation`.`date` AS `citation_date`,
`patent`.`uspatentcitation`.`name` AS `record_name`,
`patent`.`uspatentcitation`.`kind` AS `wipo_kind`,
`patent`.`uspatentcitation`.`category` AS `citation_category` 
from `patent`.`uspatentcitation` 
where `patent`.`uspatentcitation`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_us_rel_doc` AS 
select `patent`.`usreldoc`.`patent_id` AS `patent_id`,
`patent`.`usreldoc`.`reldocno` AS `related_doc_number`,
`patent`.`usreldoc`.`country` AS `published_country`,
`patent`.`usreldoc`.`doctype` AS `related_doc_type`,
`patent`.`usreldoc`.`relkind` AS `related_doc_kind`,
`patent`.`usreldoc`.`date` AS `related_doc_published_date`,
`patent`.`usreldoc`.`status` AS `related_doc_status`,
`patent`.`usreldoc`.`sequence` AS `related_doc_sequence`,
`patent`.`usreldoc`.`kind` AS `wipo_kind` 
from `patent`.`usreldoc` 
where `patent`.`usreldoc`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_us_term_of_grant` AS 
select `patent`.`us_term_of_grant`.`patent_id` AS `patent_id`,
`patent`.`us_term_of_grant`.`disclaimer_date` AS `disclaimer_date`,
`patent`.`us_term_of_grant`.`term_disclaimer` AS `term_disclaimer`,
`patent`.`us_term_of_grant`.`term_grant` AS `term_grant`,
`patent`.`us_term_of_grant`.`term_extension` AS `term_extension` 
from `patent`.`us_term_of_grant` 
where `patent`.`us_term_of_grant`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_uspc_at_issue` AS 
select `a`.`patent_id` AS `patent_id`,
`a`.`sequence` AS `uspc_sequence`,
`a`.`mainclass_id` AS `uspc_mainclass_id`,
`b`.`title` AS `uspc_mainclass_title`,
`a`.`subclass_id` AS `uspc_subclass_id`,
`c`.`title` AS `uspc_subclass_title` 
from ((`patent`.`uspc` `a` 
left join `patent`.`mainclass_current` `b` on(`a`.`mainclass_id` = `b`.`id`)) 
left join `patent`.`subclass_current` `c` on(`a`.`subclass_id` = `c`.`id`)) 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_granted`.`g_wipo_technology` AS 
select `a`.`patent_id` AS `patent_id`,
`a`.`sequence` AS `wipo_field_sequence`,
`a`.`field_id` AS `wipo_field_id`,
`b`.`sector_title` AS `wipo_sector_title`,
`b`.`field_title` AS `wipo_field_title` 
from (`patent`.`wipo` `a` 
left join `patent`.`wipo_field` `b` on(`a`.`field_id` = `b`.`id`)) 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_applicant_not_disambiguated` AS 
select `a`.`document_number` AS `pgpub_id`,
`a`.`sequence` AS `applicant_sequence`,
`a`.`name_first` AS `raw_applicant_name_first`,
`a`.`name_last` AS `raw_applicant_name_last`,
`a`.`organization` AS `raw_applicant_organization`,
`a`.`type` AS `applicant_type`,
`a`.`designation` AS `applicant_designation`,
`a`.`applicant_authority` AS `applicant_authority`,
`a`.`rawlocation_id` AS `rawlocation_id` 
from `pregrant_publications`.`us_parties` `a` 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_assignee_disambiguated` AS 
select `a`.`document_number` AS `pgpub_id`,
`a`.`sequence` AS `assignee_sequence`,
`a`.`assignee_id` AS `assignee_id`,
`b`.`name_first` AS `disambig_assignee_individual_name_first`,
`b`.`name_last` AS `disambig_assignee_individual_name_last`,
`b`.`organization` AS `disambig_assignee_organization`,
`b`.`type` AS `assignee_type`,
`c`.`location_id` AS `location_id` 
from ((`pregrant_publications`.`rawassignee` `a` 
left join `patent`.`assignee` `b` on(`a`.`assignee_id` = `b`.`id`)) 
left join `pregrant_publications`.`rawlocation` `c` on(`a`.`rawlocation_id` = `c`.`id`)) 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_assignee_not_disambiguated` AS 
select `a`.`document_number` AS `pgpub_id`,
`a`.`sequence` AS `assignee_sequence`,
`a`.`assignee_id` AS `assignee_id`,
`a`.`name_first` AS `raw_assignee_individual_name_first`,
`a`.`name_last` AS `raw_assignee_individual_name_last`,
`a`.`organization` AS `raw_assignee_organization`,
`a`.`type` AS `assignee_type`,
`a`.`rawlocation_id` AS `rawlocation_id` 
from `pregrant_publications`.`rawassignee` `a` 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_cpc_at_issue` AS 
select `pregrant_publications`.`cpc`.`document_number` AS `pgpub_id`,
`pregrant_publications`.`cpc`.`sequence` AS `cpc_sequence`,
`pregrant_publications`.`cpc`.`version` AS `cpc_version_indicator`,
`pregrant_publications`.`cpc`.`section_id` AS `cpc_section`,
`pregrant_publications`.`cpc`.`subsection_id` AS `cpc_class`,
`pregrant_publications`.`cpc`.`group_id` AS `cpc_subclass`,
`pregrant_publications`.`cpc`.`subgroup_id` AS `cpc_group`,
`pregrant_publications`.`cpc`.`category` AS `cpc_type`,
`pregrant_publications`.`cpc`.`action_date` AS `cpc_action_date` 
from `pregrant_publications`.`cpc` 
where `pregrant_publications`.`cpc`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_cpc_current` AS 
select `pregrant_publications`.`cpc_current`.`document_number` AS `pgpub_id`,
`pregrant_publications`.`cpc_current`.`sequence` AS `cpc_sequence`,
`pregrant_publications`.`cpc_current`.`section_id` AS `cpc_section`,
`pregrant_publications`.`cpc_current`.`subsection_id` AS `cpc_class`,
`pregrant_publications`.`cpc_current`.`group_id` AS `cpc_subclass`,
`pregrant_publications`.`cpc_current`.`subgroup_id` AS `cpc_group`,
`pregrant_publications`.`cpc_current`.`category` AS `cpc_type`,
`pregrant_publications`.`cpc_current`.`version` AS `cpc_version_indicator`
from `pregrant_publications`.`cpc_current` 
where `pregrant_publications`.`cpc_current`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_cpc_title` AS 
select `a`.`id` AS `cpc_subclass`,
`a`.`title` AS `cpc_subclass_title`,
`b`.`id` AS `cpc_group`,
`b`.`title` AS `cpc_group_title`,
`c`.`id` AS `cpc_class`,
`c`.`title` AS `cpc_class_title` 
from ((`patent`.`cpc_group` `a` 
left join `patent`.`cpc_subgroup` `b` on(`a`.`id` = left(`b`.`id`, 4))) 
left join `patent`.`cpc_subsection` `c` on(left(`b`.`id`, 3) = `c`.`id`));

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_foreign_priority` AS 
select `a`.`document_number` AS `pgpub_id`,
row_number() over ( partition by `a`.`document_number` order by `a`.`date`) AS `priority_claim_sequence`,
`a`.`kind` AS `priority_claim_kind`,
`a`.`foreign_doc_number` AS `foreign_application_id`,
`a`.`date` AS `filing_date`,
`a`.`country` AS `foreign_country_filed` 
from `pregrant_publications`.`foreign_priority` `a` 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_gov_interest` AS 
select `pregrant_publications`.`government_interest`.`document_number` AS `pgpub_id`,
`pregrant_publications`.`government_interest`.`gi_statement` AS `gi_statement` 
from `pregrant_publications`.`government_interest` 
where `pregrant_publications`.`government_interest`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_gov_interest_contracts` AS 
select `pc`.`document_number` AS `pgpub_id`,
`pc`.`contract_award_number` AS `contract_award_number` 
from `pregrant_publications`.`publication_contractawardnumber` `pc` 
where `pc`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_gov_interest_org` AS 
select `pg`.`document_number` AS `pgpub_id`,
`pg`.`organization_id` AS `gi_organization_id`,
`go`.`name` AS `fedagency_name`,
`go`.`level_one` AS `level_one`,
`go`.`level_two` AS `level_two`,
`go`.`level_three` AS `level_three` 
from (`pregrant_publications`.`publication_govintorg` `pg` 
left join `patent`.`government_organization` `go` on(`pg`.`organization_id` = `go`.`organization_id`)) 
where `pg`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_granted_pgpubs_crosswalk` AS 
SELECT 
`xw`.`document_number` AS `pgpub_id`,
`xw`.`patent_id` AS `patent_id`,
`xw`.`application_number` AS `application_id`,
CASE WHEN `xw`.`current_pgpub_id_flag` = 1 THEN 'TRUE' ELSE 'FALSE' END AS `current_pgpub_id_flag`,
CASE WHEN `xw`.`current_patent_id_flag` = 1 THEN 'TRUE' ELSE 'FALSE' END AS `current_patent_id_flag`
FROM `pregrant_publications`.`granted_patent_crosswalk_{{datestring}}` `xw`
WHERE (`xw`.`g_version_indicator` <= '{{datestring}}' or `xw`.`g_version_indicator` is null) 
AND (`xw`.`pg_version_indicator` <= '{{datestring}}' or `xw`.`pg_version_indicator` is null);

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_inventor_disambiguated` AS 
select `a`.`document_number` AS `pgpub_id`,
`a`.`sequence` AS `inventor_sequence`,
`a`.`inventor_id` AS `inventor_id`,
`b`.`name_first` AS `disambig_inventor_name_first`,
`b`.`name_last` AS `disambig_inventor_name_last`,
`c`.`gender_flag` AS `gender_code`,
`d`.`location_id` AS `location_id` 
from (`pregrant_publications`.`rawinventor` `a` 
    LEFT JOIN `patent`.`inventor` `b` ON (`a`.`inventor_id` = `b`.`id`)
    LEFT JOIN `gender_attribution`.`inventor_gender_{{datestring}}` `c` ON (`a`.`inventor_id` = `c`.`inventor_id`)) 
    LEFT JOIN `pregrant_publications`.`rawlocation` `d` ON (`a`.`rawlocation_id` = `d`.`id`)
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_inventor_not_disambiguated` AS 
select `a`.`document_number` AS `pgpub_id`,
`a`.`sequence` AS `inventor_sequence`,
`a`.`inventor_id` AS `inventor_id`,
`a`.`name_first` AS `raw_inventor_name_first`,
`a`.`name_last` AS `raw_inventor_name_last`,
`a`.`deceased` AS `deceased_flag`,
`a`.`rawlocation_id` AS `rawlocation_id` 
from `pregrant_publications`.`rawinventor` `a` 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_ipc_at_issue` AS 
select `pregrant_publications`.`ipcr`.`document_number` AS `pgpub_id`,
`pregrant_publications`.`ipcr`.`sequence` AS `ipc_sequence`,
`pregrant_publications`.`ipcr`.`class_level` AS `classification_level`,
`pregrant_publications`.`ipcr`.`section` AS `section`,
`pregrant_publications`.`ipcr`.`class` AS `ipc_class`,
`pregrant_publications`.`ipcr`.`subclass` AS `subclass`,
`pregrant_publications`.`ipcr`.`main_group` AS `main_group`,
`pregrant_publications`.`ipcr`.`subgroup` AS `subgroup`,
`pregrant_publications`.`ipcr`.`class_value` AS `classification_value`,
`pregrant_publications`.`ipcr`.`class_status` AS `classification_status`,
`pregrant_publications`.`ipcr`.`class_data_source` AS `classification_data_source`,
`pregrant_publications`.`ipcr`.`action_date` AS `action_date`,
`pregrant_publications`.`ipcr`.`version` AS `ipc_version_indicator` 
from `pregrant_publications`.`ipcr` 
where `pregrant_publications`.`ipcr`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_location_disambiguated` AS 
select `location`.`id` AS `location_id`,
`location`.`city` AS `disambig_city`,
`location`.`state` AS `disambig_state`,
`location`.`country` AS `disambig_country`,
`location`.`latitude` AS `latitude`,
`location`.`longitude` AS `longitude`,
`location`.`county` AS `county`,
`location`.`state_fips` AS `state_fips`,
RIGHT(`location`.`county_fips`, 3) AS `county_fips` 
from `patent`.`location` 
where `location`.`id` in (select distinct `pregrant_publications`.`rawlocation`.`location_id` 
from `pregrant_publications`.`rawlocation`
where `pregrant_publications`.`rawlocation`.`version_indicator` <= '{{datestring}}') ;

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_location_not_disambiguated` AS 
select `a`.`id` AS `rawlocation_id`,
`a`.`location_id` AS `location_id`,
`a`.`city` AS `raw_city`,
`a`.`state` AS `raw_state`,
`a`.`country` AS `raw_country` 
from `pregrant_publications`.`rawlocation` `a` 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_pct_data` AS 
select `a`.`document_number` AS `pgpub_id`,
`a`.`date` AS `published_or_filed_date`,
`a`.`us_371c124_date` AS `pct_371_date`,
`a`.`us_371c12_date` AS `pct_102_date`,
`a`.`country` AS `filed_country`,
`a`.`kind` AS `application_kind`,
`a`.`pct_doc_number` AS `pct_doc_number`,
`a`.`doc_type` AS `pct_doc_type` 
from `pregrant_publications`.`pct_data` `a` 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_persistent_assignee` AS 
select `pregrant_publications`.`persistent_assignee_disambig`.`document_number` AS `pgpub_id`,
`pregrant_publications`.`persistent_assignee_disambig`.`sequence` AS `assignee_sequence`,
`pregrant_publications`.`persistent_assignee_disambig`.`disamb_assignee_id_20201229` AS `disamb_assignee_id_20201229`,
`pregrant_publications`.`persistent_assignee_disambig`.`disamb_assignee_id_20210330` AS `disamb_assignee_id_20210330`,
`pregrant_publications`.`persistent_assignee_disambig`.`disamb_assignee_id_20210930` AS `disamb_assignee_id_20210930`,
`pregrant_publications`.`persistent_assignee_disambig`.`disamb_assignee_id_20211230` AS `disamb_assignee_id_20211230`,
`pregrant_publications`.`persistent_assignee_disambig`.`disamb_assignee_id_20220630` AS `disamb_assignee_id_20220630`,
`pregrant_publications`.`persistent_assignee_disambig`.`disamb_assignee_id_20220929` AS `disamb_assignee_id_20220929`,
`pregrant_publications`.`persistent_assignee_disambig`.`disamb_assignee_id_20230330` AS `disamb_assignee_id_20230330`,
`pregrant_publications`.`persistent_assignee_disambig`.`disamb_assignee_id_20230629` AS `disamb_assignee_id_20230629`
from `pregrant_publications`.`persistent_assignee_disambig` 
where `pregrant_publications`.`persistent_assignee_disambig`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_persistent_inventor` AS 
select `pregrant_publications`.`persistent_inventor_disambig`.`document_number` AS `pgpub_id`,
`pregrant_publications`.`persistent_inventor_disambig`.`sequence` AS `inventor_sequence`,
`pregrant_publications`.`persistent_inventor_disambig`.`disamb_inventor_id_20210330` AS `disamb_inventor_id_20210330`,
`pregrant_publications`.`persistent_inventor_disambig`.`disamb_inventor_id_20210629` AS `disamb_inventor_id_20210629`,
`pregrant_publications`.`persistent_inventor_disambig`.`disamb_inventor_id_20210930` AS `disamb_inventor_id_20210930`,
`pregrant_publications`.`persistent_inventor_disambig`.`disamb_inventor_id_20211230` AS `disamb_inventor_id_20211230`,
`pregrant_publications`.`persistent_inventor_disambig`.`disamb_inventor_id_20220630` AS `disamb_inventor_id_20220630`,
`pregrant_publications`.`persistent_inventor_disambig`.`disamb_inventor_id_20220929` AS `disamb_inventor_id_20220929`,
`pregrant_publications`.`persistent_inventor_disambig`.`disamb_inventor_id_20230330` AS `disamb_inventor_id_20230330`,
`pregrant_publications`.`persistent_inventor_disambig`.`disamb_inventor_id_20230629` AS `disamb_inventor_id_20230629` 
from `pregrant_publications`.`persistent_inventor_disambig` 
where `pregrant_publications`.`persistent_inventor_disambig`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_published_application` AS 
select `b`.`document_number` AS `pgpub_id`,
`b`.`application_number` AS `application_id`,
`b`.`date` AS `filing_date`,
`b`.`type` AS `patent_type`,
`a`.`filing_type` AS `filing_type`,
`a`.`date` AS `published_date`,
`a`.`kind` AS `wipo_kind`,
`b`.`series_code` AS `series_code`,
`b`.`invention_title` AS `application_title`,
`b`.`invention_abstract` AS `application_abstract`,
`b`.`rule_47_flag` AS `rule_47_flag`,
`b`.`filename` AS `filename` 
from (`pregrant_publications`.`publication` `a` 
join `pregrant_publications`.`application` `b` on(`a`.`document_number` = `b`.`document_number`)) 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_rel_app_text` AS 
select `pregrant_publications`.`rel_app_text`.`document_number` AS `pgpub_id`,
`pregrant_publications`.`rel_app_text`.`text` AS `rel_app_text` 
from `pregrant_publications`.`rel_app_text` 
where `pregrant_publications`.`rel_app_text`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_us_rel_doc` AS 
select `pregrant_publications`.`usreldoc`.`document_number` AS `pgpub_id`,
`pregrant_publications`.`usreldoc`.`related_doc_number` AS `related_doc_number`,
`pregrant_publications`.`usreldoc`.`country` AS `published_country`,
`pregrant_publications`.`usreldoc`.`doc_type` AS `related_doc_type`,
`pregrant_publications`.`usreldoc`.`relkind` AS `related_doc_kind`,
`pregrant_publications`.`usreldoc`.`date` AS `related_doc_published_date`,
row_number() over ( partition by `pregrant_publications`.`usreldoc`.`document_number` order by `pregrant_publications`.`usreldoc`.`country`,
`pregrant_publications`.`usreldoc`.`related_doc_number`) AS `related_doc_sequence` 
from `pregrant_publications`.`usreldoc` 
where `pregrant_publications`.`usreldoc`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_uspc_at_issue` AS 
select `a`.`document_number` AS `pgpub_id`,
`a`.`sequence` AS `uspc_sequence`,
`a`.`mainclass_id` AS `uspc_mainclass_id`,
`b`.`title` AS `uspc_mainclass_title`,
`a`.`subclass_id` AS `uspc_subclass_id`,
`c`.`title` AS `uspc_subclass_title` 
from ((`pregrant_publications`.`uspc` `a` 
left join `patent`.`mainclass_current` `b` on(`a`.`mainclass_id` = `b`.`id`)) 
left join `patent`.`subclass_current` `c` on(`a`.`subclass_id` = `c`.`id`)) 
where `a`.`version_indicator` <= '{{datestring}}';

CREATE OR REPLACE SQL SECURITY INVOKER VIEW `patentsview_export_pregrant`.`pg_wipo_technology` AS 
select `a`.`document_number` AS `pgpub_id`,
`a`.`sequence` AS `wipo_field_sequence`,
`a`.`field_id` AS `wipo_field_id`,
`b`.`sector_title` AS `wipo_sector_title`,
`b`.`field_title` AS `wipo_field_title` 
from (`pregrant_publications`.`wipo` `a` 
left join `patent`.`wipo_field` `b` on(`a`.`field_id` = `b`.`id`)) 
where `a`.`version_indicator` <= '{{datestring}}';

