use `elastic_production_{{ dag_run.logical_date | ds_nodash }}`;

CREATE TABLE IF NOT EXISTS `elastic_production_{{ dag_run.logical_date | ds_nodash }}`.`patent_applicant`
(
    `patent_id`              varchar(36) COLLATE utf8mb4_unicode_ci NOT NULL,
    `lname`                  varchar(64) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `fname`                  varchar(64) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `organization`           varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `sequence`               int(11) DEFAULT NULL,
    `designation`            varchar(20) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `applicant_type`         varchar(30) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `location_id`            int(11) DEFAULT NULL,
    `persistent_location_id` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    KEY                      `patent_id` (`patent_id`, `sequence`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `elastic_production_{{ dag_run.logical_date | ds_nodash }}`.patent_applicant;
insert into `elastic_production_{{ dag_run.logical_date | ds_nodash }}`.patent_applicant( patent_id, lname, fname, organization, sequence, designation
                                                          , applicant_type
                                                          , location_id, persistent_location_id)

select nia.patent_id
     , nia.lname
     , nia.fname
     , nia.organization
     , nia.sequence
     , nia.designation
     , nia.applicant_type
     , l.location_id
     , timl.old_location_id
from patent.non_inventor_applicant nia
         join `elastic_production_{{ dag_run.logical_date | ds_nodash }}`.patents p on nia.patent_id = p.patent_id
         left join patent.rawlocation rl on rl.id = nia.rawlocation_id
         left join `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.temp_id_mapping_location timl on timl.old_location_id = rl.location_id
         left join `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.location l on l.location_id = timl.new_location_id

