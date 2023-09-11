{% set elastic_target_database = params.elastic_database_prefix + params.version_indicator.replace("-","") %}
{% set reporting_database = params.reporting_database %}
use `{{elastic_target_database}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_target_database}}`.`patent_application`
(

    `application_id` varchar(36) COLLATE utf8mb4_unicode_ci NOT NULL,
    `patent_id`      varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
    `type`           varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `number`         varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `country`        varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `date`           date                                   DEFAULT NULL,
    `series_code`    varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `rule_47_flag`   varchar(8) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    PRIMARY KEY (`application_id`, `patent_id`),
    KEY              `ix_application_number` (`number`),
    KEY              `ix_application_patent_id` (`patent_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_target_database}}`.`patent_application`;

INSERT INTO `{{elastic_target_database}}`.patent_application( application_id, patent_id, type, number, country, date
                                                            , series_code
                                                            , rule_47_flag)
select a.application_id
     , a.patent_id
     , a.type
     , a.number
     , a.country
     , a.date
     , pa.series_code_transformed_from_type
     , x.rule_47_flag
from `{{reporting_database}}`.application a
         join patent.application pa on pa.patent_id = a.patent_id
         join (select patent_id
                    , case
                          when max(rule_47) = '0' then 'FALSE'
                          when max(rule_47) = '1' then 'TRUE'
                          else max(rule_47) end rule_47_flag
               from patent.rawinventor ri
               group by patent_id) x on x.patent_id = a.patent_id;