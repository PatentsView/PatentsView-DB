{% set elastic_db = "elastic_production_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`patent_gov_contract`
(
    `patent_id`    varchar(24) COLLATE utf8mb4_unicode_ci NOT NULL,
    `award_number` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
    PRIMARY KEY (`patent_id`, `award_number`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`patent_gov_interest_organizations`
(
    `patent_id`   varchar(32) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `name`        varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `level_one`   varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `level_two`   varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `level_three` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    KEY           `ix_government_organization_name` (`name`),
    KEY           `ix_government_organization_level_one` (`level_one`),
    KEY           `ix_government_organization_level_two` (`level_two`),
    KEY           `ix_government_organization_level_three` (`level_three`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.`patent_gov_interest_organizations`;

insert into `{{elastic_db}}`.patent_gov_interest_organizations(patent_id, name, level_one, level_two, level_three)
select
    pgi.patent_id
  , name
  , level_one
  , level_two
  , level_three
from
    `{{reporting_db}}`.government_organization go
        join `{{reporting_db}}`.patent_govintorg pgi
on pgi.organization_id = go.organization_id
    join `{{elastic_db}}`.patents p on p.patent_id = pgi.patent_id;

TRUNCATE TABLE `{{elastic_db}}`.`patent_gov_contract`;

insert into `{{elastic_db}}`.patent_gov_contract(patent_id, award_number)
select
    c.patent_id
  , contract_award_number
from
    `{{reporting_db}}`.patent_contractawardnumber c
        join `{{elastic_db}}`.patents p on p.patent_id = c.patent_id;
