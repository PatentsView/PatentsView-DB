{% set elastic_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

CREATE TABLE IF NOT EXISTS `{{elastic_db}}`.`patents`
(
    `patent_id`                                             varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
    `type`                                                  varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `number`                                                varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
    `country`                                               varchar(20) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `date`                                                  date                                    DEFAULT NULL,
    `year`                                                  smallint(5) unsigned DEFAULT NULL,
    `abstract`                                              text COLLATE utf8mb4_unicode_ci         DEFAULT NULL,
    `title`                                                 text COLLATE utf8mb4_unicode_ci         DEFAULT NULL,
    `kind`                                                  varchar(10) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `num_claims`                                            smallint(5) unsigned DEFAULT NULL,
    `num_foreign_documents_cited`                           int(10) unsigned NOT NULL,
    `num_us_applications_cited`                             int(10) unsigned NOT NULL,
    `num_us_patents_cited`                                  int(10) unsigned NOT NULL,
    `num_total_documents_cited`                             int(10) unsigned NOT NULL,
    `num_times_cited_by_us_patents`                         int(10) unsigned NOT NULL,
    `earliest_application_date`                             date                                    DEFAULT NULL,
    `patent_processing_days`                                int(10) unsigned DEFAULT NULL,
    `uspc_current_mainclass_average_patent_processing_days` int(10) unsigned DEFAULT NULL,
    `cpc_current_group_average_patent_processing_days`      int(10) unsigned DEFAULT NULL,
    `term_extension`                                        int(10) unsigned DEFAULT NULL,
    `detail_desc_length`                                    int(10) unsigned DEFAULT NULL,
    `gi_statement`                                          text                                    default null,
    `patent_zero_prefix`                                    varchar(20) COLLATE utf8mb4_unicode_ci,
    PRIMARY KEY (`patent_id`),
    KEY                                                     `ix_patent_number` (`number`),
    KEY                                                     `ix_patent_title` (`title`(128)),
    KEY                                                     `ix_patent_type` (`type`),
    KEY                                                     `ix_patent_year` (`year`),
    KEY                                                     `ix_patent_date` (`date`),
    KEY                                                     `ix_patent_num_claims` (`num_claims`),
    KEY                                                     `ix_patent_country` (`country`),
    FULLTEXT KEY `fti_patent_title` (`title`),
    FULLTEXT KEY `fti_patent_abstract` (`abstract`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `{{elastic_db}}`.`patents`;

insert into `{{elastic_db}}`.patents ( patent_id, type, number, country, date, year, abstract, title
                                                  , kind, num_claims
                                                  , num_foreign_documents_cited, num_us_applications_cited
                                                  , num_us_patents_cited
                                                  , num_total_documents_cited, num_times_cited_by_us_patents
                                                  , earliest_application_date, patent_processing_days
                                                  , uspc_current_mainclass_average_patent_processing_days
                                                  , cpc_current_group_average_patent_processing_days
                                                  , term_extension
                                                  , detail_desc_length, gi_statement, patent_zero_prefix)
select p.patent_id
     , p.type
     , number
     , country
     , date
     , year
     , abstract
     , title
     , kind
     , num_claims
     , num_foreign_documents_cited
     , num_us_applications_cited
     , num_us_patents_cited
     , num_total_documents_cited
     , num_times_cited_by_us_patents
     , earliest_application_date
     , patent_processing_days
     , uspc_current_mainclass_average_patent_processing_days
     , cpc_current_group_average_patent_processing_days
     , term_extension
     , detail_desc_length
     , gi_statement
     , pe.patent_id_eight_char
from `{{reporting_db}}`.patent p
    left join `{{reporting_db}}`.government_interest gi
on gi.patent_id = p.patent_id
    join patent.patent_to_eight_char pe on pe.id = p.patent_id;

