{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -2), "%Y-%m-%d", "%Y%m%d") %}
{% set elastic_production_patent_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -2), "%Y-%m-%d", "%Y%m%d") %}
{% set elastic_production_pgpubs_db = "elastic_production_pgpubs_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -2), "%Y-%m-%d", "%Y%m%d") %}
drop database `{{elastic_production_patent_db}}` default character set=utf8mb4 default collate=utf8mb4_unicode_ci;
drop database `{{elastic_production_pgpubs_db}}` default character set=utf8mb4 default collate=utf8mb4_unicode_ci;
drop database `{{reporting_db}}` default character set=utf8mb4 default collate=utf8mb4_unicode_ci;
