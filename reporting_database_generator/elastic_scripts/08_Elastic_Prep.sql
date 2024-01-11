{% set elastic_patent_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set elastic_pgpub_db = "elastic_production_pgpub_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
create database `{{elastic_patent_db}}` default character set=utf8mb4 default collate=utf8mb4_unicode_ci;
create database `{{elastic_pgpub_db}}` default character set=utf8mb4 default collate=utf8mb4_unicode_ci;

