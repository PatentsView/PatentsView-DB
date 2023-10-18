{% set elastic_db = "elastic_production_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
create database `{{elastic_db}}` default character set=utf8mb4 default collate=utf8mb4_unicode_ci;

