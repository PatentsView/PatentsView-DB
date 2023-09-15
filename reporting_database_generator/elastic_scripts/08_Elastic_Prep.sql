create database `elastic_production_{{ dag_run.logical_date | ds_nodash }}` default character set=utf8mb4 default collate=utf8mb4_unicode_ci;

