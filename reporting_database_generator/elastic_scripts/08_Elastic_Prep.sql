{% set elastic_target_database = params.elastic_database_prefix  +  params.version_indicator.replace('-','') %}
create
database `{{elastic_target_database}}` default character set=utf8mb4 default collate=utf8mb4_unicode_ci;

