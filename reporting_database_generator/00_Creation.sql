create database `{{params.reporting_database}}` default character set=utf8mb4 default collate=utf8mb4_unicode_ci;
drop database `{{params.last_reporting_database}}`;