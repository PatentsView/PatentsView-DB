{% set today_str = dag_run.data_interval_end %}
{% set today_date = today_str[:10] %}
{% set today_year = today_date[:4] %}
{% set today_month = today_date[5:7] %}
{% set today_day = today_date[8:10] %}
{% set today_date_obj = pendulum.datetime(int(today_year), int(today_month), int(today_day)) %}
{% set prior_quarter_end = (today_date_obj - pendulum.duration(months=(today_date_obj.month - 1) % 3, days=today_date_obj.day)).subtract(days=1) %}
{% set reporting_db = "PatentsView_" + prior_quarter_end.format("YYYYMMDD") %}
#{% set elastic_production_patent_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -2), "%Y-%m-%d", "%Y%m%d") %}
#{% set elastic_production_pgpubs_db = "elastic_production_pgpubs_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -2), "%Y-%m-%d", "%Y%m%d") %}

drop database `{{elastic_production_patent_db}}` default character set=utf8mb4 default collate=utf8mb4_unicode_ci;
#drop database `{{elastic_production_pgpubs_db}}` default character set=utf8mb4 default collate=utf8mb4_unicode_ci;
#drop database `{{reporting_db}}` default character set=utf8mb4 default collate=utf8mb4_unicode_ci;