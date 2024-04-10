{% set today_str = dag_run.data_interval_end | ds %}
{% set today_date = today_str | to_datetime %}
{% set prior_quarter_end = (today_date - relativedelta(months=(today_date.month - 1) % 3, days=today_date.day)).replace(day=1) - timedelta(days=1) %}
{% set reporting_db = "PatentsView_" + prior_quarter_end.strftime("%Y%m%d") %}
#{% set elastic_production_patent_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -2), "%Y-%m-%d", "%Y%m%d") %}
#{% set elastic_production_pgpubs_db = "elastic_production_pgpubs_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -2), "%Y-%m-%d", "%Y%m%d") %}

drop database `{{elastic_production_patent_db}}` default character set=utf8mb4 default collate=utf8mb4_unicode_ci;
#drop database `{{elastic_production_pgpubs_db}}` default character set=utf8mb4 default collate=utf8mb4_unicode_ci;
#drop database `{{reporting_db}}` default character set=utf8mb4 default collate=utf8mb4_unicode_ci;