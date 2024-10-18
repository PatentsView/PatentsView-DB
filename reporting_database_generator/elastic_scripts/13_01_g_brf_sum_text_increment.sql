{% set elastic_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set update_end_date = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set update_start_date = macros.ds_format(macros.ds_add(dag_run.data_interval_start | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set update_year = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y") %}

use `{{elastic_db}}`;

create or replace sql security invoker view `{{elastic_db}}`.g_brf_sum_text_delta as
select
    uuid,
    patent_id,
    summary_text,
    version_indicator as document_date

from
    `patent_text`.`brf_sum_text_{{update_year}}` pt

where pt.version_indicator BETWEEN '{{update_start_date}}' and '{{update_end_date}}'


