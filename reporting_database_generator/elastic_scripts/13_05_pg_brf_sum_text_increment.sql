{% set elastic_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set update_end_date = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set update_start_date = macros.ds_format(macros.ds_add(dag_run.data_interval_start | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set update_year = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y") %}

use `{{elastic_db}}`;

create or replace sql security invoker view `{{elastic_db}}`.pg_brf_sum_text_delta as
select
    id as uuid,
    pgpub_id as document_number,
    summary_text,
    version_indicator as document_date

from
    `pgpubs_text`.`brf_sum_text_{{update_year}}` pt

where pt.version_indicator BETWEEN '{{update_start_date}}' and '{{update_end_date}}'


