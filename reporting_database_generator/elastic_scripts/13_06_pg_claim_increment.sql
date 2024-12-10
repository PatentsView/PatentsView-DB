{% set elastic_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set update_end_date = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set update_start_date = macros.ds_format(macros.ds_add(dag_run.data_interval_start | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set update_year = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y") %}

use `{{elastic_db}}`;

create or replace sql security invoker view `{{elastic_db}}`.pg_claim_delta as
select
    id as uuid,
    pgpub_id as document_number,
    claim_sequence,
    claim_text,
    `dependent` as claim_dependent,
    claim_number,
    version_indicator as document_date

from
    `pgpubs_text`.`claims_{{update_year}}` pt

where pt.version_indicator BETWEEN '{{update_start_date}}' and '{{update_end_date}}'
