{% set elastic_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

create or replace sql security invoker view `{{elastic_db}}`.rel_app_text as
select
    r.uuid
  , r.patent_id
  , r.text
  , r.sequence
  , p.patent_zero_prefix
from
    patent.rel_app_text r
        join `{{elastic_db}}`.patents p on r.patent_id = p.patent_id