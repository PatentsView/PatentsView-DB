{% set elastic_db = "elastic_production_pgpub_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

create or replace sql security invoker view `{{elastic_db}}`.rel_app_text as
select
    r.id
  , r.document_number
  , r.text
from
    pregrant_publications.rel_app_text r
        join `{{elastic_db}}`.publication p on r.document_number = p.document_number