{% set elastic_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

create or replace sql security invoker view `{{elastic_db}}`.foreign_citations as
select
    fc.uuid
  , fc.patent_id
  , fc.date
  , fc.number
  , fc.country
  , fc.category
  , fc.sequence
  , p.patent_zero_prefix

from
    patent.foreigncitation fc
        join `{{elastic_db}}`.patents p on p.patent_id = fc.patent_id;
