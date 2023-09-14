use `elastic_production_{{ dag_run.logical_date | ds_nodash }}`;

create or replace sql security invoker view `elastic_production_{{ dag_run.logical_date | ds_nodash }}`.rel_app_text as
select
    r.uuid
  , r.patent_id
  , r.text
  , r.sequence
  , p.patent_zero_prefix
from
    patent.rel_app_text r
        join `elastic_production_{{ dag_run.logical_date | ds_nodash }}`.patents p on r.patent_id = p.patent_id