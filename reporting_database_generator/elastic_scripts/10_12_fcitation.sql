use `elastic_production_{{ dag_run.logical_date | ds_nodash }}`;

create or replace sql security invoker view `elastic_production_{{ dag_run.logical_date | ds_nodash }}`.foreign_citations as
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
        join `elastic_production_{{ dag_run.logical_date | ds_nodash }}`.patents p on p.patent_id = fc.patent_id;
