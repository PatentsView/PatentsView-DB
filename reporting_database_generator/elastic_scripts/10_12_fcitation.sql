
{% set elastic_target_database = params.elastic_database_prefix + params.version_indicator.replace("-","") %}
{% set reporting_database = params.reporting_database %}
use `{{elastic_target_database}}`;



create or replace sql security invoker view `{{elastic_target_database}}`.foreign_citations as
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
        join `{{elastic_target_database}}`.patents p on p.patent_id = fc.patent_id;
