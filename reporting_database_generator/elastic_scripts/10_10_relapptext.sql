{% set elastic_target_database = params.elastic_database_prefix + params.version_indicator.replace("-","") %}
{% set reporting_database = params.reporting_database %}
use `{{elastic_target_database}}`;

create or replace sql security invoker view `{{elastic_target_database}}`.rel_app_text as
select
    r.uuid
  , r.patent_id
  , r.text
  , r.sequence
  , p.patent_zero_prefix
from
    patent.rel_app_text r
        join `{{elastic_target_database}}`.patents p on r.patent_id = p.patent_id