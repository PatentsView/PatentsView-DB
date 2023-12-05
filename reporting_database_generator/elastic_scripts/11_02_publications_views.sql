{% set elastic_db = "elastic_production_pgpub_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "pregrant_publications" %}
{% set version_indicator = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

create
or replace sql security invoker view `{{elastic_db}}`.publication_cpc_current as
select c.document_number
     , c.sequence
     , c.section_id    as cpc_section
     , c.subsection_id as cpc_class
     , c.group_id      as cpc_subclass
     , c.subgroup_id   as cpc_group
     , c.category         cpc_type
from `{{reporting_db}}`.cpc_current c;

create or replace
sql security invoker view  `{{elastic_db}}`.granted_pregrant_crosswalk as

select gpc.patent_id as patent_id
     , gpc.document_number
     , gpc.application_number
     , gpc.current_pgpub_id_flag
     , gpc.current_patent_id_flag
from `{{reporting_db}}`.granted_patent_crosswalk_{{version_indicator}} gpc;


create or replace
sql security invoker view `{{elastic_db}}`.foreign_priority as

select f.document_number
     -- , f.sequence
     , f.foreign_doc_number
     , f.date
     , f.country
     , f.kind
from `{{reporting_db}}`.foreign_priority f;

create
or replace sql security invoker view `{{elastic_db}}`.ipcr as
select
    ipcr_id
  , section
  , ipc_class
  , subclass
from
    elastic_staging.ipcr
order by
    section
  , ipc_class
  , subclass;

create
or replace sql security invoker view `{{elastic_db}}`.publication_ipcr as
select i.document_number
     , i2.ipcr_id as ipcr_id
     , i.sequence
     , i.section
     , i.class
     , i.subclass
     , i.main_group
     , i.subgroup
     , i.symbol_position
     , i.class_value
     , i.class_data_source
     , i.action_date
from `{{reporting_db}}`.ipcr i
         join `{{elastic_db}}`.ipcr i2
              on i2.ipc_class = i.class and i2.section = i.section and i2.subclass = i.subclass;



create or replace
sql security invoker view `{{elastic_db}}`.publication_pct_data as
select p.document_number
     , p.doc_type
     , p.kind
     , p.pct_doc_number
     , p.date
     , p.`us_371c124_date`
     , p.`us_371c12_date`
from `{{reporting_db}}`.pct_data p;


create or replace
sql security invoker view `{{elastic_db}}`.publication_uspc_at_issue as
select u.document_number
     , sequence
     , mainclass_id
     , subclass_id
from `{{reporting_db}}`.uspc u;


create or replace
sql security invoker view `{{elastic_db}}`.publication_wipo as
select w.document_number
     , w.field_id
     , w.sequence
from `{{reporting_db}}`.wipo w;


create or replace
sql security invoker view `{{elastic_db}}`.publication_us_related_documents as
select
    u.document_number
  , doc_type
  , relkind
  , related_doc_number
  , u.country
  , u.date
--   , status
--   , sequence
--   , u.kind
from
    `{{reporting_db}}`.usreldoc u;
