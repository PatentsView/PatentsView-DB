{% set elastic_db = "elastic_production_patent_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set version_indicator = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

use `{{elastic_db}}`;

create
or replace sql security invoker view `{{elastic_db}}`.patent_cpc_current as
select c.patent_id
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
from pregrant_publications.granted_patent_crosswalk_{{version_indicator}} gpc;


create or replace
sql security invoker view `{{elastic_db}}`.patent_foreign_priority as

select f.patent_id
     , f.sequence
     , f.foreign_doc_number
     , f.date
     , f.country
     , f.kind
from `{{reporting_db}}`.foreignpriority f;

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
or replace sql security invoker view `{{elastic_db}}`.patent_ipcr as
select i.patent_id
     , i2.ipcr_id as ipcr_id
     , i.sequence
     , i.section
     , i.ipc_class
     , i.subclass
     , i.main_group
     , i.subgroup
     , i.symbol_position
     , i.classification_value
     , i.classification_data_source
     , i.action_date
from `{{reporting_db}}`.ipcr i
         join `{{elastic_db}}`.ipcr i2
              on i2.ipc_class = i.ipc_class and i2.section = i.section and i2.subclass = i.subclass;



create or replace
sql security invoker view `{{elastic_db}}`.patent_pct_data as
select p.patent_id
     , p.doc_type
     , p.kind
     , p.doc_number
     , p.date
     , p.`102_date`
     , p.`371_date`
from `{{reporting_db}}`.pctdata p;


create or replace
sql security invoker view `{{elastic_db}}`.patent_uspc_at_issue as
select u.patent_id
     , sequence
     , mainclass_id
     , subclass_id
from patent.uspc u;


create or replace
sql security invoker view `{{elastic_db}}`.patent_wipo as
select w.patent_id
     , w.field_id
     , w.sequence
from `{{reporting_db}}`.wipo w;


create or replace
sql security invoker view `{{elastic_db}}`.patent_botanic as
select b.patent_id
     , b.latin_name
     , b.variety
from patent.botanic b;



create or replace
sql security invoker view `{{elastic_db}}`.patent_figures as
select f.patent_id
     , f.num_figures
     , f.num_sheets
from patent.figures f
;
create or replace
sql security invoker view `{{elastic_db}}`.patent_us_term_of_grant as
select u.patent_id
     , u.disclaimer_date
     , u.term_disclaimer
     , u.term_grant
     , u.term_extension
from patent.us_term_of_grant u;
create or replace
sql security invoker view `{{elastic_db}}`.patent_us_related_documents as
select
    u.patent_id
  , doctype
  , relkind
  , reldocno
  , u.country
  , u.date
  , status
  , sequence
  , u.kind
from
    patent.usreldoc u;
