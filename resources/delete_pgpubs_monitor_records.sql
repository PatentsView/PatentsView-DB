{% set dbdate= (execution_date+macros.timedelta(days=7)).strftime('%Y-%m-%d')|string %}
    {% set qavi = (execution_date+macros.timedelta(days=7)).strftime('%Y%m%d')|string %}

SET FOREIGN_KEY_CHECKS=0;

delete from patent_QA.DataMonitor_categorycount where update_version = '{{qavi}}' and database_type = 'pgpubs';
delete from patent_QA.DataMonitor_count where update_version = '{{qavi}}' and database_type = 'pgpubs';
delete from patent_QA.DataMonitor_floatingentitycount where update_version = '{{qavi}}' and database_type = 'pgpubs';
delete from patent_QA.DataMonitor_govtinterestsampler where update_version = '{{qavi}}' and database_type = 'pgpubs';
delete from patent_QA.DataMonitor_locationcount where update_version = '{{qavi}}' and database_type = 'pgpubs';
delete from patent_QA.DataMonitor_maxtextlength where update_version = '{{qavi}}' and database_type = 'pgpubs';
delete from patent_QA.DataMonitor_nullcount where update_version = '{{qavi}}' and database_type = 'pgpubs';
delete from patent_QA.DataMonitor_patentwithdrawncount where update_version = '{{qavi}}' and database_type = 'pgpubs';
delete from patent_QA.DataMonitor_patentyearlycount where update_version = '{{qavi}}' and database_type = 'pgpubs';
delete from patent_QA.DataMonitor_prefixedentitycount where update_version = '{{qavi}}' and database_type = 'pgpubs';
delete from patent_QA.DataMonitor_topnentities where update_version = '{{qavi}}' and database_type = 'pgpubs';

SET FOREIGN_KEY_CHECKS=1;
