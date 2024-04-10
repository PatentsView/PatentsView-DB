{% set prior_quarter_date = macros.ds_format(macros.ds_add(dag_run.data_interval_start | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + prior_quarter_date %}
{% set elastic_production_patent_db = "elastic_production_patent_" + prior_quarter_date %}
{% set elastic_production_pgpubs_db = "elastic_production_pgpubs_" + prior_quarter_date %}


drop database `{{elastic_production_patent_db}}` ;
drop database `{{elastic_production_pgpubs_db}}` ;
drop database `{{reporting_db}}` ;
