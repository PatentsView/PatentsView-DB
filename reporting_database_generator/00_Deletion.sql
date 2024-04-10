{% set prior_quarter_date = macros.ds_format(macros.ds_add(dag_run.data_interval_start | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set reporting_db = "PatentsView_" + prior_quarter_date %}
{% set elastic_production_patent_db = "elastic_production_patent_" + prior_quarter_date %}
{% set elastic_production_pgpub_db = "elastic_production_pgpub_" + prior_quarter_date %}


# drop database `{{elastic_production_patent_db}}` ;
drop database `{{elastic_production_pgpub_db}}` ;
drop database `{{reporting_db}}` ;
