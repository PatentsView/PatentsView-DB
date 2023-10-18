{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

# Make the claim table
#takes a very long time, idk why quite so long
create table `{{reporting_db}}`.`claim` like `patent`.`claim`;
insert into `{{reporting_db}}`.`claim` select * FROM  `patent`.`claim`;


