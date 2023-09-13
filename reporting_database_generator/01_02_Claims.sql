
# Make the claim table 
#takes a very long time, idk why quite so long
create table `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`claim` like `patent`.`claim`;
insert into `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`claim` select * FROM  `patent`.`claim`;


