{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

######################################################################################################################

drop table if exists `{{reporting_db}}`.`inventor_coinventor`;
create table `{{reporting_db}}`.`inventor_coinventor`
(
  `inventor_id` int unsigned not null,
  `coinventor_id` int unsigned not null,
  `num_patents` int unsigned not null,
   unique (`inventor_id`, `coinventor_id`)
)
engine=InnoDB;


insert into `{{reporting_db}}`.`inventor_coinventor`
  (`inventor_id`, `coinventor_id`, `num_patents`)
select
  pi.inventor_id, copi.inventor_id, count(distinct copi.patent_id)
from
  `{{reporting_db}}`.`patent_inventor` pi
  inner join `{{reporting_db}}`.`patent_inventor` copi on pi.patent_id=copi.patent_id and pi.inventor_id<>copi.inventor_id
group by
  pi.inventor_id, copi.inventor_id;


###############################################################################################################################
drop table if exists `{{reporting_db}}`.`inventor_rawinventor`;
create table if not exists `{{reporting_db}}`.`inventor_rawinventor` (uuid int(10) unsigned AUTO_INCREMENT PRIMARY KEY,name_first varchar(128),name_last varchar(128),patent_id varchar(20),inventor_id int(10) unsigned);

INSERT INTO `{{reporting_db}}`.`inventor_rawinventor` (name_first,name_last,patent_id,inventor_id)
SELECT DISTINCT ri.name_first,ri.name_last,ri.patent_id,repi.inventor_id
FROM `{{reporting_db}}`.`inventor` repi
left join `patent`.`rawinventor` ri
on ri.inventor_id = repi.persistent_inventor_id;


# END inventor_rawinventor alias

#######################################################################################

alter table `{{reporting_db}}`.`inventor_coinventor` add index `ix_inventor_coinventor_coinventor_id` (`coinventor_id`);
alter table `{{reporting_db}}`.`inventor_coinventor` add index `ix_inventor_coinventor_inventor_id` (`inventor_id`);
alter table `{{reporting_db}}`.`inventor_coinventor` add index `ix_inventor_coinventor_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`inventor_rawinventor` add index `ix_inventor_rawinventor_name_first` (`name_first`);
alter table `{{reporting_db}}`.`inventor_rawinventor` add index `ix_inventor_rawinventor_name_last` (`name_last`);
alter table `{{reporting_db}}`.`inventor_rawinventor` add index `ix_inventor_rawinventor_inventor_id` (`inventor_id`);
alter table `{{reporting_db}}`.`inventor_rawinventor` add index `ix_inventor_rawinventor_patent_id` (`patent_id`);