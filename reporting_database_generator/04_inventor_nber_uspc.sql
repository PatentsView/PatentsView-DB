{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

drop table if exists `{{reporting_db}}`.`inventor_nber_subcategory`;
create table `{{reporting_db}}`.`inventor_nber_subcategory`
(
  `inventor_id` int unsigned not null,
  `subcategory_id` varchar(20) not null,
  `num_patents` int unsigned not null,
   unique (`inventor_id`, `subcategory_id`)
)
engine=InnoDB;


insert into `{{reporting_db}}`.`inventor_nber_subcategory`
  (`inventor_id`, `subcategory_id`, `num_patents`)
select
  pi.inventor_id, n.subcategory_id, count(distinct n.patent_id)
from
  `{{reporting_db}}`.`nber` n
  inner join `{{reporting_db}}`.`patent_inventor` pi using(patent_id)
where
  n.subcategory_id is not null and n.subcategory_id != ''
group by
  pi.inventor_id, n.subcategory_id;

######################################################################################################################

drop table if exists `{{reporting_db}}`.`inventor_uspc_mainclass`;
create table `{{reporting_db}}`.`inventor_uspc_mainclass`
(
  `inventor_id` int unsigned not null,
  `mainclass_id` varchar(20) not null,
  `num_patents` int unsigned not null,
   unique (`inventor_id`, `mainclass_id`)
)
engine=InnoDB;


insert into `{{reporting_db}}`.`inventor_uspc_mainclass`
  (`inventor_id`, `mainclass_id`, `num_patents`)
select
  pi.inventor_id, u.mainclass_id, count(distinct pi.patent_id)
from
  `{{reporting_db}}`.`patent_inventor` pi
  inner join `{{reporting_db}}`.`uspc_current_mainclass` u on pi.patent_id=u.patent_id
group by
  pi.inventor_id, u.mainclass_id;

######################################################################################################################

alter table `{{reporting_db}}`.`inventor_nber_subcategory` add index `ix_inventor_nber_subcategory_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`inventor_nber_subcategory` add index `ix_inventor_nber_subcategory_inventor_id` (`inventor_id`);
alter table `{{reporting_db}}`.`inventor_nber_subcategory` add index `ix_inventor_nber_subcategory_subcategory_id` (`subcategory_id`);
alter table `{{reporting_db}}`.`inventor_uspc_mainclass` add index `ix_inventor_uspc_mainclass_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`inventor_uspc_mainclass` add index `ix_inventor_uspc_mainclass_inventor_id` (`inventor_id`);
alter table `{{reporting_db}}`.`inventor_uspc_mainclass` add index `ix_inventor_uspc_mainclass_mainclass_id` (`mainclass_id`);