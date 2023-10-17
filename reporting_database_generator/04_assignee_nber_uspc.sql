{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}


# BEGIN assignee_nber_subcategory

######################################################################################################################


drop table if exists `{{reporting_db}}`.`assignee_nber_subcategory`;
create table `{{reporting_db}}`.`assignee_nber_subcategory`
(
  `assignee_id` int unsigned not null,
  `subcategory_id` varchar(20) not null,
  `num_patents` int unsigned not null,
   unique (`assignee_id`, `subcategory_id`)
)
engine=InnoDB;


insert into `{{reporting_db}}`.`assignee_nber_subcategory`
  (`assignee_id`, `subcategory_id`, `num_patents`)
select
  pa.assignee_id, n.subcategory_id, count(distinct n.patent_id)
from
  `{{reporting_db}}`.`patent_assignee` pa
  inner join `{{reporting_db}}`.`nber` n using(patent_id)
where
  n.subcategory_id is not null and n.subcategory_id != ''
group by
  pa.assignee_id, n.subcategory_id;


# END assignee_nber_subcategory

######################################################################################################################


# BEGIN assignee_uspc_mainclass

######################################################################################################################


drop table if exists `{{reporting_db}}`.`assignee_uspc_mainclass`;
create table `{{reporting_db}}`.`assignee_uspc_mainclass`
(
  `assignee_id` int unsigned not null,
  `mainclass_id` varchar(20) not null,
  `num_patents` int unsigned not null,
   unique (`assignee_id`, `mainclass_id`)
)
engine=InnoDB;


insert into `{{reporting_db}}`.`assignee_uspc_mainclass`
  (`assignee_id`, `mainclass_id`, `num_patents`)
select
  pa.assignee_id, u.mainclass_id, count(distinct pa.patent_id)
from
  `{{reporting_db}}`.`patent_assignee` pa
  inner join `{{reporting_db}}`.`uspc_current_mainclass` u on pa.patent_id=u.patent_id
group by
  pa.assignee_id, u.mainclass_id;

# END assignee_uspc_mainclass

######################################################################################################################

alter table `{{reporting_db}}`.`assignee_nber_subcategory` add index `ix_assignee_nber_subcategory_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`assignee_nber_subcategory` add index `ix_assignee_nber_subcategory_assignee_id` (`assignee_id`);
alter table `{{reporting_db}}`.`assignee_nber_subcategory` add index `ix_assignee_nber_subcategory_subcategory_id` (`subcategory_id`);
alter table `{{reporting_db}}`.`assignee_uspc_mainclass` add index `ix_assignee_uspc_mainclass_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`assignee_uspc_mainclass` add index `ix_assignee_uspc_mainclass_assignee_id` (`assignee_id`);
alter table `{{reporting_db}}`.`assignee_uspc_mainclass` add index `ix_assignee_uspc_mainclass_mainclass_id` (`mainclass_id`);