{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

# BEGIN assignee_cpc_group

######################################################################################################################

drop table if exists `{{reporting_db}}`.`assignee_cpc_group`;
create table `{{reporting_db}}`.`assignee_cpc_group`
(
  `assignee_id` int unsigned not null,
  `group_id` varchar(20) not null,
  `num_patents` int unsigned not null,
   unique (`assignee_id`, `group_id`)
)
engine=InnoDB;


insert into `{{reporting_db}}`.`assignee_cpc_group`
  (`assignee_id`, `group_id`, `num_patents`)
select
  pa.assignee_id, c.group_id, count(distinct c.patent_id)
from
  `{{reporting_db}}`.`patent_assignee` pa
  inner join `{{reporting_db}}`.`cpc_current_group` c using(patent_id)
where
  c.group_id is not null and c.group_id != ''
group by
  pa.assignee_id, c.group_id;

# END assignee_cpc_group

# BEGIN assignee_cpc_subsection

######################################################################################################################


drop table if exists `{{reporting_db}}`.`assignee_cpc_subsection`;
create table `{{reporting_db}}`.`assignee_cpc_subsection`
(
  `assignee_id` int unsigned not null,
  `subsection_id` varchar(20) not null,
  `num_patents` int unsigned not null,
   unique (`assignee_id`, `subsection_id`)
)
engine=InnoDB;



insert into `{{reporting_db}}`.`assignee_cpc_subsection`
  (`assignee_id`, `subsection_id`, `num_patents`)
select
  pa.assignee_id, c.subsection_id, count(distinct c.patent_id)
from
  `{{reporting_db}}`.`patent_assignee` pa
  inner join `{{reporting_db}}`.`cpc_current_subsection` c using(patent_id)
where
  c.subsection_id is not null and c.subsection_id != ''
group by
  pa.assignee_id, c.subsection_id;


# END assignee_cpc_subsection

######################################################################################################################


alter table `{{reporting_db}}`.`assignee_cpc_group` add index `ix_assignee_cpc_group_group_id` (`group_id`);
alter table `{{reporting_db}}`.`assignee_cpc_group` add index `ix_assignee_cpc_group_assignee_id` (`assignee_id`);
alter table `{{reporting_db}}`.`assignee_cpc_group` add index `ix_assignee_cpc_group_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`assignee_cpc_subsection` add index `ix_assignee_cpc_subsection_assignee_id` (`assignee_id`);
alter table `{{reporting_db}}`.`assignee_cpc_subsection` add index `ix_assignee_cpc_subsection_subsection_id` (`subsection_id`);
alter table `{{reporting_db}}`.`assignee_cpc_subsection` add index `ix_assignee_cpc_subsection_num_patents` (`num_patents`);