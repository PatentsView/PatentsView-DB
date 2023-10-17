{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

# BEGIN assignee_inventor ######################################################################################################################

drop table if exists `{{reporting_db}}`.`assignee_inventor`;
create table `{{reporting_db}}`.`assignee_inventor`
(
  `assignee_id` int unsigned not null,
  `inventor_id` int unsigned not null,
  `num_patents` int unsigned not null,
   unique (`assignee_id`, `inventor_id`)
)
engine=InnoDB;

insert into `{{reporting_db}}`.`assignee_inventor`
  (`assignee_id`, `inventor_id`, `num_patents`)
select
  pa.assignee_id, pi.inventor_id, count(distinct pa.patent_id)
from
  `{{reporting_db}}`.`patent_assignee` pa
  inner join `{{reporting_db}}`.`patent_inventor` pi using(patent_id)
group by
  pa.assignee_id, pi.inventor_id;

# END assignee_inventor ######################################################################################################################


alter table `{{reporting_db}}`.`assignee_inventor` add index `ix_assignee_inventor_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`assignee_inventor` add index `ix_assignee_inventor_assignee_id` (`assignee_id`);
alter table `{{reporting_db}}`.`assignee_inventor` add index `ix_assignee_inventor_inventor_id` (`inventor_id`);