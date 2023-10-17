{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

drop table if exists `{{reporting_db}}`.`location_cpc_group`;
create table `{{reporting_db}}`.`location_cpc_group`
(
  `location_id` int unsigned not null,
  `group_id` varchar(20) not null,
  `num_patents` int unsigned not null,
   unique (`location_id`, `group_id`)
)
engine=InnoDB;


insert into `{{reporting_db}}`.`location_cpc_group`
  (`location_id`, `group_id`, `num_patents`)
select
  tlp.`location_id`, cpc.`group_id`, count(distinct tlp.`patent_id`)
from
  `{{reporting_db}}`.`temp_location_patent` tlp
  inner join `{{reporting_db}}`.`cpc_current_group` cpc using(`patent_id`)
group by
  tlp.`location_id`, cpc.`group_id`;


drop table if exists `{{reporting_db}}`.`location_cpc_subsection`;
create table `{{reporting_db}}`.`location_cpc_subsection`
(
  `location_id` int unsigned not null,
  `subsection_id` varchar(20) not null,
  `num_patents` int unsigned not null,
   unique (`location_id`, `subsection_id`)
)
engine=InnoDB;


insert into `{{reporting_db}}`.`location_cpc_subsection`
  (`location_id`, `subsection_id`, `num_patents`)
select
  tlp.`location_id`, cpc.`subsection_id`, count(distinct tlp.`patent_id`)
from
  `{{reporting_db}}`.`temp_location_patent` tlp
  inner join `{{reporting_db}}`.`cpc_current_subsection` cpc using(`patent_id`)
group by
  tlp.`location_id`, cpc.`subsection_id`;


alter table `{{reporting_db}}`.`location_cpc_group` add index `ix_location_cpc_group_location_id` (`location_id`);
alter table `{{reporting_db}}`.`location_cpc_group` add index `ix_location_cpc_group_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`location_cpc_group` add index `ix_location_cpc_group_subsection_id` (`group_id`);
alter table `{{reporting_db}}`.`location_cpc_subsection` add index `ix_location_cpc_subsection_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`location_cpc_subsection` add index `ix_location_cpc_subsection_subsection_id` (`subsection_id`);
alter table `{{reporting_db}}`.`location_cpc_subsection` add index `ix_location_cpc_subsection_location_id` (`location_id`);