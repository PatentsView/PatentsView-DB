{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

######################################################################################################################


drop table if exists `{{reporting_db}}`.`location_nber_subcategory`;
create table `{{reporting_db}}`.`location_nber_subcategory`
(
  `location_id` int unsigned not null,
  `subcategory_id` varchar(20) not null,
  `num_patents` int unsigned not null,
   unique (`location_id`, `subcategory_id`)
)
engine=InnoDB;


insert into `{{reporting_db}}`.`location_nber_subcategory`
  (`location_id`, `subcategory_id`, `num_patents`)
select
  tlp.`location_id`, nber.`subcategory_id`, count(distinct tlp.`patent_id`)
from
  `{{reporting_db}}`.`temp_location_patent` tlp
  inner join `{{reporting_db}}`.`nber` nber using(`patent_id`)
group by
  tlp.`location_id`, nber.`subcategory_id`;

# BEGIN location_year ######################################################################################################################

drop table if exists `{{reporting_db}}`.`location_year`;
create table `{{reporting_db}}`.`location_year`
(
  `location_id` int unsigned not null,
  `year` smallint not null,
  `num_patents` int unsigned not null,
   unique (`location_id`, `year`)
)
engine=InnoDB;


insert into `{{reporting_db}}`.`location_year`
  (`location_id`, `year`, `num_patents`)
select
  tlp.`location_id`, p.`year`, count(distinct tlp.`patent_id`)
from
  `{{reporting_db}}`.`temp_location_patent` tlp
  inner join `{{reporting_db}}`.`patent` p using(`patent_id`)
group by
  tlp.`location_id`, p.`year`;

# BEGIN location_uspc_mainclass

######################################################################################################################


drop table if exists `{{reporting_db}}`.`location_uspc_mainclass`;
create table `{{reporting_db}}`.`location_uspc_mainclass`
(
  `location_id` int unsigned not null,
  `mainclass_id` varchar(20) not null,
  `num_patents` int unsigned not null,
   unique (`location_id`, `mainclass_id`)
)
engine=InnoDB;


insert into `{{reporting_db}}`.`location_uspc_mainclass`
  (`location_id`, `mainclass_id`, `num_patents`)
select
  tlp.`location_id`, uspc.`mainclass_id`, count(distinct tlp.`patent_id`)
from
  `{{reporting_db}}`.`temp_location_patent` tlp
  inner join `{{reporting_db}}`.`uspc_current_mainclass` uspc using(`patent_id`)
group by
  tlp.`location_id`, uspc.`mainclass_id`;


# END location_uspc_mainclass


alter table `{{reporting_db}}`.`location_nber_subcategory` add index `ix_location_nber_subcategory_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`location_nber_subcategory` add index `ix_location_nber_subcategory_mainclass_id` (`subcategory_id`);
alter table `{{reporting_db}}`.`location_nber_subcategory` add index `ix_location_nber_subcategory_location_id` (`location_id`);
alter table `{{reporting_db}}`.`location_uspc_mainclass` add index `ix_location_uspc_mainclass_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`location_uspc_mainclass` add index `ix_location_uspc_mainclass_mainclass_id` (`mainclass_id`);
alter table `{{reporting_db}}`.`location_uspc_mainclass` add index `ix_location_uspc_mainclass_location_id` (`location_id`);
alter table `{{reporting_db}}`.`location_year` add index `ix_location_year_year` (`year`);
alter table `{{reporting_db}}`.`location_year` add index `ix_location_year_location_id` (`location_id`);
alter table `{{reporting_db}}`.`location_year` add index `ix_location_year_num_patents` (`num_patents`);