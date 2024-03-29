{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set version_indicator = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

# BEGIN nber 

################################################################################################################################################

drop table if exists `{{reporting_db}}`.`temp_nber_subcategory_aggregate_counts`;
create table `{{reporting_db}}`.`temp_nber_subcategory_aggregate_counts`
(
  `subcategory_id` varchar(20) not null,
  `num_assignees` int unsigned not null,
  `num_inventors` int unsigned not null,
  `num_patents` int unsigned not null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `actual_years_active` smallint unsigned not null,
  primary key (`subcategory_id`)
)
engine=InnoDB;


insert into `{{reporting_db}}`.`temp_nber_subcategory_aggregate_counts`
(
  `subcategory_id`, `num_assignees`, `num_inventors`, `num_patents`,
  `first_seen_date`, `last_seen_date`, `actual_years_active`
)
select
  n.`subcategory_id`,
  count(distinct pa.`assignee_id`) num_assignees,
  count(distinct pii.`inventor_id`) num_inventors,
  count(distinct n.`patent_id`) num_patents,
  min(p.`date`), max(p.`date`),
  ifnull(round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365), 0)
from
  `patent`.`nber` n
  left outer join `patent`.`patent_assignee` pa on pa.`patent_id` = n.`patent_id`
  left outer join `patent`.`patent_inventor` pii on pii.`patent_id` = n.`patent_id`
  left outer join `{{reporting_db}}`.`patent` p on p.`patent_id` = n.`patent_id`  where n.version_indicator<= '{{version_indicator}}'
group by
  n.`subcategory_id`;


drop table if exists `{{reporting_db}}`.`nber`;
create table `{{reporting_db}}`.`nber`
(
  `patent_id` varchar(20) not null,
  `category_id` varchar(20) null,
  `category_title` varchar(512) null,
  `subcategory_id` varchar(20) null,
  `subcategory_title` varchar(512) null,
  `num_assignees` int unsigned null,
  `num_inventors` int unsigned null,
  `num_patents` int unsigned null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `years_active` smallint unsigned null,
  primary key (`patent_id`)
)
engine=InnoDB;


insert into `{{reporting_db}}`.`nber`
(
  `patent_id`, `category_id`, `category_title`, `subcategory_id`,
  `subcategory_title`,
  `num_assignees`, `num_inventors`, `num_patents`,
  `first_seen_date`, `last_seen_date`, `years_active`
)
select
  p.`patent_id`,
  nullif(trim(n.`category_id`), ''),
  nullif(trim(c.`title`), ''),
  nullif(trim(n.`subcategory_id`), ''),
  nullif(trim(s.`title`), ''),
  tnsac.`num_assignees`, tnsac.`num_inventors`, tnsac.`num_patents`,
  tnsac.`first_seen_date`, tnsac.`last_seen_date`,
  case when tnsac.`actual_years_active` < 1 then 1 else tnsac.`actual_years_active` end
from
  `{{reporting_db}}`.`patent` p
  inner join `patent`.`nber` n on p.`patent_id` = n.`patent_id`
  left outer join `patent`.`nber_category` c on c.`id` = n.`category_id`
  left outer join `patent`.`nber_subcategory` s on s.`id` = n.`subcategory_id`
  left outer join `{{reporting_db}}`.`temp_nber_subcategory_aggregate_counts` tnsac on tnsac.`subcategory_id` = n.`subcategory_id`;


# END nber 

################################################################################################################################################

####


# BEGIN nber_subcategory_patent_year 

##########################################################################################################################


drop table if exists `{{reporting_db}}`.`nber_subcategory_patent_year`;
create table `{{reporting_db}}`.`nber_subcategory_patent_year`
(
  `subcategory_id` varchar(20) not null,
  `patent_year` smallint unsigned not null,
  `num_patents` int unsigned not null,
  primary key (`subcategory_id`, `patent_year`)
)
engine=InnoDB;


insert into `{{reporting_db}}`.`nber_subcategory_patent_year`
  (`subcategory_id`, `patent_year`, `num_patents`)
select
  n.`subcategory_id`, year(p.`date`), count(distinct n.`patent_id`)
from
  `patent`.`nber` n
  inner join `{{reporting_db}}`.`patent` p on p.`patent_id` = n.`patent_id` and p.`date` is not null
where
  n.`subcategory_id` is not null and n.`subcategory_id` != ''  and n.version_indicator<= '{{version_indicator}}'
group by
  n.`subcategory_id`, year(p.`date`);

alter table `{{reporting_db}}`.`nber_subcategory_patent_year` add index `ix_nber_subcategory_patent_year_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`nber` add index `ix_nber_subcategory_id` (`subcategory_id`);
alter table `{{reporting_db}}`.`nber` add index `ix_nber_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`nber` add index `ix_nber_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`nber` add index `ix_nber_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`nber` add index `ix_nber_category_id` (`category_id`);
alter table `{{reporting_db}}`.`nber` add index `ix_nber_subcategory_title` (`subcategory_title`);

# END nber_subcategory_patent_year 

############################################################################################################################