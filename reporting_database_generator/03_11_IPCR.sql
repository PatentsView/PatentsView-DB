{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set version_indicator = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

# BEGIN ipcr 

################################################################################################################################################

drop table if exists `{{reporting_db}}`.`temp_ipcr_aggregations`;
create table `{{reporting_db}}`.`temp_ipcr_aggregations`
(
  `section` varchar(20) null,
  `ipc_class` varchar(20) null,
  `subclass` varchar(20) null,
  `num_assignees` int unsigned not null,
  `num_inventors` int unsigned not null,
  unique key (`section`, `ipc_class`, `subclass`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`temp_ipcr_aggregations`
  (`section`, `ipc_class`, `subclass`, `num_assignees`, `num_inventors`)
select
  i.`section`, i.`ipc_class`, i.`subclass`,
  count(distinct pa.`assignee_id`),
  count(distinct pii.`inventor_id`)
from
  `patent`.`ipcr` i
  left outer join `patent`.`patent_assignee` pa on pa.`patent_id` = i.`patent_id`
  left outer join `patent`.`patent_inventor` pii on pii.`patent_id` = i.`patent_id`  where i.version_indicator<= '{{version_indicator}}'
group by
  i.`section`, i.`ipc_class`, i.`subclass`;


drop table if exists `{{reporting_db}}`.`temp_ipcr_years_active`;
create table `{{reporting_db}}`.`temp_ipcr_years_active`
(
  `section` varchar(20) null,
  `ipc_class` varchar(20) null,
  `subclass` varchar(20) null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `actual_years_active` smallint unsigned not null,
  unique key (`section`, `ipc_class`, `subclass`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`temp_ipcr_years_active`
(
  `section`, `ipc_class`, `subclass`, `first_seen_date`,
  `last_seen_date`, `actual_years_active`
)
select
  i.`section`, i.`ipc_class`, i.`subclass`,
  min(p.`date`), max(p.`date`),
  ifnull(round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365), 0)
from
  `patent`.`ipcr` i
  inner join `{{reporting_db}}`.`patent` p on p.`patent_id`= i.`patent_id`
where
  p.`date` is not null  and i.version_indicator<= '{{version_indicator}}'
group by
  i.`section`, i.`ipc_class`, i.`subclass`;


drop table if exists `{{reporting_db}}`.`ipcr`;
create table `{{reporting_db}}`.`ipcr`
(
  `patent_id` varchar(20) not null,
  `sequence` int not null,
  `section` varchar(20) null,
  `ipc_class` varchar(20) null,
  `subclass` varchar(20) null,
  `main_group` varchar(20) null,
  `subgroup` varchar(20) null,
  `symbol_position` varchar(20) null,
  `classification_value` varchar(20) null,
  `classification_data_source` varchar(20) null,
  `action_date` date null,
  `ipc_version_indicator` date null,
  `num_assignees` int unsigned null,
  `num_inventors` int unsigned null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `years_active` smallint unsigned null,
  primary key (`patent_id`, `sequence`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

insert into `{{reporting_db}}`.`ipcr`
(
  `patent_id`, `sequence`, `section`, `ipc_class`, `subclass`, `main_group`, `subgroup`,
  `symbol_position`, `classification_value`, `classification_data_source`,
  `action_date`, `ipc_version_indicator`, `num_assignees`, `num_inventors`,
  `first_seen_date`, `last_seen_date`, `years_active`
)
select
  p.`patent_id`, i.`sequence`, nullif(trim(i.`section`), ''), nullif(trim(i.`ipc_class`), ''), nullif(trim(i.`subclass`), ''),
  nullif(trim(i.`main_group`), ''), nullif(trim(i.`subgroup`), ''), nullif(trim(i.`symbol_position`), ''),
  nullif(trim(i.`classification_value`), ''), nullif(trim(i.`classification_data_source`), ''),
  case when `action_date` > date('1899-12-31') and `action_date` < date_add(current_date, interval 10 year) then `action_date` else null end,
  case when `ipc_version_indicator` > date('1899-12-31') and `ipc_version_indicator` < date_add(current_date, interval 10 year) then 

`ipc_version_indicator` else null end,
  tia.`num_assignees`, tia.`num_inventors`, tiya.`first_seen_date`, tiya.`last_seen_date`,
  ifnull(case when tiya.`actual_years_active` < 1 then 1 else tiya.`actual_years_active` end, 0)
from
  `{{reporting_db}}`.`patent` p
  inner join `patent`.`ipcr` i on i.`patent_id` = p.`patent_id`
  left outer join `{{reporting_db}}`.`temp_ipcr_aggregations` tia on tia.`section` = i.`section` and tia.`ipc_class` = i.`ipc_class` and

tia.`subclass` = i.`subclass`
  left outer join `{{reporting_db}}`.`temp_ipcr_years_active` tiya on tiya.`section` = i.`section` and tiya.`ipc_class` = i.`ipc_class` and

tiya.`subclass` = i.`subclass` where i.version_indicator<= '{{version_indicator}}';

alter table `{{reporting_db}}`.`ipcr` add index `ix_ipcr_ipc_class` (`ipc_class`);
alter table `{{reporting_db}}`.`ipcr` add index `ix_ipcr_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`ipcr` add index `ix_ipcr_num_inventors` (`num_inventors`);

# END ipcr 

################################################################################################################################################