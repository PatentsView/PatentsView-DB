
# BEGIN cpc_current 

###########################################################################################################################################


drop table if exists `{{params.reporting_database}}`.`temp_cpc_current_subsection_aggregate_counts`;
create table `{{params.reporting_database}}`.`temp_cpc_current_subsection_aggregate_counts`
(
  `subsection_id` varchar(20) not null,
  `num_assignees` int unsigned not null,
  `num_inventors` int unsigned not null,
  `num_patents` int unsigned not null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `actual_years_active` smallint unsigned not null,
  primary key (`subsection_id`)
)
engine=InnoDB;


# 29:37
insert into `{{params.reporting_database}}`.`temp_cpc_current_subsection_aggregate_counts`
(
  `subsection_id`, `num_assignees`, `num_inventors`, `num_patents`,
  `first_seen_date`, `last_seen_date`, `actual_years_active`
)
select
  c.`subsection_id`,
  count(distinct pa.`assignee_id`) num_assignees,
  count(distinct pii.`inventor_id`) num_inventors,
  count(distinct c.`patent_id`) num_patents,
  min(p.`date`), max(p.`date`),
  ifnull(round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365), 0)
from
  `{{params.raw_database}}`.`cpc_current` c
  left outer join `{{params.raw_database}}`.`patent_assignee` pa on pa.`patent_id` = c.`patent_id`
  left outer join `{{params.raw_database}}`.`patent_inventor` pii on pii.`patent_id` = c.`patent_id`
  left outer join `{{params.reporting_database}}`.`patent` p on p.`patent_id` = c.`patent_id`
group by
  c.`subsection_id`;

drop table if exists `{{params.reporting_database}}`.`temp_cpc_subsection_title`;
create table `{{params.reporting_database}}`.`temp_cpc_subsection_title`
(
  `id` varchar(20) not null,
  `title` varchar(512) null,
  primary key (`id`)
)
engine=InnoDB;


# 0.125 sec
insert into `{{params.reporting_database}}`.`temp_cpc_subsection_title`
  (`id`, `title`)
select
  `id`,
  case when binary replace(`title`, 'e.g.', 'E.G.') = binary ucase(`title`)
    then concat(ucase(substring(trim(`title`), 1, 1)), lcase(substring(trim(nullif(`title`, '')), 2)))
    else `title`
  end
from
  `{{params.raw_database}}`.`cpc_subsection`;


drop table if exists `{{params.reporting_database}}`.`temp_cpc_current_group_aggregate_counts`;
create table `{{params.reporting_database}}`.`temp_cpc_current_group_aggregate_counts`
(
  `group_id` varchar(20) not null,
  `num_assignees` int unsigned not null,
  `num_inventors` int unsigned not null,
  `num_patents` int unsigned not null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `actual_years_active` smallint unsigned not null,
  primary key (`group_id`)
)
engine=InnoDB;


# 29:37
insert into `{{params.reporting_database}}`.`temp_cpc_current_group_aggregate_counts`
(
  `group_id`, `num_assignees`, `num_inventors`, `num_patents`,
  `first_seen_date`, `last_seen_date`, `actual_years_active`
)
select
  c.`group_id`,
  count(distinct pa.`assignee_id`) num_assignees,
  count(distinct pii.`inventor_id`) num_inventors,
  count(distinct c.`patent_id`) num_patents,
  min(p.`date`), max(p.`date`),
  ifnull(round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365), 0)
from
  `{{params.raw_database}}`.`cpc_current` c
  left outer join `{{params.raw_database}}`.`patent_assignee` pa on pa.`patent_id` = c.`patent_id`
  left outer join `{{params.raw_database}}`.`patent_inventor` pii on pii.`patent_id` = c.`patent_id`
  left outer join `{{params.reporting_database}}`.`patent` p on p.`patent_id` = c.`patent_id`
group by
  c.`group_id`;






drop table if exists `{{params.reporting_database}}`.`temp_cpc_group_title`;
create table `{{params.reporting_database}}`.`temp_cpc_group_title`
(
  `id` varchar(20) not null,
  `title` varchar(512) null,
  primary key (`id`)
)
engine=InnoDB;


# 0.156
insert into `{{params.reporting_database}}`.`temp_cpc_group_title`
  (`id`, `title`)
select
  `id`,
  case when binary replace(`title`, 'e.g.', 'E.G.') = binary ucase(`title`)
    then concat(ucase(substring(trim(`title`), 1, 1)), lcase(substring(trim(nullif(`title`, '')), 2)))
    else `title`
  end
from
  `{{params.raw_database}}`.`cpc_group`;


drop table if exists `{{params.reporting_database}}`.`temp_cpc_subgroup_title`;
create table `{{params.reporting_database}}`.`temp_cpc_subgroup_title`
(
  `id` varchar(20) not null,
  `title` varchar(2048) null,
  primary key (`id`)
)
engine=InnoDB;


# 0:07
insert into `{{params.reporting_database}}`.`temp_cpc_subgroup_title`
  (`id`, `title`)
select
  `id`,
  case when binary replace(`title`, 'e.g.', 'E.G.') = binary ucase(`title`)
    then concat(ucase(substring(trim(`title`), 1, 1)), lcase(substring(trim(nullif(`title`, '')), 2)))
    else `title`
  end
from
  `{{params.raw_database}}`.`cpc_subgroup`;


drop table if exists `{{params.reporting_database}}`.`cpc_current`;
create table `{{params.reporting_database}}`.`cpc_current`
(
  `patent_id` varchar(20) not null,
  `sequence` int unsigned not null,
  `section_id` varchar(10) null,
  `subsection_id` varchar(20) null,
  `subsection_title` varchar(512) null,
  `group_id` varchar(20) null,
  `group_title` varchar(512) null,
  `subgroup_id` varchar(20) null,
  `subgroup_title` varchar(2048) null,
  `category` varchar(36) null,
  `num_assignees` int unsigned null,
  `num_inventors` int unsigned null,
  `num_patents` int unsigned null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `years_active` smallint unsigned null,
  `num_assignees_group` int unsigned null,
  `num_inventors_group` int unsigned null,
  `num_patents_group` int unsigned null,
  `first_seen_date_group` date null,
  `last_seen_date_group` date null,
  `years_active_group` smallint unsigned null,
  primary key (`patent_id`, `sequence`)
)
engine=InnoDB;


# 23,151,381 @ 1:29:48
# 23,151,381 @ 36:32
insert into `{{params.reporting_database}}`.`cpc_current`
(
  `patent_id`, `sequence`, `section_id`, `subsection_id`,
  `subsection_title`, `group_id`, `group_title`, `subgroup_id`,
  `subgroup_title`, `category`,
  `num_assignees`, `num_inventors`, `num_patents`,
  `first_seen_date`, `last_seen_date`, `years_active`,
  `num_assignees_group`, `num_inventors_group`, `num_patents_group`,
  `first_seen_date_group`, `last_seen_date_group`, `years_active_group`
)
select
  p.`patent_id`, c.`sequence`,
  nullif(trim(c.`section_id`), ''),
  nullif(trim(c.`subsection_id`), ''),
  nullif(trim(s.`title`), ''),
  nullif(trim(c.`group_id`), ''),
  nullif(trim(g.`title`), ''),
  nullif(trim(c.`subgroup_id`), ''),
  nullif(trim(sg.`title`), ''),
  c.`category`, tccsac.`num_assignees`, tccsac.`num_inventors`,
  tccsac.`num_patents`, tccsac.`first_seen_date`, tccsac.`last_seen_date`,
  case when tccsac.`actual_years_active` < 1 then 1 else tccsac.`actual_years_active` end,
  tccgac.`num_assignees`, tccgac.`num_inventors`,
  tccgac.`num_patents`, tccgac.`first_seen_date`, tccgac.`last_seen_date`,
  case when tccgac.`actual_years_active` < 1 then 1 else tccgac.`actual_years_active` end
from
  `{{params.reporting_database}}`.`patent` p
  inner join `{{params.raw_database}}`.`cpc_current` c on p.`patent_id` = c.`patent_id`
  left outer join `{{params.reporting_database}}`.`temp_cpc_subsection_title` s on s.`id` = c.`subsection_id`
  left outer join `{{params.reporting_database}}`.`temp_cpc_group_title` g on g.`id` = c.`group_id`
  left outer join `{{params.reporting_database}}`.`temp_cpc_subgroup_title` sg on sg.`id` = c.`subgroup_id`
  left outer join `{{params.reporting_database}}`.`temp_cpc_current_subsection_aggregate_counts` tccsac on tccsac.`subsection_id` = c.`subsection_id`
  left outer join `{{params.reporting_database}}`.`temp_cpc_current_group_aggregate_counts` tccgac on tccgac.`group_id` = c.`group_id` where p.version_indicator<= {{ params.version_indicator }};


drop table if exists `{{params.reporting_database}}`.`cpc_current_subsection`;
create table `{{params.reporting_database}}`.`cpc_current_subsection`
(
  `patent_id` varchar(20) not null,
  `section_id` varchar(10) null,
  `subsection_id` varchar(20) null,
  `subsection_title` varchar(512) null,
  `num_assignees` int unsigned null,
  `num_inventors` int unsigned null,
  `num_patents` int unsigned null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `years_active` smallint unsigned null,
  primary key (`patent_id`, `subsection_id`)
)
engine=InnoDB;


# 7,240,381 @ 19:00
insert into `{{params.reporting_database}}`.`cpc_current_subsection`
(
  `patent_id`, `section_id`, `subsection_id`, `subsection_title`,
  `num_assignees`, `num_inventors`, `num_patents`,
  `first_seen_date`, `last_seen_date`, `years_active`
)
select
  c.`patent_id`,
  c.`section_id`,
  c.`subsection_id`,
  nullif(trim(s.`title`), ''),
  tccsac.`num_assignees`, tccsac.`num_inventors`,
  tccsac.`num_patents`, tccsac.`first_seen_date`, tccsac.`last_seen_date`,
  case when tccsac.`actual_years_active` < 1 then 1 else tccsac.`actual_years_active` end
from
  (select distinct `patent_id`, `section_id`, `subsection_id` from `{{params.reporting_database}}`.`cpc_current`) c
  left outer join `{{params.reporting_database}}`.`temp_cpc_subsection_title` s on s.`id` = c.`subsection_id`
  left outer join `{{params.reporting_database}}`.`temp_cpc_current_subsection_aggregate_counts` tccsac on tccsac.`subsection_id` = c.`subsection_id`;




drop table if exists `{{params.reporting_database}}`.`cpc_current_group`;
create table `{{params.reporting_database}}`.`cpc_current_group`
(
  `patent_id` varchar(20) not null,
  `section_id` varchar(10) null,
  `group_id` varchar(20) null,
  `group_title` varchar(512) null,
  `num_assignees` int unsigned null,
  `num_inventors` int unsigned null,
  `num_patents` int unsigned null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `years_active` smallint unsigned null,
  primary key (`patent_id`, `group_id`)
)
engine=InnoDB;


# 7,240,381 @ 19:00
insert into `{{params.reporting_database}}`.`cpc_current_group`
(
  `patent_id`, `section_id`, `group_id`, `group_title`,
  `num_assignees`, `num_inventors`, `num_patents`,
  `first_seen_date`, `last_seen_date`, `years_active`
)
select
  c.`patent_id`,
  c.`section_id`,
  c.`group_id`,
  nullif(trim(s.`title`), ''),
  tccgac.`num_assignees`, tccgac.`num_inventors`,
  tccgac.`num_patents`, tccgac.`first_seen_date`, tccgac.`last_seen_date`,
  case when tccgac.`actual_years_active` < 1 then 1 else tccgac.`actual_years_active` end
from
  (select distinct `patent_id`, `section_id`, `group_id` from `{{params.reporting_database}}`.`cpc_current`) c
  left outer join `{{params.reporting_database}}`.`temp_cpc_group_title` s on s.`id` = c.`group_id`
  left outer join `{{params.reporting_database}}`.`temp_cpc_current_group_aggregate_counts` tccgac on tccgac.`group_id` = c.`group_id`;



# END cpc_current 

#############################################################################################################################################


# BEGIN cpc_current_subsection_patent_year 

####################################################################################################################


drop table if exists `{{params.reporting_database}}`.`cpc_current_subsection_patent_year`;
create table `{{params.reporting_database}}`.`cpc_current_subsection_patent_year`
(
  `subsection_id` varchar(20) not null,
  `patent_year` smallint unsigned not null,
  `num_patents` int unsigned not null,
  primary key (`subsection_id`, `patent_year`)
)
engine=InnoDB;


# 13:24
insert into `{{params.reporting_database}}`.`cpc_current_subsection_patent_year`
  (`subsection_id`, `patent_year`, `num_patents`)
select
  c.`subsection_id`, year(p.`date`), count(distinct c.`patent_id`)
from
  `{{params.raw_database}}`.`cpc_current` c
  inner join `{{params.reporting_database}}`.`patent` p on p.`patent_id` = c.`patent_id` and p.`date` is not null
where
  c.`subsection_id` is not null and c.`subsection_id` != ''
group by
  c.`subsection_id`, year(p.`date`);


# END cpc_current_subsection_patent_year 

######################################################################################################################

# BEGIN cpc_current_group_patent_year 

####################################################################################################################


drop table if exists `{{params.reporting_database}}`.`cpc_current_group_patent_year`;
create table `{{params.reporting_database}}`.`cpc_current_group_patent_year`
(
  `group_id` varchar(20) not null,
  `patent_year` smallint unsigned not null,
  `num_patents` int unsigned not null,
  primary key (`group_id`, `patent_year`)
)
engine=InnoDB;


# 13:24
insert into `{{params.reporting_database}}`.`cpc_current_group_patent_year`
  (`group_id`, `patent_year`, `num_patents`)
select
  c.`group_id`, year(p.`date`), count(distinct c.`patent_id`)
from
  `{{params.raw_database}}`.`cpc_current` c
  inner join `{{params.reporting_database}}`.`patent` p on p.`patent_id` = c.`patent_id` and p.`date` is not null
where
  c.`group_id` is not null and c.`group_id` != ''
group by
  c.`group_id`, year(p.`date`);


# END cpc_current_group_patent_year 

######################################################################################################################


# BEGIN cpc_current_group_application_year 

###############################################################################################################


drop table if exists `{{params.reporting_database}}`.`cpc_current_group_application_year`;
create table `{{params.reporting_database}}`.`cpc_current_group_application_year`
(
  `group_id` varchar(20) not null,
  `application_year` smallint unsigned not null,
  `sample_size` int unsigned not null,
  `average_patent_processing_days` int unsigned null,
  primary key (`group_id`, `application_year`)
)
engine=InnoDB;


# 20,241 @ 0:56
insert into `{{params.reporting_database}}`.`cpc_current_group_application_year`
  (`group_id`, `application_year`, `sample_size`, `average_patent_processing_days`)
select
  u.`group_id`,
  year(p.`earliest_application_date`),
  count(*),
  round(avg(p.`patent_processing_days`))
from
  `{{params.reporting_database}}`.`patent` p
  inner join `{{params.reporting_database}}`.`cpc_current` u on u.`patent_id` = p.`patent_id`
where
  p.`patent_processing_days` is not null and u.`sequence` = 0
group by
  u.`group_id`, year(p.`earliest_application_date`);


# 5,406,673 @ 32:45
# Update the patent with the average mainclass processing days.
update
  `{{params.reporting_database}}`.`patent` p
  inner join `{{params.reporting_database}}`.`cpc_current` u on
    u.`patent_id` = p.`patent_id` and u.`sequence` = 0
  inner join `{{params.reporting_database}}`.`cpc_current_group_application_year` c on
    c.`group_id` = u.`group_id` and c.`application_year` = year(p.`earliest_application_date`)
set
  p.`cpc_current_group_average_patent_processing_days` = c.`average_patent_processing_days`;


# END cpc_current_group_application_year 

#################################################################################################################

