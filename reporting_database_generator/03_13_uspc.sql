{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

# BEGIN uspc_current 

##########################################################################################################################################


drop table if exists `{{reporting_db}}`.`temp_mainclass_current_aggregate_counts`;
create table `{{reporting_db}}`.`temp_mainclass_current_aggregate_counts`
(
  `mainclass_id` varchar(20) not null,
  `num_assignees` int unsigned not null,
  `num_inventors` int unsigned not null,
  `num_patents` int unsigned not null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `actual_years_active` smallint unsigned not null,
  primary key (`mainclass_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`temp_mainclass_current_aggregate_counts`
(
  `mainclass_id`, `num_assignees`, `num_inventors`, `num_patents`,
  `first_seen_date`, `last_seen_date`, `actual_years_active`
)
select
  u.`mainclass_id`,
  count(distinct pa.`assignee_id`),
  count(distinct pii.`inventor_id`),
  count(distinct u.`patent_id`),
  min(p.`date`), max(p.`date`),
  ifnull(round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365), 0)
from
  `patent`.`uspc_current` u
  left outer join `patent`.`patent_assignee` pa on pa.`patent_id` = u.`patent_id`
  left outer join `patent`.`patent_inventor` pii on pii.`patent_id` = u.`patent_id`
  left outer join `{{reporting_db}}`.`patent` p on p.`patent_id` = u.`patent_id` and p.`date` is not null
where
  u.`mainclass_id` is not null and u.`mainclass_id` != ''
group by
  u.`mainclass_id`;


drop table if exists `{{reporting_db}}`.`temp_mainclass_current_title`;
create table `{{reporting_db}}`.`temp_mainclass_current_title`
(
  `id` varchar(20) not null,
  `title` varchar(512) null,
  primary key (`id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`temp_mainclass_current_title`
  (`id`, `title`)
select
  `id`,
  case when binary replace(`title`, 'e.g.', 'E.G.') = binary ucase(`title`)
    then concat(ucase(substring(trim(`title`), 1, 1)), lcase(substring(trim(nullif(`title`, '')), 2)))
    else `title`
  end
from
  `patent`.`mainclass_current`;


# Fix casing of subclass_current.
drop table if exists `{{reporting_db}}`.`temp_subclass_current_title`;
create table `{{reporting_db}}`.`temp_subclass_current_title`
(
  `id` varchar(20) not null,
  `title` varchar(512) null,
  primary key (`id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`temp_subclass_current_title`
  (`id`, `title`)
select
  `id`,
  case when binary replace(`title`, 'e.g.', 'E.G.') = binary ucase(`title`)
    then concat(ucase(substring(trim(`title`), 1, 1)), lcase(substring(trim(nullif(`title`, '')), 2)))
    else `title`
  end
from
  `patent`.`subclass_current`;


drop table if exists `{{reporting_db}}`.`uspc_current`;
create table `{{reporting_db}}`.`uspc_current`
(
  `patent_id` varchar(20) not null,
  `sequence` int unsigned not null,
  `mainclass_id` varchar(20) null,
  `mainclass_title` varchar(256) null,
  `subclass_id` varchar(20) null,
  `subclass_title` varchar(512) null,
  `num_assignees` int unsigned null,
  `num_inventors` int unsigned null,
  `num_patents` int unsigned null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `years_active` smallint unsigned null,
  primary key (`patent_id`, `sequence`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert ignore into `{{reporting_db}}`.`uspc_current`
(
  `patent_id`, `sequence`, `mainclass_id`,
  `mainclass_title`, `subclass_id`, `subclass_title`,
  `num_assignees`, `num_inventors`, `num_patents`,
  `first_seen_date`, `last_seen_date`, `years_active`
)
select
  p.`patent_id`, u.`sequence`,
  nullif(trim(u.`mainclass_id`), ''),
  nullif(trim(m.`title`), ''),
  nullif(trim(u.`subclass_id`), ''),
  nullif(trim(s.`title`), ''),
  tmcac.`num_assignees`, tmcac.`num_inventors`, tmcac.`num_patents`,
  tmcac.`first_seen_date`, tmcac.`last_seen_date`,
  ifnull(case when tmcac.`actual_years_active` < 1 then 1 else tmcac.`actual_years_active` end, 0)
from
  `{{reporting_db}}`.`patent` p
  inner join `patent`.`uspc_current` u on u.`patent_id` = p.`patent_id`
  left outer join `{{reporting_db}}`.`temp_mainclass_current_title` m on m.`id` = u.`mainclass_id`
  left outer join `{{reporting_db}}`.`temp_subclass_current_title` s on s.`id` = u.`subclass_id`
  left outer join `{{reporting_db}}`.`temp_mainclass_current_aggregate_counts` tmcac on tmcac.`mainclass_id` = u.`mainclass_id`;


drop table if exists `{{reporting_db}}`.`uspc_current_mainclass`;
create table `{{reporting_db}}`.`uspc_current_mainclass`
(
  `patent_id` varchar(20) not null,
  `mainclass_id` varchar(20) null,
  `mainclass_title` varchar(256) null,
  `num_assignees` int unsigned null,
  `num_inventors` int unsigned null,
  `num_patents` int unsigned null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `years_active` smallint unsigned null,
  primary key (`patent_id`, `mainclass_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`uspc_current_mainclass`
(
  `patent_id`, `mainclass_id`, `mainclass_title`,
  `num_assignees`, `num_inventors`, `num_patents`,
  `first_seen_date`, `last_seen_date`, `years_active`
)
select
  u.`patent_id`,
  u.`mainclass_id`,
  nullif(trim(m.`title`), ''),
  tmcac.`num_assignees`, tmcac.`num_inventors`, tmcac.`num_patents`,
  tmcac.`first_seen_date`, tmcac.`last_seen_date`,
  ifnull(case when tmcac.`actual_years_active` < 1 then 1 else tmcac.`actual_years_active` end, 0)
from
  (select distinct `patent_id`, `mainclass_id` from `{{reporting_db}}`.`uspc_current`) u
  left join `{{reporting_db}}`.`temp_mainclass_current_title` m on m.`id` = u.`mainclass_id`
  left join `{{reporting_db}}`.`temp_mainclass_current_aggregate_counts` tmcac on tmcac.`mainclass_id` = u.`mainclass_id`;


# END uspc_current 

############################################################################################################################################


# BEGIN uspc_current_mainclass_application_year 

###############################################################################################################


drop table if exists `{{reporting_db}}`.`uspc_current_mainclass_application_year`;
create table `{{reporting_db}}`.`uspc_current_mainclass_application_year`
(
  `mainclass_id` varchar(20) not null,
  `application_year` smallint unsigned not null,
  `sample_size` int unsigned not null,
  `average_patent_processing_days` int unsigned null,
  primary key (`mainclass_id`, `application_year`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`uspc_current_mainclass_application_year`
  (`mainclass_id`, `application_year`, `sample_size`, `average_patent_processing_days`)
select
  u.`mainclass_id`,
  year(p.`earliest_application_date`),
  count(*),
  round(avg(p.`patent_processing_days`))
from
  `{{reporting_db}}`.`patent` p
  inner join `{{reporting_db}}`.`uspc_current` u on u.`patent_id` = p.`patent_id`
where
  p.`patent_processing_days` is not null and u.`sequence` = 0
group by
  u.`mainclass_id`, year(p.`earliest_application_date`);


# Update the patent with the average mainclass processing days.
update
  `{{reporting_db}}`.`patent` p
  inner join `{{reporting_db}}`.`uspc_current` u on
    u.`patent_id` = p.`patent_id` and u.`sequence` = 0
  inner join `{{reporting_db}}`.`uspc_current_mainclass_application_year` c on
    c.`mainclass_id` = u.`mainclass_id` and c.`application_year` = year(p.`earliest_application_date`)
set
  p.`uspc_current_mainclass_average_patent_processing_days` = c.`average_patent_processing_days`;


# END uspc_current_mainclass_application_year 

#################################################################################################################

# BEGIN uspc_current_mainclass_patent_year 

####################################################################################################################


drop table if exists `{{reporting_db}}`.`uspc_current_mainclass_patent_year`;
create table `{{reporting_db}}`.`uspc_current_mainclass_patent_year`
(
  `mainclass_id` varchar(20) not null,
  `patent_year` smallint unsigned not null,
  `num_patents` int unsigned not null,
  primary key (`mainclass_id`, `patent_year`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`uspc_current_mainclass_patent_year`
  (`mainclass_id`, `patent_year`, `num_patents`)
select
  u.`mainclass_id`, year(p.`date`), count(distinct u.`patent_id`)
from
  `patent`.`uspc_current` u
  inner join `{{reporting_db}}`.`patent` p on p.`patent_id` = u.`patent_id` and p.`date` is not null
where
  u.`mainclass_id` is not null and u.`mainclass_id` != ''
group by
  u.`mainclass_id`, year(p.`date`);

alter table `{{reporting_db}}`.`uspc_current_mainclass_patent_year` add index `ix_uspc_current_mainclass_patent_year_num_patents`(`num_patents`);
alter table `{{reporting_db}}`.`uspc_current_mainclass` add index `ix_uspc_current_mainclass_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`uspc_current_mainclass` add fulltext index `fti_uspc_current_mainclass_mainclass_title` (`mainclass_title`);
alter table `{{reporting_db}}`.`uspc_current_mainclass` add index `ix_uspc_current_mainclass_mainclass_title` (`mainclass_title`);
alter table `{{reporting_db}}`.`uspc_current_mainclass` add index `ix_uspc_current_mainclass_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`uspc_current_mainclass` add index `ix_uspc_current_mainclass_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`uspc_current_mainclass` add index `ix_uspc_current_mainclass_mainclass_id` (`mainclass_id`);

# END uspc_current_mainclass_patent_year 

######################################################################################################################