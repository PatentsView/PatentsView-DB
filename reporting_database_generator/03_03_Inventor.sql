
# BEGIN inventor

##############################################################################################################################################


SET collation_connection = 'utf8mb4_unicode_ci';

drop table if exists `{{params.reporting_database}}`.`temp_inventor_lastknown_location`;
create table `{{params.reporting_database}}`.`temp_inventor_lastknown_location`
(
  `inventor_id` varchar(256) not null,
  `location_id` int unsigned null,
  `persistent_location_id` varchar(128) null,
  `city` varchar(256) null,
  `state` varchar(20) null,
  `country` varchar(10) null,
  `latitude` float null,
  `longitude` float null,
  primary key (`inventor_id`)
)
engine=InnoDB;


# Populate temp_inventor_lastknown_location table.  The goal here is to grab the location associated
# with the most recent patent associated with the inventor.  It is possible for a patent/inventor
# combination not to have a location, so we will grab the most recent KNOWN location.
# 3,437,668 @ 22:05


insert into `{{params.reporting_database}}`.`temp_inventor_lastknown_location`
select
  t.`inventor_id`,
  tl.`new_location_id`,
  tl.`old_location_id_transformed`,
  nullif(trim(l.`city`), ''),
  nullif(trim(l.`state`), ''),
  nullif(trim(l.`country`), ''),
  l.`latitude`,
  l.`longitude`

  from (select * from

	(select
          ROW_NUMBER() OVER (PARTITION BY t.inventor_id ORDER BY t.`date` desc) AS rownum,
          t.`inventor_id`,
          t.`location_id`
        from
          (
            select
              ri.`inventor_id`,
              rl.`location_id`,
	      p.`date`,
	      p.`id`
            from
              `{{params.raw_database}}`.`rawinventor` ri
              inner join `{{params.raw_database}}`.`patent` p on p.`id` = ri.`patent_id`
              inner join `{{params.raw_database}}`.`rawlocation` rl on rl.`id` = ri.`rawlocation_id`
            where
              ri.`inventor_id` is not null and
              rl.`location_id` is not null and ri.version_indicator <={{ params.version_indicator }}
            order by
              ri.`inventor_id`,
              p.`date` desc,
              p.`id` desc
          ) t) t where t.rownum = 1 ) t
       left join `{{params.raw_database}}`.`location` l on l.`id` = t.`location_id`
left join `{{params.reporting_database}}`.`temp_id_mapping_location` tl on tl.`old_location_id`=t.`location_id`;



drop table if exists `{{params.reporting_database}}`.`temp_inventor_num_patents`;
create table `{{params.reporting_database}}`.`temp_inventor_num_patents`
(
  `inventor_id` varchar(256) not null,
  `num_patents` int unsigned not null,
  primary key (`inventor_id`)
)
engine=InnoDB;


# 2:06
insert into `{{params.reporting_database}}`.`temp_inventor_num_patents`
  (`inventor_id`, `num_patents`)
select
  `inventor_id`, count(distinct `patent_id`)
from
  `{{params.raw_database}}`.`patent_inventor`  pi join `{{ params.raw_database }}`.`patent` p on p.id=pi.patent_id where p.version_indicator <={{ params.version_indicator }}
group by
  `inventor_id`;

drop table if exists `{{params.reporting_database}}`.`temp_inventor_num_assignees`;
create table `{{params.reporting_database}}`.`temp_inventor_num_assignees`
(
  `inventor_id` varchar(256) not null,
  `num_assignees` int unsigned not null,
  primary key (`inventor_id`)
)
engine=InnoDB;


# 0:15
insert into `{{params.reporting_database}}`.`temp_inventor_num_assignees`
  (`inventor_id`, `num_assignees`)
select
  ii.`inventor_id`, count(distinct aa.`assignee_id`)
from
  `{{params.raw_database}}`.`patent_inventor` ii
  join `{{params.raw_database}}`.`patent_assignee` aa
  on aa.`patent_id` = ii.`patent_id`  join `{{ params.raw_database }}`.`patent` p on p.id=ii.patent_id where p.version_indicator <={{ params.version_indicator }}
group by
  ii.`inventor_id`;


drop table if exists `{{params.reporting_database}}`.`temp_inventor_years_active`;
create table `{{params.reporting_database}}`.`temp_inventor_years_active`
(
  `inventor_id` varchar(256) not null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `actual_years_active` smallint unsigned not null,
  primary key (`inventor_id`)
)
engine=InnoDB;


# 5:42
insert into `{{params.reporting_database}}`.`temp_inventor_years_active`
  (`inventor_id`, `first_seen_date`, `last_seen_date`, `actual_years_active`)
select
  pa.`inventor_id`, min(p.`date`), max(p.`date`),
  ifnull(round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365), 0)
from
  `{{params.raw_database}}`.`patent_inventor` pa
  inner join `{{params.reporting_database}}`.`patent` p on p.`patent_id`= pa.`patent_id`
where
  p.`date` is not null
group by
  pa.`inventor_id`;

drop table if exists `{{ params.reporting_database }}`.`temp_first_inventor`;
create table `{{ params.reporting_database }}`.`temp_first_inventor`
(
  `patent_id` varchar(20) not null,
  `inventor_id` varchar(256)  not null,
  `sequence` smallint unsigned not null,
  primary key (`patent_id`, `inventor_id`),
  unique index ak_temp_first_inventor (`inventor_id`, `patent_id`)
)
engine=InnoDB;
insert into `{{params.reporting_database}}`.`temp_first_inventor`
(
  `patent_id`, `inventor_id`, `sequence`
)
select patent_id, inventor_id, min(sequence) sequence from `{{params.raw_database}}`.`rawinventor` where version_indicator<={{ params.version_indicator }} group by patent_id, inventor_id;

drop table if exists `{{params.reporting_database}}`.`patent_inventor`;
create table `{{params.reporting_database}}`.`patent_inventor`
(
  `patent_id` varchar(20) not null,
  `inventor_id` int unsigned not null,
  `location_id` int unsigned null,
  `sequence` smallint unsigned not null,
  primary key (`patent_id`, `inventor_id`),
  unique index ak_patent_inventor (`inventor_id`, `patent_id`)
)
engine=InnoDB;


# 12,389,559 @ 29:50
insert into `{{params.reporting_database}}`.`patent_inventor`
(
  `patent_id`, `inventor_id`, `location_id`, `sequence`
)
select distinct
  pii.`patent_id`, t.`new_inventor_id`, tl.`new_location_id`, ri.`sequence`
from
  `{{params.raw_database}}`.`patent_inventor` pii
  inner join `{{params.reporting_database}}`.`temp_id_mapping_inventor` t on t.`old_inventor_id` = pii.`inventor_id`
  left outer join  `{{params.reporting_database}}`.temp_first_inventor t2

on t2.`patent_id` = pii.`patent_id` and t2.`inventor_id` = pii.`inventor_id`
  left outer join `{{params.raw_database}}`.`rawinventor` ri on ri.`patent_id` = t.`patent_id` and ri.`inventor_id` = t.`inventor_id` and ri.`sequence`
= t.`sequence`
  left outer join `{{params.raw_database}}`.`rawlocation` rl on rl.`id` = ri.`rawlocation_id`
  left outer join `{{params.reporting_database}}`.`temp_id_mapping_location` tl on tl.`old_location_id` = rl.`location_id`;


drop table if exists `{{params.reporting_database}}`.`location_inventor`;
create table `{{params.reporting_database}}`.`location_inventor`
(
  `location_id` int unsigned not null,
  `inventor_id` int unsigned not null,
  `num_patents` int unsigned,
  primary key (`location_id`, `inventor_id`)
)
engine=InnoDB;


# 4,188,507 @ 0:50
insert into `{{params.reporting_database}}`.`location_inventor`
  (`location_id`, `inventor_id`, `num_patents`)
select distinct
  timl.`new_location_id`,
  timi.`new_inventor_id`,
  null
from
  `{{params.raw_database}}`.`location_inventor` la
  inner join `{{params.reporting_database}}`.`temp_id_mapping_location` timl on timl.`old_location_id` = la.`location_id`
  inner join `{{params.reporting_database}}`.`temp_id_mapping_inventor` timi on timi.`old_inventor_id` = la.`inventor_id`;


drop table if exists `{{params.reporting_database}}`.`inventor`;
create table `{{params.reporting_database}}`.`inventor`
(
  `inventor_id` int unsigned not null,
  `name_first` varchar(128) null,
  `name_last` varchar(128) null,
  `num_patents` int unsigned not null,
  `num_assignees` int unsigned not null,
  `lastknown_location_id` int unsigned null,
  `lastknown_persistent_location_id` varchar(128) null,
  `lastknown_city` varchar(256) null,
  `lastknown_state` varchar(20) null,
  `lastknown_country` varchar(10) null,
  `lastknown_latitude` float null,
  `lastknown_longitude` float null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `years_active` smallint unsigned not null,
  `persistent_inventor_id` varchar(256) not null,
  primary key (`inventor_id`)
)
engine=InnoDB;


# 3,572,763 @ 1:57
insert into `{{params.reporting_database}}`.`inventor`
(
  `inventor_id`, `name_first`, `name_last`, `num_patents`, `num_assignees`,
  `lastknown_location_id`, `lastknown_persistent_location_id`, `lastknown_city`,
  `lastknown_state`, `lastknown_country`, `lastknown_latitude`, `lastknown_longitude`,
  `first_seen_date`, `last_seen_date`, `years_active`, `persistent_inventor_id`
)
select
  t.`new_inventor_id`, nullif(trim(i.`name_first`), ''), nullif(trim(i.`name_last`), ''),
  tinp.`num_patents`, ifnull(tina.`num_assignees`, 0), tilkl.`location_id`, tilkl.`persistent_location_id`, tilkl.`city`, tilkl.`state`,
  tilkl.`country`, tilkl.`latitude`, tilkl.`longitude`, tifls.`first_seen_date`, tifls.`last_seen_date`,
  ifnull(case when tifls.`actual_years_active` < 1 then 1 else tifls.`actual_years_active` end, 0),
  i.`id`
from
  `{{params.raw_database}}`.`inventor` i
  inner join `{{params.reporting_database}}`.`temp_id_mapping_inventor` t on t.`old_inventor_id` = i.`id`
  left outer join `{{params.reporting_database}}`.`temp_inventor_lastknown_location` tilkl on tilkl.`inventor_id` = i.`id`
  inner join `{{params.reporting_database}}`.`temp_inventor_num_patents` tinp on tinp.`inventor_id` = i.`id`
  left outer join `{{params.reporting_database}}`.`temp_inventor_years_active` tifls on tifls.`inventor_id` = i.`id`
  left outer join `{{params.reporting_database}}`.`temp_inventor_num_assignees` tina on tina.`inventor_id` = i.`id`;


# END inventor

################################################################################################################################################