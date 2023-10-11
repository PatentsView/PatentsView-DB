{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set version_indicator = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

# BEGIN inventor

##############################################################################################################################################


SET collation_connection = 'utf8mb4_unicode_ci';

drop table if exists `{{reporting_db}}`.`temp_inventor_lastknown_location`;
create table `{{reporting_db}}`.`temp_inventor_lastknown_location`
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
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


# Populate temp_inventor_lastknown_location table.  The goal here is to grab the location associated
# with the most recent patent associated with the inventor.  It is possible for a patent/inventor
# combination not to have a location, so we will grab the most recent KNOWN location.


insert into `{{reporting_db}}`.`temp_inventor_lastknown_location`
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
              `patent`.`rawinventor` ri
              inner join `patent`.`patent` p on p.`id` = ri.`patent_id`
              inner join `patent`.`rawlocation` rl on rl.`id` = ri.`rawlocation_id`
            where
              ri.`inventor_id` is not null and
              rl.`location_id` is not null and ri.version_indicator <='{{version_indicator}}'
            order by
              ri.`inventor_id`,
              p.`date` desc,
              p.`id` desc
          ) t) t where t.rownum = 1 ) t
       left join `patent`.`location` l on l.`id` = t.`location_id`
left join `{{reporting_db}}`.`temp_id_mapping_location` tl on tl.`old_location_id`=t.`location_id`;



drop table if exists `{{reporting_db}}`.`temp_inventor_num_patents`;
create table `{{reporting_db}}`.`temp_inventor_num_patents`
(
  `inventor_id` varchar(256) not null,
  `num_patents` int unsigned not null,
  primary key (`inventor_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`temp_inventor_num_patents`
  (`inventor_id`, `num_patents`)
select
  `inventor_id`, count(distinct `patent_id`)
from
  `patent`.`patent_inventor`  pi join `patent`.`patent` p on p.id=pi.patent_id where p.version_indicator <='{{version_indicator}}'
group by
  `inventor_id`;

drop table if exists `{{reporting_db}}`.`temp_inventor_num_assignees`;
create table `{{reporting_db}}`.`temp_inventor_num_assignees`
(
  `inventor_id` varchar(256) not null,
  `num_assignees` int unsigned not null,
  primary key (`inventor_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`temp_inventor_num_assignees`
  (`inventor_id`, `num_assignees`)
select
  ii.`inventor_id`, count(distinct aa.`assignee_id`)
from
  `patent`.`patent_inventor` ii
  join `patent`.`patent_assignee` aa
  on aa.`patent_id` = ii.`patent_id`  join `patent`.`patent` p on p.id=ii.patent_id where p.version_indicator <='{{version_indicator}}'
group by
  ii.`inventor_id`;


drop table if exists `{{reporting_db}}`.`temp_inventor_years_active`;
create table `{{reporting_db}}`.`temp_inventor_years_active`
(
  `inventor_id` varchar(256) not null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `actual_years_active` smallint unsigned not null,
  primary key (`inventor_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`temp_inventor_years_active`
  (`inventor_id`, `first_seen_date`, `last_seen_date`, `actual_years_active`)
select
  pa.`inventor_id`, min(p.`date`), max(p.`date`),
  ifnull(round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365), 0)
from
  `patent`.`patent_inventor` pa
  inner join `{{reporting_db}}`.`patent` p on p.`patent_id`= pa.`patent_id`
where
  p.`date` is not null
group by
  pa.`inventor_id`;

drop table if exists `{{reporting_db}}`.`patent_inventor`;
create table `{{reporting_db}}`.`patent_inventor`
(
  `patent_id` varchar(20) not null,
  `inventor_id` int unsigned not null,
  `location_id` int unsigned null,
  `sequence` smallint unsigned not null,
  primary key (`patent_id`, `inventor_id`,`sequence`),
  unique index ak_patent_inventor (`inventor_id`, `patent_id`,`sequence`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert ignore into `{{reporting_db}}`.`patent_inventor`
(
  `patent_id`, `inventor_id`, `location_id`, `sequence`
)
select
  pii.`patent_id`, t.`new_inventor_id`, tl.`new_location_id`, pii.`sequence`
from
  `patent`.`patent_inventor` pii
  inner join `{{reporting_db}}`.`temp_id_mapping_inventor` t on t.`old_inventor_id` = pii.`inventor_id`
  left outer join `patent`.`location` rl on rl.`id` = pii.`location_id`
  left outer join `{{reporting_db}}`.`temp_id_mapping_location` tl on tl.`old_location_id` = rl.`id`;


drop table if exists `{{reporting_db}}`.`location_inventor`;
create table `{{reporting_db}}`.`location_inventor`
(
  `location_id` int unsigned not null,
  `inventor_id` int unsigned not null,
  `num_patents` int unsigned,
  primary key (`location_id`, `inventor_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`location_inventor`
  (`location_id`, `inventor_id`, `num_patents`)
select distinct
  timl.`new_location_id`,
  timi.`new_inventor_id`,
  null
from
  `patent`.`location_inventor` la
  inner join `{{reporting_db}}`.`temp_id_mapping_location` timl on timl.`old_location_id` = la.`location_id`
  inner join `{{reporting_db}}`.`temp_id_mapping_inventor` timi on timi.`old_inventor_id` = la.`inventor_id`;


drop table if exists `{{reporting_db}}`.`inventor`;
create table `{{reporting_db}}`.`inventor`
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
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`inventor`
(
  `inventor_id`, `name_first`, `name_last`, `num_patents`, `num_assignees`,
  `lastknown_location_id`, `lastknown_persistent_location_id`, `lastknown_city`,
  `lastknown_state`, `lastknown_country`, `lastknown_latitude`, `lastknown_longitude`,
  `first_seen_date`, `last_seen_date`, `years_active`, `persistent_inventor_id`, `gender_code`
)
select
  t.`new_inventor_id`, nullif(trim(i.`name_first`), ''), nullif(trim(i.`name_last`), ''),
  tinp.`num_patents`, ifnull(tina.`num_assignees`, 0), tilkl.`location_id`, tilkl.`persistent_location_id`, tilkl.`city`, tilkl.`state`,
  tilkl.`country`, tilkl.`latitude`, tilkl.`longitude`, tifls.`first_seen_date`, tifls.`last_seen_date`,
  ifnull(case when tifls.`actual_years_active` < 1 then 1 else tifls.`actual_years_active` end, 0),
  i.`id`, gender_code
from
  `patent`.`inventor` i
  inner join `{{reporting_db}}`.`temp_id_mapping_inventor` t on t.`old_inventor_id` = i.`id`
  left outer join `{{reporting_db}}`.`temp_inventor_lastknown_location` tilkl on tilkl.`inventor_id` = i.`id`
  inner join `{{reporting_db}}`.`temp_inventor_num_patents` tinp on tinp.`inventor_id` = i.`id`
  left outer join `{{reporting_db}}`.`temp_inventor_years_active` tifls on tifls.`inventor_id` = i.`id`
  left outer join `{{reporting_db}}`.`temp_inventor_num_assignees` tina on tina.`inventor_id` = i.`id`
  left join gender_attribution.inventor_gender_{{version_indicator}} ig on i.id=ig.inventor_id;

# BEGIN inventor_year ######################################################################################################################

drop table if exists `{{reporting_db}}`.`inventor_year`;
create table `{{reporting_db}}`.`inventor_year`
(
  `inventor_id` int unsigned not null,
  `patent_year` smallint not null,
  `num_patents` int unsigned not null,
   unique (`inventor_id`, `patent_year`)
)
engine=InnoDB;


insert into `{{reporting_db}}`.`inventor_year`
(`inventor_id`, `patent_year`, `num_patents`)
select
  pi.inventor_id, p.year, count(distinct pi.patent_id)
from
  `{{reporting_db}}`.`patent_inventor` pi
  inner join `{{reporting_db}}`.`patent` p using(patent_id)
group by
  pi.inventor_id, p.year;

# END inventor_year ######################################################################################################################

update
  `{{reporting_db}}`.`location_inventor` li
  inner join
  (
    select
      `location_id`, `inventor_id`, count(distinct `patent_id`) num_patents
    from
      `{{reporting_db}}`.`patent_inventor`
    group by
      `location_id`, `inventor_id`
  ) pii on pii.`location_id` = li.`location_id` and pii.`inventor_id` = li.`inventor_id`
set
  li.`num_patents` = pii.`num_patents`;



alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_lastknown_location_id` (`lastknown_location_id`);
alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_first_seen_date` (`first_seen_date`);
alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_last_seen_date` (`last_seen_date`);
alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_lastknown_persistent_location_id` (`lastknown_persistent_location_id`);
alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_num_assignees` (`num_assignees`);
alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_name_first` (`name_first`);
alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_name_last` (`name_last`);
alter table `{{reporting_db}}`.`inventor` add index `ix_inventor_persistent_inventor_id` (`persistent_inventor_id`);

alter table `{{reporting_db}}`.`patent_inventor` add index `ix_patent_inventor_location_id` (`location_id`);
alter table `{{reporting_db}}`.`inventor_year` add index `ix_inventor_year_inventor_id` (`inventor_id`);
alter table `{{reporting_db}}`.`inventor_year` add index `ix_inventor_year_year` (`patent_year`);
alter table `{{reporting_db}}`.`inventor_year` add index `ix_inventor_year_num_patents` (`num_patents`);

alter table `{{reporting_db}}`.`location_inventor` add index `ix_location_inventor_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`location_inventor` add index `ix_location_inventor_inventor_id` (`inventor_id`);

# END inventor

################################################################################################################################################