{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set version_indicator = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

# BEGIN assignee

##############################################################################################################################################

#this is necessary because otherwise the subqueries wierdly have a different collation
SET collation_connection = 'utf8mb4_unicode_ci';

drop table if exists `{{reporting_db}}`.`temp_assignee_lastknown_location`;

create table `{{reporting_db}}`.`temp_assignee_lastknown_location`
(
    `assignee_id`            varchar(64)  not null,
    `location_id`            int unsigned null,
    `persistent_location_id` varchar(128) null,
    `city`                   varchar(128) null,
    `state`                  varchar(128)  null,
    `country`                varchar(128)  null,
    `latitude`               float        null,
    `longitude`              float        null,
    primary key (`assignee_id`)
)
    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


# Populate temp_assignee_lastknown_location table.  The goal here is to grab the location associated
# with the most recent patent associated with the assignee.  It is possible for a patent/assignee
# combination not to have a location, so we will grab the most recent KNOWN location.
# 320,156 @ 3:51
insert into `{{reporting_db}}`.`temp_assignee_lastknown_location`
(`assignee_id`, `location_id`, `persistent_location_id`, `city`, `state`, `country`, `latitude`, `longitude`)
select lastknown.`assignee_id`,
       tl.`new_location_id`,
       tl.`old_location_id_transformed`,
       nullif(trim(l.`city`), ''),
       nullif(trim(l.`state`), ''),
       nullif(trim(l.`country`), ''),
       l.`latitude`,
       l.`longitude`
from (
         select srw.`assignee_id`,
                srw.`location_id`
         from (select ROW_NUMBER() OVER (PARTITION BY rw.assignee_id ORDER BY rw.`date` desc) AS rownum,
                      rw.`assignee_id`,
                      rw.`location_id`
               from (
                        select ra.`assignee_id`,
                               rl.`location_id`,
                               p.`date`,
                               p.`id`
                        from `patent`.`rawassignee` ra
                                 inner join `patent`.`patent` p on p.`id` = ra.`patent_id`
                                 inner join `patent`.`rawlocation` rl on rl.`id` = ra.`rawlocation_id`
                          and ra.`assignee_id` is not null and ra.version_indicator <='{{version_indicator}}'
                        order by ra.`assignee_id`,
                                 p.`date` desc,
                                 p.`id` desc
                    ) rw) srw
         where rownum = 1) lastknown
         left join `patent`.`location` l on l.`id` = lastknown.`location_id`
         left join `{{reporting_db}}`.`temp_id_mapping_location` tl
                   on tl.`old_location_id` = lastknown.`location_id`;

drop table if exists `{{reporting_db}}`.`temp_assignee_num_patents`;
create table `{{reporting_db}}`.`temp_assignee_num_patents`
(
    `assignee_id` varchar(64)  not null,
    `num_patents` int unsigned not null,
    primary key (`assignee_id`)
)
    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`temp_assignee_num_patents`
    (`assignee_id`, `num_patents`)
select `assignee_id`,
       count(distinct `patent_id`)
from `patent`.`patent_assignee` pa join `patent`.`patent` p on p.id=pa.patent_id where p.version_indicator <='{{version_indicator}}'
group by `assignee_id`;

drop table if exists `{{reporting_db}}`.`temp_assignee_num_inventors`;
create table `{{reporting_db}}`.`temp_assignee_num_inventors`
(
    `assignee_id`   varchar(64)  not null,
    `num_inventors` int unsigned not null,
    primary key (`assignee_id`)
)
    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

insert into `{{reporting_db}}`.`temp_assignee_num_inventors`
    (`assignee_id`, `num_inventors`)
select aa.`assignee_id`,
       count(distinct ii.`inventor_id`)
from `patent`.`patent_assignee` aa
         join `patent`.`patent_inventor` ii on ii.patent_id = aa.patent_id  join `patent`.`patent` p on p.id=aa.patent_id where p.version_indicator <='{{version_indicator}}'
group by aa.`assignee_id`;

drop table if exists `{{reporting_db}}`.`temp_assignee_years_active`;
create table `{{reporting_db}}`.`temp_assignee_years_active`
(
    `assignee_id`         varchar(64)       not null,
    `first_seen_date`     date              null,
    `last_seen_date`      date              null,
    `actual_years_active` smallint unsigned not null,
    primary key (`assignee_id`)
)
    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


# Years active is essentially the number of years difference between first associated patent and last.
insert into `{{reporting_db}}`.`temp_assignee_years_active`
(`assignee_id`, `first_seen_date`, `last_seen_date`, `actual_years_active`)
select pa.`assignee_id`,
       min(p.`date`),
       max(p.`date`),
       ifnull(round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365), 0)
from `patent`.`patent_assignee` pa
         inner join `{{reporting_db}}`.`patent` p on p.`patent_id` = pa.`patent_id`
where p.`date` is not null
group by pa.`assignee_id`;

drop table if exists `{{reporting_db}}`.`patent_assignee`;
create table `{{reporting_db}}`.`patent_assignee`
(
    `patent_id`   varchar(20)       not null,
    `assignee_id` int unsigned      not null,
    `location_id` int unsigned      null,
    `sequence`    smallint unsigned not null,
    primary key (`patent_id`, `assignee_id`),
    unique index ak_patent_assignee (`assignee_id`, `patent_id`)
)
    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`patent_assignee`
(`patent_id`, `assignee_id`, `location_id`, `sequence`)
select distinct pa.`patent_id`,
                t.`new_assignee_id`,
                tl.`new_location_id`,
                ra.`sequence`
from `patent`.`patent_assignee` pa
         inner join `{{reporting_db}}`.`temp_id_mapping_assignee` t
                    on t.`old_assignee_id` = pa.`assignee_id`
         left join (select patent_id, assignee_id, min(sequence) sequence
                    from `patent`.`rawassignee`
                    where assignee_id is not null
                    group by patent_id, assignee_id) t
                   on t.`patent_id` = pa.`patent_id` and t.`assignee_id` = pa.`assignee_id`
         left join `patent`.`rawassignee` ra
                   on ra.`patent_id` = t.`patent_id` and ra.`assignee_id` = t.`assignee_id` and ra.`sequence`
                       = t.`sequence`
         left join `patent`.`rawlocation` rl on rl.`id` = ra.`rawlocation_id`
         left join `{{reporting_db}}`.`temp_id_mapping_location` tl
                   on tl.`old_location_id` = rl.`location_id`;



drop table if exists `{{reporting_db}}`.`location_assignee`;
create table `{{reporting_db}}`.`location_assignee`
(
    `location_id` int unsigned not null,
    `assignee_id` int unsigned not null,
    `num_patents` int unsigned,
    primary key (`location_id`, `assignee_id`)
)
    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`location_assignee`
    (`location_id`, `assignee_id`, `num_patents`)
select distinct timl.`new_location_id`,
                tima.`new_assignee_id`,
                null
from `patent`.`location_assignee` la
         inner join `{{reporting_db}}`.`temp_id_mapping_location` timl
                    on timl.`old_location_id` = la.`location_id`
         inner join `{{reporting_db}}`.`temp_id_mapping_assignee` tima
                    on tima.`old_assignee_id` = la.`assignee_id`;


drop table if exists `{{reporting_db}}`.`assignee`;
create table `{{reporting_db}}`.`assignee`
(
    `assignee_id`                      int unsigned      not null,
    `type`                             varchar(10)       null,
    `name_first`                       varchar(64)       null,
    `name_last`                        varchar(64)       null,
    `organization`                     varchar(256)      null,
    `num_patents`                      int unsigned      not null,
    `num_inventors`                    int unsigned      not null,
    `lastknown_location_id`            int unsigned      null,
    `lastknown_persistent_location_id` varchar(128)      null,
    `lastknown_city`                   varchar(128)      null,
    `lastknown_state`                  varchar(20)       null,
    `lastknown_country`                varchar(10)       null,
    `lastknown_latitude`               float             null,
    `lastknown_longitude`              float             null,
    `first_seen_date`                  date              null,
    `last_seen_date`                   date              null,
    `years_active`                     smallint unsigned not null,
    `persistent_assignee_id`           varchar(64)       not null,
    primary key (`assignee_id`)
)
    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`assignee`
(`assignee_id`, `type`, `name_first`, `name_last`, `organization`,
 `num_patents`, `num_inventors`, `lastknown_location_id`, `lastknown_persistent_location_id`, `lastknown_city`,
 `lastknown_state`, `lastknown_country`, `lastknown_latitude`, `lastknown_longitude`,
 `first_seen_date`, `last_seen_date`, `years_active`, `persistent_assignee_id`)
select t.`new_assignee_id`,
       trim(leading '0' from nullif(trim(a.`type`), '')),
       nullif(trim(a.`name_first`), ''),
       nullif(trim(a.`name_last`), ''),
       nullif(trim(a.`organization`), ''),
       tanp.`num_patents`,
       ifnull(tani.`num_inventors`, 0),
       talkl.`location_id`,
       talkl.`persistent_location_id`,
       talkl.`city`,
       talkl.`state`,
       talkl.`country`,
       talkl.`latitude`,
       talkl.`longitude`,
       tafls.`first_seen_date`,
       tafls.`last_seen_date`,
       ifnull(case when tafls.`actual_years_active` < 1 then 1 else tafls.`actual_years_active` end, 0),
       a.`id`
from `patent`.`assignee` a
         inner join `{{reporting_db}}`.`temp_id_mapping_assignee` t on t.`old_assignee_id` = a.`id`
         left outer join `{{reporting_db}}`.`temp_assignee_lastknown_location` talkl
                         on talkl.`assignee_id` = a.`id`
         inner join `{{reporting_db}}`.`temp_assignee_num_patents` tanp on tanp.`assignee_id` = a.`id`
         left outer join `{{reporting_db}}`.`temp_assignee_years_active` tafls
                         on tafls.`assignee_id` = a.`id`
         left outer join `{{reporting_db}}`.`temp_assignee_num_inventors` tani
                         on tani.`assignee_id` = a.`id`;

# BEGIN assignee_year ######################################################################################################################


drop table if exists `{{reporting_db}}`.`assignee_year`;
create table `{{reporting_db}}`.`assignee_year`
(
  `assignee_id` int unsigned not null,
  `patent_year` smallint not null,
  `num_patents` int unsigned not null,
   unique (`assignee_id`, `patent_year`)
)
engine=InnoDB;

insert into `{{reporting_db}}`.`assignee_year`
  (`assignee_id`, `patent_year`, `num_patents`)
select
  pa.assignee_id, p.year, count(distinct pa.patent_id)
from
  `{{reporting_db}}`.`patent_assignee` pa
  inner join `{{reporting_db}}`.`patent` p using(patent_id)
group by
  pa.assignee_id, p.year;


update
  `{{reporting_db}}`.`location_assignee` la
  inner join
  (
    select
      `location_id`, `assignee_id`, count(distinct `patent_id`) num_patents
    from
      `{{reporting_db}}`.`patent_assignee`
    group by
      `location_id`, `assignee_id`
  ) pa on pa.`location_id` = la.`location_id` and pa.`assignee_id` = la.`assignee_id`
set
  la.`num_patents` = pa.`num_patents`;

alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_first_seen_date` (`first_seen_date`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_last_seen_date` (`last_seen_date`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_lastknown_persistent_location_id` (`lastknown_persistent_location_id`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_lastknown_location_id` (`lastknown_location_id`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_name_first` (`name_first`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_name_last` (`name_last`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_organization` (`organization`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`assignee` add index `ix_assignee_persistent_assignee_id` (`persistent_assignee_id`);

alter table `{{reporting_db}}`.`patent_assignee` add index `ix_patent_assignee_location_id` (`location_id`);
alter table `{{reporting_db}}`.`assignee_year` add index `ix_assignee_year_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`assignee_year` add index `ix_assignee_year_year` (`patent_year`);
alter table `{{reporting_db}}`.`assignee_year` add index `ix_assignee_year_assignee_id` (`assignee_id`);

alter table `{{reporting_db}}`.`location_assignee` add index `ix_location_assignee_assignee_id` (`assignee_id`);
alter table `{{reporting_db}}`.`location_assignee` add index `ix_location_assignee_num_patents` (`num_patents`);

# END assignee

################################################################################################################################################