# BEGIN assignee

##############################################################################################################################################

#this is necessary because otherwise the subqueries wierdly have a different collation
SET collation_connection = 'utf8mb4_unicode_ci';

drop table if exists `{{params.reporting_database}}`.`temp_assignee_lastknown_location`;

create table `{{params.reporting_database}}`.`temp_assignee_lastknown_location`
(
    `assignee_id`            varchar(64)  not null,
    `location_id`            int unsigned null,
    `persistent_location_id` varchar(128) null,
    `city`                   varchar(128) null,
    `state`                  varchar(20)  null,
    `country`                varchar(10)  null,
    `latitude`               float        null,
    `longitude`              float        null,
    primary key (`assignee_id`)
)
    engine = InnoDB;


# Populate temp_assignee_lastknown_location table.  The goal here is to grab the location associated
# with the most recent patent associated with the assignee.  It is possible for a patent/assignee
# combination not to have a location, so we will grab the most recent KNOWN location.
# 320,156 @ 3:51
insert into `{{params.reporting_database}}`.`temp_assignee_lastknown_location`
(`assignee_id`, `location_id`, `persistent_location_id`, `city`, `state`, `country`, `latitude`, `longitude`)
select t.`assignee_id`,
       tl.`new_location_id`,
       tl.`old_location_id_transformed`,
       nullif(trim(l.`city`), ''),
       nullif(trim(l.`state`), ''),
       nullif(trim(l.`country`), ''),
       l.`latitude`,
       l.`longitude`
from (
         select t.`assignee_id`,
                t.`location_id`,
                t.`location_id_transformed`
         from (select ROW_NUMBER() OVER (PARTITION BY t.assignee_id ORDER BY t.`date` desc) AS rownum,
                      t.`assignee_id`,
                      t.`location_id`,
                      t.`location_id_transformed`
               from (
                        select ra.`assignee_id`,
                               rl.`location_id`,
                               rl.`location_id_transformed`,
                               p.`date`,
                               p.`id`
                        from `{{params.raw_database}}`.`rawassignee` ra
                                 inner join `{{params.raw_database}}`.`patent` p on p.`id` = ra.`patent_id`
                                 inner join `{{params.raw_database}}`.`rawlocation` rl on rl.`id` = ra.`rawlocation_id`
                        where rl.`location_id_transformed` is not null
                          and ra.`assignee_id` is not null
                        order by ra.`assignee_id`,
                                 p.`date` desc,
                                 p.`id` desc
                    ) t) t
         where rownum = 1) t
         left join `{{params.raw_database}}`.`location` l on l.`id` = t.`location_id`
         left join `{{params.reporting_database}}`.`temp_id_mapping_location_transformed` tl
                   on tl.`old_location_id_transformed` = t.`location_id_transformed`;

drop table if exists `{{params.reporting_database}}`.`temp_assignee_num_patents`;
create table `{{params.reporting_database}}`.`temp_assignee_num_patents`
(
    `assignee_id` varchar(64)  not null,
    `num_patents` int unsigned not null,
    primary key (`assignee_id`)
)
    engine = InnoDB;


insert into `{{params.reporting_database}}`.`temp_assignee_num_patents`
    (`assignee_id`, `num_patents`)
select `assignee_id`,
       count(distinct `patent_id`)
from `{{params.raw_database}}`.`patent_assignee`
group by `assignee_id`;

drop table if exists `{{params.reporting_database}}`.`temp_assignee_num_inventors`;
create table `{{params.reporting_database}}`.`temp_assignee_num_inventors`
(
    `assignee_id`   varchar(64)  not null,
    `num_inventors` int unsigned not null,
    primary key (`assignee_id`)
)
    engine = InnoDB;

# 0:15
insert into `{{params.reporting_database}}`.`temp_assignee_num_inventors`
    (`assignee_id`, `num_inventors`)
select aa.`assignee_id`,
       count(distinct ii.`inventor_id`)
from `{{params.raw_database}}`.`patent_assignee` aa
         join `{{params.raw_database}}`.`patent_inventor` ii on ii.patent_id = aa.patent_id
group by aa.`assignee_id`;

drop table if exists `{{params.reporting_database}}`.`temp_assignee_years_active`;
create table `{{params.reporting_database}}`.`temp_assignee_years_active`
(
    `assignee_id`         varchar(64)       not null,
    `first_seen_date`     date              null,
    `last_seen_date`      date              null,
    `actual_years_active` smallint unsigned not null,
    primary key (`assignee_id`)
)
    engine = InnoDB;


# Years active is essentially the number of years difference between first associated patent and last.
# 1:15
insert into `{{params.reporting_database}}`.`temp_assignee_years_active`
(`assignee_id`, `first_seen_date`, `last_seen_date`, `actual_years_active`)
select pa.`assignee_id`,
       min(p.`date`),
       max(p.`date`),
       ifnull(round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365), 0)
from `{{params.raw_database}}`.`patent_assignee` pa
         inner join `{{params.reporting_database}}`.`patent` p on p.`patent_id` = pa.`patent_id`
where p.`date` is not null
group by pa.`assignee_id`;

drop table if exists `{{params.reporting_database}}`.`patent_assignee`;
create table `{{params.reporting_database}}`.`patent_assignee`
(
    `patent_id`   varchar(20)       not null,
    `assignee_id` int unsigned      not null,
    `location_id` int unsigned      null,
    `sequence`    smallint unsigned not null,
    primary key (`patent_id`, `assignee_id`),
    unique index ak_patent_assignee (`assignee_id`, `patent_id`)
)
    engine = InnoDB;


# 4,825,748 @ 7:20
insert into `{{params.reporting_database}}`.`patent_assignee`
(`patent_id`, `assignee_id`, `location_id`, `sequence`)
select distinct pa.`patent_id`,
                t.`new_assignee_id`,
                tl.`new_location_id`,
                ra.`sequence`
from `{{params.raw_database}}`.`patent_assignee` pa
         inner join `{{params.reporting_database}}`.`temp_id_mapping_assignee` t
                    on t.`old_assignee_id` = pa.`assignee_id`
         left join (select patent_id, assignee_id, min(sequence) sequence
                    from `{{params.raw_database}}`.`rawassignee`
                    where assignee_id is not null
                    group by patent_id, assignee_id) t
                   on t.`patent_id` = pa.`patent_id` and t.`assignee_id` = pa.`assignee_id`
         left join `{{params.raw_database}}`.`rawassignee` ra
                   on ra.`patent_id` = t.`patent_id` and ra.`assignee_id` = t.`assignee_id` and ra.`sequence`
                       = t.`sequence`
         left join `{{params.raw_database}}`.`rawlocation` rl on rl.`id` = ra.`rawlocation_id`
         left join `{{params.reporting_database}}`.`temp_id_mapping_location` tl
                   on tl.`old_location_id` = rl.`location_id`;



drop table if exists `{{params.reporting_database}}`.`location_assignee`;
create table `{{params.reporting_database}}`.`location_assignee`
(
    `location_id` int unsigned not null,
    `assignee_id` int unsigned not null,
    `num_patents` int unsigned,
    primary key (`location_id`, `assignee_id`)
)
    engine = InnoDB;


# 438,452 @ 0:07
insert into `{{params.reporting_database}}`.`location_assignee`
    (`location_id`, `assignee_id`, `num_patents`)
select distinct timl.`new_location_id`,
                tima.`new_assignee_id`,
                null
from `{{params.raw_database}}`.`location_assignee` la
         inner join `{{params.reporting_database}}`.`temp_id_mapping_location_transformed` timl
                    on timl.`old_location_id_transformed` = la.`location_id`
         inner join `{{params.reporting_database}}`.`temp_id_mapping_assignee` tima
                    on tima.`old_assignee_id` = la.`assignee_id`;


drop table if exists `{{params.reporting_database}}`.`assignee`;
create table `{{params.reporting_database}}`.`assignee`
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
    engine = InnoDB;


# 345,185 @ 0:15
insert into `{{params.reporting_database}}`.`assignee`
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
from `{{params.raw_database}}`.`assignee` a
         inner join `{{params.reporting_database}}`.`temp_id_mapping_assignee` t on t.`old_assignee_id` = a.`id`
         left outer join `{{params.reporting_database}}`.`temp_assignee_lastknown_location` talkl
                         on talkl.`assignee_id` = a.`id`
         inner join `{{params.reporting_database}}`.`temp_assignee_num_patents` tanp on tanp.`assignee_id` = a.`id`
         left outer join `{{params.reporting_database}}`.`temp_assignee_years_active` tafls
                         on tafls.`assignee_id` = a.`id`
         left outer join `{{params.reporting_database}}`.`temp_assignee_num_inventors` tani
                         on tani.`assignee_id` = a.`id`;


# END assignee 

################################################################################################################################################