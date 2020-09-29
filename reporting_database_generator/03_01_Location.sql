# BEGIN location

##############################################################################################################################################


drop table if exists `{{params.reporting_database}}`.`temp_location_num_assignees`;
create table `{{params.reporting_database}}`.`temp_location_num_assignees`
(
    `location_id`   int unsigned not null,
    `num_assignees` int unsigned not null,
    primary key (`location_id`)
)
    engine = InnoDB;


# 34,018 @ 0:02
insert into `{{params.reporting_database}}`.`temp_location_num_assignees`
    (`location_id`, `num_assignees`)
select timl.`new_location_id`,
       count(distinct la.`assignee_id`)
from `{{params.reporting_database}}`.`temp_id_mapping_location` timl
         inner join `{{params.raw_database}}`.`location_assignee` la on la.`location_id` = timl.`old_location_id`
group by timl.`new_location_id`;


drop table if exists `{{params.reporting_database}}`.`temp_location_num_inventors`;
create table `{{params.reporting_database}}`.`temp_location_num_inventors`
(
    `location_id`   int unsigned not null,
    `num_inventors` int unsigned not null,
    primary key (`location_id`)
)
    engine = InnoDB;


# 94,350 @ 0:50
insert into `{{params.reporting_database}}`.`temp_location_num_inventors`
    (`location_id`, `num_inventors`)
select timl.`new_location_id`,
       count(distinct li.`inventor_id`)
from `{{params.reporting_database}}`.`temp_id_mapping_location` timl
         inner join `{{params.raw_database}}`.`location_inventor` li on li.`location_id` = timl.`old_location_id`
group by timl.`new_location_id`;


/*
  So after many, many attempts, the fastest way I found to calculate patents per location was the following:
    1) Remap IDs to integers
    2) Insert location_id and patent_id in a temp table with no primary key
    3) Build a non-unique index on this new table
    4) Run the calculation

  The total run time of this method is in the neighborhood of 18 minutes.  The original "straightforward"
  calculation whereby I ran the query directly against the source data using a "union all" between inventor
  and assignee locations ran well over 2 hours.
*/


drop table if exists `{{params.reporting_database}}`.`temp_location_patent`;
create table `{{params.reporting_database}}`.`temp_location_patent`
(
    `location_id` int unsigned not null,
    `patent_id`   varchar(20)  not null
)
    engine = InnoDB;


# 11,867,513 @ 3:41
insert into `{{params.reporting_database}}`.`temp_location_patent`
    (`location_id`, `patent_id`)
select timl.`new_location_id`,
       ri.`patent_id`
from `{{params.reporting_database}}`.`temp_id_mapping_location` timl
         inner join `{{params.raw_database}}`.`rawlocation` rl on rl.`location_id` = timl.`old_location_id`
         inner join `{{params.raw_database}}`.`rawinventor` ri on ri.`rawlocation_id` = rl.`id`;


# 4,457,955 @ 2:54
insert into `{{params.reporting_database}}`.`temp_location_patent`
    (`location_id`, `patent_id`)
select timl.`new_location_id`,
       ra.`patent_id`
from `{{params.reporting_database}}`.`temp_id_mapping_location` timl
         inner join `{{params.raw_database}}`.`rawlocation` rl on rl.`location_id` = timl.`old_location_id`
         inner join `{{params.raw_database}}`.`rawassignee` ra on ra.`rawlocation_id` = rl.`id`;


# 15:00
alter table `{{params.reporting_database}}`.`temp_location_patent`
    add index (`location_id`, `patent_id`);
alter table `{{params.reporting_database}}`.`temp_location_patent`
    add index (`patent_id`, `location_id`);


drop table if exists `{{params.reporting_database}}`.`temp_location_num_patents`;
create table `{{params.reporting_database}}`.`temp_location_num_patents`
(
    `location_id` int unsigned not null,
    `num_patents` int unsigned not null,
    primary key (`location_id`)
)
    engine = InnoDB;


# 121,475 @ 1:10
insert into `{{params.reporting_database}}`.`temp_location_num_patents`
    (`location_id`, `num_patents`)
select `location_id`,
       count(distinct patent_id)
from `{{params.reporting_database}}`.`temp_location_patent`
group by `location_id`;


drop table if exists `{{params.reporting_database}}`.`location`;
create table `{{params.reporting_database}}`.`location`
(
    `location_id`            int unsigned not null,
    `city`                   varchar(256) null,
    `state`                  varchar(20)  null,
    `country`                varchar(10)  null,
    `county`                 varchar(60)  null,
    `state_fips`             varchar(2)   null,
    `county_fips`            varchar(6)   null,
    `latitude`               float        null,
    `longitude`              float        null,
    `num_assignees`          int unsigned not null,
    `num_inventors`          int unsigned not null,
    `num_patents`            int unsigned not null,
    `persistent_location_id` varchar(128) not null,
    primary key (`location_id`)
)
    engine = InnoDB;


# 121,477 @ 0:02
insert into `{{params.reporting_database}}`.`location`
(`location_id`, `city`, `state`, `country`,
 `county`, `state_fips`, `county_fips`,
 `latitude`, `longitude`, `num_assignees`, `num_inventors`,
 `num_patents`, `persistent_location_id`)
select timl.`new_location_id`,
       nullif(trim(l.`city`), ''),
       nullif(trim(l.`state`), ''),
       nullif(trim(l.`country`), ''),
       nullif(trim(l.`county`), ''),
       nullif(trim(l.`state_fips`), ''),
       nullif(trim(l.`county_fips`), ''),
       l.`latitude`,
       l.`longitude`,
       ifnull(tlna.`num_assignees`, 0),
       ifnull(tlni.`num_inventors`, 0),
       ifnull(tlnp.`num_patents`, 0),
       timlt.`old_location_id_transformed`
from `{{params.raw_database}}`.`location` l
         inner join `{{params.reporting_database}}`.`temp_id_mapping_location` timl on timl.`old_location_id` = l.`id`
         left outer join `{{params.reporting_database}}`.`temp_id_mapping_location_transformed` timlt
                         on timlt.`new_location_id` = timl.`new_location_id`
         left outer join `{{params.reporting_database}}`.`temp_location_num_assignees` tlna
                         on tlna.`location_id` = timl.`new_location_id`
         left outer join `{{params.reporting_database}}`.`temp_location_num_inventors` tlni
                         on tlni.`location_id` = timl.`new_location_id`
         left outer join `{{params.reporting_database}}`.`temp_location_num_patents` tlnp
                         on tlnp.`location_id` = timl.`new_location_id`;


# END location 

################################################################################################################################################