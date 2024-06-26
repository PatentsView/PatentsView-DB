{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

# BEGIN location

##############################################################################################################################################


drop table if exists `{{reporting_db}}`.`temp_location_num_assignees`;
create table `{{reporting_db}}`.`temp_location_num_assignees`
(
    `location_id`   int unsigned not null,
    `num_assignees` int unsigned not null,
    primary key (`location_id`)
)
    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`temp_location_num_assignees`
    (`location_id`, `num_assignees`)
select timl.`new_location_id`,
       count(distinct la.`assignee_id`)
from `{{reporting_db}}`.`temp_id_mapping_location` timl
         inner join `patent`.`location_assignee` la on la.`location_id` = timl.`old_location_id`
group by timl.`new_location_id`;


drop table if exists `{{reporting_db}}`.`temp_location_num_inventors`;
create table `{{reporting_db}}`.`temp_location_num_inventors`
(
    `location_id`   int unsigned not null,
    `num_inventors` int unsigned not null,
    primary key (`location_id`)
)
    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


# 94,350 @ 0:50
insert into `{{reporting_db}}`.`temp_location_num_inventors`
    (`location_id`, `num_inventors`)
select timl.`new_location_id`,
       count(distinct li.`inventor_id`)
from `{{reporting_db}}`.`temp_id_mapping_location` timl
         inner join `patent`.`location_inventor` li on li.`location_id` = timl.`old_location_id`
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


drop table if exists `{{reporting_db}}`.`temp_location_patent`;
create table `{{reporting_db}}`.`temp_location_patent`
(
    `location_id` int unsigned not null,
    `patent_id`   varchar(20)  not null
)
    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`temp_location_patent`
    (`location_id`, `patent_id`)
select timl.`new_location_id`,
       ri.`patent_id`
from `{{reporting_db}}`.`temp_id_mapping_location` timl
         inner join `patent`.`rawlocation` rl on rl.`location_id` = timl.`old_location_id`
         inner join `patent`.`rawinventor` ri on ri.`rawlocation_id` = rl.`id`;


insert into `{{reporting_db}}`.`temp_location_patent`
    (`location_id`, `patent_id`)
select timl.`new_location_id`,
       ra.`patent_id`
from `{{reporting_db}}`.`temp_id_mapping_location` timl
         inner join `patent`.`rawlocation` rl on rl.`location_id` = timl.`old_location_id`
         inner join `patent`.`rawassignee` ra on ra.`rawlocation_id` = rl.`id`;


alter table `{{reporting_db}}`.`temp_location_patent`
    add index (`location_id`, `patent_id`);
alter table `{{reporting_db}}`.`temp_location_patent`
    add index (`patent_id`, `location_id`);


drop table if exists `{{reporting_db}}`.`temp_location_num_patents`;
create table `{{reporting_db}}`.`temp_location_num_patents`
(
    `location_id` int unsigned not null,
    `num_patents` int unsigned not null,
    primary key (`location_id`)
)
    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`temp_location_num_patents`
    (`location_id`, `num_patents`)
select `location_id`,
       count(distinct patent_id)
from `{{reporting_db}}`.`temp_location_patent`
group by `location_id`;


drop table if exists `{{reporting_db}}`.`location`;
create table `{{reporting_db}}`.`location`
(
    `location_id`            int unsigned not null,
    `city`                   varchar(256) null,
    `state`                  varchar(256)  null,
    `country`                varchar(256)  null,
    `county`                 varchar(256)  null,
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
    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`location`
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
       timl.`old_location_id_transformed`
from `patent`.`location` l
         inner join `{{reporting_db}}`.`temp_id_mapping_location` timl on timl.`old_location_id` = l.`id`
         left outer join `{{reporting_db}}`.`temp_location_num_assignees` tlna
                         on tlna.`location_id` = timl.`new_location_id`
         left outer join `{{reporting_db}}`.`temp_location_num_inventors` tlni
                         on tlni.`location_id` = timl.`new_location_id`
         left outer join `{{reporting_db}}`.`temp_location_num_patents` tlnp
                         on tlnp.`location_id` = timl.`new_location_id`;


alter table `{{reporting_db}}`.`location` add index `ix_location_state_fips` (`state_fips`);
alter table `{{reporting_db}}`.`location` add index `ix_location_county_fips` (`county_fips`);
alter table `{{reporting_db}}`.`location` add index `ix_location_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`location` add index `ix_location_city` (`city`);
alter table `{{reporting_db}}`.`location` add index `ix_location_country` (`country`);
alter table `{{reporting_db}}`.`location` add index `ix_location_persistent_location_id` (`persistent_location_id`);
alter table `{{reporting_db}}`.`location` add index `ix_location_state` (`state`);
alter table `{{reporting_db}}`.`location` add index `ix_location_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`location` add index `ix_location_num_assignees` (`num_assignees`);

# END location 

################################################################################################################################################