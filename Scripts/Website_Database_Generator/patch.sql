

drop table if exists `patent_20171226`.`temp_location_num_inventors`;
create table `patent_20171226`.`temp_location_num_inventors`
(
  `location_id` int unsigned not null,
  `num_inventors` int unsigned not null,
  primary key (`location_id`)
)
engine=InnoDB;


# 94,350 @ 0:50
insert into `patent_20171226`.`temp_location_num_inventors`
  (`location_id`, `num_inventors`)
select
  timl.`new_location_id`,
  count(distinct li.`inventor_id`)
from
  `patent_20171226`.`temp_id_mapping_location` timl
  inner join `patent_20171226`.`location_inventor` li on li.`location_id` = timl.`old_location_id`
group by
  timl.`new_location_id`;

  
drop table if exists `PatentsView_20171226`.`location`;
create table `PatentsView_20171226`.`location`
(
  `location_id` int unsigned not null,
  `city` varchar(128) null,
  `state` varchar(20) null,
  `country` varchar(10) null,
  `county` varchar(60) null,
  `state_fips` varchar(2) null,
  `county_fips` varchar(3) null,
  `latitude` float null,
  `longitude` float null,
  `num_assignees` int unsigned not null,
  `num_inventors` int unsigned not null,
  `num_patents` int unsigned not null,
  `persistent_location_id` varchar(128) not null,
  primary key (`location_id`)
)
engine=InnoDB;


# 121,477 @ 0:02
insert into `PatentsView_20171226`.`location`
(
  `location_id`, `city`, `state`, `country`, 
  `county`, `state_fips`, `county_fips`,
  `latitude`, `longitude`, `num_assignees`, `num_inventors`,
  `num_patents`, `persistent_location_id`
)
select
  timl.`new_location_id`,
  nullif(trim(l.`city`), ''), nullif(trim(l.`state`), ''), nullif(trim(l.`country`), ''), 
  nullif(trim(l.`county`), ''), nullif(trim(l.`state_fips`), ''), nullif(trim(l.`county_fips`), ''), 
  l.`latitude`, l.`longitude`, ifnull(tlna.`num_assignees`, 0), ifnull(tlni.`num_inventors`, 0),
  ifnull(tlnp.`num_patents`, 0), timlt.`old_location_id_transformed`
from
  `patent_20171226`.`location` l
  inner join `patent_20171226`.`temp_id_mapping_location` timl on timl.`old_location_id` = l.`id`
  left outer join `patent_20171226`.`temp_id_mapping_location_transformed` timlt on timlt.`new_location_id` = timl.`new_location_id`
  left outer join `patent_20171226`.`temp_location_num_assignees` tlna on tlna.`location_id` = timl.`new_location_id`
  left outer join `patent_20171226`.`temp_location_num_inventors` tlni on tlni.`location_id` = timl.`new_location_id`
  left outer join `patent_20171226`.`temp_location_num_patents` tlnp on tlnp.`location_id` = timl.`new_location_id`;

