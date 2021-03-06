
# BEGIN assignee id mapping

###################################################################################################################################


# We need this early for firstnamed stuff.
drop table if exists `{{params.reporting_database}}`.`temp_id_mapping_assignee`;
create table `{{params.reporting_database}}`.`temp_id_mapping_assignee`
(
  `old_assignee_id` varchar(72) not null,
  `new_assignee_id` int unsigned not null auto_increment,
  primary key (`old_assignee_id`),
  unique index `ak_temp_id_mapping_assignee` (`new_assignee_id`)
)
engine=InnoDB;


# There are assignees in the raw data that are not linked to anything so we will take our
# assignee ids from the patent_assignee table to ensure we don't copy any unused assignees over.
# 345,185 @ 0:23
insert into
  `{{params.reporting_database}}`.`temp_id_mapping_assignee` (`old_assignee_id`)
select distinct
  pa.`assignee_id`
from
  `{{params.raw_database}}`.`patent_assignee` pa;


# END assignee id mapping

#####################################################################################################################################


# BEGIN inventor id mapping

###################################################################################################################################


# We need this early for firstnamed stuff.
drop table if exists `{{params.reporting_database}}`.`temp_id_mapping_inventor`;
create table `{{params.reporting_database}}`.`temp_id_mapping_inventor`
(
  `old_inventor_id` varchar(256) not null,
  `new_inventor_id` int unsigned not null auto_increment,
  primary key (`old_inventor_id`),
  unique index `ak_temp_id_mapping_inventor` (`new_inventor_id`)
)
engine=InnoDB;


# There are inventors in the raw data that are not linked to anything so we will take our
# inventor ids from the patent_inventor table to ensure we don't copy any unused inventors over.
# 3,572,763 @ 1:08
insert into
  `{{params.reporting_database}}`.`temp_id_mapping_inventor` (`old_inventor_id`)
select distinct
  `inventor_id`
from
  `{{params.raw_database}}`.`patent_inventor`;


# END inventor id mapping

#####################################################################################################################################


# BEGIN lawyer id mapping

###################################################################################################################################


# We need this early for firstnamed stuff.
drop table if exists `{{params.reporting_database}}`.`temp_id_mapping_lawyer`;
create table `{{params.reporting_database}}`.`temp_id_mapping_lawyer`
(
  `old_lawyer_id` varchar(36) not null,
  `new_lawyer_id` int unsigned not null auto_increment,
  primary key (`old_lawyer_id`),
  unique index `ak_temp_id_mapping_lawyer` (`new_lawyer_id`)
)
engine=InnoDB;


# There are inventors in the raw data that are not linked to anything so we will take our
# lawyer ids from the patent_lawyer table to ensure we don't copy any unused lawyers over.
# 3,572,763 @ 1:08
insert into
  `{{params.reporting_database}}`.`temp_id_mapping_lawyer` (`old_lawyer_id`)
select distinct
  `lawyer_id`
from
  `{{params.raw_database}}`.`patent_lawyer`
  where lawyer_id is not null and lawyer_id !=  '';


# END lawyer id mapping

#####################################################################################################################################


# BEGIN examiner id mapping

###################################################################################################################################


# We need this early for firstnamed stuff.
drop table if exists `{{params.reporting_database}}`.`temp_id_mapping_examiner`;
create table `{{params.reporting_database}}`.`temp_id_mapping_examiner`
(
  `old_examiner_id` varchar(36) not null,
  `new_examiner_id` int unsigned not null auto_increment,
  primary key (`old_examiner_id`),
  unique index `ak_temp_id_mapping_examiner` (`new_examiner_id`)
)
engine=InnoDB;


# There are inventors in the raw data that are not linked to anything so we will take our
# lawyer ids from the patent_lawyer table to ensure we don't copy any unused lawyers over.
# 3,572,763 @ 1:08
insert into
  `{{params.reporting_database}}`.`temp_id_mapping_examiner` (`old_examiner_id`)
select distinct
  `uuid`
from
  `{{params.raw_database}}`.`rawexaminer`;


# END examiner id mapping

#####################################################################################################################################


# BEGIN location id mapping

###################################################################################################################################


# This bit has changed.  Prior to February 2015, there were many locations that were the same but had
# slightly different lat-longs.  As of February, locations that shared a city, state, and country have
# been forced to use the same lag-long.  The algorithm used to determine which lat-long is outside of
# the scope of this discussion.
#
# So how does this affect PatentsView?  Well, for starters, we need to use the new location_update
# table instead of the old location table.  Additionally, we need to use the new
# rawlocation.location_id_transformed column instead of the old rawlocation.location_id column.  The
# problem, though, is that we have denormalized so much location data that these changes affect many,
# many tables.  In an effort to minimize changes to this script and, HOPEFULLY, minimize impact to
# performance, we are going to map new_location_id to location_id_transformed and then map location_id
# to location_id_transformed which will give us a direct pathway from location_id to new_location_id
# rather than having to drag rawlocation into all queries.


drop table if exists `{{params.reporting_database}}`.`temp_id_mapping_location_transformed`;
create table `{{params.reporting_database}}`.`temp_id_mapping_location_transformed`
(
  `old_location_id_transformed` varchar(128) not null,
  `new_location_id` int unsigned not null auto_increment,
  primary key (`old_location_id_transformed`),
  unique index `ak_temp_id_mapping_location_transformed` (`new_location_id`),
  unique index `ak_old_id_mapping_location_transformed` (`old_location_id_transformed`)
)
engine=InnoDB;


# 97,725 @ 0:02
insert into
  `{{params.reporting_database}}`.`temp_id_mapping_location_transformed` (`old_location_id_transformed`)
select distinct
  `location_id_transformed`
from
  `{{params.raw_database}}`.`rawlocation`
where
  `location_id_transformed` is not null and `location_id_transformed` != '' and `location_id_transformed`!='undisambiguated';


drop table if exists `{{params.reporting_database}}`.`temp_id_mapping_location`;
create table `{{params.reporting_database}}`.`temp_id_mapping_location`
(
  `old_location_id` varchar(128) not null,
  `new_location_id` int unsigned not null,
  primary key (`old_location_id`),
  index `ak_temp_id_mapping_location` (`new_location_id`),
  index `ak_old_id_mapping_location` (`old_location_id`)
)
engine=InnoDB;


# 120,449 @ 3:27
insert into
  `{{params.reporting_database}}`.`temp_id_mapping_location` (`old_location_id`, `new_location_id`)
select distinct
  rl.`location_id`,
  t.`new_location_id`
from
  (select distinct location_id, location_id_transformed from `{{params.raw_database}}`.`rawlocation` where location_id != '' and location_id is not null and `location_id_transformed`!='undisambiguated') rl
  inner join `{{params.reporting_database}}`.`temp_id_mapping_location_transformed` t on
    t.`old_location_id_transformed` = rl.`location_id_transformed`;


# END location id mapping

#####################################################################################################################################