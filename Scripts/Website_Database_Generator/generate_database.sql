# Use these to ease global replace:
#  source database:       `patent_20141215`
#  destination database:  `PatentsView_20141215_dev`


# Figures above each query (N,NNN @ N:NN) are row and time estimates for each query based on server
# settings at the time the query was run and are just used for sanity checking purposes.  Server settings,
# load, data, and a million other things can affect these values.  Take them with a grain of salt.


drop database if exists `PatentsView_20141215_dev`;
create database if not exists `PatentsView_20141215_dev` default character set=utf8 default collate=utf8_general_ci;


# BEGIN assignee id mapping ###################################################################################################################################


# We need this early for firstnamed stuff.
drop table if exists `PatentsView_20141215_dev`.`temp_id_mapping_assignee`;
create table `PatentsView_20141215_dev`.`temp_id_mapping_assignee`
(
  `old_assignee_id` varchar(36) not null,
  `new_assignee_id` int unsigned not null auto_increment,
  primary key (`old_assignee_id`),
  unique index `ak_temp_id_mapping_assignee` (`new_assignee_id`)
)
engine=InnoDB;


# There are assignees in the raw data that are not linked to anything so we will take our
# assignee ids from the patent_assignee table to ensure we don't copy any unused assignees over.
# 345,185 @ 0:23
insert into
  `PatentsView_20141215_dev`.`temp_id_mapping_assignee` (`old_assignee_id`)
select distinct
  pa.`assignee_id`
from
  `patent_20141215`.`patent_assignee` pa;


# END assignee id mapping #####################################################################################################################################


# BEGIN inventor id mapping ###################################################################################################################################


# We need this early for firstnamed stuff.
drop table if exists `PatentsView_20141215_dev`.`temp_id_mapping_inventor`;
create table `PatentsView_20141215_dev`.`temp_id_mapping_inventor`
(
  `old_inventor_id` varchar(36) not null,
  `new_inventor_id` int unsigned not null auto_increment,
  primary key (`old_inventor_id`),
  unique index `ak_temp_id_mapping_inventor` (`new_inventor_id`)
)
engine=InnoDB;


# There are inventors in the raw data that are not linked to anything so we will take our
# inventor ids from the patent_inventor table to ensure we don't copy any unused inventors over.
# 3,572,763 @ 1:08
insert into
  `PatentsView_20141215_dev`.`temp_id_mapping_inventor` (`old_inventor_id`)
select distinct
  `inventor_id`
from
  `patent_20141215`.`patent_inventor`;


# END inventor id mapping #####################################################################################################################################


# BEGIN location id mapping ###################################################################################################################################


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


drop table if exists `PatentsView_20141215_dev`.`temp_id_mapping_location_transformed`;
create table `PatentsView_20141215_dev`.`temp_id_mapping_location_transformed`
(
  `old_location_id_transformed` varchar(128) not null,
  `new_location_id` int unsigned not null auto_increment,
  primary key (`old_location_id_transformed`),
  unique index `ak_temp_id_mapping_location_transformed` (`new_location_id`)
)
engine=InnoDB;


# 97,725 @ 0:02
insert into
  `PatentsView_20141215_dev`.`temp_id_mapping_location_transformed` (`old_location_id_transformed`)
select distinct
  `location_id_transformed`
from
  `patent_20141215`.`rawlocation`
where
  `location_id_transformed` is not null and `location_id_transformed` != '';


drop table if exists `PatentsView_20141215_dev`.`temp_id_mapping_location`;
create table `PatentsView_20141215_dev`.`temp_id_mapping_location`
(
  `old_location_id` varchar(128) not null,
  `new_location_id` int unsigned not null,
  primary key (`old_location_id`),
  index `ak_temp_id_mapping_location` (`new_location_id`)
)
engine=InnoDB;


# 120,449 @ 3:27
insert into
  `PatentsView_20141215_dev`.`temp_id_mapping_location` (`old_location_id`, `new_location_id`)
select distinct
  rl.`location_id`,
  t.`new_location_id`
from
  (select distinct `location_id`, `location_id_transformed` from `patent_20141215`.`rawlocation` where `location_id` is not null and `location_id` != '') rl
  inner join `PatentsView_20141215_dev`.`temp_id_mapping_location_transformed` t on
    t.`old_location_id_transformed` = rl.`location_id_transformed`;


# END location id mapping #####################################################################################################################################


# BEGIN patent ################################################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`temp_patent_firstnamed_assignee`;
create table `PatentsView_20141215_dev`.`temp_patent_firstnamed_assignee`
(
  `patent_id` varchar(20) not null,
  `assignee_id` int unsigned null,
  `persistent_assignee_id` varchar(36) null,
  `location_id` int unsigned null,
  `persistent_location_id` varchar(128) null,
  `city` varchar(128) null,
  `state` varchar(20) null,
  `country` varchar(10) null,
  `latitude` float null,
  `longitude` float null,
  primary key (`patent_id`)
)
engine=InnoDB;


# 4,694,651 @ 2:22
insert into `PatentsView_20141215_dev`.`temp_patent_firstnamed_assignee`
(
  `patent_id`, `assignee_id`, `persistent_assignee_id`, `location_id`,
  `persistent_location_id`, `city`, `state`, `country`, `latitude`, `longitude`
)
select
  p.`id`,
  ta.`new_assignee_id`,
  ta.`old_assignee_id`,
  tl.`new_location_id`,
  tl.`old_location_id`,
  nullif(l.`city`, ''),
  nullif(l.`state`, ''),
  nullif(l.`country`, ''),
  l.`latitude`,
  l.`longitude`
from
  `patent_20141215`.`patent` p
  left outer join `patent_20141215`.`rawassignee` ra on ra.`patent_id` = p.`id` and ra.`sequence` = 0
  left outer join `PatentsView_20141215_dev`.`temp_id_mapping_assignee` ta on ta.`old_assignee_id` = ra.`assignee_id`
  left outer join `patent_20141215`.`rawlocation` rl on rl.`id` = ra.`rawlocation_id`
  left outer join `patent_20141215`.`location_update` l on l.`id` = rl.`location_id_transformed`
  left outer join `PatentsView_20141215_dev`.`temp_id_mapping_location` tl on tl.`old_location_id` = l.`id`
where
  ta.`new_assignee_id` is not null or
  tl.`new_location_id` is not null;


drop table if exists `PatentsView_20141215_dev`.`temp_patent_firstnamed_inventor`;
create table `PatentsView_20141215_dev`.`temp_patent_firstnamed_inventor`
(
  `patent_id` varchar(20) not null,
  `inventor_id` int unsigned null,
  `persistent_inventor_id` varchar(36) null,
  `location_id` int unsigned null,
  `persistent_location_id` varchar(128) null,
  `city` varchar(128) null,
  `state` varchar(20) null,
  `country` varchar(10) null,
  `latitude` float null,
  `longitude` float null,
  primary key (`patent_id`)
)
engine=InnoDB;


# 5,425,008 @ 6:03
insert into `PatentsView_20141215_dev`.`temp_patent_firstnamed_inventor`
(
  `patent_id`, `inventor_id`, `persistent_inventor_id`, `location_id`,
  `persistent_location_id`, `city`, `state`, `country`, `latitude`, `longitude`
)
select
  p.`id`,
  ti.`new_inventor_id`,
  ti.`old_inventor_id`,
  tl.`new_location_id`,
  tl.`old_location_id`,
  nullif(l.`city`, ''),
  nullif(l.`state`, ''),
  nullif(l.`country`, ''),
  l.`latitude`,
  l.`longitude`
from
  `patent_20141215`.`patent` p
  left outer join `patent_20141215`.`rawinventor` ri on ri.`patent_id` = p.`id` and ri.`sequence` = 0
  left outer join `PatentsView_20141215_dev`.`temp_id_mapping_inventor` ti on ti.`old_inventor_id` = ri.`inventor_id`
  left outer join `patent_20141215`.`rawlocation` rl on rl.`id` = ri.`rawlocation_id`
  left outer join `patent_20141215`.`location_update` l on l.`id` = rl.`location_id_transformed`
  left outer join `PatentsView_20141215_dev`.`temp_id_mapping_location` tl on tl.`old_location_id` = l.`id`
where
  ti.`new_inventor_id` is not null or
  tl.`new_location_id` is not null;


drop table if exists `PatentsView_20141215_dev`.`temp_num_foreign_documents_cited`;
create table `PatentsView_20141215_dev`.`temp_num_foreign_documents_cited`
(
  `patent_id` varchar(20) not null,
  `num_foreign_documents_cited` int unsigned not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# The number of foreign documents cited.
# 2,751,072 @ 1:52
insert into `PatentsView_20141215_dev`.`temp_num_foreign_documents_cited`
  (`patent_id`, `num_foreign_documents_cited`)
select
  `patent_id`, count(*)
from
  `patent_20141215`.`foreigncitation`
group by
  `patent_id`;


drop table if exists `PatentsView_20141215_dev`.`temp_num_us_applications_cited`;
create table `PatentsView_20141215_dev`.`temp_num_us_applications_cited`
(
  `patent_id` varchar(20) not null,
  `num_us_applications_cited` int unsigned not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# The number of U.S. patent applications cited.
# 1,534,484 @ 0:21
insert into `PatentsView_20141215_dev`.`temp_num_us_applications_cited`
  (`patent_id`, `num_us_applications_cited`)
select
  `patent_id`, count(*)
from
  `patent_20141215`.`usapplicationcitation`
group by
  `patent_id`;


drop table if exists `PatentsView_20141215_dev`.`temp_num_us_patents_cited`;
create table `PatentsView_20141215_dev`.`temp_num_us_patents_cited`
(
  `patent_id` varchar(20) not null,
  `num_us_patents_cited` int unsigned not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# The number of U.S. patents cited.
# 5,231,893 @ 7:17
insert into `PatentsView_20141215_dev`.`temp_num_us_patents_cited`
  (`patent_id`, `num_us_patents_cited`)
select
  `patent_id`, count(*)
from
  `patent_20141215`.`uspatentcitation`
group by
  `patent_id`;


drop table if exists `PatentsView_20141215_dev`.`temp_num_times_cited_by_us_patents`;
create table `PatentsView_20141215_dev`.`temp_num_times_cited_by_us_patents`
(
  `patent_id` varchar(20) not null,
  `num_times_cited_by_us_patents` int unsigned not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# The number of times a U.S. patent was cited.
# 6,333,277 @ 7:27
insert into `PatentsView_20141215_dev`.`temp_num_times_cited_by_us_patents`
  (`patent_id`, `num_times_cited_by_us_patents`)
select
  `citation_id`, count(*)
from
  `patent_20141215`.`uspatentcitation`
where
  `citation_id` is not null and `citation_id` != ''
group by
  `citation_id`;


drop table if exists `PatentsView_20141215_dev`.`temp_patent_aggregations`;
create table `PatentsView_20141215_dev`.`temp_patent_aggregations`
(
  `patent_id` varchar(20) not null,
  `num_foreign_documents_cited` int unsigned not null,
  `num_us_applications_cited` int unsigned not null,
  `num_us_patents_cited` int unsigned not null,
  `num_total_documents_cited` int unsigned not null,
  `num_times_cited_by_us_patents` int unsigned not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# Combine all of our patent aggregations.
# 5,425,879 @ 2:14
insert into `PatentsView_20141215_dev`.`temp_patent_aggregations`
(
  `patent_id`, `num_foreign_documents_cited`, `num_us_applications_cited`,
  `num_us_patents_cited`, `num_total_documents_cited`, `num_times_cited_by_us_patents`
)
select
  p.`id`,
  ifnull(t1.num_foreign_documents_cited, 0),
  ifnull(t2.num_us_applications_cited, 0),
  ifnull(t3.num_us_patents_cited, 0),
  ifnull(t1.num_foreign_documents_cited, 0) + ifnull(t2.num_us_applications_cited, 0) + ifnull(t3.num_us_patents_cited, 0),
  ifnull(t4.num_times_cited_by_us_patents, 0)
from
  `patent_20141215`.`patent` p
  left outer join `PatentsView_20141215_dev`.`temp_num_foreign_documents_cited` t1 on t1.`patent_id` = p.`id`
  left outer join `PatentsView_20141215_dev`.`temp_num_us_applications_cited` t2 on t2.`patent_id` = p.`id`
  left outer join `PatentsView_20141215_dev`.`temp_num_us_patents_cited` t3 on t3.`patent_id` = p.`id`
  left outer join `PatentsView_20141215_dev`.`temp_num_times_cited_by_us_patents` t4 on t4.`patent_id` = p.`id`;


drop table if exists `PatentsView_20141215_dev`.`temp_patent_earliest_application_date`;
create table `PatentsView_20141215_dev`.`temp_patent_earliest_application_date`
(
  `patent_id` varchar(20) not null,
  `earliest_application_date` date not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# Find the earliest application date for each patent.
# 5,425,837 @ 1:35
insert into `PatentsView_20141215_dev`.`temp_patent_earliest_application_date`
  (`patent_id`, `earliest_application_date`)
select
  a.`patent_id`, min(a.`date`)
from
  `patent_20141215`.`application` a
where
  a.`date` is not null and a.`date` > date('1899-12-31') and a.`date` < date_add(current_date, interval 10 year)
group by
  a.`patent_id`;


drop table if exists `PatentsView_20141215_dev`.`temp_patent_date`;
create table `PatentsView_20141215_dev`.`temp_patent_date`
(
  `patent_id` varchar(20) not null,
  `date` date null,
  primary key (`patent_id`)
)
engine=InnoDB;


# Eliminate obviously bad patent dates.
# 5,425,875 @ 0:37
insert into `PatentsView_20141215_dev`.`temp_patent_date`
  (`patent_id`, `date`)
select
  p.`id`, p.`date`
from
  `patent_20141215`.`patent` p
where
  p.`date` is not null and p.`date` > date('1899-12-31') and p.`date` < date_add(current_date, interval 10 year);


drop table if exists `PatentsView_20141215_dev`.`patent`;
create table `PatentsView_20141215_dev`.`patent`
(
  `patent_id` varchar(20) not null,
  `type` varchar(100) null,
  `number` varchar(64) not null,
  `country` varchar(20) null,
  `date` date null,
  `year` smallint unsigned null,
  `abstract` text null,
  `title` text null,
  `kind` varchar(10) null,
  `num_claims` smallint unsigned null,
  `firstnamed_assignee_id` int unsigned null,
  `firstnamed_assignee_persistent_id` varchar(36) null,
  `firstnamed_assignee_location_id` int unsigned null,
  `firstnamed_assignee_persistent_location_id` varchar(128) null,
  `firstnamed_assignee_city` varchar(128) null,
  `firstnamed_assignee_state` varchar(20) null,
  `firstnamed_assignee_country` varchar(10) null,
  `firstnamed_assignee_latitude` float null,
  `firstnamed_assignee_longitude` float null,
  `firstnamed_inventor_id` int unsigned null,
  `firstnamed_inventor_persistent_id` varchar(36) null,
  `firstnamed_inventor_location_id` int unsigned null,
  `firstnamed_inventor_persistent_location_id` varchar(128) null,
  `firstnamed_inventor_city` varchar(128) null,
  `firstnamed_inventor_state` varchar(20) null,
  `firstnamed_inventor_country` varchar(10) null,
  `firstnamed_inventor_latitude` float null,
  `firstnamed_inventor_longitude` float null,
  `num_foreign_documents_cited` int unsigned not null,
  `num_us_applications_cited` int unsigned not null,
  `num_us_patents_cited` int unsigned not null,
  `num_total_documents_cited` int unsigned not null,
  `num_times_cited_by_us_patents` int unsigned not null,
  `earliest_application_date` date null,
  `patent_processing_days` int unsigned null,
  `uspc_current_mainclass_average_patent_processing_days` int unsigned null, # This will have to be updated once we've calculated the value in the uspc_current section below.
  primary key (`patent_id`)
)
engine=InnoDB;


# 5,425,879 @ 6:45
insert into `PatentsView_20141215_dev`.`patent`
(
  `patent_id`, `type`, `number`, `country`, `date`, `year`,
  `abstract`, `title`, `kind`, `num_claims`,
  `firstnamed_assignee_id`, `firstnamed_assignee_persistent_id`,
  `firstnamed_assignee_location_id`, `firstnamed_assignee_persistent_location_id`,
  `firstnamed_assignee_city`, `firstnamed_assignee_state`,
  `firstnamed_assignee_country`, `firstnamed_assignee_latitude`,
  `firstnamed_assignee_longitude`, `firstnamed_inventor_id`,
  `firstnamed_inventor_persistent_id`, `firstnamed_inventor_location_id`,
  `firstnamed_inventor_persistent_location_id`, `firstnamed_inventor_city`,
  `firstnamed_inventor_state`, `firstnamed_inventor_country`,
  `firstnamed_inventor_latitude`, `firstnamed_inventor_longitude`,
  `num_foreign_documents_cited`, `num_us_applications_cited`,
  `num_us_patents_cited`, `num_total_documents_cited`,
  `num_times_cited_by_us_patents`,
  `earliest_application_date`, `patent_processing_days`
)
select
  p.`id`, case when ifnull(p.`type`, '') = 'sir' then 'statutory invention registration' else nullif(trim(p.`type`), '') end,
  `number`, nullif(trim(p.`country`), ''), tpd.`date`, year(tpd.`date`),
  nullif(trim(p.`abstract`), ''), nullif(trim(p.`title`), ''), nullif(trim(p.`kind`), ''), p.`num_claims`,
  tpfna.`assignee_id`, tpfna.`persistent_assignee_id`, tpfna.`location_id`,
  tpfna.`persistent_location_id`, tpfna.`city`,
  tpfna.`state`, tpfna.`country`, tpfna.`latitude`, tpfna.`longitude`,
  tpfni.`inventor_id`, tpfni.`persistent_inventor_id`, tpfni.`location_id`,
  tpfni.`persistent_location_id`, tpfni.`city`,
  tpfni.`state`, tpfni.`country`, tpfni.`latitude`, tpfni.`longitude`,
  tpa.`num_foreign_documents_cited`, tpa.`num_us_applications_cited`,
  tpa.`num_us_patents_cited`, tpa.`num_total_documents_cited`,
  tpa.`num_times_cited_by_us_patents`,
  tpead.`earliest_application_date`,
  case when tpead.`earliest_application_date` <= p.`date` then timestampdiff(day, tpead.`earliest_application_date`, tpd.`date`) else null end
from
  `patent_20141215`.`patent` p
  left outer join `PatentsView_20141215_dev`.`temp_patent_date` tpd on tpd.`patent_id` = p.`id`
  left outer join `PatentsView_20141215_dev`.`temp_patent_firstnamed_assignee` tpfna on tpfna.`patent_id` = p.`id`
  left outer join `PatentsView_20141215_dev`.`temp_patent_firstnamed_inventor` tpfni on tpfni.`patent_id` = p.`id`
  left outer join `PatentsView_20141215_dev`.`temp_patent_aggregations` tpa on tpa.`patent_id` = p.`id`
  left outer join `PatentsView_20141215_dev`.`temp_patent_earliest_application_date` tpead on tpead.`patent_id` = p.`id`;


# END patent ##################################################################################################################################################


# BEGIN application ###########################################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`application`;
create table `PatentsView_20141215_dev`.`application`
(
  `application_id` varchar(36) not null,
  `patent_id` varchar(20) not null,
  `type` varchar(20) null,
  `number` varchar(64) null,
  `country` varchar(20) null,
  `date` date null,
  primary key (`application_id`, `patent_id`)
)
engine=InnoDB;


# 5,425,879 @ 1:11
insert into `PatentsView_20141215_dev`.`application`
  (`application_id`, `patent_id`, `type`, `number`, `country`, `date`)
select
  `id_transformed`, `patent_id`, nullif(trim(`type`), ''),
  nullif(trim(`number_transformed`), ''), nullif(trim(`country`), ''),
  case when `date` > date('1899-12-31') and `date` < date_add(current_date, interval 10 year) then `date` else null end
from
  `patent_20141215`.`application`;


# END application #############################################################################################################################################


# BEGIN location ##############################################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`temp_location_num_assignees`;
create table `PatentsView_20141215_dev`.`temp_location_num_assignees`
(
  `location_id` int unsigned not null,
  `num_assignees` int unsigned not null,
  primary key (`location_id`)
)
engine=InnoDB;


# 34,018 @ 0:02
insert into `PatentsView_20141215_dev`.`temp_location_num_assignees`
  (`location_id`, `num_assignees`)
select
  timl.`new_location_id`,
  count(distinct la.`assignee_id`)
from
  `PatentsView_20141215_dev`.`temp_id_mapping_location` timl
  inner join `patent_20141215`.`location_assignee` la on la.`location_id` = timl.`old_location_id`
group by
  timl.`new_location_id`;


drop table if exists `PatentsView_20141215_dev`.`temp_location_num_inventors`;
create table `PatentsView_20141215_dev`.`temp_location_num_inventors`
(
  `location_id` int unsigned not null,
  `num_inventors` int unsigned not null,
  primary key (`location_id`)
)
engine=InnoDB;


# 94,350 @ 0:50
insert into `PatentsView_20141215_dev`.`temp_location_num_inventors`
  (`location_id`, `num_inventors`)
select
  timl.`new_location_id`,
  count(distinct li.`inventor_id`)
from
  `PatentsView_20141215_dev`.`temp_id_mapping_location` timl
  inner join `patent_20141215`.`location_inventor` li on li.`location_id` = timl.`old_location_id`
group by
  timl.`new_location_id`;


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


drop table if exists `PatentsView_20141215_dev`.`temp_location_patent`;
create table `PatentsView_20141215_dev`.`temp_location_patent`
(
  `location_id` int unsigned not null,
  `patent_id` varchar(20) not null
)
engine=InnoDB;


# 24,294,018 @ 3:12
insert into `PatentsView_20141215_dev`.`temp_location_patent`
  (`location_id`, `patent_id`)
select
  timl.`new_location_id`,
  pii.`patent_id`
from
  `PatentsView_20141215_dev`.`temp_id_mapping_location` timl
  inner join `patent_20141215`.`location_inventor` li on li.`location_id` = timl.`old_location_id`
  inner join `patent_20141215`.`patent_inventor` pii on pii.`inventor_id` = li.`inventor_id`;


# 50,566,007 @ 5:33
insert into `PatentsView_20141215_dev`.`temp_location_patent`
  (`location_id`, `patent_id`)
select
  timl.`new_location_id`,
  pa.`patent_id`
from
  `PatentsView_20141215_dev`.`temp_id_mapping_location` timl
  inner join `patent_20141215`.`location_assignee` la on la.`location_id` = timl.`old_location_id`
  inner join `patent_20141215`.`patent_assignee` pa on pa.`assignee_id` = la.`assignee_id`;


# 7:28
alter table `PatentsView_20141215_dev`.`temp_location_patent` add index (`location_id`, `patent_id`);


drop table if exists `PatentsView_20141215_dev`.`temp_location_num_patents`;
create table `PatentsView_20141215_dev`.`temp_location_num_patents`
(
  `location_id` int unsigned not null,
  `num_patents` int unsigned not null,
  primary key (`location_id`)
)
engine=InnoDB;


# 121,475 @ 1:10
insert into `PatentsView_20141215_dev`.`temp_location_num_patents`
  (`location_id`, `num_patents`)
select
  `location_id`,
  count(distinct patent_id)
from
  `PatentsView_20141215_dev`.`temp_location_patent`
group by
  `location_id`;


drop table if exists `PatentsView_20141215_dev`.`location`;
create table `PatentsView_20141215_dev`.`location`
(
  `location_id` int unsigned not null,
  `city` varchar(128) null,
  `state` varchar(20) null,
  `country` varchar(10) null,
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
insert into `PatentsView_20141215_dev`.`location`
(
  `location_id`, `city`, `state`, `country`,
  `latitude`, `longitude`, `num_assignees`, `num_inventors`,
  `num_patents`, `persistent_location_id`
)
select
  timl.`new_location_id`,
  nullif(trim(l.`city`), ''), nullif(trim(l.`state`), ''), nullif(trim(l.`country`), ''),
  l.`latitude`, l.`longitude`, ifnull(tlna.`num_assignees`, 0), ifnull(tlni.`num_inventors`, 0),
  ifnull(tlnp.`num_patents`, 0), l.`id`
from
  `patent_20141215`.`location_update` l
  inner join `PatentsView_20141215_dev`.`temp_id_mapping_location` timl on timl.`old_location_id` = l.`id`
  left outer join `PatentsView_20141215_dev`.`temp_location_num_assignees` tlna on tlna.`location_id` = timl.`new_location_id`
  left outer join `PatentsView_20141215_dev`.`temp_location_num_inventors` tlni on tlni.`location_id` = timl.`new_location_id`
  left outer join `PatentsView_20141215_dev`.`temp_location_num_patents` tlnp on tlnp.`location_id` = timl.`new_location_id`;


# END location ################################################################################################################################################


# BEGIN assignee ##############################################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`temp_assignee_lastknown_location`;
create table `PatentsView_20141215_dev`.`temp_assignee_lastknown_location`
(
  `assignee_id` varchar(36) not null,
  `location_id` int unsigned null,
  `persistent_location_id` varchar(128) null,
  `city` varchar(128) null,
  `state` varchar(20) null,
  `country` varchar(10) null,
  `latitude` float null,
  `longitude` float null,
  primary key (`assignee_id`)
)
engine=InnoDB;


# Populate temp_assignee_lastknown_location table.  The goal here is to grab the location associated
# with the most recent patent associated with the assignee.  It is possible for a patent/assignee
# combination not to have a location, so we will grab the most recent KNOWN location.
# 320,156 @ 3:51
insert into `PatentsView_20141215_dev`.`temp_assignee_lastknown_location`
(
  `assignee_id`, `location_id`, `persistent_location_id`, `city`, `state`, `country`, `latitude`, `longitude`
)
select
  t.`assignee_id`,
  tl.`new_location_id`,
  tl.`old_location_id`,
  nullif(trim(l.`city`), ''),
  nullif(trim(l.`state`), ''),
  nullif(trim(l.`country`), ''),
  l.`latitude`,
  l.`longitude`
from
  (
    select
      t.`assignee_id`,
      t.`location_id_transformed`
    from
      (
        select
          @rownum := case when @assignee_id = t.`assignee_id` then @rownum + 1 else 1 end `rownum`,
          @assignee_id := t.`assignee_id` `assignee_id`,
          t.`location_id_transformed`
        from
          (
            select
              ra.`assignee_id`,
              rl.`location_id_transformed`
            from
              `patent_20141215`.`rawassignee` ra
              inner join `patent_20141215`.`patent` p on p.`id` = ra.`patent_id`
              inner join `patent_20141215`.`rawlocation` rl on rl.`id` = ra.`rawlocation_id`
            where
              rl.`location_id_transformed` is not null and
              ra.`assignee_id` is not null
            order by
              ra.`assignee_id`,
              p.`date` desc,
              p.`id` desc
          ) t,
          (select @rownum := 0, @assignee_id := '') r
      ) t
    where
      t.`rownum` < 2
  ) t
  left outer join `patent_20141215`.`location_update` l on l.`id` = t.`location_id_transformed`
  left outer join `PatentsView_20141215_dev`.`temp_id_mapping_location` tl on tl.`old_location_id` = t.`location_id_transformed`;


drop table if exists `PatentsView_20141215_dev`.`temp_assignee_num_patents`;
create table `PatentsView_20141215_dev`.`temp_assignee_num_patents`
(
  `assignee_id` varchar(36) not null,
  `num_patents` int unsigned not null,
  primary key (`assignee_id`)
)
engine=InnoDB;


# 0:47
insert into `PatentsView_20141215_dev`.`temp_assignee_num_patents`
(
  `assignee_id`, `num_patents`
)
select
  `assignee_id`,
  count(distinct `patent_id`)
from
  `patent_20141215`.`patent_assignee`
group by
  `assignee_id`;


drop table if exists `PatentsView_20141215_dev`.`temp_assignee_years_active`;
create table `PatentsView_20141215_dev`.`temp_assignee_years_active`
(
  `assignee_id` varchar(36) not null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `actual_years_active` smallint unsigned not null,
  primary key (`assignee_id`)
)
engine=InnoDB;


# Years active is essentially the number of years difference between first associated patent and last.
# 1:15
insert into `PatentsView_20141215_dev`.`temp_assignee_years_active`
  (`assignee_id`, `first_seen_date`, `last_seen_date`, `actual_years_active`)
select
  pa.`assignee_id`, min(p.`date`), max(p.`date`),
  ifnull(round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365), 0)
from
  `patent_20141215`.`patent_assignee` pa
  inner join `PatentsView_20141215_dev`.`patent` p on p.`patent_id`= pa.`patent_id`
where
  p.`date` is not null
group by
  pa.`assignee_id`;


drop table if exists `PatentsView_20141215_dev`.`patent_assignee`;
create table `PatentsView_20141215_dev`.`patent_assignee`
(
  `patent_id` varchar(20) not null,
  `assignee_id` int unsigned not null,
  `location_id` int unsigned null,
  `sequence` smallint unsigned not null,
  primary key (`patent_id`, `assignee_id`),
  unique index ak_patent_assignee (`assignee_id`, `patent_id`)
)
engine=InnoDB;


# 4,825,748 @ 7:20
insert into `PatentsView_20141215_dev`.`patent_assignee`
(
  `patent_id`, `assignee_id`, `location_id`, `sequence`
)
select distinct
  pa.`patent_id`, t.`new_assignee_id`, tl.`new_location_id`, ra.`sequence`
from
  `patent_20141215`.`patent_assignee` pa
  inner join `PatentsView_20141215_dev`.`temp_id_mapping_assignee` t on t.`old_assignee_id` = pa.`assignee_id`
  left outer join (select patent_id, assignee_id, min(sequence) sequence from `patent_20141215`.`rawassignee` group by patent_id, assignee_id) t on t.`patent_id` = pa.`patent_id` and t.`assignee_id` = pa.`assignee_id`
  left outer join `patent_20141215`.`rawassignee` ra on ra.`patent_id` = t.`patent_id` and ra.`assignee_id` = t.`assignee_id` and ra.`sequence` = t.`sequence`
  left outer join `patent_20141215`.`rawlocation` rl on rl.`id` = ra.`rawlocation_id`
  left outer join `PatentsView_20141215_dev`.`temp_id_mapping_location` tl on tl.`old_location_id` = rl.`location_id`;


drop table if exists `PatentsView_20141215_dev`.`location_assignee`;
create table `PatentsView_20141215_dev`.`location_assignee`
(
  `location_id` int unsigned not null,
  `assignee_id` int unsigned not null,
  `num_patents` int unsigned,
  primary key (`location_id`, `assignee_id`)
)
engine=InnoDB;


# 438,452 @ 0:07
insert into `PatentsView_20141215_dev`.`location_assignee`
  (`location_id`, `assignee_id`, `num_patents`)
select distinct
  timl.`new_location_id`,
  tima.`new_assignee_id`,
  null
from
  `patent_20141215`.`location_assignee` la
  inner join `PatentsView_20141215_dev`.`temp_id_mapping_location` timl on timl.`old_location_id` = la.`location_id`
  inner join `PatentsView_20141215_dev`.`temp_id_mapping_assignee` tima on tima.`old_assignee_id` = la.`assignee_id`;


drop table if exists `PatentsView_20141215_dev`.`assignee`;
create table `PatentsView_20141215_dev`.`assignee`
(
  `assignee_id` int unsigned not null,
  `type` varchar(10) null,
  `name_first` varchar(64) null,
  `name_last` varchar(64) null,
  `organization` varchar(256) null,
  `num_patents` int unsigned not null,
  `lastknown_location_id` int unsigned null,
  `lastknown_persistent_location_id` varchar(128) null,
  `lastknown_city` varchar(128) null,
  `lastknown_state` varchar(20) null,
  `lastknown_country` varchar(10) null,
  `lastknown_latitude` float null,
  `lastknown_longitude` float null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `years_active` smallint unsigned not null,
  `persistent_assignee_id` varchar(36) not null,
  primary key (`assignee_id`)
)
engine=InnoDB;


# 345,185 @ 0:15
insert into `PatentsView_20141215_dev`.`assignee`
(
  `assignee_id`, `type`, `name_first`, `name_last`, `organization`,
  `num_patents`, `lastknown_location_id`, `lastknown_persistent_location_id`, `lastknown_city`,
  `lastknown_state`, `lastknown_country`, `lastknown_latitude`, `lastknown_longitude`,
  `first_seen_date`, `last_seen_date`, `years_active`, `persistent_assignee_id`
)
select
  t.`new_assignee_id`, trim(leading '0' from nullif(trim(a.`type`), '')), nullif(trim(a.`name_first`), ''),
  nullif(trim(a.`name_last`), ''), nullif(trim(a.`organization`), ''),
  tanp.`num_patents`, talkl.`location_id`, talkl.`persistent_location_id`, talkl.`city`, talkl.`state`,
  talkl.`country`, talkl.`latitude`, talkl.`longitude`,
  tafls.`first_seen_date`, tafls.`last_seen_date`,
  ifnull(case when tafls.`actual_years_active` < 1 then 1 else tafls.`actual_years_active` end, 0),
  a.`id`
from
  `patent_20141215`.`assignee` a
  inner join `PatentsView_20141215_dev`.`temp_id_mapping_assignee` t on t.`old_assignee_id` = a.`id`
  left outer join `PatentsView_20141215_dev`.`temp_assignee_lastknown_location` talkl on talkl.`assignee_id` = a.`id`
  inner join `PatentsView_20141215_dev`.`temp_assignee_num_patents` tanp on tanp.`assignee_id` = a.`id`
  left outer join `PatentsView_20141215_dev`.`temp_assignee_years_active` tafls on tafls.`assignee_id` = a.`id`;


# END assignee ################################################################################################################################################


# BEGIN inventor ##############################################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`temp_inventor_lastknown_location`;
create table `PatentsView_20141215_dev`.`temp_inventor_lastknown_location`
(
  `inventor_id` varchar(36) not null,
  `location_id` int unsigned null,
  `persistent_location_id` varchar(128) null,
  `city` varchar(128) null,
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
insert into `PatentsView_20141215_dev`.`temp_inventor_lastknown_location`
(
  `inventor_id`, `location_id`, `persistent_location_id`, `city`, `state`, `country`, `latitude`, `longitude`
)
select
  t.`inventor_id`,
  tl.`new_location_id`,
  tl.`old_location_id`,
  nullif(trim(l.`city`), ''),
  nullif(trim(l.`state`), ''),
  nullif(trim(l.`country`), ''),
  l.`latitude`,
  l.`longitude`
from
  (
    select
      t.`inventor_id`,
      t.`location_id_transformed`
    from
      (
        select
          @rownum := case when @inventor_id = t.`inventor_id` then @rownum + 1 else 1 end `rownum`,
          @inventor_id := t.`inventor_id` `inventor_id`,
          t.`location_id_transformed`
        from
          (
            select
              ri.`inventor_id`,
              rl.`location_id_transformed`
            from
              `patent_20141215`.`rawinventor` ri
              inner join `patent_20141215`.`patent` p on p.`id` = ri.`patent_id`
              inner join `patent_20141215`.`rawlocation` rl on rl.`id` = ri.`rawlocation_id`
            where
              ri.`inventor_id` is not null and
              rl.`location_id_transformed` is not null
            order by
              ri.`inventor_id`,
              p.`date` desc,
              p.`id` desc
          ) t,
          (select @rownum := 0, @inventor_id := '') r
      ) t
    where
      t.`rownum` < 2
  ) t
  left outer join `patent_20141215`.`location` l on l.`id` = t.`location_id_transformed`
  left outer join `PatentsView_20141215_dev`.`temp_id_mapping_location` tl on tl.`old_location_id` = t.`location_id_transformed`;


drop table if exists `PatentsView_20141215_dev`.`temp_inventor_num_patents`;
create table `PatentsView_20141215_dev`.`temp_inventor_num_patents`
(
  `inventor_id` varchar(36) not null,
  `num_patents` int unsigned not null,
  primary key (`inventor_id`)
)
engine=InnoDB;


# 2:06
insert into `PatentsView_20141215_dev`.`temp_inventor_num_patents`
  (`inventor_id`, `num_patents`)
select
  `inventor_id`, count(distinct `patent_id`)
from
  `patent_20141215`.`patent_inventor`
group by
  `inventor_id`;


drop table if exists `PatentsView_20141215_dev`.`temp_inventor_years_active`;
create table `PatentsView_20141215_dev`.`temp_inventor_years_active`
(
  `inventor_id` varchar(36) not null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `actual_years_active` smallint unsigned not null,
  primary key (`inventor_id`)
)
engine=InnoDB;


# 5:42
insert into `PatentsView_20141215_dev`.`temp_inventor_years_active`
  (`inventor_id`, `first_seen_date`, `last_seen_date`, `actual_years_active`)
select
  pa.`inventor_id`, min(p.`date`), max(p.`date`),
  ifnull(round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365), 0)
from
  `patent_20141215`.`patent_inventor` pa
  inner join `PatentsView_20141215_dev`.`patent` p on p.`patent_id`= pa.`patent_id`
where
  p.`date` is not null
group by
  pa.`inventor_id`;


drop table if exists `PatentsView_20141215_dev`.`patent_inventor`;
create table `PatentsView_20141215_dev`.`patent_inventor`
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
insert into `PatentsView_20141215_dev`.`patent_inventor`
(
  `patent_id`, `inventor_id`, `location_id`, `sequence`
)
select distinct
  pii.`patent_id`, t.`new_inventor_id`, tl.`new_location_id`, ri.`sequence`
from
  `patent_20141215`.`patent_inventor` pii
  inner join `PatentsView_20141215_dev`.`temp_id_mapping_inventor` t on t.`old_inventor_id` = pii.`inventor_id`
  left outer join (select patent_id, inventor_id, min(sequence) sequence from `patent_20141215`.`rawinventor` group by patent_id, inventor_id) t on t.`patent_id` = pii.`patent_id` and t.`inventor_id` = pii.`inventor_id`
  left outer join `patent_20141215`.`rawinventor` ri on ri.`patent_id` = t.`patent_id` and ri.`inventor_id` = t.`inventor_id` and ri.`sequence` = t.`sequence`
  left outer join `patent_20141215`.`rawlocation` rl on rl.`id` = ri.`rawlocation_id`
  left outer join `PatentsView_20141215_dev`.`temp_id_mapping_location` tl on tl.`old_location_id` = rl.`location_id`;


drop table if exists `PatentsView_20141215_dev`.`location_inventor`;
create table `PatentsView_20141215_dev`.`location_inventor`
(
  `location_id` int unsigned not null,
  `inventor_id` int unsigned not null,
  `num_patents` int unsigned,
  primary key (`location_id`, `inventor_id`)
)
engine=InnoDB;


# 4,188,507 @ 0:50
insert into `PatentsView_20141215_dev`.`location_inventor`
  (`location_id`, `inventor_id`, `num_patents`)
select distinct
  timl.`new_location_id`,
  timi.`new_inventor_id`,
  null
from
  `patent_20141215`.`location_inventor` la
  inner join `PatentsView_20141215_dev`.`temp_id_mapping_location` timl on timl.`old_location_id` = la.`location_id`
  inner join `PatentsView_20141215_dev`.`temp_id_mapping_inventor` timi on timi.`old_inventor_id` = la.`inventor_id`;


drop table if exists `PatentsView_20141215_dev`.`inventor`;
create table `PatentsView_20141215_dev`.`inventor`
(
  `inventor_id` int unsigned not null,
  `name_first` varchar(64) null,
  `name_last` varchar(64) null,
  `num_patents` int unsigned not null,
  `lastknown_location_id` int unsigned null,
  `lastknown_persistent_location_id` varchar(128) null,
  `lastknown_city` varchar(128) null,
  `lastknown_state` varchar(20) null,
  `lastknown_country` varchar(10) null,
  `lastknown_latitude` float null,
  `lastknown_longitude` float null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `years_active` smallint unsigned not null,
  `persistent_inventor_id` varchar(36) not null,
  primary key (`inventor_id`)
)
engine=InnoDB;


# 3,572,763 @ 1:57
insert into `PatentsView_20141215_dev`.`inventor`
(
  `inventor_id`, `name_first`, `name_last`, `num_patents`,
  `lastknown_location_id`, `lastknown_persistent_location_id`, `lastknown_city`,
  `lastknown_state`, `lastknown_country`, `lastknown_latitude`, `lastknown_longitude`,
  `first_seen_date`, `last_seen_date`, `years_active`, `persistent_inventor_id`
)
select
  t.`new_inventor_id`, nullif(trim(i.`name_first`), ''), nullif(trim(i.`name_last`), ''),
  tinp.`num_patents`, tilkl.`location_id`, tilkl.`persistent_location_id`, tilkl.`city`, tilkl.`state`,
  tilkl.`country`, tilkl.`latitude`, tilkl.`longitude`, tifls.`first_seen_date`, tifls.`last_seen_date`,
  ifnull(case when tifls.`actual_years_active` < 1 then 1 else tifls.`actual_years_active` end, 0),
  i.`id`
from
  `patent_20141215`.`inventor` i
  inner join `PatentsView_20141215_dev`.`temp_id_mapping_inventor` t on t.`old_inventor_id` = i.`id`
  left outer join `PatentsView_20141215_dev`.`temp_inventor_lastknown_location` tilkl on tilkl.`inventor_id` = i.`id`
  inner join `PatentsView_20141215_dev`.`temp_inventor_num_patents` tinp on tinp.`inventor_id` = i.`id`
  left outer join `PatentsView_20141215_dev`.`temp_inventor_years_active` tifls on tifls.`inventor_id` = i.`id`;


# END inventor ################################################################################################################################################


# BEGIN usapplicationcitation #################################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`usapplicationcitation`;
create table `PatentsView_20141215_dev`.`usapplicationcitation`
(
  `citing_patent_id` varchar(20) not null,
  `sequence` int not null,
  `cited_application_id` varchar(20) null,
  `date` date null,
  `name` varchar(64) null,
  `kind` varchar(10) null,
  `category` varchar(20) null,
  primary key (`citing_patent_id`, `sequence`)
)
engine=InnoDB;


# 13,617,656 @ 8:22
insert into `PatentsView_20141215_dev`.`usapplicationcitation`
(
  `citing_patent_id`, `sequence`, `cited_application_id`,
  `date`, `name`, `kind`, `category`
)
select
  ac.`patent_id`, ac.`sequence`, ac.`application_id_transformed`,
  case when ac.`date` > date('1899-12-31') and ac.`date` < date_add(current_date, interval 10 year) then ac.`date` else null end,
  nullif(trim(ac.`name`), ''),
  nullif(trim(ac.`kind`), ''),
  nullif(trim(ac.`category`), '')
from
  `PatentsView_20141215_dev`.`patent` p
  inner join `patent_20141215`.`usapplicationcitation` ac on ac.`patent_id` = p.`patent_id`;


# END usapplicationcitation ###################################################################################################################################


# BEGIN uspatentcitation ######################################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`uspatentcitation`;
create table `PatentsView_20141215_dev`.`uspatentcitation`
(
  `citing_patent_id` varchar(20) not null,
  `sequence` int not null,
  `cited_patent_id` varchar(20) null,
  `category` varchar(20) null,
  primary key (`citing_patent_id`, `sequence`)
)
engine=InnoDB;


# 71,126,097 @ 32:52
insert into `PatentsView_20141215_dev`.`uspatentcitation`
  (`citing_patent_id`, `sequence`, `cited_patent_id`, `category`)
select
  pc.`patent_id`, pc.`sequence`, nullif(trim(pc.`citation_id`), ''), nullif(trim(pc.`category`), '')
from
  `PatentsView_20141215_dev`.`patent` p
  inner join `patent_20141215`.`uspatentcitation` pc on pc.`patent_id` = p.`patent_id`;


# END uspatentcitation ########################################################################################################################################


# BEGIN cpc_current ###########################################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`temp_cpc_current_subsection_aggregate_counts`;
create table `PatentsView_20141215_dev`.`temp_cpc_current_subsection_aggregate_counts`
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
insert into `PatentsView_20141215_dev`.`temp_cpc_current_subsection_aggregate_counts`
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
  `patent_20141215`.`cpc_current` c
  left outer join `patent_20141215`.`patent_assignee` pa on pa.`patent_id` = c.`patent_id`
  left outer join `patent_20141215`.`patent_inventor` pii on pii.`patent_id` = c.`patent_id`
  left outer join `PatentsView_20141215_dev`.`patent` p on p.`patent_id` = c.`patent_id`
group by
  c.`subsection_id`;


drop table if exists `PatentsView_20141215_dev`.`temp_cpc_subsection_title`;
create table `PatentsView_20141215_dev`.`temp_cpc_subsection_title`
(
  `id` varchar(20) not null,
  `title` varchar(256) null,
  primary key (`id`)
)
engine=InnoDB;


# 0.125 sec
insert into `PatentsView_20141215_dev`.`temp_cpc_subsection_title`
  (`id`, `title`)
select
  `id`,
  case when binary replace(`title`, 'e.g.', 'E.G.') = binary ucase(`title`)
    then concat(ucase(substring(trim(`title`), 1, 1)), lcase(substring(trim(nullif(`title`, '')), 2)))
    else `title`
  end
from
  `patent_20141215`.`cpc_subsection`;


drop table if exists `PatentsView_20141215_dev`.`temp_cpc_group_title`;
create table `PatentsView_20141215_dev`.`temp_cpc_group_title`
(
  `id` varchar(20) not null,
  `title` varchar(256) null,
  primary key (`id`)
)
engine=InnoDB;


# 0.156
insert into `PatentsView_20141215_dev`.`temp_cpc_group_title`
  (`id`, `title`)
select
  `id`,
  case when binary replace(`title`, 'e.g.', 'E.G.') = binary ucase(`title`)
    then concat(ucase(substring(trim(`title`), 1, 1)), lcase(substring(trim(nullif(`title`, '')), 2)))
    else `title`
  end
from
  `patent_20141215`.`cpc_group`;


drop table if exists `PatentsView_20141215_dev`.`temp_cpc_subgroup_title`;
create table `PatentsView_20141215_dev`.`temp_cpc_subgroup_title`
(
  `id` varchar(20) not null,
  `title` varchar(512) null,
  primary key (`id`)
)
engine=InnoDB;


# 0:07
insert into `PatentsView_20141215_dev`.`temp_cpc_subgroup_title`
  (`id`, `title`)
select
  `id`,
  case when binary replace(`title`, 'e.g.', 'E.G.') = binary ucase(`title`)
    then concat(ucase(substring(trim(`title`), 1, 1)), lcase(substring(trim(nullif(`title`, '')), 2)))
    else `title`
  end
from
  `patent_20141215`.`cpc_subgroup`;


drop table if exists `PatentsView_20141215_dev`.`cpc_current`;
create table `PatentsView_20141215_dev`.`cpc_current`
(
  `patent_id` varchar(20) not null,
  `sequence` int unsigned not null,
  `section_id` varchar(10) null,
  `subsection_id` varchar(20) null,
  `subsection_title` varchar(512) null,
  `group_id` varchar(20) null,
  `group_title` varchar(256) null,
  `subgroup_id` varchar(20) null,
  `subgroup_title` varchar(512) null,
  `category` varchar(36) null,
  `num_assignees` int unsigned null,
  `num_inventors` int unsigned null,
  `num_patents` int unsigned null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `years_active` smallint unsigned null,
  primary key (`patent_id`, `sequence`)
)
engine=InnoDB;


# 23,151,381 @ 1:29:48
# 23,151,381 @ 36:32
insert into `PatentsView_20141215_dev`.`cpc_current`
(
  `patent_id`, `sequence`, `section_id`, `subsection_id`,
  `subsection_title`, `group_id`, `group_title`, `subgroup_id`,
  `subgroup_title`, `category`,
  `num_assignees`, `num_inventors`, `num_patents`,
  `first_seen_date`, `last_seen_date`, `years_active`
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
  case when tccsac.`actual_years_active` < 1 then 1 else tccsac.`actual_years_active` end
from
  `PatentsView_20141215_dev`.`patent` p
  inner join `patent_20141215`.`cpc_current` c on p.`patent_id` = c.`patent_id`
  left outer join `PatentsView_20141215_dev`.`temp_cpc_subsection_title` s on s.`id` = c.`subsection_id`
  left outer join `PatentsView_20141215_dev`.`temp_cpc_group_title` g on g.`id` = c.`group_id`
  left outer join `PatentsView_20141215_dev`.`temp_cpc_subgroup_title` sg on sg.`id` = c.`subgroup_id`
  left outer join `PatentsView_20141215_dev`.`temp_cpc_current_subsection_aggregate_counts` tccsac on tccsac.`subsection_id` = c.`subsection_id`;


drop table if exists `PatentsView_20141215_dev`.`cpc_current_subsection`;
create table `PatentsView_20141215_dev`.`cpc_current_subsection`
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
insert into `PatentsView_20141215_dev`.`cpc_current_subsection`
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
  (select distinct `patent_id`, `section_id`, `subsection_id` from `PatentsView_20141215_dev`.`cpc_current`) c
  left outer join `PatentsView_20141215_dev`.`temp_cpc_subsection_title` s on s.`id` = c.`subsection_id`
  left outer join `PatentsView_20141215_dev`.`temp_cpc_current_subsection_aggregate_counts` tccsac on tccsac.`subsection_id` = c.`subsection_id`;


# END cpc_current #############################################################################################################################################


# BEGIN cpc_current_subsection_patent_year ####################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`cpc_current_subsection_patent_year`;
create table `PatentsView_20141215_dev`.`cpc_current_subsection_patent_year`
(
  `subsection_id` varchar(20) not null,
  `patent_year` smallint unsigned not null,
  `num_patents` int unsigned not null,
  primary key (`subsection_id`, `patent_year`)
)
engine=InnoDB;


# 13:24
insert into `PatentsView_20141215_dev`.`cpc_current_subsection_patent_year`
  (`subsection_id`, `patent_year`, `num_patents`)
select
  c.`subsection_id`, year(p.`date`), count(distinct c.`patent_id`)
from
  `patent_20141215`.`cpc_current` c
  inner join `PatentsView_20141215_dev`.`patent` p on p.`patent_id` = c.`patent_id` and p.`date` is not null
where
  c.`subsection_id` is not null and c.`subsection_id` != ''
group by
  c.`subsection_id`, year(p.`date`);


# END cpc_current_subsection_patent_year ######################################################################################################################


# BEGIN ipcr ##################################################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`temp_ipcr_aggregations`;
create table `PatentsView_20141215_dev`.`temp_ipcr_aggregations`
(
  `section` varchar(20) null,
  `ipc_class` varchar(20) null,
  `subclass` varchar(20) null,
  `num_assignees` int unsigned not null,
  `num_inventors` int unsigned not null,
  primary key (`section`, `ipc_class`, `subclass`)
)
engine=InnoDB;


# 11:53
insert into `PatentsView_20141215_dev`.`temp_ipcr_aggregations`
  (`section`, `ipc_class`, `subclass`, `num_assignees`, `num_inventors`)
select
  i.`section`, i.`ipc_class`, i.`subclass`,
  count(distinct pa.`assignee_id`),
  count(distinct pii.`inventor_id`)
from
  `patent_20141215`.`ipcr` i
  left outer join `patent_20141215`.`patent_assignee` pa on pa.`patent_id` = i.`patent_id`
  left outer join `patent_20141215`.`patent_inventor` pii on pii.`patent_id` = i.`patent_id`
group by
  i.`section`, i.`ipc_class`, i.`subclass`;


drop table if exists `PatentsView_20141215_dev`.`temp_ipcr_years_active`;
create table `PatentsView_20141215_dev`.`temp_ipcr_years_active`
(
  `section` varchar(20) null,
  `ipc_class` varchar(20) null,
  `subclass` varchar(20) null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `actual_years_active` smallint unsigned not null,
  primary key (`section`, `ipc_class`, `subclass`)
)
engine=InnoDB;


# 2:17
insert into `PatentsView_20141215_dev`.`temp_ipcr_years_active`
(
  `section`, `ipc_class`, `subclass`, `first_seen_date`,
  `last_seen_date`, `actual_years_active`
)
select
  i.`section`, i.`ipc_class`, i.`subclass`,
  min(p.`date`), max(p.`date`),
  ifnull(round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365), 0)
from
  `patent_20141215`.`ipcr` i
  inner join `PatentsView_20141215_dev`.`patent` p on p.`patent_id`= i.`patent_id`
where
  p.`date` is not null
group by
  i.`section`, i.`ipc_class`, i.`subclass`;


drop table if exists `PatentsView_20141215_dev`.`ipcr`;
create table `PatentsView_20141215_dev`.`ipcr`
(
  `patent_id` varchar(20) not null,
  `sequence` int not null,
  `section` varchar(20) null,
  `ipc_class` varchar(20) null,
  `subclass` varchar(20) null,
  `main_group` varchar(20) null,
  `subgroup` varchar(20) null,
  `symbol_position` varchar(20) null,
  `classification_value` varchar(20) null,
  `classification_data_source` varchar(20) null,
  `action_date` date null,
  `ipc_version_indicator` date null,
  `num_assignees` int unsigned null,
  `num_inventors` int unsigned null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `years_active` smallint unsigned null,
  primary key (`patent_id`, `sequence`)
)
engine=InnoDB;


# 7,702,885 @ 6:38
insert into `PatentsView_20141215_dev`.`ipcr`
(
  `patent_id`, `sequence`, `section`, `ipc_class`, `subclass`, `main_group`, `subgroup`,
  `symbol_position`, `classification_value`, `classification_data_source`,
  `action_date`, `ipc_version_indicator`, `num_assignees`, `num_inventors`,
  `first_seen_date`, `last_seen_date`, `years_active`
)
select
  p.`patent_id`, i.`sequence`, nullif(trim(i.`section`), ''), nullif(trim(i.`ipc_class`), ''), nullif(trim(i.`subclass`), ''),
  nullif(trim(i.`main_group`), ''), nullif(trim(i.`subgroup`), ''), nullif(trim(i.`symbol_position`), ''),
  nullif(trim(i.`classification_value`), ''), nullif(trim(i.`classification_data_source`), ''),
  case when `action_date` > date('1899-12-31') and `action_date` < date_add(current_date, interval 10 year) then `action_date` else null end,
  case when `ipc_version_indicator` > date('1899-12-31') and `ipc_version_indicator` < date_add(current_date, interval 10 year) then `ipc_version_indicator` else null end,
  tia.`num_assignees`, tia.`num_inventors`, tiya.`first_seen_date`, tiya.`last_seen_date`,
  ifnull(case when tiya.`actual_years_active` < 1 then 1 else tiya.`actual_years_active` end, 0)
from
  `PatentsView_20141215_dev`.`patent` p
  inner join `patent_20141215`.`ipcr` i on i.`patent_id` = p.`patent_id`
  left outer join `PatentsView_20141215_dev`.`temp_ipcr_aggregations` tia on tia.`section` = i.`section` and tia.`ipc_class` = i.`ipc_class` and tia.`subclass` = i.`subclass`
  left outer join `PatentsView_20141215_dev`.`temp_ipcr_years_active` tiya on tiya.`section` = i.`section` and tiya.`ipc_class` = i.`ipc_class` and tiya.`subclass` = i.`subclass`;


# END ipcr ####################################################################################################################################################


# BEGIN nber ##################################################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`temp_nber_subcategory_aggregate_counts`;
create table `PatentsView_20141215_dev`.`temp_nber_subcategory_aggregate_counts`
(
  `subcategory_id` varchar(20) not null,
  `num_assignees` int unsigned not null,
  `num_inventors` int unsigned not null,
  `num_patents` int unsigned not null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `actual_years_active` smallint unsigned not null,
  primary key (`subcategory_id`)
)
engine=InnoDB;


# 38 @ 4:45
insert into `PatentsView_20141215_dev`.`temp_nber_subcategory_aggregate_counts`
(
  `subcategory_id`, `num_assignees`, `num_inventors`, `num_patents`,
  `first_seen_date`, `last_seen_date`, `actual_years_active`
)
select
  n.`subcategory_id`,
  count(distinct pa.`assignee_id`) num_assignees,
  count(distinct pii.`inventor_id`) num_inventors,
  count(distinct n.`patent_id`) num_patents,
  min(p.`date`), max(p.`date`),
  ifnull(round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365), 0)
from
  `patent_20141215`.`nber` n
  left outer join `patent_20141215`.`patent_assignee` pa on pa.`patent_id` = n.`patent_id`
  left outer join `patent_20141215`.`patent_inventor` pii on pii.`patent_id` = n.`patent_id`
  left outer join `PatentsView_20141215_dev`.`patent` p on p.`patent_id` = n.`patent_id`
group by
  n.`subcategory_id`;


drop table if exists `PatentsView_20141215_dev`.`nber`;
create table `PatentsView_20141215_dev`.`nber`
(
  `patent_id` varchar(20) not null,
  `category_id` varchar(20) null,
  `category_title` varchar(512) null,
  `subcategory_id` varchar(20) null,
  `subcategory_title` varchar(512) null,
  `num_assignees` int unsigned null,
  `num_inventors` int unsigned null,
  `num_patents` int unsigned null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `years_active` smallint unsigned null,
  primary key (`patent_id`)
)
engine=InnoDB;


# 4,927,287 @ 1:47
insert into `PatentsView_20141215_dev`.`nber`
(
  `patent_id`, `category_id`, `category_title`, `subcategory_id`,
  `subcategory_title`,
  `num_assignees`, `num_inventors`, `num_patents`,
  `first_seen_date`, `last_seen_date`, `years_active`
)
select
  p.`patent_id`,
  nullif(trim(n.`category_id`), ''),
  nullif(trim(c.`title`), ''),
  nullif(trim(n.`subcategory_id`), ''),
  nullif(trim(s.`title`), ''),
  tnsac.`num_assignees`, tnsac.`num_inventors`, tnsac.`num_patents`,
  tnsac.`first_seen_date`, tnsac.`last_seen_date`,
  case when tnsac.`actual_years_active` < 1 then 1 else tnsac.`actual_years_active` end
from
  `PatentsView_20141215_dev`.`patent` p
  inner join `patent_20141215`.`nber` n on p.`patent_id` = n.`patent_id`
  left outer join `patent_20141215`.`nber_category` c on c.`id` = n.`category_id`
  left outer join `patent_20141215`.`nber_subcategory` s on s.`id` = n.`subcategory_id`
  left outer join `PatentsView_20141215_dev`.`temp_nber_subcategory_aggregate_counts` tnsac on tnsac.`subcategory_id` = n.`subcategory_id`;


# END nber ####################################################################################################################################################


# BEGIN nber_subcategory_patent_year ##########################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`nber_subcategory_patent_year`;
create table `PatentsView_20141215_dev`.`nber_subcategory_patent_year`
(
  `subcategory_id` varchar(20) not null,
  `patent_year` smallint unsigned not null,
  `num_patents` int unsigned not null,
  primary key (`subcategory_id`, `patent_year`)
)
engine=InnoDB;


# 1,483 @ 1:01
insert into `PatentsView_20141215_dev`.`nber_subcategory_patent_year`
  (`subcategory_id`, `patent_year`, `num_patents`)
select
  n.`subcategory_id`, year(p.`date`), count(distinct n.`patent_id`)
from
  `patent_20141215`.`nber` n
  inner join `PatentsView_20141215_dev`.`patent` p on p.`patent_id` = n.`patent_id` and p.`date` is not null
where
  n.`subcategory_id` is not null and n.`subcategory_id` != ''
group by
  n.`subcategory_id`, year(p.`date`);


# END nber_subcategory_patent_year ############################################################################################################################


# BEGIN uspc_current ##########################################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`temp_mainclass_current_aggregate_counts`;
create table `PatentsView_20141215_dev`.`temp_mainclass_current_aggregate_counts`
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
engine=InnoDB;


# 24:52
insert into `PatentsView_20141215_dev`.`temp_mainclass_current_aggregate_counts`
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
  `patent_20141215`.`uspc_current` u
  left outer join `patent_20141215`.`patent_assignee` pa on pa.`patent_id` = u.`patent_id`
  left outer join `patent_20141215`.`patent_inventor` pii on pii.`patent_id` = u.`patent_id`
  left outer join `PatentsView_20141215_dev`.`patent` p on p.`patent_id` = u.`patent_id` and p.`date` is not null
where
  u.`mainclass_id` is not null and u.`mainclass_id` != ''
group by
  u.`mainclass_id`;


drop table if exists `PatentsView_20141215_dev`.`temp_mainclass_current_title`;
create table `PatentsView_20141215_dev`.`temp_mainclass_current_title`
(
  `id` varchar(20) not null,
  `title` varchar(512) null,
  primary key (`id`)
)
engine=InnoDB;


# "Fix" casing where necessary.
# 0.125 sec
insert into `PatentsView_20141215_dev`.`temp_mainclass_current_title`
  (`id`, `title`)
select
  `id`,
  case when binary replace(`title`, 'e.g.', 'E.G.') = binary ucase(`title`)
    then concat(ucase(substring(trim(`title`), 1, 1)), lcase(substring(trim(nullif(`title`, '')), 2)))
    else `title`
  end
from
  `patent_20141215`.`mainclass_current`;


# Fix casing of subclass_current.
drop table if exists `PatentsView_20141215_dev`.`temp_subclass_current_title`;
create table `PatentsView_20141215_dev`.`temp_subclass_current_title`
(
  `id` varchar(20) not null,
  `title` varchar(512) null,
  primary key (`id`)
)
engine=InnoDB;


# "Fix" casing where necessary.
# 1.719 sec
insert into `PatentsView_20141215_dev`.`temp_subclass_current_title`
  (`id`, `title`)
select
  `id`,
  case when binary replace(`title`, 'e.g.', 'E.G.') = binary ucase(`title`)
    then concat(ucase(substring(trim(`title`), 1, 1)), lcase(substring(trim(nullif(`title`, '')), 2)))
    else `title`
  end
from
  `patent_20141215`.`subclass_current`;


drop table if exists `PatentsView_20141215_dev`.`uspc_current`;
create table `PatentsView_20141215_dev`.`uspc_current`
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
engine=InnoDB;


# 21,191,230 @ 16:54
# 21,175,812 @ 1:02:06
# 21,175,812 @ 11:36
insert into `PatentsView_20141215_dev`.`uspc_current`
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
  `PatentsView_20141215_dev`.`patent` p
  inner join `patent_20141215`.`uspc_current` u on u.`patent_id` = p.`patent_id`
  left outer join `PatentsView_20141215_dev`.`temp_mainclass_current_title` m on m.`id` = u.`mainclass_id`
  left outer join `PatentsView_20141215_dev`.`temp_subclass_current_title` s on s.`id` = u.`subclass_id`
  left outer join `PatentsView_20141215_dev`.`temp_mainclass_current_aggregate_counts` tmcac on tmcac.`mainclass_id` = u.`mainclass_id`;


drop table if exists `PatentsView_20141215_dev`.`uspc_current_mainclass`;
create table `PatentsView_20141215_dev`.`uspc_current_mainclass`
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
engine=InnoDB;


# 9,054,003 @ 9:27
insert into `PatentsView_20141215_dev`.`uspc_current_mainclass`
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
  (select distinct `patent_id`, `mainclass_id` from `PatentsView_20141215_dev`.`uspc_current`) u
  left outer join `PatentsView_20141215_dev`.`temp_mainclass_current_title` m on m.`id` = u.`mainclass_id`
  left outer join `PatentsView_20141215_dev`.`temp_mainclass_current_aggregate_counts` tmcac on tmcac.`mainclass_id` = u.`mainclass_id`;


# END uspc_current ############################################################################################################################################


# BEGIN uspc_current_mainclass_application_year ###############################################################################################################


drop table if exists `PatentsView_20141215_dev`.`uspc_current_mainclass_application_year`;
create table `PatentsView_20141215_dev`.`uspc_current_mainclass_application_year`
(
  `mainclass_id` varchar(20) not null,
  `application_year` smallint unsigned not null,
  `sample_size` int unsigned not null,
  `average_patent_processing_days` int unsigned null,
  primary key (`mainclass_id`, `application_year`)
)
engine=InnoDB;


# 20,241 @ 0:56
insert into `PatentsView_20141215_dev`.`uspc_current_mainclass_application_year`
  (`mainclass_id`, `application_year`, `sample_size`, `average_patent_processing_days`)
select
  u.`mainclass_id`,
  year(p.`earliest_application_date`),
  count(*),
  round(avg(p.`patent_processing_days`))
from
  `PatentsView_20141215_dev`.`patent` p
  inner join `PatentsView_20141215_dev`.`uspc_current` u on u.`patent_id` = p.`patent_id`
where
  p.`patent_processing_days` is not null and u.`sequence` = 0
group by
  u.`mainclass_id`, year(p.`earliest_application_date`);


# 5,406,673 @ 32:45
# Update the patent with the average mainclass processing days.
update
  `PatentsView_20141215_dev`.`patent` p
  inner join `PatentsView_20141215_dev`.`uspc_current` u on
    u.`patent_id` = p.`patent_id` and u.`sequence` = 0
  inner join `PatentsView_20141215_dev`.`uspc_current_mainclass_application_year` c on
    c.`mainclass_id` = u.`mainclass_id` and c.`application_year` = year(p.`earliest_application_date`)
set
  p.`uspc_current_mainclass_average_patent_processing_days` = c.`average_patent_processing_days`;


# END uspc_current_mainclass_application_year #################################################################################################################


# BEGIN uspc_current_mainclass_patent_year ####################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`uspc_current_mainclass_patent_year`;
create table `PatentsView_20141215_dev`.`uspc_current_mainclass_patent_year`
(
  `mainclass_id` varchar(20) not null,
  `patent_year` smallint unsigned not null,
  `num_patents` int unsigned not null,
  primary key (`mainclass_id`, `patent_year`)
)
engine=InnoDB;


# 18,316 @ 12:56
insert into `PatentsView_20141215_dev`.`uspc_current_mainclass_patent_year`
  (`mainclass_id`, `patent_year`, `num_patents`)
select
  u.`mainclass_id`, year(p.`date`), count(distinct u.`patent_id`)
from
  `patent_20141215`.`uspc_current` u
  inner join `PatentsView_20141215_dev`.`patent` p on p.`patent_id` = u.`patent_id` and p.`date` is not null
where
  u.`mainclass_id` is not null and u.`mainclass_id` != ''
group by
  u.`mainclass_id`, year(p.`date`);


# END uspc_current_mainclass_patent_year ######################################################################################################################


# BEGIN assignee_inventor ######################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`assignee_inventor`;
create table `PatentsView_20141215_dev`.`assignee_inventor`
(
  `assignee_id` int unsigned not null,
  `inventor_id` int unsigned not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;


# 4,352,502 @ 1:52
insert into `PatentsView_20141215_dev`.`assignee_inventor`
  (`assignee_id`, `inventor_id`, `num_patents`)
select
  pa.assignee_id, pi.inventor_id, count(distinct pa.patent_id)
from
  `PatentsView_20141215_dev`.`patent_assignee` pa
  inner join `PatentsView_20141215_dev`.`patent_inventor` pi using(patent_id)
group by
  pa.assignee_id, pi.inventor_id;


# END assignee_inventor ######################################################################################################################


# BEGIN inventor_coinventor ######################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`inventor_coinventor`;
create table `PatentsView_20141215_dev`.`inventor_coinventor`
(
  `inventor_id` int unsigned not null,
  `coinventor_id` int unsigned not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;

# 16,742,248 @ 11:55
insert into `PatentsView_20141215_dev`.`inventor_coinventor`
  (`inventor_id`, `coinventor_id`, `num_patents`)
select
  pi.inventor_id, copi.inventor_id, count(distinct copi.patent_id)
from
  `PatentsView_20141215_dev`.`patent_inventor` pi
  inner join `PatentsView_20141215_dev`.`patent_inventor` copi on pi.patent_id=copi.patent_id and pi.inventor_id<>copi.inventor_id
group by
  pi.inventor_id, copi.inventor_id;


# END inventor_coinventor ######################################################################################################################


# BEGIN inventor_cpc_subsection ######################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`inventor_cpc_subsection`;
create table `PatentsView_20141215_dev`.`inventor_cpc_subsection`
(
  `inventor_id` int unsigned not null,
  `subsection_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;


# 7,171,415 @ 11:55
insert into `PatentsView_20141215_dev`.`inventor_cpc_subsection`
  (`inventor_id`, `subsection_id`, `num_patents`)
select
  pi.inventor_id, c.subsection_id, count(distinct c.patent_id)
from
  `PatentsView_20141215_dev`.`patent_inventor` pi
  inner join `PatentsView_20141215_dev`.`cpc_current_subsection` c using(patent_id)
where
  c.subsection_id is not null and c.subsection_id != ''
group by
  pi.inventor_id, c.subsection_id;


# END inventor_cpc_subsection ######################################################################################################################


# BEGIN inventor_nber_subcategory ######################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`inventor_nber_subcategory`;
create table `PatentsView_20141215_dev`.`inventor_nber_subcategory`
(
  `inventor_id` int unsigned not null,
  `subcategory_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;

#
insert into `PatentsView_20141215_dev`.`inventor_nber_subcategory`
  (`inventor_id`, `subcategory_id`, `num_patents`)
select
  pi.inventor_id, n.subcategory_id, count(distinct n.patent_id)
from
  `PatentsView_20141215_dev`.`nber` n
  inner join `PatentsView_20141215_dev`.`patent_inventor` pi using(patent_id)
where
  n.subcategory_id is not null and n.subcategory_id != ''
group by
  pi.inventor_id, n.subcategory_id;


# END inventor_nber_subcategory ######################################################################################################################


# BEGIN inventor_uspc_mainclass ######################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`inventor_uspc_mainclass`;
create table `PatentsView_20141215_dev`.`inventor_uspc_mainclass`
(
  `inventor_id` int unsigned not null,
  `mainclass_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;

# 10,350,577 @ 14:44
insert into `PatentsView_20141215_dev`.`inventor_uspc_mainclass`
  (`inventor_id`, `mainclass_id`, `num_patents`)
select
  pi.inventor_id, u.mainclass_id, count(distinct pi.patent_id)
from
  `PatentsView_20141215_dev`.`patent_inventor` pi
  inner join `PatentsView_20141215_dev`.`uspc_current_mainclass` u on pi.patent_id=u.patent_id
group by
  pi.inventor_id, u.mainclass_id;


# END inventor_uspc_mainclass ######################################################################################################################


# BEGIN inventor_year ######################################################################################################################

drop table if exists `PatentsView_20141215_dev`.`inventor_year`;
create table `PatentsView_20141215_dev`.`inventor_year`
(
  `inventor_id` int unsigned not null,
  `patent_year` smallint not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;

# 8,140,017 @ 2:19
insert into `PatentsView_20141215_dev`.`inventor_year`
(`inventor_id`, `patent_year`, `num_patents`)
select
  pi.inventor_id, p.year, count(distinct pi.patent_id)
from
  `PatentsView_20141215_dev`.`patent_inventor` pi
  inner join `PatentsView_20141215_dev`.`patent` p using(patent_id)
group by
  pi.inventor_id, p.year;


# END inventor_year ######################################################################################################################


# BEGIN assignee_cpc_subsection ######################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`assignee_cpc_subsection`;
create table `PatentsView_20141215_dev`.`assignee_cpc_subsection`
(
  `assignee_id` int unsigned not null,
  `subsection_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;


# 933,903 @ 2:22
insert into `PatentsView_20141215_dev`.`assignee_cpc_subsection`
  (`assignee_id`, `subsection_id`, `num_patents`)
select
  pa.assignee_id, c.subsection_id, count(distinct c.patent_id)
from
  `PatentsView_20141215_dev`.`patent_assignee` pa
  inner join `PatentsView_20141215_dev`.`cpc_current_subsection` c using(patent_id)
where
  c.subsection_id is not null and c.subsection_id != ''
group by
  pa.assignee_id, c.subsection_id;


# END assignee_cpc_subsection ######################################################################################################################


# BEGIN assignee_nber_subcategory ######################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`assignee_nber_subcategory`;
create table `PatentsView_20141215_dev`.`assignee_nber_subcategory`
(
  `assignee_id` int unsigned not null,
  `subcategory_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;

# 618,873 @ 0:48
insert into `PatentsView_20141215_dev`.`assignee_nber_subcategory`
  (`assignee_id`, `subcategory_id`, `num_patents`)
select
  pa.assignee_id, n.subcategory_id, count(distinct n.patent_id)
from
  `PatentsView_20141215_dev`.`patent_assignee` pa
  inner join `PatentsView_20141215_dev`.`nber` n using(patent_id)
where
  n.subcategory_id is not null and n.subcategory_id != ''
group by
  pa.assignee_id, n.subcategory_id;


# END assignee_nber_subcategory ######################################################################################################################


# BEGIN assignee_uspc_mainclass ######################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`assignee_uspc_mainclass`;
create table `PatentsView_20141215_dev`.`assignee_uspc_mainclass`
(
  `assignee_id` int unsigned not null,
  `mainclass_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;

# 1,534,644 @ 3:30
insert into `PatentsView_20141215_dev`.`assignee_uspc_mainclass`
  (`assignee_id`, `mainclass_id`, `num_patents`)
select
  pa.assignee_id, u.mainclass_id, count(distinct pa.patent_id)
from
  `PatentsView_20141215_dev`.`patent_assignee` pa
  inner join `PatentsView_20141215_dev`.`uspc_current_mainclass` u on pa.patent_id=u.patent_id
group by
  pa.assignee_id, u.mainclass_id;


# END assignee_uspc_mainclass ######################################################################################################################


# BEGIN assignee_year ######################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`assignee_year`;
create table `PatentsView_20141215_dev`.`assignee_year`
(
  `assignee_id` int unsigned not null,
  `patent_year` smallint not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;

# 931,856 @ 2:00
insert into `PatentsView_20141215_dev`.`assignee_year`
  (`assignee_id`, `patent_year`, `num_patents`)
select
  pa.assignee_id, p.year, count(distinct pa.patent_id)
from
  `PatentsView_20141215_dev`.`patent_assignee` pa
  inner join `PatentsView_20141215_dev`.`patent` p using(patent_id)
group by
  pa.assignee_id, p.year;


# END assignee_year ######################################################################################################################


# BEGIN location_assignee update num_patents ###################################################################################################################################


# @2:33
update location_assignee
  set num_patents=(select count(distinct patent_id) from patent_assignee where location_assignee.location_id=patent_assignee.location_id and location_assignee.assignee_id=patent_assignee.assignee_id);


# END location_assignee update num_patents ###################################################################################################################################


# BEGIN location_inventor update num_patents ###################################################################################################################################


update location_inventor
  set num_patents=(select count(distinct patent_id) from patent_inventor where location_inventor.location_id=patent_inventor.location_id and location_inventor.inventor_id=patent_inventor.inventor_id);


# END location_assignee update num_patents ###################################################################################################################################


# BEGIN location_cpc_subsection ######################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`location_cpc_subsection`;
create table `PatentsView_20141215_dev`.`location_cpc_subsection`
(
  `location_id` int unsigned not null,
  `subsection_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;

# 1,141,331 @ 10:42
insert into `PatentsView_20141215_dev`.`location_cpc_subsection`
  (`location_id`, `subsection_id`, `num_patents`)
select
  pi.location_id, cpc.subsection_id, count(distinct pi.patent_id)
from
  `PatentsView_20141215_dev`.`patent_inventor` pi
  inner join `PatentsView_20141215_dev`.`cpc_current_subsection` cpc using(patent_id)
where
  location_id is not null
group by
  pi.location_id, cpc.subsection_id;


# END location_cpc_subsection ######################################################################################################################


# BEGIN location_uspc_mainclass ######################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`location_uspc_mainclass`;
create table `PatentsView_20141215_dev`.`location_uspc_mainclass`
(
  `location_id` int unsigned not null,
  `mainclass_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;

# 2,360,512 @ 14:35
insert into `PatentsView_20141215_dev`.`location_uspc_mainclass`
  (`location_id`, `mainclass_id`, `num_patents`)
select
  pi.location_id, uspc.mainclass_id, count(distinct pi.patent_id)
from
  `PatentsView_20141215_dev`.`patent_inventor` pi
  inner join `PatentsView_20141215_dev`.`uspc_current_mainclass` uspc using(patent_id)
where
  location_id is not null
group by
  pi.location_id, uspc.mainclass_id;


# END location_uspc_mainclass ######################################################################################################################


# BEGIN location_year ######################################################################################################################


drop table if exists `PatentsView_20141215_dev`.`location_year`;
create table `PatentsView_20141215_dev`.`location_year`
(
  `location_id` int unsigned not null,
  `year` smallint not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;

# 867,942 @ 1:19
insert into `PatentsView_20141215_dev`.`location_year`
  (`location_id`, `year`, `num_patents`)
select
  pi.location_id, p.year, count(distinct p.patent_id)
from
  `PatentsView_20141215_dev`.`patent_inventor` pi
  inner join `PatentsView_20141215_dev`.`patent` p using(patent_id)
where
  location_id is not null
group by
  pi.location_id, p.year;


# END location_year ######################################################################################################################


# BEGIN additional indexing ###################################################################################################################################


# 1:53:23
alter table `PatentsView_20141215_dev`.`application` add index `ix_application_number` (`number`);
alter table `PatentsView_20141215_dev`.`application` add index `ix_application_patent_id` (`patent_id`);
alter table `PatentsView_20141215_dev`.`assignee` add index `ix_assignee_name_first` (`name_first`);
alter table `PatentsView_20141215_dev`.`assignee` add index `ix_assignee_name_last` (`name_last`);
alter table `PatentsView_20141215_dev`.`assignee` add index `ix_assignee_organization` (`organization`);
alter table `PatentsView_20141215_dev`.`assignee` add index `ix_assignee_persistent_assignee_id` (`persistent_assignee_id`);
alter table `PatentsView_20141215_dev`.`assignee_cpc_subsection` add index `ix_assignee_cpc_subsection_assignee_id` (`assignee_id`);
alter table `PatentsView_20141215_dev`.`assignee_cpc_subsection` add index `ix_assignee_cpc_subsection_subsection_id` (`subsection_id`);
alter table `PatentsView_20141215_dev`.`assignee_inventor` add index `ix_assignee_inventor_assignee_id` (`assignee_id`);
alter table `PatentsView_20141215_dev`.`assignee_inventor` add index `ix_assignee_inventor_inventor_id` (`inventor_id`);
alter table `PatentsView_20141215_dev`.`assignee_nber_subcategory` add index `ix_assignee_nber_subcategory_assignee_id` (`assignee_id`);
alter table `PatentsView_20141215_dev`.`assignee_nber_subcategory` add index `ix_assignee_nber_subcategory_subcategory_id` (`subcategory_id`);
alter table `PatentsView_20141215_dev`.`assignee_uspc_mainclass` add index `ix_assignee_uspc_mainclass_assignee_id` (`assignee_id`);
alter table `PatentsView_20141215_dev`.`assignee_uspc_mainclass` add index `ix_assignee_uspc_mainclass_mainclass_id` (`mainclass_id`);
alter table `PatentsView_20141215_dev`.`assignee_year` add index `ix_assignee_year_assignee_id` (`assignee_id`);
alter table `PatentsView_20141215_dev`.`assignee_year` add index `ix_assignee_year_year` (`patent_year`);
alter table `PatentsView_20141215_dev`.`cpc_current_subsection` add index `ix_cpc_current_subsection_subsection_id` (`subsection_id`);
alter table `PatentsView_20141215_dev`.`cpc_current_subsection` add index `ix_cpc_current_subsection_title` (`subsection_title`);
alter table `PatentsView_20141215_dev`.`cpc_current` add index `ix_cpc_current_group_id` (`group_id`);
alter table `PatentsView_20141215_dev`.`cpc_current` add index `ix_cpc_current_subgroup_id` (`subgroup_id`);
alter table `PatentsView_20141215_dev`.`cpc_current` add index `ix_cpc_current_subsection_id` (`subsection_id`);
alter table `PatentsView_20141215_dev`.`inventor` add index `ix_inventor_name_first` (`name_first`);
alter table `PatentsView_20141215_dev`.`inventor` add index `ix_inventor_name_last` (`name_last`);
alter table `PatentsView_20141215_dev`.`inventor` add index `ix_inventor_persistent_inventor_id` (`persistent_inventor_id`);
alter table `PatentsView_20141215_dev`.`inventor_coinventor` add index `ix_inventor_coinventor_inventor_id` (`inventor_id`);
alter table `PatentsView_20141215_dev`.`inventor_coinventor` add index `ix_inventor_coinventor_coinventor_id` (`coinventor_id`);
alter table `PatentsView_20141215_dev`.`inventor_cpc_subsection` add index `ix_inventor_cpc_subsection_inventor_id` (`inventor_id`);
alter table `PatentsView_20141215_dev`.`inventor_cpc_subsection` add index `ix_inventor_cpc_subsection_subsection_id` (`subsection_id`);
alter table `PatentsView_20141215_dev`.`inventor_nber_subcategory` add index `ix_inventor_nber_subcategory_inventor_id` (`inventor_id`);
alter table `PatentsView_20141215_dev`.`inventor_nber_subcategory` add index `ix_inventor_nber_subcategory_subcategory_id` (`subcategory_id`);
alter table `PatentsView_20141215_dev`.`inventor_uspc_mainclass` add index `ix_inventor_uspc_mainclass_inventor_id` (`inventor_id`);
alter table `PatentsView_20141215_dev`.`inventor_uspc_mainclass` add index `ix_inventor_uspc_mainclass_mainclass_id` (`mainclass_id`);
alter table `PatentsView_20141215_dev`.`inventor_year` add index `ix_inventor_year_inventor_id` (`inventor_id`);
alter table `PatentsView_20141215_dev`.`inventor_year` add index `ix_inventor_year_year` (`patent_year`);
alter table `PatentsView_20141215_dev`.`ipcr` add index `ix_ipcr_ipc_class` (`ipc_class`);
alter table `PatentsView_20141215_dev`.`location_assignee` add index `ix_location_assignee_assignee_id` (`assignee_id`);
alter table `PatentsView_20141215_dev`.`location_inventor` add index `ix_location_inventor_inventor_id` (`inventor_id`);
alter table `PatentsView_20141215_dev`.`location` add index `ix_location_city` (`city`);
alter table `PatentsView_20141215_dev`.`location` add index `ix_location_country` (`country`);
alter table `PatentsView_20141215_dev`.`location` add index `ix_location_persistent_location_id` (`persistent_location_id`);
alter table `PatentsView_20141215_dev`.`location` add index `ix_location_state` (`state`);
alter table `PatentsView_20141215_dev`.`location_cpc_subsection` add index `ix_location_cpc_subsection_location_id` (`location_id`);
alter table `PatentsView_20141215_dev`.`location_cpc_subsection` add index `ix_location_cpc_subsection_subsection_id` (`subsection_id`);
alter table `PatentsView_20141215_dev`.`location_uspc_mainclass` add index `ix_location_uspc_mainclass_location_id` (`location_id`);
alter table `PatentsView_20141215_dev`.`location_uspc_mainclass` add index `ix_location_uspc_mainclass_mainclass_id` (`mainclass_id`);
alter table `PatentsView_20141215_dev`.`location_year` add index `ix_location_year_location_id` (`location_id`);
alter table `PatentsView_20141215_dev`.`location_year` add index `ix_location_year_year` (`year`);
alter table `PatentsView_20141215_dev`.`nber` add index `ix_nber_subcategory_id` (`subcategory_id`);
alter table `PatentsView_20141215_dev`.`nber` add index `ix_nber_subcategory_title` (`subcategory_title`);
alter table `PatentsView_20141215_dev`.`patent_assignee` add index `ix_patent_assignee_location_id` (`location_id`);
alter table `PatentsView_20141215_dev`.`patent_inventor` add index `ix_patent_inventor_location_id` (`location_id`);
alter table `PatentsView_20141215_dev`.`patent` add index `ix_patent_date` (`date`);
alter table `PatentsView_20141215_dev`.`patent` add index `ix_patent_number` (`number`);
alter table `PatentsView_20141215_dev`.`patent` add index `ix_patent_title` (`title`(128));
alter table `PatentsView_20141215_dev`.`patent` add index `ix_patent_type` (`type`);
alter table `PatentsView_20141215_dev`.`patent` add index `ix_patent_year` (`year`);
alter table `PatentsView_20141215_dev`.`usapplicationcitation` add index `ix_usapplicationcitation_cited_application_id` (`cited_application_id`);
alter table `PatentsView_20141215_dev`.`uspatentcitation` add index `ix_uspatentcitation_cited_patent_id` (`cited_patent_id`);
alter table `PatentsView_20141215_dev`.`uspc_current_mainclass` add index `ix_uspc_current_mainclass_mainclass_id` (`mainclass_id`);
alter table `PatentsView_20141215_dev`.`uspc_current_mainclass` add index `ix_uspc_current_mainclass_mainclass_title` (`mainclass_title`);
alter table `PatentsView_20141215_dev`.`uspc_current` add index `ix_uspc_current_mainclass_id` (`mainclass_id`);
alter table `PatentsView_20141215_dev`.`uspc_current` add index `ix_uspc_current_subclass_id` (`subclass_id`);
alter table `PatentsView_20141215_dev`.`uspc_current` add index `ix_uspc_current_mainclass_title` (`mainclass_title`);
alter table `PatentsView_20141215_dev`.`uspc_current` add index `ix_uspc_current_subclass_title` (`subclass_title`);


# END additional indexing #####################################################################################################################################


# BEGIN temporary table removal ###############################################################################################################################


# select concat('drop table if exists `', `table_schema`, '`.`', `table_name`, '`;') from `information_schema`.`tables` where `table_schema` = 'PatentsView_20141215_dev' and `table_name` like 'temp\_%' order by `table_name`;
drop table if exists `PatentsView_20141215_dev`.`temp_assignee_lastknown_location`;
drop table if exists `PatentsView_20141215_dev`.`temp_assignee_num_patents`;
drop table if exists `PatentsView_20141215_dev`.`temp_assignee_years_active`;
drop table if exists `PatentsView_20141215_dev`.`temp_cpc_current_subsection_aggregate_counts`;
drop table if exists `PatentsView_20141215_dev`.`temp_cpc_group_title`;
drop table if exists `PatentsView_20141215_dev`.`temp_cpc_subgroup_title`;
drop table if exists `PatentsView_20141215_dev`.`temp_cpc_subsection_title`;
drop table if exists `PatentsView_20141215_dev`.`temp_id_mapping_assignee`;
drop table if exists `PatentsView_20141215_dev`.`temp_id_mapping_inventor`;
drop table if exists `PatentsView_20141215_dev`.`temp_id_mapping_location`;
drop table if exists `PatentsView_20141215_dev`.`temp_id_mapping_location_transformed`;
drop table if exists `PatentsView_20141215_dev`.`temp_inventor_lastknown_location`;
drop table if exists `PatentsView_20141215_dev`.`temp_inventor_num_patents`;
drop table if exists `PatentsView_20141215_dev`.`temp_inventor_years_active`;
drop table if exists `PatentsView_20141215_dev`.`temp_ipcr_aggregations`;
drop table if exists `PatentsView_20141215_dev`.`temp_ipcr_years_active`;
drop table if exists `PatentsView_20141215_dev`.`temp_location_num_assignees`;
drop table if exists `PatentsView_20141215_dev`.`temp_location_num_inventors`;
drop table if exists `PatentsView_20141215_dev`.`temp_location_num_patents`;
drop table if exists `PatentsView_20141215_dev`.`temp_location_patent`;
drop table if exists `PatentsView_20141215_dev`.`temp_mainclass_current_aggregate_counts`;
drop table if exists `PatentsView_20141215_dev`.`temp_mainclass_current_title`;
drop table if exists `PatentsView_20141215_dev`.`temp_nber_subcategory_aggregate_counts`;
drop table if exists `PatentsView_20141215_dev`.`temp_num_foreign_documents_cited`;
drop table if exists `PatentsView_20141215_dev`.`temp_num_times_cited_by_us_patents`;
drop table if exists `PatentsView_20141215_dev`.`temp_num_us_applications_cited`;
drop table if exists `PatentsView_20141215_dev`.`temp_num_us_patents_cited`;
drop table if exists `PatentsView_20141215_dev`.`temp_patent_aggregations`;
drop table if exists `PatentsView_20141215_dev`.`temp_patent_date`;
drop table if exists `PatentsView_20141215_dev`.`temp_patent_earliest_application_date`;
drop table if exists `PatentsView_20141215_dev`.`temp_patent_firstnamed_assignee`;
drop table if exists `PatentsView_20141215_dev`.`temp_patent_firstnamed_inventor`;
drop table if exists `PatentsView_20141215_dev`.`temp_subclass_current_title`;


# END temporary table removal #################################################################################################################################


# Run UnencodeHTMLEntities Python script followed by add_full_text_indexes SQL script.
