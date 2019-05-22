

# BEGIN patent 

################################################################################################################################################


drop table if exists `{{params.reporting_database}}`.`temp_patent_firstnamed_assignee`;
create table `{{params.reporting_database}}`.`temp_patent_firstnamed_assignee`
(
  `patent_id` varchar(20) not null,
  `assignee_id` int unsigned null,
  `persistent_assignee_id` varchar(64) null,
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
insert into `{{params.reporting_database}}`.`temp_patent_firstnamed_assignee`
(
  `patent_id`, `assignee_id`, `persistent_assignee_id`, `location_id`,
  `persistent_location_id`, `city`, `state`, `country`, `latitude`, `longitude`
)
select
  p.`id`,
  ta.`new_assignee_id`,
  ta.`old_assignee_id`,
  tl.`new_location_id`,
  tl.`old_location_id_transformed`,
  nullif(l.`city`, ''),
  nullif(l.`state`, ''),
  nullif(l.`country`, ''),
  l.`latitude`,
  l.`longitude`
from
  `{{params.raw_database}}`.`patent` p
  left outer join `{{params.raw_database}}`.`rawassignee` ra on ra.`patent_id` = p.`id` and ra.`sequence` = 0
  left outer join `{{params.reporting_database}}`.`temp_id_mapping_assignee` ta on ta.`old_assignee_id` = ra.`assignee_id`
  left outer join `{{params.raw_database}}`.`rawlocation` rl on rl.`id` = ra.`rawlocation_id`
  left outer join `{{params.raw_database}}`.`location` l on l.`id` = rl.`location_id`
  left outer join `{{params.reporting_database}}`.`temp_id_mapping_location_transformed` tl on tl.`old_location_id_transformed` = 

rl.`location_id_transformed`
where
  ta.`new_assignee_id` is not null or
  tl.`new_location_id` is not null;


drop table if exists `{{params.reporting_database}}`.`temp_patent_firstnamed_inventor`;
create table `{{params.reporting_database}}`.`temp_patent_firstnamed_inventor`
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
insert into `{{params.reporting_database}}`.`temp_patent_firstnamed_inventor`
(
  `patent_id`, `inventor_id`, `persistent_inventor_id`, `location_id`,
  `persistent_location_id`, `city`, `state`, `country`, `latitude`, `longitude`
)
select
  p.`id`,
  ti.`new_inventor_id`,
  ti.`old_inventor_id`,
  tl.`new_location_id`,
  tl.`old_location_id_transformed`,
  nullif(l.`city`, ''),
  nullif(l.`state`, ''),
  nullif(l.`country`, ''),
  l.`latitude`,
  l.`longitude`
from
  `{{params.raw_database}}`.`patent` p
  left outer join `{{params.raw_database}}`.`rawinventor` ri on ri.`patent_id` = p.`id` and ri.`sequence` = 0
  left outer join `{{params.reporting_database}}`.`temp_id_mapping_inventor` ti on ti.`old_inventor_id` = ri.`inventor_id`
  left outer join `{{params.raw_database}}`.`rawlocation` rl on rl.`id` = ri.`rawlocation_id`
  left outer join `{{params.raw_database}}`.`location` l on l.`id` = rl.`location_id`
  left outer join `{{params.reporting_database}}`.`temp_id_mapping_location_transformed` tl on tl.`old_location_id_transformed` = 

rl.`location_id_transformed`
where
  ti.`new_inventor_id` is not null or
  tl.`new_location_id` is not null;


drop table if exists `{{params.reporting_database}}`.`temp_num_foreign_documents_cited`;
create table `{{params.reporting_database}}`.`temp_num_foreign_documents_cited`
(
  `patent_id` varchar(20) not null,
  `num_foreign_documents_cited` int unsigned not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# The number of foreign documents cited.
# 2,751,072 @ 1:52
insert into `{{params.reporting_database}}`.`temp_num_foreign_documents_cited`
  (`patent_id`, `num_foreign_documents_cited`)
select
  `patent_id`, count(*)
from
  `{{params.raw_database}}`.`foreigncitation`
group by
  `patent_id`;


drop table if exists `{{params.reporting_database}}`.`temp_num_us_applications_cited`;
create table `{{params.reporting_database}}`.`temp_num_us_applications_cited`
(
  `patent_id` varchar(20) not null,
  `num_us_applications_cited` int unsigned not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# The number of U.S. patent applications cited.
# 1,534,484 @ 0:21
insert into `{{params.reporting_database}}`.`temp_num_us_applications_cited`
  (`patent_id`, `num_us_applications_cited`)
select
  `patent_id`, count(*)
from
  `{{params.raw_database}}`.`usapplicationcitation`
group by
  `patent_id`;


drop table if exists `{{params.reporting_database}}`.`temp_num_us_patents_cited`;
create table `{{params.reporting_database}}`.`temp_num_us_patents_cited`
(
  `patent_id` varchar(20) not null,
  `num_us_patents_cited` int unsigned not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# The number of U.S. patents cited.
# 5,231,893 @ 7:17
insert into `{{params.reporting_database}}`.`temp_num_us_patents_cited`
  (`patent_id`, `num_us_patents_cited`)
select
  `patent_id`, count(*)
from
  `{{params.raw_database}}`.`uspatentcitation`
group by
  `patent_id`;


drop table if exists `{{params.reporting_database}}`.`temp_num_times_cited_by_us_patents`;
create table `{{params.reporting_database}}`.`temp_num_times_cited_by_us_patents`
(
  `patent_id` varchar(20) not null,
  `num_times_cited_by_us_patents` int unsigned not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# The number of times a U.S. patent was cited.
# 6,333,277 @ 7:27
insert into `{{params.reporting_database}}`.`temp_num_times_cited_by_us_patents`
  (`patent_id`, `num_times_cited_by_us_patents`)
select
  `citation_id`, count(*)
from
  `{{params.raw_database}}`.`uspatentcitation`
where
  `citation_id` is not null and `citation_id` != ''
group by
  `citation_id`;


drop table if exists `{{params.reporting_database}}`.`temp_patent_aggregations`;
create table `{{params.reporting_database}}`.`temp_patent_aggregations`
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
insert into `{{params.reporting_database}}`.`temp_patent_aggregations`
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
  `{{params.raw_database}}`.`patent` p
  left outer join `{{params.reporting_database}}`.`temp_num_foreign_documents_cited` t1 on t1.`patent_id` = p.`id`
  left outer join `{{params.reporting_database}}`.`temp_num_us_applications_cited` t2 on t2.`patent_id` = p.`id`
  left outer join `{{params.reporting_database}}`.`temp_num_us_patents_cited` t3 on t3.`patent_id` = p.`id`
  left outer join `{{params.reporting_database}}`.`temp_num_times_cited_by_us_patents` t4 on t4.`patent_id` = p.`id`;


drop table if exists `{{params.reporting_database}}`.`temp_patent_earliest_application_date`;
create table `{{params.reporting_database}}`.`temp_patent_earliest_application_date`
(
  `patent_id` varchar(20) not null,
  `earliest_application_date` date not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# Find the earliest application date for each patent.
# 5,425,837 @ 1:35
insert into `{{params.reporting_database}}`.`temp_patent_earliest_application_date`
  (`patent_id`, `earliest_application_date`)
select
  a.`patent_id`, min(a.`date`)
from
  `{{params.raw_database}}`.`application` a
where
  a.`date` is not null and a.`date` > date('1899-12-31') and a.`date` < date_add(current_date, interval 10 year)
group by
  a.`patent_id`;


drop table if exists `{{params.reporting_database}}`.`temp_patent_date`;
create table `{{params.reporting_database}}`.`temp_patent_date`
(
  `patent_id` varchar(20) not null,
  `date` date null,
  primary key (`patent_id`)
)
engine=InnoDB;


# Eliminate obviously bad patent dates.
# 5,425,875 @ 0:37
insert into `{{params.reporting_database}}`.`temp_patent_date`
  (`patent_id`, `date`)
select
  p.`id`, p.`date`
from
  `{{params.raw_database}}`.`patent` p
where
  p.`date` is not null and p.`date` > date('1899-12-31') and p.`date` < date_add(current_date, interval 10 year);


drop table if exists `{{params.reporting_database}}`.`patent`;

create table `{{params.reporting_database}}`.`patent`
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
  `firstnamed_assignee_persistent_id` varchar(64) null,
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
  `uspc_current_mainclass_average_patent_processing_days` int unsigned null, 
  `cpc_current_group_average_patent_processing_days` int unsigned null, 
  `term_extension` int unsigned null,
  `detail_desc_length` int unsigned null,
  primary key (`patent_id`)
)
engine=InnoDB;


# 5,425,879 @ 6:45
insert into `{{params.reporting_database}}`.`patent`
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
  `earliest_application_date`, `patent_processing_days`,
  `term_extension`
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
  case when tpead.`earliest_application_date` <= p.`date` then timestampdiff(day, tpead.`earliest_application_date`, tpd.`date`) else null end,
  ustog.`term_extension`
from
  `{{params.raw_database}}`.`patent` p
  left outer join `{{params.reporting_database}}`.`temp_patent_date` tpd on tpd.`patent_id` = p.`id`
  left outer join `{{params.reporting_database}}`.`temp_patent_firstnamed_assignee` tpfna on tpfna.`patent_id` = p.`id`
  left outer join `{{params.reporting_database}}`.`temp_patent_firstnamed_inventor` tpfni on tpfni.`patent_id` = p.`id`
  left outer join `{{params.reporting_database}}`.`temp_patent_aggregations` tpa on tpa.`patent_id` = p.`id`
  left outer join `{{params.reporting_database}}`.`temp_patent_earliest_application_date` tpead on tpead.`patent_id` = p.`id`
  left outer join `{{params.raw_database}}`.`us_term_of_grant` ustog on ustog.`patent_id`=p.`id`;

# END patent 

################################################################################################################################################