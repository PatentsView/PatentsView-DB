{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set version_indicator = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

# BEGIN patent 

################################################################################################################################################


drop table if exists `{{reporting_db}}`.`temp_patent_firstnamed_assignee`;
create table `{{reporting_db}}`.`temp_patent_firstnamed_assignee`
(
  `patent_id` varchar(20) not null,
  `assignee_id` int unsigned null,
  `persistent_assignee_id` varchar(64) null,
  `location_id` int unsigned null,
  `persistent_location_id` varchar(128) null,
  `city` varchar(256) null,
  `state` varchar(256) null,
  `country` varchar(256) null,
  `latitude` float null,
  `longitude` float null,
  primary key (`patent_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

insert into `{{reporting_db}}`.`temp_patent_firstnamed_assignee`
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
  `patent`.`patent` p
  left outer join `patent`.`rawassignee` ra on ra.`patent_id` = p.`id` and ra.`sequence` = 0
  left outer join `{{reporting_db}}`.`temp_id_mapping_assignee` ta on ta.`old_assignee_id` = ra.`assignee_id`
  left outer join `patent`.`rawlocation` rl on rl.`id` = ra.`rawlocation_id`
  left outer join `patent`.`location` l on l.`id` = rl.`location_id`
  left outer join `{{reporting_db}}`.`temp_id_mapping_location` tl on l.`id` =  tl.`old_location_id`
where
  (ta.`new_assignee_id` is not null or
  tl.`new_location_id` is not null) and  p.version_indicator<='{{version_indicator}}';


drop table if exists `{{reporting_db}}`.`temp_patent_firstnamed_inventor`;
create table `{{reporting_db}}`.`temp_patent_firstnamed_inventor`
(
  `patent_id` varchar(20) not null,
  `inventor_id` int unsigned null,
  `persistent_inventor_id` varchar(256) null,
  `location_id` int unsigned null,
  `persistent_location_id` varchar(128) null,
  `city` varchar(256) null,
  `state` varchar(256) null,
  `country` varchar(256) null,
  `latitude` float null,
  `longitude` float null,
  primary key (`patent_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

insert into `{{reporting_db}}`.`temp_patent_firstnamed_inventor`
(
  `patent_id`, `inventor_id`, `persistent_inventor_id`, `location_id`,
  `persistent_location_id`, `city`, `state`, `country`, `latitude`, `longitude`
)
select
  p.`id`,
  ti.`new_inventor_id`,
  ti.`old_inventor_id`,
  tli.`new_location_id`,
  tli.`old_location_id_transformed`,
  nullif(l.`city`, ''),
  nullif(l.`state`, ''),
  nullif(l.`country`, ''),
  l.`latitude`,
  l.`longitude`
from
  `patent`.`patent` p
  left outer join `patent`.`rawinventor` ri on ri.`patent_id` = p.`id` and ri.`sequence` = 0
  left outer join `{{reporting_db}}`.`temp_id_mapping_inventor` ti on ti.`old_inventor_id` = ri.`inventor_id`
  left outer join `patent`.`rawlocation` rl on rl.`id` = ri.`rawlocation_id`
  left outer join `patent`.`location` l on l.`id` = rl.`location_id`
  left outer join `{{reporting_db}}`.`temp_id_mapping_location` tli on tli.`old_location_id` =  l.`id`
where
  (ti.`new_inventor_id` is not null or
  tli.`new_location_id` is not null)and  p.version_indicator<='{{version_indicator}}';


drop table if exists `{{reporting_db}}`.`temp_num_foreign_documents_cited`;
create table `{{reporting_db}}`.`temp_num_foreign_documents_cited`
(
  `patent_id` varchar(20) not null,
  `num_foreign_documents_cited` int unsigned not null,
  primary key (`patent_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`temp_num_foreign_documents_cited`
  (`patent_id`, `num_foreign_documents_cited`)
select
  `patent_id`, count(*)
from
  `patent`.`foreigncitation`  where version_indicator<='{{version_indicator}}'
group by
  `patent_id`;


drop table if exists `{{reporting_db}}`.`temp_num_us_applications_cited`;
create table `{{reporting_db}}`.`temp_num_us_applications_cited`
(
  `patent_id` varchar(20) not null,
  `num_us_applications_cited` int unsigned not null,
  primary key (`patent_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`temp_num_us_applications_cited`
  (`patent_id`, `num_us_applications_cited`)
select
  `patent_id`, count(*)
from
  `patent`.`usapplicationcitation`  where version_indicator<='{{version_indicator}}'
group by
  `patent_id`;


drop table if exists `{{reporting_db}}`.`temp_num_us_patents_cited`;
create table `{{reporting_db}}`.`temp_num_us_patents_cited`
(
  `patent_id` varchar(20) not null,
  `num_us_patents_cited` int unsigned not null,
  primary key (`patent_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



insert into `{{reporting_db}}`.`temp_num_us_patents_cited`
  (`patent_id`, `num_us_patents_cited`)
select
  `patent_id`, count(*)
from
  `patent`.`uspatentcitation`  where version_indicator<='{{version_indicator}}'
group by
  `patent_id`;


drop table if exists `{{reporting_db}}`.`temp_num_times_cited_by_us_patents`;
create table `{{reporting_db}}`.`temp_num_times_cited_by_us_patents`
(
  `patent_id` varchar(20) not null,
  `num_times_cited_by_us_patents` int unsigned not null,
  primary key (`patent_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`temp_num_times_cited_by_us_patents`
  (`patent_id`, `num_times_cited_by_us_patents`)
select
  `citation_id`, count(*)
from
  `patent`.`uspatentcitation`
where
  `citation_id` is not null and `citation_id` != ''  and version_indicator<='{{version_indicator}}'
group by
  `citation_id`;


drop table if exists `{{reporting_db}}`.`temp_patent_aggregations`;
create table `{{reporting_db}}`.`temp_patent_aggregations`
(
  `patent_id` varchar(20) not null,
  `num_foreign_documents_cited` int unsigned not null,
  `num_us_applications_cited` int unsigned not null,
  `num_us_patents_cited` int unsigned not null,
  `num_total_documents_cited` int unsigned not null,
  `num_times_cited_by_us_patents` int unsigned not null,
  primary key (`patent_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



insert into `{{reporting_db}}`.`temp_patent_aggregations`
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
  `patent`.`patent` p
  left outer join `{{reporting_db}}`.`temp_num_foreign_documents_cited` t1 on t1.`patent_id` = p.`id`
  left outer join `{{reporting_db}}`.`temp_num_us_applications_cited` t2 on t2.`patent_id` = p.`id`
  left outer join `{{reporting_db}}`.`temp_num_us_patents_cited` t3 on t3.`patent_id` = p.`id`
  left outer join `{{reporting_db}}`.`temp_num_times_cited_by_us_patents` t4 on t4.`patent_id` = p.`id`  where version_indicator<='{{version_indicator}}';


drop table if exists `{{reporting_db}}`.`temp_patent_earliest_application_date`;
create table `{{reporting_db}}`.`temp_patent_earliest_application_date`
(
  `patent_id` varchar(20) not null,
  `earliest_application_date` date not null,
  primary key (`patent_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`temp_patent_earliest_application_date`
  (`patent_id`, `earliest_application_date`)
select
  a.`patent_id`, min(a.`date`)
from
  `patent`.`application` a
where
  a.`date` is not null and a.`date` > date('1899-12-31') and a.`date` < date_add(current_date, interval 10 year)  and version_indicator<='{{version_indicator}}'
group by
  a.`patent_id`;


drop table if exists `{{reporting_db}}`.`temp_patent_date`;
create table `{{reporting_db}}`.`temp_patent_date`
(
  `patent_id` varchar(20) not null,
  `date` date null,
  primary key (`patent_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`temp_patent_date`
  (`patent_id`, `date`)
select
  p.`id`, p.`date`
from
  `patent`.`patent` p
where
  p.`date` is not null and p.`date` > date('1899-12-31') and p.`date` < date_add(current_date, interval 10 year) and version_indicator<='{{version_indicator}}';


drop table if exists `{{reporting_db}}`.`patent`;
create table `{{reporting_db}}`.`patent`
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
  `firstnamed_assignee_city` varchar(256) null,
  `firstnamed_assignee_state` varchar(256) null,
  `firstnamed_assignee_country` varchar(256) null,
  `firstnamed_assignee_latitude` float null,
  `firstnamed_assignee_longitude` float null,
  `firstnamed_inventor_id` int unsigned null,
  `firstnamed_inventor_persistent_id` varchar(256) null,
  `firstnamed_inventor_location_id` int unsigned null,
  `firstnamed_inventor_persistent_location_id` varchar(128) null,
  `firstnamed_inventor_city` varchar(256) null,
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
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

insert into `{{reporting_db}}`.`patent`
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
  `num_us_patents_cited`,
  `num_total_documents_cited`,
  `num_times_cited_by_us_patents`,
  `earliest_application_date`,
  `patent_processing_days`,
  `uspc_current_mainclass_average_patent_processing_days`,
  `cpc_current_group_average_patent_processing_days`,
  `term_extension`, `detail_desc_length`
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
  tpa.`num_us_patents_cited`,
  tpa.`num_total_documents_cited`,
  tpa.`num_times_cited_by_us_patents`,
  tpead.`earliest_application_date`,
  case when tpead.`earliest_application_date` <= p.`date` then timestampdiff(day, tpead.`earliest_application_date`, tpd.`date`) else null end,
  null,
  null,
  ustog.`term_extension`, `detail_desc_length`
from
  `patent`.`patent` p
  left outer join `{{reporting_db}}`.`temp_patent_date` tpd on tpd.`patent_id` = p.`id`
  left outer join `{{reporting_db}}`.`temp_patent_firstnamed_assignee` tpfna on tpfna.`patent_id` = p.`id`
  left outer join `{{reporting_db}}`.`temp_patent_firstnamed_inventor` tpfni on tpfni.`patent_id` = p.`id`
  left outer join `{{reporting_db}}`.`temp_patent_aggregations` tpa on tpa.`patent_id` = p.`id`
  left outer join `{{reporting_db}}`.`temp_patent_earliest_application_date` tpead on tpead.`patent_id` = p.`id`
  left outer join `patent`.`us_term_of_grant` ustog on ustog.`patent_id`=p.`id`
  left outer join `patent`.`detail_desc_length` ddl on ddl.`patent_id` = p.`id`
 where  p.version_indicator<='{{version_indicator}}';

alter table `{{reporting_db}}`.`patent` add index `ix_patent_firstnamed_inventor_location_id` (`firstnamed_inventor_location_id`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_number` (`number`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_title` (`title`(128));
alter table `{{reporting_db}}`.`patent` add index `ix_patent_type` (`type`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_year` (`year`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_date` (`date`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_firstnamed_inventor_persistent_location_id`(`firstnamed_inventor_persistent_location_id`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_firstnamed_inventor_persistent_id` (`firstnamed_inventor_persistent_id`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_firstnamed_assignee_location_id` (`firstnamed_assignee_location_id`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_firstnamed_assignee_persistent_location_id`(`firstnamed_assignee_persistent_location_id`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_firstnamed_assignee_persistent_id` (`firstnamed_assignee_persistent_id`);
alter table `{{reporting_db}}`.`patent` add fulltext index `fti_patent_title` (`title`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_firstnamed_inventor_id` (`firstnamed_inventor_id`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_firstnamed_assignee_id` (`firstnamed_assignee_id`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_num_claims` (`num_claims`);
alter table `{{reporting_db}}`.`patent` add index `ix_patent_country` (`country`);
alter table `{{reporting_db}}`.`patent` add fulltext index `fti_patent_abstract` (`abstract`);

# END patent 

################################################################################################################################################