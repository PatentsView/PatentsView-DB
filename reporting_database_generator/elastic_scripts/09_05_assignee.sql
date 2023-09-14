use `PatentsView_{{ dag_run.logical_date | ds_nodash }}`;

CREATE TABLE IF NOT EXISTS `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`assignees` (
  `assignee_id` int(10) unsigned NOT NULL,
  `type` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `name_first` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `name_last` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `organization` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `num_patents` int(10) unsigned NOT NULL,
  `num_inventors` int(10) unsigned NOT NULL,
  `lastknown_location_id` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `lastknown_persistent_location_id` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `lastknown_city` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `lastknown_state` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `lastknown_country` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `lastknown_latitude` float DEFAULT NULL,
  `lastknown_longitude` float DEFAULT NULL,
  `first_seen_date` date DEFAULT NULL,
  `last_seen_date` date DEFAULT NULL,
  `years_active` smallint(5) unsigned NOT NULL,
  `persistent_assignee_id` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`assignee_id`),
  KEY `ix_assignee_num_patents` (`num_patents`),
  KEY `ix_assignee_first_seen_date` (`first_seen_date`),
  KEY `ix_assignee_last_seen_date` (`last_seen_date`),
  KEY `ix_assignee_lastknown_persistent_location_id` (`lastknown_persistent_location_id`),
  KEY `ix_assignee_lastknown_location_id` (`lastknown_location_id`),
  KEY `ix_assignee_name_first` (`name_first`),
  KEY `ix_assignee_name_last` (`name_last`),
  KEY `ix_assignee_organization` (`organization`),
  KEY `ix_assignee_num_inventors` (`num_inventors`),
  KEY `ix_assignee_persistent_assignee_id` (`persistent_assignee_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.`assignee_years` (
  `assignee_id` int(10) unsigned NOT NULL,
  `patent_year` smallint(6) NOT NULL,
  `num_patents` int(10) unsigned NOT NULL,
  KEY `ix_assignee_year_num_patents` (`num_patents`),
  KEY `ix_assignee_year_year` (`patent_year`),
  KEY `ix_assignee_year_assignee_id` (`assignee_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

TRUNCATE TABLE `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.assignees;
INSERT INTO `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.assignees( assignee_id, type, name_first, name_last, organization, num_patents
                                     , num_inventors, lastknown_location_id, lastknown_persistent_location_id
                                     , lastknown_city, lastknown_state, lastknown_country, lastknown_latitude
                                     , lastknown_longitude, first_seen_date, last_seen_date, years_active
                                     , persistent_assignee_id)
select
    a.assignee_id
  , a.type
  , name_first
  , name_last
  , organization
  , num_patents
  , num_inventors
  , timl.old_location_id
  , lastknown_persistent_location_id
  , lastknown_city
  , lastknown_state
  , lastknown_country
  , lastknown_latitude
  , lastknown_longitude
  , first_seen_date
  , last_seen_date
  , years_active
  , persistent_assignee_id
from
    `{{reporting_database}}`.`assignee` a
        left join `{{reporting_database}}`.`temp_id_mapping_location` timl on timl.new_location_id = a.lastknown_location_id;




TRUNCATE TABLE `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.assignee_years;
INSERT INTO `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.assignee_years(assignee_id, patent_year, num_patents)
select
    ay.assignee_id
  , patent_year
  , ay.num_patents
from
    `{{reporting_database}}`.`assignee_year` ay
        join `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.assignees a
where
    a.assignee_id = ay.assignee_id;