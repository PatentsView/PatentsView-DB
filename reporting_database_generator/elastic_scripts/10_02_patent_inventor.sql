use `elastic_search_{{ dag_run.logical_date | ds_nodash }}`;

CREATE TABLE IF NOT EXISTS `elastic_search_{{ dag_run.logical_date | ds_nodash }}`.`patent_inventor`
(
    `inventor_id`            int(10) unsigned                       NOT NULL,
    `persistent_inventor_id` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `patent_id`              varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL,
    `sequence`               int(11)                                NOT NULL,
    `name_first`             varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `name_last`              varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `city`                   varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `state`                  varchar(20) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `country`                varchar(10) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `location_id`            int(11)                                 DEFAULT NULL,
    `persistent_location_id` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    PRIMARY KEY (`patent_id`, `inventor_id`, `sequence`),
    KEY `ix_inventor_name_first` (`name_first`),
    KEY `ix_inventor_name_last` (`name_last`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

TRUNCATE TABLE `elastic_search_{{ dag_run.logical_date | ds_nodash }}`.`patent_inventor`;

insert into `elastic_search_{{ dag_run.logical_date | ds_nodash }}`.patent_inventor ( inventor_id, patent_id, sequence, name_first, name_last
                                                          , city, state
                                                          , country, location_id, persistent_inventor_id
                                                          , persistent_location_id)

select pi.inventor_id
     , pi.patent_id
     , pi.sequence
     , i.name_first
     , i.name_last
     , l.city
     , l.state
     , l.country
     , pi.location_id
     , timi.old_inventor_id
     , timl.old_location_id
from `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.patent_inventor pi
         join `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.inventor i on i.inventor_id = pi.inventor_id
         join `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.temp_id_mapping_inventor timi on timi.new_inventor_id = i.inventor_id
         left join `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.location l on l.location_id = pi.location_id
         left join `PatentsView_{{ dag_run.logical_date | ds_nodash }}`.temp_id_mapping_location timl on timl.new_location_id = l.location_id
         join `elastic_search_{{ dag_run.logical_date | ds_nodash }}`.patents p on p.patent_id = pi.patent_id;

