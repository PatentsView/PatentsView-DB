{% set elastic_target_database = params.elastic_database_prefix + params.version_indicator.replace("-","") %}
{% set reporting_database = params.reporting_database %}
use `elastic_production_{{ dag_run.logical_date | ds_nodash }}`;

CREATE TABLE IF NOT EXISTS `elastic_production_{{ dag_run.logical_date | ds_nodash }}`.`locations`
(
    `location_id`            int(10) unsigned                        NOT NULL,
    `city`                   varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `state`                  varchar(20) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `country`                varchar(10) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `county`                 varchar(60) COLLATE utf8mb4_unicode_ci  DEFAULT NULL,
    `state_fips`             varchar(2) COLLATE utf8mb4_unicode_ci   DEFAULT NULL,
    `county_fips`            varchar(6) COLLATE utf8mb4_unicode_ci   DEFAULT NULL,
    `place_type`             varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `latitude`               float                                   DEFAULT NULL,
    `longitude`              float                                   DEFAULT NULL,
    `num_assignees`          int(10) unsigned                        NOT NULL,
    `num_inventors`          int(10) unsigned                        NOT NULL,
    `num_patents`            int(10) unsigned                        NOT NULL,
    `persistent_location_id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
    PRIMARY KEY (`location_id`),
    KEY `ix_location_county` (`county`),
    KEY `ix_location_state_fips` (`state_fips`),
    KEY `ix_location_county_fips` (`county_fips`),
    KEY `ix_location_num_inventors` (`num_inventors`),
    KEY `ix_location_city` (`city`),
    KEY `ix_location_country` (`country`),
    KEY `ix_location_persistent_location_id` (`persistent_location_id`),
    KEY `ix_location_state` (`state`),
    KEY `ix_location_num_patents` (`num_patents`),
    KEY `ix_location_num_assignees` (`num_assignees`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;




TRUNCATE TABLE `elastic_production_{{ dag_run.logical_date | ds_nodash }}`.locations;
INSERT INTO `elastic_production_{{ dag_run.logical_date | ds_nodash }}`.locations( location_id, city, state, country, county, state_fips, county_fips, latitude
                                        , longitude, num_assignees, num_inventors, num_patents, persistent_location_id
                                        , locations.place_type)
select
    l.location_id
  , l.city
  , l.state
  , l.country
  , l.county
  , l.state_fips
  , l.county_fips
  , l.latitude
  , l.longitude
  , num_assignees
  , num_inventors
  , num_patents
  , timl.old_location_id
  , cl.place
from
    `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`location` l
        join `PatentsView_{{ macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") }}`.`temp_id_mapping_location` timl on timl.new_location_id = l.location_id
        left join patent.location l2 on l2.id = timl.old_location_id
        left join geo_data.curated_locations cl on cl.id = l2.curated_location_id;
