{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

alter table `{{reporting_db}}`.`location_uspc_mainclass` add index `ix_location_uspc_mainclass_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`location_uspc_mainclass` add index `ix_location_uspc_mainclass_mainclass_id` (`mainclass_id`);
alter table `{{reporting_db}}`.`location_uspc_mainclass` add index `ix_location_uspc_mainclass_location_id` (`location_id`);
alter table `{{reporting_db}}`.`location_year` add index `ix_location_year_year` (`year`);
alter table `{{reporting_db}}`.`location_year` add index `ix_location_year_location_id` (`location_id`);
alter table `{{reporting_db}}`.`location_year` add index `ix_location_year_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`location` add index `ix_location_county` (`county`);
alter table `{{reporting_db}}`.`location` add index `ix_location_state_fips` (`state_fips`);
alter table `{{reporting_db}}`.`location` add index `ix_location_county_fips` (`county_fips`);
alter table `{{reporting_db}}`.`location` add index `ix_location_num_inventors` (`num_inventors`);
alter table `{{reporting_db}}`.`location` add index `ix_location_city` (`city`);
alter table `{{reporting_db}}`.`location` add index `ix_location_country` (`country`);
alter table `{{reporting_db}}`.`location` add index `ix_location_persistent_location_id` (`persistent_location_id`);
alter table `{{reporting_db}}`.`location` add index `ix_location_state` (`state`);
alter table `{{reporting_db}}`.`location` add index `ix_location_num_patents` (`num_patents`);
alter table `{{reporting_db}}`.`location` add index `ix_location_num_assignees` (`num_assignees`);