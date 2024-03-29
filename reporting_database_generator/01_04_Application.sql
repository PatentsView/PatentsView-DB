{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}
{% set version_indicator = macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

# BEGIN application

###########################################################################################################################################


drop table if exists `{{reporting_db}}`.`application`;
create table `{{reporting_db}}`.`application`
(
  `application_id` varchar(36) not null,
  `patent_id` varchar(20) not null,
  `type` varchar(20) null,
  `number` varchar(64) null,
  `country` varchar(20) null,
  `date` date null,
  primary key (`application_id`, `patent_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`application`
  (`application_id`, `patent_id`, `type`, `number`, `country`, `date`)
select
  `id_transformed`, `patent_id`, nullif(trim(`type`), ''),
  nullif(trim(`number_transformed`), ''), nullif(trim(`country`), ''),
  case when `date` > date('1899-12-31') and `date` < date_add(current_date, interval 10 year) then `date` else null end
from
  `patent`.`application`  where version_indicator<='{{version_indicator}}';

alter table `{{reporting_db}}`.`application` add index `ix_application_number` (`number`);
alter table `{{reporting_db}}`.`application` add index `ix_application_patent_id` (`patent_id`);

# END application 

#############################################################################################################################################