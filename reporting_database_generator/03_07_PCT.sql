
# BEGIN pctdata

#################################################################################################################################


drop table if exists `{{params.reporting_database}}`.`pctdata`;
create table `{{params.reporting_database}}`.`pctdata`
(
  `patent_id` varchar(20) not null,
  `doc_type` varchar(20) not null,
  `kind` varchar(2) not null,
  `doc_number` varchar(20) null,
  `date` date null,
  `102_date` date null,
  `371_date` date null,
  primary key (`patent_id`, `kind`)
)
engine=InnoDB;


# 13,617,656 @ 8:22
insert into `{{params.reporting_database}}`.`pctdata`
(
  `patent_id`, `doc_type`, `kind`, `doc_number`, `date`, `102_date`, `371_date`
)
select
  ac.`patent_id`, ac.`doc_type`, ac.`kind`, ac.`rel_id`,
  case when ac.`date` > date('1899-12-31') and ac.`date` < date_add(current_date, interval 10 year) then ac.`date` else null end,
  case when ac.`102_date` > date('1899-12-31') and ac.`102_date` < date_add(current_date, interval 10 year) then ac.`102_date` else null end,
  case when ac.`371_date` > date('1899-12-31') and ac.`371_date` < date_add(current_date, interval 10 year) then ac.`371_date` else null end
from
  `{{params.reporting_database}}`.`patent` p
  inner join `{{params.raw_database}}`.`pct_data` ac on ac.`patent_id` = p.`patent_id`  where p.version_indicator<= {{ params.version_indicator }};


# END pctdata

###################################################################################################################################