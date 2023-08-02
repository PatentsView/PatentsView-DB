
# BEGIN pctdata

#################################################################################################################################


drop table if exists `{{params.reporting_database}}`.`pctdata`;
create table `{{params.reporting_database}}`.`pctdata`
( `uuid` varchar(36) not null,
  `patent_id` varchar(20) not null,
  `doc_type` varchar(20) not null,
  `kind` varchar(2) not null,
  `doc_number` varchar(20) null,
  `date` date null,
  `102_date` date null,
  `371_date` date null,
  primary key (`uuid`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{params.reporting_database}}`.`pctdata`
(
  `uuid`, `patent_id`, `doc_type`, `kind`, `doc_number`, `date`, `102_date`, `371_date`
)
select ac.`uuid`,
  ac.`patent_id`, ac.`doc_type`, ac.`kind`, ac.`rel_id`,
  case when ac.`date` > date('1899-12-31') and ac.`date` < date_add(current_date, interval 10 year) then ac.`date` else null end,
  case when ac.`102_date` > date('1899-12-31') and ac.`102_date` < date_add(current_date, interval 10 year) then ac.`102_date` else null end,
  case when ac.`371_date` > date('1899-12-31') and ac.`371_date` < date_add(current_date, interval 10 year) then ac.`371_date` else null end
from
  `{{params.reporting_database}}`.`patent` p
  inner join `{{params.raw_database}}`.`pct_data` ac on ac.`patent_id` = p.`patent_id`;


# END pctdata

###################################################################################################################################