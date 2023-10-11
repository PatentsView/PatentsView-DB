{% set reporting_db = "PatentsView_" + macros.ds_format(macros.ds_add(dag_run.data_interval_end | ds, -1), "%Y-%m-%d", "%Y%m%d") %}

# BEGIN pctdata

#################################################################################################################################


drop table if exists `{{reporting_db}}`.`pctdata`;
create table `{{reporting_db}}`.`pctdata`
( `uuid` varchar(36) not null,
  `patent_id` varchar(20) not null,
  `doc_type` varchar(20) not null,
  `kind` varchar(2) null,
  `doc_number` varchar(20) null,
  `date` date null,
  `102_date` date null,
  `371_date` date null,
  primary key (`uuid`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


insert into `{{reporting_db}}`.`pctdata`
(
  `uuid`, `patent_id`, `doc_type`, `kind`, `doc_number`, `date`, `102_date`, `371_date`
)
select ac.`uuid`,
  ac.`patent_id`, ac.`doc_type`, ac.`kind`, ac.`rel_id`,
  case when ac.`date` > date('1899-12-31') and ac.`date` < date_add(current_date, interval 10 year) then ac.`date` else null end,
  case when ac.`102_date` > date('1899-12-31') and ac.`102_date` < date_add(current_date, interval 10 year) then ac.`102_date` else null end,
  case when ac.`371_date` > date('1899-12-31') and ac.`371_date` < date_add(current_date, interval 10 year) then ac.`371_date` else null end
from
  `{{reporting_db}}`.`patent` p
  inner join `patent`.`pct_data` ac on ac.`patent_id` = p.`patent_id`;

alter table `{{reporting_db}}`.`pctdata` add index `ix_pctdata_102_date` (`102_date`);
alter table `{{reporting_db}}`.`pctdata` add index `ix_pctdata_date` (`date`);
alter table `{{reporting_db}}`.`pctdata` add index `ix_pctdata_doc_number` (`doc_number`);
alter table `{{reporting_db}}`.`pctdata` add index `ix_pctdata_doc_type` (`doc_type`);
alter table `{{reporting_db}}`.`pctdata` add index `ix_pctdata_371_date` (`371_date`);

# END pctdata

###################################################################################################################################