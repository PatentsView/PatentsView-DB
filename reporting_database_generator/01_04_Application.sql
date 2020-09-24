

# BEGIN application 

###########################################################################################################################################


drop table if exists `{{params.reporting_database}}`.`application`;
create table `{{params.reporting_database}}`.`application`
(
  `application_id` varchar(36) not null,
  `patent_id` varchar(20) not null,
  `type` varchar(20) null,
  `number` varchar(64) null,
  `country` varchar(20) null,
  `date` date null,
  primary key (`application_id`, `patent_id`)
)
engine=InnoDB;


# 5,425,879 @ 1:11
insert into `{{params.reporting_database}}`.`application`
  (`application_id`, `patent_id`, `type`, `number`, `country`, `date`)
select
  `id_transformed`, `patent_id`, nullif(trim(`type`), ''),
  nullif(trim(`number_transformed`), ''), nullif(trim(`country`), ''),
  case when `date` > date('1899-12-31') and `date` < date_add(current_date, interval 10 year) then `date` else null end
from
  `{{params.raw_database}}`.`application`;


# END application 

#############################################################################################################################################