
# BEGIN examiner 

##############################################################################################################################################

drop table if exists `{{params.reporting_database}}`.`examiner`;
create table `{{params.reporting_database}}`.`examiner`
(
  `examiner_id` int unsigned not null,
  `name_first` varchar(64) null,
  `name_last` varchar(64) null,
  `role` varchar(20) null,
  `group` varchar(20) null,
  `persistent_examiner_id` varchar(36) not null,
  primary key (`examiner_id`)
)
engine=InnoDB;


# 3,572,763 @ 1:57
insert into `{{params.reporting_database}}`.`examiner`
(
  `examiner_id`, `name_first`, `name_last`, `role`, `group`, `persistent_examiner_id`
)
select
  t.`new_examiner_id`, nullif(trim(i.`name_first`), ''), nullif(trim(i.`name_last`), ''),  nullif(trim(i.`role`), ''),  nullif(trim(i.`group`), ''),
  i.`uuid`
from
  `{{params.raw_database}}`.`rawexaminer` i
  inner join `{{params.reporting_database}}`.`temp_id_mapping_examiner` t on t.`old_examiner_id` = i.`uuid`;


drop table if exists `{{params.reporting_database}}`.`patent_examiner`;
create table `{{params.reporting_database}}`.`patent_examiner`
(
  `patent_id` varchar(20) not null,
  `examiner_id` int unsigned not null,
  `role` varchar(20) not null,
  primary key (`patent_id`, `examiner_id`),
  unique index ak_patent_examiner (`examiner_id`, `patent_id`)
)
engine=InnoDB;


# 12,389,559 @ 29:50
insert into `{{params.reporting_database}}`.`patent_examiner`
(
  `patent_id`, `examiner_id`, `role`
)
select distinct
  ri.`patent_id`, t.`new_examiner_id`, ri.`role`
from
  `{{params.raw_database}}`.`rawexaminer` ri
  inner join `{{params.reporting_database}}`.`temp_id_mapping_examiner` t on t.`old_examiner_id` = ri.`uuid`;

# END examiner

################################################################################################################################################
