
# BEGIN lawyer 

##############################################################################################################################################


drop table if exists `{{params.reporting_database}}`.`temp_lawyer_num_patents`;
create table `{{params.reporting_database}}`.`temp_lawyer_num_patents`
(
  `lawyer_id` varchar(36) not null,
  `num_patents` int unsigned not null,
  primary key (`lawyer_id`)
)
engine=InnoDB;


# 2:06
insert into `{{params.reporting_database}}`.`temp_lawyer_num_patents`
  (`lawyer_id`, `num_patents`)
select
  `lawyer_id`, count(distinct `patent_id`)
from
  `{{params.raw_database}}`.`patent_lawyer`  pl join `{{ params.raw_database }}`.`patent` p on p.id=pl.patent_id where p.version_indicator <={{ params.version_indicator }} and
   `lawyer_id` is not null
group by
  `lawyer_id`;

drop table if exists `{{params.reporting_database}}`.`temp_lawyer_num_assignees`;
create table `{{params.reporting_database}}`.`temp_lawyer_num_assignees`
(
  `lawyer_id` varchar(36) not null,
  `num_assignees` int unsigned not null,
  primary key (`lawyer_id`)
)
engine=InnoDB;


# 0:15
insert into `{{params.reporting_database}}`.`temp_lawyer_num_assignees`
  (`lawyer_id`, `num_assignees`)
select
  ii.`lawyer_id`, count(distinct aa.`assignee_id`)
from
  `{{params.raw_database}}`.`patent_lawyer` ii
  join `{{params.raw_database}}`.`patent_assignee` aa
  on aa.`patent_id` = ii.`patent_id`  join `{{ params.raw_database }}`.`patent` p on p.id=ii.patent_id where p.version_indicator <={{ params.version_indicator }}
  and `lawyer_id` is not null
group by
  ii.`lawyer_id`;


drop table if exists `{{params.reporting_database}}`.`temp_lawyer_num_inventors`;
create table `{{params.reporting_database}}`.`temp_lawyer_num_inventors`
(
  `lawyer_id` varchar(36) not null,
  `num_inventors` int unsigned not null,
  primary key (`lawyer_id`)
)
engine=InnoDB;

# 0:15
insert into `{{params.reporting_database}}`.`temp_lawyer_num_inventors`
  (`lawyer_id`, `num_inventors`)
select
  aa.`lawyer_id`,
  count(distinct ii.`inventor_id`)
from
  `{{params.raw_database}}`.`patent_lawyer` aa
  join `{{params.raw_database}}`.`patent_inventor` ii on ii.patent_id = aa.patent_id  join `{{ params.raw_database }}`.`patent` p on p.id=aa.patent_id where p.version_indicator <={{ params.version_indicator }}
   and `lawyer_id` is not null
group by
  aa.`lawyer_id`;



drop table if exists `{{params.reporting_database}}`.`temp_lawyer_years_active`;
create table `{{params.reporting_database}}`.`temp_lawyer_years_active`
(
  `lawyer_id` varchar(36) not null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `actual_years_active` smallint unsigned not null,
  primary key (`lawyer_id`)
)
engine=InnoDB;


# 5:42
insert into `{{params.reporting_database}}`.`temp_lawyer_years_active`
  (`lawyer_id`, `first_seen_date`, `last_seen_date`, `actual_years_active`)
select
  pa.`lawyer_id`, min(p.`date`), max(p.`date`),
  ifnull(round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365), 0)
from
  `{{params.raw_database}}`.`patent_lawyer` pa
  inner join `{{params.reporting_database}}`.`patent` p on p.`patent_id`= pa.`patent_id`
where
  p.`date` is not null
and `lawyer_id` is not null
group by
  pa.`lawyer_id`;


drop table if exists `{{params.reporting_database}}`.`patent_lawyer`;
create table `{{params.reporting_database}}`.`patent_lawyer`
(
  `patent_id` varchar(20) not null,
  `lawyer_id` int unsigned not null,
  `sequence` smallint unsigned not null,
  primary key (`patent_id`, `lawyer_id`),
  unique index ak_patent_lawyer (`lawyer_id`, `patent_id`)
)
engine=InnoDB;

create table `{{params.reporting_database}}`.`patent_lawyer_unique` (
select p.patent_id, lawyer_id, min(sequence) sequence
from `patent`.`rawlawyer` rl
	left join patent p on rl.patent_id=p.patent_id
group by 1,2
);


# 12,389,559 @ 29:50
insert into `{{params.reporting_database}}`.`patent_lawyer`
(
  `patent_id`, `lawyer_id`, `sequence`
)
select distinct
  pii.`patent_id`, t.`new_lawyer_id`, ri.`sequence`
from
  `{{params.raw_database}}`.`patent_lawyer` pii
  inner join `{{params.reporting_database}}`.`temp_id_mapping_lawyer` t on t.`old_lawyer_id` = pii.`lawyer_id`
  inner join `{{params.reporting_database}}`.`patent_lawyer_unique` u on u.`patent_id` = pii.`patent_id` and u.`lawyer_id` = pii.`lawyer_id`


drop table if exists `{{params.reporting_database}}`.`lawyer`;
create table `{{params.reporting_database}}`.`lawyer`
(
  `lawyer_id` int unsigned not null,
  `name_first` varchar(64) null,
  `name_last` varchar(64) null,
  `organization` varchar(256) null,
  `num_patents` int unsigned not null,
  `num_assignees` int unsigned not null,
  `num_inventors` int unsigned not null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `years_active` smallint unsigned not null,
  `persistent_lawyer_id` varchar(36) not null,
  primary key (`lawyer_id`)
)
engine=InnoDB;


# 3,572,763 @ 1:57
insert into `{{params.reporting_database}}`.`lawyer`
(
  `lawyer_id`, `name_first`, `name_last`, `organization`, `num_patents`, `num_assignees`, `num_inventors`,
  `first_seen_date`, `last_seen_date`, `years_active`, `persistent_lawyer_id`
)
select
  t.`new_lawyer_id`, nullif(trim(i.`name_first`), ''), nullif(trim(i.`name_last`), ''), nullif(trim(i.`organization`), ''),
  tinp.`num_patents`, ifnull(tina.`num_assignees`, 0), ifnull(tini.`num_inventors`, 0), tifls.`first_seen_date`, tifls.`last_seen_date`,
  ifnull(case when tifls.`actual_years_active` < 1 then 1 else tifls.`actual_years_active` end, 0),
  i.`id`
from
  `{{params.raw_database}}`.`lawyer` i
  inner join `{{params.reporting_database}}`.`temp_id_mapping_lawyer` t on t.`old_lawyer_id` = i.`id`
  inner join `{{params.reporting_database}}`.`temp_lawyer_num_patents` tinp on tinp.`lawyer_id` = i.`id`
  left outer join `{{params.reporting_database}}`.`temp_lawyer_years_active` tifls on tifls.`lawyer_id` = i.`id`
  left outer join `{{params.reporting_database}}`.`temp_lawyer_num_assignees` tina on tina.`lawyer_id` = i.`id`
  left outer join `{{params.reporting_database}}`.`temp_lawyer_num_inventors` tini on tini.`lawyer_id` = i.`id`;


# END lawyer

################################################################################################################################################
