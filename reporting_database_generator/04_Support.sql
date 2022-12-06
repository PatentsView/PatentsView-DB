

# BEGIN assignee_inventor ######################################################################################################################


drop table if exists `{{params.reporting_database}}`.`assignee_inventor`;
create table `{{params.reporting_database}}`.`assignee_inventor`
(
  `assignee_id` int unsigned not null,
  `inventor_id` int unsigned not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;


# 4,352,502 @ 1:52
insert into `{{params.reporting_database}}`.`assignee_inventor`
  (`assignee_id`, `inventor_id`, `num_patents`)
select
  pa.assignee_id, pi.inventor_id, count(distinct pa.patent_id)
from
  `{{params.reporting_database}}`.`patent_assignee` pa
  inner join `{{params.reporting_database}}`.`patent_inventor` pi using(patent_id)
group by
  pa.assignee_id, pi.inventor_id;


# END assignee_inventor ######################################################################################################################


# BEGIN inventor_coinventor

######################################################################################################################


drop table if exists `{{params.reporting_database}}`.`inventor_coinventor`;
create table `{{params.reporting_database}}`.`inventor_coinventor`
(
  `inventor_id` int unsigned not null,
  `coinventor_id` int unsigned not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;

# 16,742,248 @ 11:55
insert into `{{params.reporting_database}}`.`inventor_coinventor`
  (`inventor_id`, `coinventor_id`, `num_patents`)
select
  pi.inventor_id, copi.inventor_id, count(distinct copi.patent_id)
from
  `{{params.reporting_database}}`.`patent_inventor` pi
  inner join `{{params.reporting_database}}`.`patent_inventor` copi on pi.patent_id=copi.patent_id and pi.inventor_id<>copi.inventor_id
group by
  pi.inventor_id, copi.inventor_id;


# END inventor_coinventor ######################################################################################################################


# BEGIN inventor_cpc_subsection

######################################################################################################################


drop table if exists `{{params.reporting_database}}`.`inventor_cpc_subsection`;
create table `{{params.reporting_database}}`.`inventor_cpc_subsection`
(
  `inventor_id` int unsigned not null,
  `subsection_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;


# 7,171,415 @ 11:55
insert into `{{params.reporting_database}}`.`inventor_cpc_subsection`
  (`inventor_id`, `subsection_id`, `num_patents`)
select
  pi.inventor_id, c.subsection_id, count(distinct c.patent_id)
from
  `{{params.reporting_database}}`.`patent_inventor` pi
  inner join `{{params.reporting_database}}`.`cpc_current_subsection` c using(patent_id)
where
  c.subsection_id is not null and c.subsection_id != ''
group by
  pi.inventor_id, c.subsection_id;


# END inventor_cpc_subsection

######################################################################################################################


# BEGIN inventor_cpc_group

######################################################################################################################


drop table if exists `{{params.reporting_database}}`.`inventor_cpc_group`;
create table `{{params.reporting_database}}`.`inventor_cpc_group`
(
  `inventor_id` int unsigned not null,
  `group_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;


# 7,171,415 @ 11:55
insert into `{{params.reporting_database}}`.`inventor_cpc_group`
  (`inventor_id`, `group_id`, `num_patents`)
select
  pi.inventor_id, c.group_id, count(distinct c.patent_id)
from
  `{{params.reporting_database}}`.`patent_inventor` pi
  inner join `{{params.reporting_database}}`.`cpc_current_group` c using(patent_id)
where
  c.group_id is not null and c.group_id != ''
group by
  pi.inventor_id, c.group_id;


# END inventor_cpc_group

######################################################################################################################


# BEGIN inventor_nber_subcategory

######################################################################################################################


drop table if exists `{{params.reporting_database}}`.`inventor_nber_subcategory`;
create table `{{params.reporting_database}}`.`inventor_nber_subcategory`
(
  `inventor_id` int unsigned not null,
  `subcategory_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;

#
insert into `{{params.reporting_database}}`.`inventor_nber_subcategory`
  (`inventor_id`, `subcategory_id`, `num_patents`)
select
  pi.inventor_id, n.subcategory_id, count(distinct n.patent_id)
from
  `{{params.reporting_database}}`.`nber` n
  inner join `{{params.reporting_database}}`.`patent_inventor` pi using(patent_id)
where
  n.subcategory_id is not null and n.subcategory_id != ''
group by
  pi.inventor_id, n.subcategory_id;


# END inventor_nber_subcategory

######################################################################################################################


# BEGIN inventor_uspc_mainclass

######################################################################################################################


drop table if exists `{{params.reporting_database}}`.`inventor_uspc_mainclass`;
create table `{{params.reporting_database}}`.`inventor_uspc_mainclass`
(
  `inventor_id` int unsigned not null,
  `mainclass_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;

# 10,350,577 @ 14:44
insert into `{{params.reporting_database}}`.`inventor_uspc_mainclass`
  (`inventor_id`, `mainclass_id`, `num_patents`)
select
  pi.inventor_id, u.mainclass_id, count(distinct pi.patent_id)
from
  `{{params.reporting_database}}`.`patent_inventor` pi
  inner join `{{params.reporting_database}}`.`uspc_current_mainclass` u on pi.patent_id=u.patent_id
group by
  pi.inventor_id, u.mainclass_id;


# END inventor_uspc_mainclass

######################################################################################################################


# BEGIN inventor_year ######################################################################################################################

drop table if exists `{{params.reporting_database}}`.`inventor_year`;
create table `{{params.reporting_database}}`.`inventor_year`
(
  `inventor_id` int unsigned not null,
  `patent_year` smallint not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;

# 8,140,017 @ 2:19
insert into `{{params.reporting_database}}`.`inventor_year`
(`inventor_id`, `patent_year`, `num_patents`)
select
  pi.inventor_id, p.year, count(distinct pi.patent_id)
from
  `{{params.reporting_database}}`.`patent_inventor` pi
  inner join `{{params.reporting_database}}`.`patent` p using(patent_id)
group by
  pi.inventor_id, p.year;


# END inventor_year ######################################################################################################################


# BEGIN assignee_cpc_subsection

######################################################################################################################


drop table if exists `{{params.reporting_database}}`.`assignee_cpc_subsection`;
create table `{{params.reporting_database}}`.`assignee_cpc_subsection`
(
  `assignee_id` int unsigned not null,
  `subsection_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;


# 933,903 @ 2:22
insert into `{{params.reporting_database}}`.`assignee_cpc_subsection`
  (`assignee_id`, `subsection_id`, `num_patents`)
select
  pa.assignee_id, c.subsection_id, count(distinct c.patent_id)
from
  `{{params.reporting_database}}`.`patent_assignee` pa
  inner join `{{params.reporting_database}}`.`cpc_current_subsection` c using(patent_id)
where
  c.subsection_id is not null and c.subsection_id != ''
group by
  pa.assignee_id, c.subsection_id;


# END assignee_cpc_subsection

######################################################################################################################


# BEGIN assignee_cpc_group

######################################################################################################################


drop table if exists `{{params.reporting_database}}`.`assignee_cpc_group`;
create table `{{params.reporting_database}}`.`assignee_cpc_group`
(
  `assignee_id` int unsigned not null,
  `group_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;


# 933,903 @ 2:22
insert into `{{params.reporting_database}}`.`assignee_cpc_group`
  (`assignee_id`, `group_id`, `num_patents`)
select
  pa.assignee_id, c.group_id, count(distinct c.patent_id)
from
  `{{params.reporting_database}}`.`patent_assignee` pa
  inner join `{{params.reporting_database}}`.`cpc_current_group` c using(patent_id)
where
  c.group_id is not null and c.group_id != ''
group by
  pa.assignee_id, c.group_id;


# END assignee_cpc_group

######################################################################################################################

# BEGIN assignee_nber_subcategory

######################################################################################################################


drop table if exists `{{params.reporting_database}}`.`assignee_nber_subcategory`;
create table `{{params.reporting_database}}`.`assignee_nber_subcategory`
(
  `assignee_id` int unsigned not null,
  `subcategory_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;

# 618,873 @ 0:48
insert into `{{params.reporting_database}}`.`assignee_nber_subcategory`
  (`assignee_id`, `subcategory_id`, `num_patents`)
select
  pa.assignee_id, n.subcategory_id, count(distinct n.patent_id)
from
  `{{params.reporting_database}}`.`patent_assignee` pa
  inner join `{{params.reporting_database}}`.`nber` n using(patent_id)
where
  n.subcategory_id is not null and n.subcategory_id != ''
group by
  pa.assignee_id, n.subcategory_id;


# END assignee_nber_subcategory

######################################################################################################################


# BEGIN assignee_uspc_mainclass

######################################################################################################################


drop table if exists `{{params.reporting_database}}`.`assignee_uspc_mainclass`;
create table `{{params.reporting_database}}`.`assignee_uspc_mainclass`
(
  `assignee_id` int unsigned not null,
  `mainclass_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;

# 1,534,644 @ 3:30
insert into `{{params.reporting_database}}`.`assignee_uspc_mainclass`
  (`assignee_id`, `mainclass_id`, `num_patents`)
select
  pa.assignee_id, u.mainclass_id, count(distinct pa.patent_id)
from
  `{{params.reporting_database}}`.`patent_assignee` pa
  inner join `{{params.reporting_database}}`.`uspc_current_mainclass` u on pa.patent_id=u.patent_id
group by
  pa.assignee_id, u.mainclass_id;


# END assignee_uspc_mainclass

######################################################################################################################


# BEGIN assignee_year ######################################################################################################################


drop table if exists `{{params.reporting_database}}`.`assignee_year`;
create table `{{params.reporting_database}}`.`assignee_year`
(
  `assignee_id` int unsigned not null,
  `patent_year` smallint not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;

# 931,856 @ 2:00
insert into `{{params.reporting_database}}`.`assignee_year`
  (`assignee_id`, `patent_year`, `num_patents`)
select
  pa.assignee_id, p.year, count(distinct pa.patent_id)
from
  `{{params.reporting_database}}`.`patent_assignee` pa
  inner join `{{params.reporting_database}}`.`patent` p using(patent_id)
group by
  pa.assignee_id, p.year;


# END assignee_year ######################################################################################################################


# BEGIN location_assignee update num_patents

###################################################################################################################################


# 434,823 @ 0:17
update
  `{{params.reporting_database}}`.`location_assignee` la
  inner join
  (
    select
      `location_id`, `assignee_id`, count(distinct `patent_id`) num_patents
    from
      `{{params.reporting_database}}`.`patent_assignee`
    group by
      `location_id`, `assignee_id`
  ) pa on pa.`location_id` = la.`location_id` and pa.`assignee_id` = la.`assignee_id`
set
  la.`num_patents` = pa.`num_patents`;


# END location_assignee update num_patents

###################################################################################################################################


# BEGIN location_inventor update num_patents

###################################################################################################################################


# 4,167,939 @ 2:33
update
  `{{params.reporting_database}}`.`location_inventor` li
  inner join
  (
    select
      `location_id`, `inventor_id`, count(distinct `patent_id`) num_patents
    from
      `{{params.reporting_database}}`.`patent_inventor`
    group by
      `location_id`, `inventor_id`
  ) pii on pii.`location_id` = li.`location_id` and pii.`inventor_id` = li.`inventor_id`
set
  li.`num_patents` = pii.`num_patents`;


# END location_assignee update num_patents

###################################################################################################################################


# BEGIN location_cpc_subsection

######################################################################################################################


drop table if exists `{{params.reporting_database}}`.`location_cpc_subsection`;
create table `{{params.reporting_database}}`.`location_cpc_subsection`
(
  `location_id` int unsigned not null,
  `subsection_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;


# 1,077,971 @ 6:19
insert into `{{params.reporting_database}}`.`location_cpc_subsection`
  (`location_id`, `subsection_id`, `num_patents`)
select
  tlp.`location_id`, cpc.`subsection_id`, count(distinct tlp.`patent_id`)
from
  `{{params.reporting_database}}`.`temp_location_patent` tlp
  inner join `{{params.reporting_database}}`.`cpc_current_subsection` cpc using(`patent_id`)
group by
  tlp.`location_id`, cpc.`subsection_id`;


# END location_cpc_subsection

######################################################################################################################

# BEGIN location_cpc_group

######################################################################################################################


drop table if exists `{{params.reporting_database}}`.`location_cpc_group`;
create table `{{params.reporting_database}}`.`location_cpc_group`
(
  `location_id` int unsigned not null,
  `group_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;


# 1,077,971 @ 6:19
insert into `{{params.reporting_database}}`.`location_cpc_group`
  (`location_id`, `group_id`, `num_patents`)
select
  tlp.`location_id`, cpc.`group_id`, count(distinct tlp.`patent_id`)
from
  `{{params.reporting_database}}`.`temp_location_patent` tlp
  inner join `{{params.reporting_database}}`.`cpc_current_group` cpc using(`patent_id`)
group by
  tlp.`location_id`, cpc.`group_id`;


# END location_cpc_group

######################################################################################################################


# BEGIN location_uspc_mainclass

######################################################################################################################


drop table if exists `{{params.reporting_database}}`.`location_uspc_mainclass`;
create table `{{params.reporting_database}}`.`location_uspc_mainclass`
(
  `location_id` int unsigned not null,
  `mainclass_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;


# 2,260,351 @ 7:47
insert into `{{params.reporting_database}}`.`location_uspc_mainclass`
  (`location_id`, `mainclass_id`, `num_patents`)
select
  tlp.`location_id`, uspc.`mainclass_id`, count(distinct tlp.`patent_id`)
from
  `{{params.reporting_database}}`.`temp_location_patent` tlp
  inner join `{{params.reporting_database}}`.`uspc_current_mainclass` uspc using(`patent_id`)
group by
  tlp.`location_id`, uspc.`mainclass_id`;


# END location_uspc_mainclass

######################################################################################################################


# BEGIN location_nber_subcategory

######################################################################################################################


drop table if exists `{{params.reporting_database}}`.`location_nber_subcategory`;
create table `{{params.reporting_database}}`.`location_nber_subcategory`
(
  `location_id` int unsigned not null,
  `subcategory_id` varchar(20) not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;


#
insert into `{{params.reporting_database}}`.`location_nber_subcategory`
  (`location_id`, `subcategory_id`, `num_patents`)
select
  tlp.`location_id`, nber.`subcategory_id`, count(distinct tlp.`patent_id`)
from
  `{{params.reporting_database}}`.`temp_location_patent` tlp
  inner join `{{params.reporting_database}}`.`nber` nber using(`patent_id`)
group by
  tlp.`location_id`, nber.`subcategory_id`;


# END location_nber_subcategory

######################################################################################################################


# BEGIN location_year ######################################################################################################################


drop table if exists `{{params.reporting_database}}`.`location_year`;
create table `{{params.reporting_database}}`.`location_year`
(
  `location_id` int unsigned not null,
  `year` smallint not null,
  `num_patents` int unsigned not null
)
engine=InnoDB;


# 867,942 @ 1:19
insert into `{{params.reporting_database}}`.`location_year`
  (`location_id`, `year`, `num_patents`)
select
  tlp.`location_id`, p.`year`, count(distinct tlp.`patent_id`)
from
  `{{params.reporting_database}}`.`temp_location_patent` tlp
  inner join `{{params.reporting_database}}`.`patent` p using(`patent_id`)
group by
  tlp.`location_id`, p.`year`;


# END location_year ######################################################################################################################

# BEGIN inventor_rawinventor alias 

###############################################################################################################################
drop table if exists `{{params.reporting_database}}`.`inventor_rawinventor`;
create table if not exists `{{params.reporting_database}}`.`inventor_rawinventor` (uuid int(10) unsigned AUTO_INCREMENT PRIMARY KEY,name_first varchar(64),name_last varchar(64),patent_id varchar(20),inventor_id int(10) unsigned);

INSERT INTO `{{params.reporting_database}}`.`inventor_rawinventor` (name_first,name_last,patent_id,inventor_id)
SELECT DISTINCT ri.name_first,ri.name_last,ri.patent_id,repi.inventor_id
FROM `{{params.reporting_database}}`.`inventor` repi
left join `{{params.raw_database}}`.`rawinventor` ri
on ri.inventor_id = repi.persistent_inventor_id;

alter table `{{params.reporting_database}}`.`inventor_rawinventor` add index `ix_inventor_rawinventor_name_first` (`name_first`);
alter table `{{params.reporting_database}}`.`inventor_rawinventor` add index `ix_inventor_rawinventor_name_last` (`name_last`);
alter table `{{params.reporting_database}}`.`inventor_rawinventor` add index `ix_inventor_rawinventor_inventor_id` (`inventor_id`);
alter table `{{params.reporting_database}}`.`inventor_rawinventor` add index `ix_inventor_rawinventor_patent_id` (`patent_id`);

# END inventor_rawinventor alias 

###############################################################################################################################