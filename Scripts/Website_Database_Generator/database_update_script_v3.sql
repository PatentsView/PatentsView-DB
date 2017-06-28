

# BEGIN lawyer

##############################################################################################################################################

drop table if exists `PatentsView_dev`.`patent_lawyer`;
create table `PatentsView_dev`.`patent_lawyer`
(
  `patent_id` varchar(20) not null,
  `lawyer_id` varchar(36) not null,
  `sequence` int unsigned,
  primary key (`patent_id`, `lawyer_id`),
  unique index ak_patent_lawyer (`lawyer_id`, `patent_id`)
)
engine=InnoDB;


insert into `PatentsView_dev`.`patent_lawyer`
(
  `patent_id`, `lawyer_id`, `sequence`
)
select distinct patent_id, lawyer_id, min(sequence) sequence from `new_parser_qa`.`rawlawyer` group by patent_id, lawyer_id;

drop table if exists `PatentsView_dev`.`temp_lawyer_num_assignees`;
create table `PatentsView_dev`.`temp_lawyer_num_assignees`
(
  `lawyer_id` varchar(36) not null,
  `num_assignees` int unsigned not null,
  primary key (`lawyer_id`)
)
engine=InnoDB;


# 0:15
insert into `PatentsView_dev`.`temp_lawyer_num_assignees`
  (`lawyer_id`, `num_assignees`)
select
  ii.`lawyer_id`, count(distinct aa.`assignee_id`)
from
  `new_parser_qa`.`patent_lawyer` ii
  join `new_parser_qa`.`patent_assignee` aa
  on aa.`patent_id` = ii.`patent_id`
group by
  ii.`lawyer_id`;


drop table if exists `PatentsView_dev`.`temp_lawyer_num_inventors`;
create table `PatentsView_dev`.`temp_lawyer_num_inventors`
(
  `lawyer_id` varchar(36) not null,
  `num_inventors` int unsigned not null,
  primary key (`lawyer_id`)
)
engine=InnoDB;


# 0:15
insert into `PatentsView_dev`.`temp_lawyer_num_inventors`
  (`lawyer_id`, `num_inventors`)
select
  ii.`lawyer_id`, count(distinct aa.`inventor_id`)
from
  `new_parser_qa`.`patent_lawyer` ii
  join `new_parser_qa`.`patent_inventor` aa
  on aa.`patent_id` = ii.`patent_id`
group by
  ii.`lawyer_id`;


drop table if exists `PatentsView_dev`.`temp_lawyer_num_patents`;
create table `PatentsView_dev`.`temp_lawyer_num_patents`
(
  `lawyer_id` varchar(36) not null,
  `num_patents` int unsigned not null,
  primary key (`lawyer_id`)
)
engine=InnoDB;

# 0:15
insert into `PatentsView_dev`.`temp_lawyer_num_patents`
  (`lawyer_id`, `num_patents`)
select
  `lawyer_id`, count(distinct `patent_id`)
from
  `new_parser_qa`.`patent_lawyer`
group by
  `lawyer_id`;



drop table if exists `PatentsView_dev`.`temp_lawyer_years_active`;
create table `PatentsView_dev`.`temp_lawyer_years_active`
(
  `lawyer_id` varchar(36) not null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `actual_years_active` smallint unsigned not null,
  primary key (`lawyer_id`)
)
engine=InnoDB;


# 5:42
insert into `PatentsView_dev`.`temp_lawyer_years_active`
  (`lawyer_id`, `first_seen_date`, `last_seen_date`, `actual_years_active`)
select
  pa.`lawyer_id`, min(p.`date`), max(p.`date`),
  ifnull(round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365), 0)
from
  `PatentsView_dev`.`patent_lawyer` pa
  inner join `new_parser_qa`.`patent` p on p.`id`= pa.`patent_id`
where
  p.`date` is not null
group by
  pa.`lawyer_id`;

drop table if exists `PatentsView_dev`.`lawyer`;
create table `PatentsView_dev`.`lawyer`
(
  `id` varchar(36) NOT NULL,
  `name_first` varchar(64) DEFAULT NULL,
  `name_last` varchar(64) DEFAULT NULL,
  `organization` varchar(64) DEFAULT NULL,
  `num_patents` int unsigned not null,
  `num_inventors` int unsigned not null,
  `num_assignees` int unsigned not null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `years_active` smallint unsigned not null,
  PRIMARY KEY (`id`)
)
engine=InnoDB;

insert into `PatentsView_dev`.`lawyer`
  (`id`, `name_first`, `name_last`, `organization`,`num_patents`, `num_inventors`,
  `num_assignees`,`first_seen_date`,`last_seen_date`, `years_active`
)

select
  r.`id`, nullif(trim(r.`name_first`), ''), nullif(trim(r.`name_last`), ''), nullif(trim(r.`organization`), ''),
  p.`num_patents`, ifnull(i.`num_inventors`, 0), ifnull(a.`num_assignees`, 0),
  y.`first_seen_date`, y.`last_seen_date`,
  ifnull(case when y.`actual_years_active` < 1 then 1 else y.`actual_years_active` end, 0)
from
  `new_parser_qa`.`lawyer` r
  left outer join `PatentsView_dev`.`temp_lawyer_num_patents` p on p.`lawyer_id` = r.`id`
  left outer join `PatentsView_dev`.`temp_lawyer_num_inventors` i on i.`lawyer_id` = r.`id`
  left outer join `PatentsView_dev`.`temp_lawyer_num_assignees` a on a.`lawyer_id` = r.`id`
  left outer join `PatentsView_dev`.`temp_lawyer_years_active` y on y.`lawyer_id` = r.`id`;





################################################################################################################################################
# BEGIN examiner

##############################################################################################################################################


drop table if exists `PatentsView_dev`.`patent_examiner`;
create table `PatentsView_dev`.`patent_examiner`
(
  `patent_id` varchar(36) not null,
  `examiner_id` varchar(36)  not null,
  primary key (`patent_id`, `examiner_id`),
  unique index ak_patent_examiner (`examiner_id`, `patent_id`)
)
engine=InnoDB;

insert into `PatentsView_dev`.`patent_examiner`
( `patent_id`, `examiner_id`
)
select distinct patent_id, uuid
from `new_parser_qa`.`rawexaminer` group by patent_id, uuid;



drop table if exists `PatentsView_dev`.`temp_patent_primary_examiner`;
create table `PatentsView_dev`.`temp_patent_primary_examiner`
(
  `patent_id` varchar(36) not null,
  `examiner_id` varchar(36)  not null,
  primary key (`patent_id`, `examiner_id`),
  unique index ak_patent_examiner (`examiner_id`, `patent_id`)
)
engine=InnoDB;

insert into `PatentsView_dev`.`temp_patent_primary_examiner`
(
  `patent_id`, `examiner_id`
)
select distinct patent_id, uuid from `new_parser_qa`.`rawexaminer` where role = 'primary' group by patent_id, uuid;

#### 
#add assignee and examiner lookups

drop table if exists `PatentsView_dev`.`temp_assignee_num_lawyers`;
create table `PatentsView_dev`.`temp_assignee_num_lawyers`
(
  `assignee_id` varchar(36) not null,
  `num_lawyers` int unsigned not null,
  primary key (`assignee_id`)
)
engine=InnoDB;

# 0:15
insert into `PatentsView_dev`.`temp_assignee_num_lawyers`
  (`assignee_id`, `num_lawyers`)
select
  aa.`assignee_id`,
  count(distinct ii.`lawyer_id`)
from
  `new_parser_qa`.`patent_assignee` aa
  join `new_parser_qa`.`patent_lawyer` ii on ii.patent_id = aa.patent_id
group by
  aa.`assignee_id`;


drop table if exists `PatentsView_dev`.`temp_inventor_num_lawyers`;
create table `PatentsView_dev`.`temp_inventor_num_lawyers`
(
  `inventor_id` varchar(36) not null,
  `num_lawyers` int unsigned not null,
  primary key (`inventor_id`)
)
engine=InnoDB;


# 2:06
insert into `PatentsView_dev`.`temp_inventor_num_lawyers`
  (`inventor_id`, `num_lawyers`)
select
  aa.`inventor_id`,
  count(distinct ii.`lawyer_id`)
from
  `new_parser_qa`.`patent_inventor` aa
  join `new_parser_qa`.`patent_lawyer` ii on ii.patent_id = aa.patent_id
group by
  aa.`inventor_id`;


drop table if exists `PatentsView_dev`.`examiner`;
create table `PatentsView_dev`.`examiner`
(
  `id` varchar(36) NOT NULL,
  `name_first` varchar(64) DEFAULT NULL,
  `name_last` varchar(64) DEFAULT NULL,
  `group` varchar(32) DEFAULT NULL,
  `role` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`)
)
engine=InnoDB;

insert into `PatentsView_dev`.`examiner`
  (`id`, `name_first`, `name_last`, `group`, `role`)
select
  `uuid`, `name_first`, `name_last`, `group`, `role`
from
  `new_parser_qa`.`rawexaminer`;

#########################################################################################################
#usreldoc
#########################################################################################################
drop table if exists `PatentsView_dev`.`usrelateddocuments`;
create table `PatentsView_dev`.`usrelateddocuments`
as select `patent_id`,`doctype` as `document_type`,`relkind` as `relationship`,
`reldocno` as `document_number`,`country`,`date`,`status`,`sequence`,`kind`
from `new_parser_qa`.`usreldoc`;


drop table if exists `PatentsView_dev`.`temp_num_us_rel_doc`;
create table `PatentsView_dev`.`temp_num_us_rel_doc`
(
  `patent_id` varchar(20) not null,
  `num_us_rel_doc` int unsigned not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# The number of us related documents for a given patent
insert into `PatentsView_dev`.`temp_num_us_rel_doc`
  (`patent_id`, `num_us_rel_doc`)
select
  `patent_id`, count(*)
from
  `new_parser_qa`.`usreldoc`
group by
  `patent_id`;



###########################################################################################
#foriegn priority
############################################################################################
drop table if exists `PatentsView_dev`.`foreign_priority`;
create table `PatentsView_dev`.`foreign_priority`
as select `patent_id`, `sequence`, `kind`, `number`, `date`, `country`
from `new_parser_qa`.`foreign_priority`;


###########################################################################################
#related application text
############################################################################################
drop table if exists `PatentsView_dev`.`rel_app_text`;
create table `PatentsView_dev`.`rel_app_text`
as select `patent_id`, `text`
from `new_parser_qa`.`rel_app_text`;

##########################################################################################


###########################################################################################
#pct data
############################################################################################
drop table if exists `PatentsView_dev`.`pct_data`;
create table `PatentsView_dev`.`pct_data`
as select `patent_id`,`rel_id` as `related_patent_id`,`date`,`371_date`,`country`,`kind`,`doc_type` as `document_type`,`102_date`

from `new_parser_qa`.`pct_data`;

##########################################################################################

drop table if exists `PatentsView_dev`.`temp_id_mapping_inventor`;
create table `PatentsView_dev`.`temp_id_mapping_inventor`
(
  `old_inventor_id` varchar(36) not null,
  `new_inventor_id` int unsigned not null auto_increment,
  primary key (`old_inventor_id`),
  unique index `ak_temp_id_mapping_inventor` (`new_inventor_id`)
)
engine=InnoDB;


# There are inventors in the raw data that are not linked to anything so we will take our
# inventor ids from the patent_inventor table to ensure we don't copy any unused inventors over.
# 3,572,763 @ 1:08
insert into
  `PatentsView_dev`.`temp_id_mapping_inventor` (`old_inventor_id`)
select distinct
  `inventor_id`
from
  `new_parser_qa`.`patent_inventor`;
  
  drop table if exists `PatentsView_dev`.`temp_id_mapping_location_transformed`;
create table `PatentsView_dev`.`temp_id_mapping_location_transformed`

(
  `old_location_id_transformed` varchar(128) not null,
  `new_location_id` int unsigned not null auto_increment,
  primary key (`old_location_id_transformed`),
  unique index `ak_temp_id_mapping_location_transformed` (`new_location_id`)
)
engine=InnoDB;


# 97,725 @ 0:02
insert into
  `PatentsView_dev`.`temp_id_mapping_location_transformed` (`old_location_id_transformed`)
select distinct
  `location_id_transformed`
from
  `new_parser_qa`.`rawlocation`
where
  `location_id_transformed` is not null and `location_id_transformed` != '';


drop table if exists `PatentsView_dev`.`temp_id_mapping_location`;
create table `PatentsView_dev`.`temp_id_mapping_location`
(
  `old_location_id` varchar(128) not null,
  `new_location_id` int unsigned not null,
  primary key (`old_location_id`),
  index `ak_temp_id_mapping_location` (`new_location_id`)
)
engine=InnoDB;

# 120,449 @ 3:27
insert into
  `PatentsView_dev`.`temp_id_mapping_location` (`old_location_id`, `new_location_id`)
select distinct
  rl.`location_id`,
  t.`new_location_id`
from
  (select distinct `location_id`, `location_id_transformed` from `new_parser_qa`.`rawlocation` where `location_id` is not null and 

`location_id` != '') rl
  inner join `PatentsView_dev`.`temp_id_mapping_location_transformed` t on
    t.`old_location_id_transformed` = rl.`location_id_transformed`;


  
  
  
  
drop table if exists `PatentsView_dev`.`temp_inventor_lastknown_location`;
create table `PatentsView_dev`.`temp_inventor_lastknown_location`
(
  `inventor_id` varchar(36) not null,
  `location_id` int unsigned null,
  `persistent_location_id` varchar(128) null,
  `city` varchar(128) null,
  `state` varchar(20) null,
  `country` varchar(10) null,
  `latitude` float null,
  `longitude` float null,
  primary key (`inventor_id`)
)
engine=InnoDB;


# Populate temp_inventor_lastknown_location table.  The goal here is to grab the location associated
# with the most recent patent associated with the inventor.  It is possible for a patent/inventor
# combination not to have a location, so we will grab the most recent KNOWN location.
# 3,437,668 @ 22:05
insert into `PatentsView_dev`.`temp_inventor_lastknown_location`
(
  `inventor_id`, `location_id`, `persistent_location_id`, `city`, `state`, `country`, `latitude`, `longitude`
)
select
  t.`inventor_id`,
  tl.`new_location_id`,
  tl.`old_location_id_transformed`,
  nullif(trim(l.`city`), ''),
  nullif(trim(l.`state`), ''),
  nullif(trim(l.`country`), ''),
  l.`latitude`,
  l.`longitude`
from
  (
    select
      t.`inventor_id`,
      t.`location_id`,
      t.`location_id_transformed`
    from
      (
        select
          @rownum := case when @inventor_id = t.`inventor_id` then @rownum + 1 else 1 end `rownum`,
          @inventor_id := t.`inventor_id` `inventor_id`,
          t.`location_id`,
	  t.`location_id_transformed`
        from
          (
            select
              ri.`inventor_id`,
              rl.`location_id`,
	      rl.`location_id_transformed`
            from
              `new_parser_qa`.`rawinventor` ri
              inner join `new_parser_qa`.`patent` p on p.`id` = ri.`patent_id`
              inner join `new_parser_qa`.`rawlocation` rl on rl.`id` = ri.`rawlocation_id`
            where
              ri.`inventor_id` is not null and
              rl.`location_id` is not null
            order by
              ri.`inventor_id`,
              p.`date` desc,
              p.`id` desc
          ) t,
          (select @rownum := 0, @inventor_id := '') r
      ) t
    where
      t.`rownum` < 2
  ) t
  left outer join `new_parser_qa`.`location` l on l.`id` = t.`location_id`
  left outer join `PatentsView_dev`.`temp_id_mapping_location_transformed` tl on tl.`old_location_id_transformed` = 

t.`location_id_transformed`;


drop table if exists `PatentsView_dev`.`temp_inventor_num_patents`;
create table `PatentsView_dev`.`temp_inventor_num_patents`
(
  `inventor_id` varchar(36) not null,
  `num_patents` int unsigned not null,
  primary key (`inventor_id`)
)
engine=InnoDB;


# 2:06
insert into `PatentsView_dev`.`temp_inventor_num_patents`
  (`inventor_id`, `num_patents`)
select
  `inventor_id`, count(distinct `patent_id`)
from
  `new_parser_qa`.`patent_inventor`
group by
  `inventor_id`;

drop table if exists `PatentsView_dev`.`temp_inventor_num_assignees`;
create table `PatentsView_dev`.`temp_inventor_num_assignees`
(
  `inventor_id` varchar(36) not null,
  `num_assignees` int unsigned not null,
  primary key (`inventor_id`)
)
engine=InnoDB;


# 0:15
insert into `PatentsView_dev`.`temp_inventor_num_assignees`
  (`inventor_id`, `num_assignees`)
select
  ii.`inventor_id`, count(distinct aa.`assignee_id`)
from
  `new_parser_qa`.`patent_inventor` ii
  join `new_parser_qa`.`patent_assignee` aa
  on aa.`patent_id` = ii.`patent_id`
group by
  ii.`inventor_id`;


drop table if exists `PatentsView_dev`.`temp_inventor_years_active`;
create table `PatentsView_dev`.`temp_inventor_years_active`
(
  `inventor_id` varchar(36) not null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `actual_years_active` smallint unsigned not null,
  primary key (`inventor_id`)
)
engine=InnoDB;


# 5:42
insert into `PatentsView_dev`.`temp_inventor_years_active`
  (`inventor_id`, `first_seen_date`, `last_seen_date`, `actual_years_active`)
select
  pa.`inventor_id`, min(p.`date`), max(p.`date`),
  ifnull(round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365), 0)
from
  `new_parser_qa`.`patent_inventor` pa
  inner join `PatentsView_dev`.`patent` p on p.`patent_id`= pa.`patent_id`
where
  p.`date` is not null
group by
  pa.`inventor_id`;

drop table if exists `PatentsView_dev`.`inventor`;
create table `PatentsView_dev`.`inventor`
(
  `inventor_id` int unsigned not null,
  `name_first` varchar(64) null,
  `name_last` varchar(64) null,
  `num_patents` int unsigned not null,
  `num_assignees` int unsigned not null,
  `lastknown_location_id` int unsigned null,
  `lastknown_persistent_location_id` varchar(128) null,
  `lastknown_city` varchar(128) null,
  `lastknown_state` varchar(20) null,
  `lastknown_country` varchar(10) null,
  `lastknown_latitude` float null,
  `lastknown_longitude` float null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `years_active` smallint unsigned not null,
  `persistent_inventor_id` varchar(36) not null,
  `num_lawyers` int unsigned not null,
  primary key (`inventor_id`)
)
engine=InnoDB;


# 3,572,763 @ 1:57
insert into `PatentsView_dev`.`inventor`
(
  `inventor_id`, `name_first`, `name_last`, `num_patents`, `num_assignees`,
  `lastknown_location_id`, `lastknown_persistent_location_id`, `lastknown_city`,
  `lastknown_state`, `lastknown_country`, `lastknown_latitude`, `lastknown_longitude`,
  `first_seen_date`, `last_seen_date`, `years_active`, `persistent_inventor_id`, `num_lawyers`
)
select
  t.`new_inventor_id`, nullif(trim(i.`name_first`), ''), nullif(trim(i.`name_last`), ''),
  tinp.`num_patents`, ifnull(tina.`num_assignees`, 0), tilkl.`location_id`, tilkl.`persistent_location_id`, tilkl.`city`, tilkl.`state`,
  tilkl.`country`, tilkl.`latitude`, tilkl.`longitude`, tifls.`first_seen_date`, tifls.`last_seen_date`,
  ifnull(case when tifls.`actual_years_active` < 1 then 1 else tifls.`actual_years_active` end, 0),
  i.`id`, ifnull(tinl.`num_lawyers`, 0)
from
  `new_parser_qa`.`inventor` i
  inner join `PatentsView_dev`.`temp_id_mapping_inventor` t on t.`old_inventor_id` = i.`id`
  left outer join `PatentsView_dev`.`temp_inventor_lastknown_location` tilkl on tilkl.`inventor_id` = i.`id`
  inner join `PatentsView_dev`.`temp_inventor_num_patents` tinp on tinp.`inventor_id` = i.`id`
  left outer join `PatentsView_dev`.`temp_inventor_years_active` tifls on tifls.`inventor_id` = i.`id`
  left outer join `PatentsView_dev`.`temp_inventor_num_assignees` tina on tina.`inventor_id` = i.`id`
  left outer join `PatentsView_dev`.`temp_inventor_num_lawyers` tinl on tinl.`inventor_id` = i.`id`;





drop table if exists `PatentsView_dev`.`temp_id_mapping_assignee`;
create table `PatentsView_dev`.`temp_id_mapping_assignee`
(
  `old_assignee_id` varchar(36) not null,
  `new_assignee_id` int unsigned not null auto_increment,
  primary key (`old_assignee_id`),
  unique index `ak_temp_id_mapping_assignee` (`new_assignee_id`)
)
engine=InnoDB;


# There are assignees in the raw data that are not linked to anything so we will take our
# assignee ids from the patent_assignee table to ensure we don't copy any unused assignees over.
# 345,185 @ 0:23
insert into
  `PatentsView_dev`.`temp_id_mapping_assignee` (`old_assignee_id`)
select distinct
  pa.`assignee_id`
from
  `new_parser_qa`.`patent_assignee` pa;




drop table if exists `PatentsView_dev`.`temp_patent_firstnamed_assignee`;
create table `PatentsView_dev`.`temp_patent_firstnamed_assignee`
(
  `patent_id` varchar(20) not null,
  `assignee_id` int unsigned null,
  `persistent_assignee_id` varchar(36) null,
  `location_id` int unsigned null,
  `persistent_location_id` varchar(128) null,
  `city` varchar(128) null,
  `state` varchar(20) null,
  `country` varchar(10) null,
  `latitude` float null,
  `longitude` float null,
  primary key (`patent_id`)
)
engine=InnoDB;


# 4,694,651 @ 2:22
insert into `PatentsView_dev`.`temp_patent_firstnamed_assignee`
(
  `patent_id`, `assignee_id`, `persistent_assignee_id`, `location_id`,
  `persistent_location_id`, `city`, `state`, `country`, `latitude`, `longitude`
)
select
  p.`id`,
  ta.`new_assignee_id`,
  ta.`old_assignee_id`,
  tl.`new_location_id`,
  tl.`old_location_id_transformed`,
  nullif(l.`city`, ''),
  nullif(l.`state`, ''),
  nullif(l.`country`, ''),
  l.`latitude`,
  l.`longitude`
from
  `new_parser_qa`.`patent` p
  left outer join `new_parser_qa`.`rawassignee` ra on ra.`patent_id` = p.`id` and ra.`sequence` = 0
  left outer join `PatentsView_dev`.`temp_id_mapping_assignee` ta on ta.`old_assignee_id` = ra.`assignee_id`
  left outer join `new_parser_qa`.`rawlocation` rl on rl.`id` = ra.`rawlocation_id`
  left outer join `new_parser_qa`.`location` l on l.`id` = rl.`location_id`
  left outer join `PatentsView_dev`.`temp_id_mapping_location_transformed` tl on tl.`old_location_id_transformed` = 

rl.`location_id_transformed`
where
  ta.`new_assignee_id` is not null or
  tl.`new_location_id` is not null;


drop table if exists `PatentsView_dev`.`temp_patent_firstnamed_inventor`;
create table `PatentsView_dev`.`temp_patent_firstnamed_inventor`
(
  `patent_id` varchar(20) not null,
  `inventor_id` int unsigned null,
  `persistent_inventor_id` varchar(36) null,
  `location_id` int unsigned null,
  `persistent_location_id` varchar(128) null,
  `city` varchar(128) null,
  `state` varchar(20) null,
  `country` varchar(10) null,
  `latitude` float null,
  `longitude` float null,
  primary key (`patent_id`)
)
engine=InnoDB;


# 5,425,008 @ 6:03
insert into `PatentsView_dev`.`temp_patent_firstnamed_inventor`
(
  `patent_id`, `inventor_id`, `persistent_inventor_id`, `location_id`,
  `persistent_location_id`, `city`, `state`, `country`, `latitude`, `longitude`
)
select
  p.`id`,
  ti.`new_inventor_id`,
  ti.`old_inventor_id`,
  tl.`new_location_id`,
  tl.`old_location_id_transformed`,
  nullif(l.`city`, ''),
  nullif(l.`state`, ''),
  nullif(l.`country`, ''),
  l.`latitude`,
  l.`longitude`
from
  `new_parser_qa`.`patent` p
  left outer join `new_parser_qa`.`rawinventor` ri on ri.`patent_id` = p.`id` and ri.`sequence` = 0
  left outer join `PatentsView_dev`.`temp_id_mapping_inventor` ti on ti.`old_inventor_id` = ri.`inventor_id`
  left outer join `new_parser_qa`.`rawlocation` rl on rl.`id` = ri.`rawlocation_id`
  left outer join `new_parser_qa`.`location` l on l.`id` = rl.`location_id`
  left outer join `PatentsView_dev`.`temp_id_mapping_location_transformed` tl on tl.`old_location_id_transformed` = 

rl.`location_id_transformed`
where
  ti.`new_inventor_id` is not null or
  tl.`new_location_id` is not null;


drop table if exists `PatentsView_dev`.`temp_num_foreign_documents_cited`;
create table `PatentsView_dev`.`temp_num_foreign_documents_cited`
(
  `patent_id` varchar(20) not null,
  `num_foreign_documents_cited` int unsigned not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# The number of foreign documents cited.
# 2,751,072 @ 1:52
insert into `PatentsView_dev`.`temp_num_foreign_documents_cited`
  (`patent_id`, `num_foreign_documents_cited`)
select
  `patent_id`, count(*)
from
  `new_parser_qa`.`foreigncitation`
group by
  `patent_id`;


drop table if exists `PatentsView_dev`.`temp_num_us_applications_cited`;
create table `PatentsView_dev`.`temp_num_us_applications_cited`
(
  `patent_id` varchar(20) not null,
  `num_us_applications_cited` int unsigned not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# The number of U.S. patent applications cited.
# 1,534,484 @ 0:21
insert into `PatentsView_dev`.`temp_num_us_applications_cited`
  (`patent_id`, `num_us_applications_cited`)
select
  `patent_id`, count(*)
from
  `new_parser_qa`.`usapplicationcitation`
group by
  `patent_id`;


drop table if exists `PatentsView_dev`.`temp_num_us_patents_cited`;
create table `PatentsView_dev`.`temp_num_us_patents_cited`
(
  `patent_id` varchar(20) not null,
  `num_us_patents_cited` int unsigned not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# The number of U.S. patents cited.
# 5,231,893 @ 7:17
insert into `PatentsView_dev`.`temp_num_us_patents_cited`
  (`patent_id`, `num_us_patents_cited`)
select
  `patent_id`, count(*)
from
  `new_parser_qa`.`uspatentcitation`
group by
  `patent_id`;


drop table if exists `PatentsView_dev`.`temp_num_times_cited_by_us_patents`;
create table `PatentsView_dev`.`temp_num_times_cited_by_us_patents`
(
  `patent_id` varchar(20) not null,
  `num_times_cited_by_us_patents` int unsigned not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# The number of times a U.S. patent was cited.
# 6,333,277 @ 7:27
insert into `PatentsView_dev`.`temp_num_times_cited_by_us_patents`
  (`patent_id`, `num_times_cited_by_us_patents`)
select
  `citation_id`, count(*)
from
  `new_parser_qa`.`uspatentcitation`
where
  `citation_id` is not null and `citation_id` != ''
group by
  `citation_id`;


drop table if exists `PatentsView_dev`.`temp_patent_aggregations`;
create table `PatentsView_dev`.`temp_patent_aggregations`
(
  `patent_id` varchar(20) not null,
  `num_foreign_documents_cited` int unsigned not null,
  `num_us_applications_cited` int unsigned not null,
  `num_us_patents_cited` int unsigned not null,
  `num_total_documents_cited` int unsigned not null,
  `num_times_cited_by_us_patents` int unsigned not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# Combine all of our patent aggregations.
# 5,425,879 @ 2:14
insert into `PatentsView_dev`.`temp_patent_aggregations`
(
  `patent_id`, `num_foreign_documents_cited`, `num_us_applications_cited`,
  `num_us_patents_cited`, `num_total_documents_cited`, `num_times_cited_by_us_patents`
)
select
  p.`id`,
  ifnull(t1.num_foreign_documents_cited, 0),
  ifnull(t2.num_us_applications_cited, 0),
  ifnull(t3.num_us_patents_cited, 0),
  ifnull(t1.num_foreign_documents_cited, 0) + ifnull(t2.num_us_applications_cited, 0) + ifnull(t3.num_us_patents_cited, 0),
  ifnull(t4.num_times_cited_by_us_patents, 0)
from
  `new_parser_qa`.`patent` p
  left outer join `PatentsView_dev`.`temp_num_foreign_documents_cited` t1 on t1.`patent_id` = p.`id`
  left outer join `PatentsView_dev`.`temp_num_us_applications_cited` t2 on t2.`patent_id` = p.`id`
  left outer join `PatentsView_dev`.`temp_num_us_patents_cited` t3 on t3.`patent_id` = p.`id`
  left outer join `PatentsView_dev`.`temp_num_times_cited_by_us_patents` t4 on t4.`patent_id` = p.`id`;


drop table if exists `PatentsView_dev`.`temp_patent_earliest_application_date`;
create table `PatentsView_dev`.`temp_patent_earliest_application_date`
(
  `patent_id` varchar(20) not null,
  `earliest_application_date` date not null,
  primary key (`patent_id`)
)
engine=InnoDB;


# Find the earliest application date for each patent.
# 5,425,837 @ 1:35
insert into `PatentsView_dev`.`temp_patent_earliest_application_date`
  (`patent_id`, `earliest_application_date`)
select
  a.`patent_id`, min(a.`date`)
from
  `new_parser_qa`.`application` a
where
  a.`date` is not null and a.`date` > date('1899-12-31') and a.`date` < date_add(current_date, interval 10 year)
group by
  a.`patent_id`;


drop table if exists `PatentsView_dev`.`temp_patent_date`;
create table `PatentsView_dev`.`temp_patent_date`
(
  `patent_id` varchar(20) not null,
  `date` date null,
  primary key (`patent_id`)
)
engine=InnoDB;



# Eliminate obviously bad patent dates.
# 5,425,875 @ 0:37
insert into `PatentsView_dev`.`temp_patent_date`
  (`patent_id`, `date`)
select
  p.`id`, p.`date`
from
  `new_parser_qa`.`patent` p
where
  p.`date` is not null and p.`date` > date('1899-12-31') and p.`date` < date_add(current_date, interval 10 year);



drop table if exists `PatentsView_dev`.`patent`;
create table `PatentsView_dev`.`patent`
(
  `patent_id` varchar(20) not null,
  `type` varchar(100) null,
  `number` varchar(64) not null,
  `country` varchar(20) null,
  `date` date null,
  `year` smallint unsigned null,
  `abstract` text null,
  `title` text null,
  `kind` varchar(10) null,
  `num_claims` smallint unsigned null,
  `firstnamed_assignee_id` int unsigned null,
  `firstnamed_assignee_persistent_id` varchar(36) null,
  `firstnamed_assignee_location_id` int unsigned null,
  `firstnamed_assignee_persistent_location_id` varchar(128) null,
  `firstnamed_assignee_city` varchar(128) null,
  `firstnamed_assignee_state` varchar(20) null,
  `firstnamed_assignee_country` varchar(10) null,
  `firstnamed_assignee_latitude` float null,
  `firstnamed_assignee_longitude` float null,
  `firstnamed_inventor_id` int unsigned null,
  `firstnamed_inventor_persistent_id` varchar(36) null,
  `firstnamed_inventor_location_id` int unsigned null,
  `firstnamed_inventor_persistent_location_id` varchar(128) null,
  `firstnamed_inventor_city` varchar(128) null,
  `firstnamed_inventor_state` varchar(20) null,
  `firstnamed_inventor_country` varchar(10) null,
  `firstnamed_inventor_latitude` float null,
  `firstnamed_inventor_longitude` float null,
  `num_foreign_documents_cited` int unsigned not null,
  `num_us_applications_cited` int unsigned not null,
  `num_us_patents_cited` int unsigned not null,
  `num_total_documents_cited` int unsigned not null,
  `num_times_cited_by_us_patents` int unsigned not null,
  `num_usrelateddocuments` int unsigned not null,
  `earliest_application_date` date null,
  `patent_processing_days` int unsigned null,
  `uspc_current_mainclass_average_patent_processing_days` int unsigned null, # This will have to be updated once we've calculated the value in the uspc_current section below.
 `cpc_current_group_average_patent_processing_days` int unsigned null, # This will have to be updated once we've calculated the value in the uspc_current section below.
  primary key (`patent_id`)
)
engine=InnoDB;


# 5,425,879 @ 6:45
insert into `PatentsView_dev`.`patent`
(
  `patent_id`, `type`, `number`, `country`, `date`, `year`,
  `abstract`, `title`, `kind`, `num_claims`,
  `firstnamed_assignee_id`, `firstnamed_assignee_persistent_id`,
  `firstnamed_assignee_location_id`, `firstnamed_assignee_persistent_location_id`,
  `firstnamed_assignee_city`, `firstnamed_assignee_state`,
  `firstnamed_assignee_country`, `firstnamed_assignee_latitude`,
  `firstnamed_assignee_longitude`, `firstnamed_inventor_id`,
  `firstnamed_inventor_persistent_id`, `firstnamed_inventor_location_id`,
  `firstnamed_inventor_persistent_location_id`, `firstnamed_inventor_city`,
  `firstnamed_inventor_state`, `firstnamed_inventor_country`,
  `firstnamed_inventor_latitude`, `firstnamed_inventor_longitude`,
  `num_foreign_documents_cited`, `num_us_applications_cited`,
  `num_us_patents_cited`, `num_total_documents_cited`,
  `num_times_cited_by_us_patents`,`num_usrelateddocuments`,
  `earliest_application_date`, `patent_processing_days`
)
select
  p.`id`, case when ifnull(p.`type`, '') = 'sir' then 'statutory invention registration' else nullif(trim(p.`type`), '') end,
  `number`, nullif(trim(p.`country`), ''), tpd.`date`, year(tpd.`date`),
  nullif(trim(p.`abstract`), ''), nullif(trim(p.`title`), ''), nullif(trim(p.`kind`), ''), p.`num_claims`,
  tpfna.`assignee_id`, tpfna.`persistent_assignee_id`, tpfna.`location_id`,
  tpfna.`persistent_location_id`, tpfna.`city`,
  tpfna.`state`, tpfna.`country`, tpfna.`latitude`, tpfna.`longitude`,
  tpfni.`inventor_id`, tpfni.`persistent_inventor_id`, tpfni.`location_id`,
  tpfni.`persistent_location_id`, tpfni.`city`,
  tpfni.`state`, tpfni.`country`, tpfni.`latitude`, tpfni.`longitude`,
  tpa.`num_foreign_documents_cited`, tpa.`num_us_applications_cited`,
  tpa.`num_us_patents_cited`, tpa.`num_total_documents_cited`,
  tpa.`num_times_cited_by_us_patents`,tpnl.`num_us_rel_doc`,
  tpead.`earliest_application_date`,
  case when tpead.`earliest_application_date` <= p.`date` then timestampdiff(day, tpead.`earliest_application_date`, tpd.`date`) else null end
from
  `new_parser_qa`.`patent` p
  left outer join `PatentsView_dev`.`temp_patent_date` tpd on tpd.`patent_id` = p.`id`
  left outer join `PatentsView_dev`.`temp_patent_firstnamed_assignee` tpfna on tpfna.`patent_id` = p.`id`
  left outer join `PatentsView_dev`.`temp_patent_firstnamed_inventor` tpfni on tpfni.`patent_id` = p.`id`
  left outer join `PatentsView_dev`.`temp_patent_aggregations` tpa on tpa.`patent_id` = p.`id`
  left outer join `PatentsView_dev`.`temp_patent_earliest_application_date` tpead on tpead.`patent_id` = p.`id`
  left outer join `PatentsView_dev`.`temp_num_us_rel_doc` tpnl on tpnl.`patent_id` = p.`id`;


drop table if exists `PatentsView_dev`.`assignee`;
create table `PatentsView_dev`.`assignee`
(
  `assignee_id` int unsigned not null,
  `type` varchar(10) null,
  `name_first` varchar(64) null,
  `name_last` varchar(64) null,
  `organization` varchar(256) null,
  `num_patents` int unsigned not null,
  `num_inventors` int unsigned not null,
  `num_lawyers` int unsigned not null,
  `lastknown_location_id` int unsigned null,
  `lastknown_persistent_location_id` varchar(128) null,
  `lastknown_city` varchar(128) null,
  `lastknown_state` varchar(20) null,
  `lastknown_country` varchar(10) null,
  `lastknown_latitude` float null,
  `lastknown_longitude` float null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `years_active` smallint unsigned not null,
  `persistent_assignee_id` varchar(36) not null,
  primary key (`assignee_id`)
)
engine=InnoDB;




drop table if exists `PatentsView_dev`.`temp_assignee_lastknown_location`;
create table `PatentsView_dev`.`temp_assignee_lastknown_location`
(
  `assignee_id` varchar(36) not null,
  `location_id` int unsigned null,
  `persistent_location_id` varchar(128) null,
  `city` varchar(128) null,
  `state` varchar(20) null,
  `country` varchar(10) null,
  `latitude` float null,
  `longitude` float null,
  primary key (`assignee_id`)
)
engine=InnoDB;


# Populate temp_assignee_lastknown_location table.  The goal here is to grab the location associated
# with the most recent patent associated with the assignee.  It is possible for a patent/assignee
# combination not to have a location, so we will grab the most recent KNOWN location.
# 320,156 @ 3:51
insert into `PatentsView_dev`.`temp_assignee_lastknown_location`
(
  `assignee_id`, `location_id`, `persistent_location_id`, `city`, `state`, `country`, `latitude`, `longitude`
)
select
  t.`assignee_id`,
  tl.`new_location_id`,
  tl.`old_location_id_transformed`,
  nullif(trim(l.`city`), ''),
  nullif(trim(l.`state`), ''),
  nullif(trim(l.`country`), ''),
  l.`latitude`,
  l.`longitude`
from
  (
    select
      t.`assignee_id`,
      t.`location_id`,
      t.`location_id_transformed`
    from
      (
        select
          @rownum := case when @assignee_id = t.`assignee_id` then @rownum + 1 else 1 end `rownum`,
          @assignee_id := t.`assignee_id` `assignee_id`,
	  t.`location_id`,
          t.`location_id_transformed`
        from
          (
            select
              ra.`assignee_id`,
              rl.`location_id`,
	      rl.`location_id_transformed`
            from
              `new_parser_qa`.`rawassignee` ra
              inner join `new_parser_qa`.`patent` p on p.`id` = ra.`patent_id`
              inner join `new_parser_qa`.`rawlocation` rl on rl.`id` = ra.`rawlocation_id`
            where
              rl.`location_id_transformed` is not null and
              ra.`assignee_id` is not null
            order by
              ra.`assignee_id`,
              p.`date` desc,
              p.`id` desc
          ) t,
          (select @rownum := 0, @assignee_id := '') r
      ) t
    where
      t.`rownum` < 2
  ) t
  left outer join `new_parser_qa`.`location` l on l.`id` = t.`location_id`
  left outer join `PatentsView_dev`.`temp_id_mapping_location_transformed` tl on tl.`old_location_id_transformed` = 

t.`location_id_transformed`;


drop table if exists `PatentsView_dev`.`temp_assignee_num_patents`;
create table `PatentsView_dev`.`temp_assignee_num_patents`
(
  `assignee_id` varchar(36) not null,
  `num_patents` int unsigned not null,
  primary key (`assignee_id`)
)
engine=InnoDB;


#
insert into `PatentsView_dev`.`temp_assignee_num_patents`
  (`assignee_id`, `num_patents`)
select
  `assignee_id`,
  count(distinct `patent_id`)
from
  `new_parser_qa`.`patent_assignee`
group by
  `assignee_id`;

drop table if exists `PatentsView_dev`.`temp_assignee_num_inventors`;
create table `PatentsView_dev`.`temp_assignee_num_inventors`
(
  `assignee_id` varchar(36) not null,
  `num_inventors` int unsigned not null,
  primary key (`assignee_id`)
)
engine=InnoDB;

# 0:15
insert into `PatentsView_dev`.`temp_assignee_num_inventors`
  (`assignee_id`, `num_inventors`)
select
  aa.`assignee_id`,
  count(distinct ii.`inventor_id`)
from
  `new_parser_qa`.`patent_assignee` aa
  join `new_parser_qa`.`patent_inventor` ii on ii.patent_id = aa.patent_id
group by
  aa.`assignee_id`;
  
drop table if exists `PatentsView_dev`.`temp_assignee_years_active`;
create table `PatentsView_dev`.`temp_assignee_years_active`
(
  `assignee_id` varchar(36) not null,
  `first_seen_date` date null,
  `last_seen_date` date null,
  `actual_years_active` smallint unsigned not null,
  primary key (`assignee_id`)
)
engine=InnoDB;


# Years active is essentially the number of years difference between first associated patent and last.
# 1:15
insert into `PatentsView_dev`.`temp_assignee_years_active`
  (`assignee_id`, `first_seen_date`, `last_seen_date`, `actual_years_active`)
select
  pa.`assignee_id`, min(p.`date`), max(p.`date`),
  ifnull(round(timestampdiff(day, min(p.`date`), max(p.`date`)) / 365), 0)
from
  `new_parser_qa`.`patent_assignee` pa
  inner join `PatentsView_dev`.`patent` p on p.`patent_id`= pa.`patent_id`
where
  p.`date` is not null
group by
  pa.`assignee_id`;


drop table if exists `PatentsView_dev`.`patent_assignee`;
create table `PatentsView_dev`.`patent_assignee`
(
  `patent_id` varchar(20) not null,
  `assignee_id` int unsigned not null,
  `location_id` int unsigned null,
  `sequence` smallint unsigned not null,
  primary key (`patent_id`, `assignee_id`),
  unique index ak_patent_assignee (`assignee_id`, `patent_id`)
)
engine=InnoDB;



# 345,185 @ 0:15
insert into `PatentsView_dev`.`assignee`
(
  `assignee_id`, `type`, `name_first`, `name_last`, `organization`,
  `num_patents`, `num_inventors`, `num_lawyers`, `lastknown_location_id`, `lastknown_persistent_location_id`, `lastknown_city`,
  `lastknown_state`, `lastknown_country`, `lastknown_latitude`, `lastknown_longitude`,
  `first_seen_date`, `last_seen_date`, `years_active`, `persistent_assignee_id`
)
select
  t.`new_assignee_id`, trim(leading '0' from nullif(trim(a.`type`), '')), nullif(trim(a.`name_first`), ''),
  nullif(trim(a.`name_last`), ''), nullif(trim(a.`organization`), ''),
  tanp.`num_patents`, ifnull(tani.`num_inventors`, 0), ifnull(tanl.`num_lawyers`, 0), talkl.`location_id`, talkl.`persistent_location_id`, talkl.`city`, talkl.`state`,
  talkl.`country`, talkl.`latitude`, talkl.`longitude`,
  tafls.`first_seen_date`, tafls.`last_seen_date`,
  ifnull(case when tafls.`actual_years_active` < 1 then 1 else tafls.`actual_years_active` end, 0),
  a.`id`
from
  `new_parser_qa`.`assignee` a
  inner join `PatentsView_dev`.`temp_id_mapping_assignee` t on t.`old_assignee_id` = a.`id`
  left outer join `PatentsView_dev`.`temp_assignee_lastknown_location` talkl on talkl.`assignee_id` = a.`id`
  inner join `PatentsView_dev`.`temp_assignee_num_patents` tanp on tanp.`assignee_id` = a.`id`
  left outer join `PatentsView_dev`.`temp_assignee_years_active` tafls on tafls.`assignee_id` = a.`id`
  left outer join `PatentsView_dev`.`temp_assignee_num_inventors` tani on tani.`assignee_id` = a.`id`
  left outer join `PatentsView_dev`.`temp_assignee_num_lawyers` tanl on tanl.`assignee_id` = a.`id`;


select * from PatentsView_dev.examiner limit 100;
# END inventor 

