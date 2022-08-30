

attorney_disambiguated = """
CREATE OR REPLACE VIEW patentsview_exports_granted.attorney_disambiguated as
select a.patent_id as patent_id
   , a.sequence as attorney_sequence
	, b.name_first as attorney_name_first
	, b.name_last as attorney_name_last 
	, b.organization as attorney_organization 
	, b.country as attorney_country
from patent.patent_lawyer a 
	inner join patent.lawyer b on a.lawyer_id=b.id
where b.version_indicator <= '2022-06-30';
"""

attorney_not_disambiguated = """
drop view patentsview_exports_granted.attorney_not_disambiguated;
create view patentsview_exports_granted.attorney_not_disambiguated as
select lawyer_id as attorney_id
    , sequence as attorney_sequence
    , patent_id
	, name_first as attorney_name_first
	, name_last as attorney_name_last 
	, organization as attorney_organization 
	, country as attorney_country
from patent.rawlawyer;
"""

botanic = """
CREATE OR REPLACE VIEW patentsview_exports_granted.botanic as
select patent_id
	, latin_name 
	, variety as plant_variety
from patent.botanic
where version_indicator <= '2022-06-30';
"""

examiner_not_disambiguated = """
CREATE OR REPLACE VIEW patentsview_exports_granted.examiner_not_disambiguated as
select patent_id
	, name_first as examiner_name_first
	, name_last as examiner_name_last
	, role as examiner_role
	, `group` as art_group
from patent.rawexaminer
where version_indicator <= '2022-06-30';
"""

foreign_citation = """
CREATE OR REPLACE VIEW patentsview_exports_granted.foreign_citation as
select patent_id
	, sequence as citation_sequence 
	, number as citation_application_i
	, date as citation_date
	, category as citation_category
	, country as citation_country
from patent.foreigncitation
where version_indicator <= '2022-06-30';	
"""

other_reference = """
CREATE OR REPLACE VIEW patentsview_exports_granted.other_reference as
select patent_id
	, sequence as other_reference_sequence 
	, text as other_reference_text
from patent.otherreference
where version_indicator <= '2022-06-30';
"""

persistent_assignee = """
CREATE OR REPLACE VIEW patentsview_exports_granted.persistent_assignee as
select current_rawassignee_id as rawassignee_uuid,
  `disamb_assignee_id_20181127`,
  `disamb_assignee_id_20190312`,
  `disamb_assignee_id_20190820`,
  `disamb_assignee_id_20191008`,
  `disamb_assignee_id_20191231`,
  `disamb_assignee_id_20200331`,
  `disamb_assignee_id_20200630`,
  `disamb_assignee_id_20200929`,
  `disamb_assignee_id_20201229`,
  `disamb_assignee_id_20210330`,
  `disamb_assignee_id_20210629` ,
  `disamb_assignee_id_20211230`,
  `disamb_assignee_id_20210930`,
  `disamb_assignee_id_20210708`
from patent.persistent_assignee_disambig
where version_indicator <= '2022-06-30';
"""

persistent_inventor = """
CREATE OR REPLACE VIEW patentsview_exports_granted.persistent_inventor as
select `current_rawinventor_id` as rawinventor_uuid,
  `disamb_inventor_id_20170808` ,
  `disamb_inventor_id_20171003` ,
  `disamb_inventor_id_20171226` ,
  `disamb_inventor_id_20180528` ,
  `disamb_inventor_id_20181127` ,
  `disamb_inventor_id_20190312` ,
  `disamb_inventor_id_20190820` ,
  `disamb_inventor_id_20191008` ,
  `disamb_inventor_id_20191231` ,
  `disamb_inventor_id_20200331` ,
  `disamb_inventor_id_20200630` ,
  `disamb_inventor_id_20200929` ,
  `disamb_inventor_id_20201229` ,
  `disamb_inventor_id_20211230` 
from patent.persistent_inventor_disambig
where version_indicator <= '2022-06-30';
"""

us_term_of_grant = """
CREATE OR REPLACE VIEW patentsview_exports_granted.us_term_of_grant as
select patent_id,
disclaimer_date,
term_disclaimer,
term_grant,
term_extension
from patent.us_term_of_grant
where version_indicator <= '2022-06-30';
"""

us_application_citation = """
CREATE OR REPLACE VIEW patentsview_exports_granted.us_application_citation as
select patent_id as patent_id,
sequence as citation_sequence,
number as citation_document_number,
date as citation_date,
name as record_name,
kind as wipo_kind,
category as citation_category
from usapplicationcitation
where version_indicator <= '2022-06-30'
"""

us_patent_citation = """
CREATE OR REPLACE VIEW patentsview_exports_granted.us_patent_citation as
select patent_id as patent_id,
sequence as citation_sequence,
citation_id as citation_patent_id,
date as citation_date,
name as record_name,
kind as wipo_kind,
category as citation_category
from uspatentcitation
where version_indicator <= '2022-06-30';
"""

application = """
CREATE OR REPLACE VIEW patentsview_exports_pregrant.application as
select b.document_number,
application_number as application_id,
b.date As filing_date,
b.type as patent_type,
a.filing_type as filing_type,
a.date as published_date,
a.kind as wipo_kind,
b.series_code, 
invention_title as application_title,
invention_abstract as application_abstract,
b.rule_47_flag,
b.filename
from pregrant_publications.publication a
	inner join pregrant_publications.application b on a.document_number=b.document_number
where a.version_indicator <= '2022-06-30';
"""

granted_patent_crosswalk = """
CREATE OR REPLACE VIEW patentsview_exports_pregrant.granted_patent_crosswalk as
select document_number,
patent_number as patent_id,
application_number as application_id
from granted_patent_crosswalk
where version_indicator <= '2022-06-30';
"""

pregrant_cpc_at_issue = """
CREATE OR REPLACE VIEW patentsview_exports_pregrant.cpc_at_issue as
select document_number,
sequence as cpc_sequence,
version as cpc_version_indicator,
section_id as cpc_section,
subsection_id as cpc_class,
group_id as cpc_subclass,
subgroup_id as cpc_group,
symbol_position as cpc_symbol_position,
category as cpc_type,
action_date as cpc_action_date
from pregrant_publications.cpc
where version_indicator <= '2022-06-30';
"""

grant_cpc_current = """
CREATE OR REPLACE VIEW patentsview_exports_granted.cpc_current as
select patent_id,
sequence as cpc_sequence,
section_id as cpc_section,
subsection_id as cpc_class,
group_id as cpc_subclass,
subgroup_id as cpc_group,
category as cpc_type,
null as cpc_version_indicator,
null as cpc_symbol_position,
null as cpc_action_date
from patent.cpc_current
where version_indicator <= '2022-06-30';"""


pregrant_cpc_current = """
CREATE OR REPLACE VIEW patentsview_exports_pregrant.cpc_current as
select document_number,
sequence as cpc_sequence,
section_id as cpc_section,
subsection_id as cpc_class,
group_id as cpc_subclass,
subgroup_id as cpc_group,
category as cpc_type,
version_indicator as cpc_version_indicator,
symbol_position as cpc_symbol_position,
version as cpc_action_date
from pregrant_publications.cpc_current
where version_indicator <= '2022-06-30';
"""

cpc_title = """
CREATE OR REPLACE VIEW patentsview_exports_granted.cpc_title as
select a.id as cpc_subclass,
	a.title as cpc_subclass_title, 
	b.id as cpc_group,
	b.title as cpc_group_title, 
	c.id as cpc_class,
	c.title as cpc_class_title 
from patent.cpc_group a 
	inner join patent.cpc_subgroup b on a.id=left(b.id, 4)
	inner join patent.cpc_subsection c on left(b.id, 3)=c.id
"""

grant_ipcr = """
CREATE OR REPLACE VIEW patentsview_exports_granted.ipcr as
select patent_id,
sequence as ipc_sequence,
classification_level,
section,
ipc_class,
subclass,
main_group,
subgroup,
symbol_position,
classification_value,
classification_status,
classification_data_source,
action_date,
ipc_version_indicator
from patent.ipcr
where version_indicator <= '2022-06-30';
"""

pregrant_ipcr = """
CREATE OR REPLACE VIEW patentsview_exports_pregrant.ipcr as
select document_number,
sequence as ipc_sequence,
class_level as classification_level,
section,
class as ipc_class,
subclass,
main_group,
subgroup,
symbol_position,
class_value as classification_value,
class_status as classification_status,
class_data_source as classification_data_source,
action_date,
version as ipc_version_indicator
from pregrant_publications.ipcr
where version_indicator <= '2022-06-30';
"""

grant_uspc_at_issue = """
CREATE OR REPLACE VIEW patentsview_exports_granted.uspc_at_issue as
select a.patent_id,
a.sequence as uspc_sequence,
a.mainclass_id as uspc_mainclass_id,
b.title as uspc_mainclass_title, 
a.subclass_id as uspc_subclass_id,
c.title as uspc_subclass_title
from patent.uspc a
	inner join patent.mainclass_current b on a.mainclass_id=b.id
	inner join patent.subclass_current c on a.subclass_id=c.id
where a.version_indicator <= '2022-06-30';
"""

pregrant_uspc_at_issue = """
CREATE OR REPLACE VIEW patentsview_exports_pregrant.uspc_at_issue as
select a.document_number,
a.sequence as uspc_sequence,
a.mainclass_id as uspc_mainclass_id,
b.title as uspc_mainclass_title, 
a.subclass_id as uspc_subclass_id,
c.title as uspc_subclass_title
from pregrant_publications.uspc a
	inner join patent.mainclass_current b on a.mainclass_id=b.id
	inner join patent.subclass_current c on a.subclass_id=c.id
where a.version_indicator <= '2022-06-30';
"""

pregrant_wipo = """
CREATE OR REPLACE VIEW patentsview_exports_pregrant.wipo as
select document_number,
   a.sequence as wipo_field_sequence,
	a.field_id as wipo_field_id,
	b.sector_title as wipo_sector_title,
	b.field_title as wipo_field_title
from pregrant_publications.wipo a 
	inner join patent.wipo_field b on a.field_id=b.id
where a.version_indicator <= '2022-06-30';
"""

grant_wipo = """
CREATE OR REPLACE VIEW patentsview_exports_granted.wipo as
select patent_id,
   a.sequence as wipo_field_sequence,
	a.field_id as wipo_field_id,
	b.sector_title as wipo_sector_title,
	b.field_title as wipo_field_title
from patent.wipo a 
	inner join patent.wipo_field b on a.field_id=b.id
where a.version_indicator <= '2022-06-30';
"""