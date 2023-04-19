{% set dbdate= (execution_date+macros.timedelta(days=7)).strftime('%Y-%m-%d')|string %}
    {% set qavi = (execution_date+macros.timedelta(days=7)).strftime('%Y%m%d')|string %}

SET FOREIGN_KEY_CHECKS=0;

delete from pregrant_publications.publication where version_indicator = '{{dbdate}}';
delete from pregrant_publications.application where version_indicator = '{{dbdate}}';
delete from pregrant_publications.cpc where version_indicator = '{{dbdate}}';
delete from pregrant_publications.foreign_priority where version_indicator = '{{dbdate}}';
delete from pregrant_publications.further_cpc where version_indicator = '{{dbdate}}';
delete from pregrant_publications.ipcr where version_indicator = '{{dbdate}}';
delete from pregrant_publications.main_cpc where version_indicator = '{{dbdate}}';
delete from pregrant_publications.pct_data where version_indicator = '{{dbdate}}';
delete from pregrant_publications.rawassignee where version_indicator = '{{dbdate}}';
delete from pregrant_publications.rawinventor where version_indicator = '{{dbdate}}';
delete from pregrant_publications.rawlocation where version_indicator = '{{dbdate}}';
delete from pregrant_publications.rawuspc where version_indicator = '{{dbdate}}';
delete from pregrant_publications.rel_app_text where version_indicator = '{{dbdate}}';
delete from pregrant_publications.us_parties where version_indicator = '{{dbdate}}';
delete from pregrant_publications.uspc where version_indicator = '{{dbdate}}';
delete from pregrant_publications.usreldoc where version_indicator = '{{dbdate}}';
delete from pregrant_publications.government_interest where version_indicator = '{{dbdate}}';
delete from pregrant_publications.publication_govintorg where version_indicator = '{{dbdate}}';
delete from pregrant_publications.publication_contractawardnumber where version_indicator = '{{dbdate}}';
-- delete from pgpubs_text.brf_sum_text_2021 where version_indicator = '{{dbdate}}';
delete from pgpubs_text.brf_sum_text_2022 where version_indicator = '{{dbdate}}';
-- delete from pgpubs_text.claims_2021 where version_indicator = '{{dbdate}}';
delete from pgpubs_text.claims_2022 where version_indicator = '{{dbdate}}';
delete from pgpubs_text.detail_desc_text_2022 where version_indicator = '{{dbdate}}';
-- delete from pgpubs_text.detail_desc_text_2021 where version_indicator = '{{dbdate}}';
-- delete from pgpubs_text.draw_desc_text_2021 where version_indicator = '{{dbdate}}';
delete from pgpubs_text.draw_desc_text_2022 where version_indicator = '{{dbdate}}';

SET FOREIGN_KEY_CHECKS=1;
