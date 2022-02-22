SET FOREIGN_KEY_CHECKS=0;

delete from pregrant_publications.publication where version_indicator =@dbdate;
delete from pregrant_publications.application where version_indicator =@dbdate;
delete from pregrant_publications.cpc where version_indicator =@dbdate;
delete from pregrant_publications.foreign_priority where version_indicator =@dbdate;
delete from pregrant_publications.further_cpc where version_indicator =@dbdate;
delete from pregrant_publications.ipcr where version_indicator =@dbdate;
delete from pregrant_publications.main_cpc where version_indicator =@dbdate;
delete from pregrant_publications.pct_data where version_indicator =@dbdate;
delete from pregrant_publications.rawassignee where version_indicator =@dbdate;
delete from pregrant_publications.rawinventor where version_indicator =@dbdate;
delete from pregrant_publications.rawlocation where version_indicator =@dbdate;
delete from pregrant_publications.rawuspc where version_indicator =@dbdate;
delete from pregrant_publications.rel_app_text where version_indicator =@dbdate;
delete from pregrant_publications.us_parties where version_indicator =@dbdate;
delete from pregrant_publications.uspc where version_indicator =@dbdate;
delete from pregrant_publications.usreldoc where version_indicator =@dbdate;
delete from pgpubs_text.brf_sum_text_2021 where version_indicator =@dbdate;
# delete from pgpubs_text.brf_sum_text_2022 where version_indicator =@dbdate;
delete from pgpubs_text.claim_2021 where version_indicator =@dbdate;
# delete from pgpubs_text.claim_2022 where version_indicator =@dbdate;
# delete from pgpubs_text.detail_desc_text_2022 where version_indicator =@dbdate;
delete from pgpubs_text.detail_desc_text_2021 where version_indicator =@dbdate;
delete from pgpubs_text.draw_desc_text_2021 where version_indicator =@dbdate;
# delete from pgpubs_text.draw_desc_text_2022 where version_indicator =@dbdate;

delete from Patent_QA.DataMonitor_categorycount where update_version = @qavi and database_type = 'pgpubs';
delete from Patent_QA.DataMonitor_count where update_version = @qavi and database_type = 'pgpubs';
delete from Patent_QA.DataMonitor_floatingentitycount where update_version = @qavi and database_type = 'pgpubs';
delete from Patent_QA.DataMonitor_govtinterestsampler where update_version = @qavi and database_type = 'pgpubs';
delete from Patent_QA.DataMonitor_locationcount where update_version = @qavi and database_type = 'pgpubs';
delete from Patent_QA.DataMonitor_maxtextlength where update_version = @qavi and database_type = 'pgpubs';
delete from Patent_QA.DataMonitor_nullcount where update_version = @qavi and database_type = 'pgpubs';
delete from Patent_QA.DataMonitor_patentwithdrawncount where update_version = @qavi and database_type = 'pgpubs';
delete from Patent_QA.DataMonitor_patentyearlycount where update_version = @qavi and database_type = 'pgpubs';
delete from Patent_QA.DataMonitor_prefixedentitycount where update_version = @qavi and database_type = 'pgpubs';
delete from Patent_QA.DataMonitor_topnentites where update_version = @qavi and database_type = 'pgpubs';


SET FOREIGN_KEY_CHECKS=1;