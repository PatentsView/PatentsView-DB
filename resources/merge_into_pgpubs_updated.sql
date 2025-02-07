{% set source_database = params.database %}
    {% set dbdate= execution_date+macros.timedelta(days=7) %}
    {% set year = dbdate.strftime('%Y') %}
    {% if params.add_suffix %}
    {% set source_database = source_database  +  dbdate.strftime('%Y%m%d')|string  %}
    {% endif %}

INSERT INTO pregrant_publications.publication SELECT * FROM `{{source_database}}`.publication
ON DUPLICATE KEY UPDATE
    `date` = VALUES(`date`),
    `country` = VALUES(`country`),
    `kind` = VALUES(`kind`),
    `filing_type` = VALUES(`filing_type`),
    `filename` = VALUES(`filename`),
    `updated_date` = VALUES(`updated_date`),
    `version_indicator` = VALUES(`version_indicator`);

INSERT INTO pregrant_publications.application SELECT * FROM `{{source_database}}`.application
ON DUPLICATE KEY UPDATE
    `type` = VALUES(`type`),
    `application_number` = VALUES(`application_number`),
    `date` = VALUES(`date`),
    `country` = VALUES(`country`),
    `series_code` = VALUES(`series_code`),
    `invention_title` = VALUES(`invention_title`),
    `invention_abstract` = VALUES(`invention_abstract`),
    `rule_47_flag` = VALUES(`rule_47_flag`),
    `filename` = VALUES(`filename`),
    `updated_date` = VALUES(`updated_date`),
    `version_indicator` = VALUES(`version_indicator`);

INSERT INTO pgpubs_text.brf_sum_text_{{year}} (`id`, `pgpub_id`, `summary_text`, `version_indicator`)
    SELECT `id`, `pgpub_id`, `summary_text`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_{{year}}
    ON DUPLICATE KEY UPDATE
    `summary_text` = VALUES(`summary_text`),
    `version_indicator` = VALUES(`version_indicator`);

INSERT INTO pgpubs_text.claims_{{year}}(`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `version_indicator`, `claim_number`)
    SELECT `id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `version_indicator`, `claim_number` FROM `{{source_database}}`.claims_{{year}}
    ON DUPLICATE KEY UPDATE
    `claim_text` = VALUES(`claim_text`),
    `dependent` = VALUES(`dependent`),
    `version_indicator` = VALUES(`version_indicator`),
    `claim_number` = VALUES(`claim_number`);


INSERT INTO pregrant_publications.cpc SELECT * FROM `{{source_database}}`.cpc
    ON DUPLICATE KEY UPDATE
        `version` = VALUES(`version`),
        `section_id` = VALUES(`section_id`),
        `subsection_id` = VALUES(`subsection_id`),
        `group_id` = VALUES(`group_id`),
        `subgroup_id` = VALUES(`subgroup_id`),
        `symbol_position` = VALUES(`symbol_position`),
        `value` = VALUES(`value`),
        `category` = VALUES(`category`),
        `action_date` = VALUES(`action_date`),
        `filename` = VALUES(`filename`),
        `updated_date` = VALUES(`updated_date`),
        `version_indicator` = VALUES(`version_indicator`);

INSERT INTO pgpubs_text.detail_desc_text_{{year}} (`id`, `pgpub_id`, `description_text`, `description_length`, `version_indicator`)
    SELECT `id`, `pgpub_id`, `description_text`, `description_length`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_{{year}}
    ON DUPLICATE KEY UPDATE
    `description_text` = VALUES(`description_text`),
    `description_length` = VALUES(`description_length`),
    `version_indicator` = VALUES(`version_indicator`);


INSERT INTO pgpubs_text.draw_desc_text_{{year}} (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `version_indicator`)
    SELECT `id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_{{year}}
    ON DUPLICATE KEY UPDATE
    `draw_desc_text` = VALUES(`draw_desc_text`),
    `version_indicator` = VALUES(`version_indicator`);

INSERT INTO pregrant_publications.foreign_priority SELECT * FROM `{{source_database}}`.foreign_priority
ON DUPLICATE KEY UPDATE
    `country` = VALUES(`country`),
    `date` = VALUES(`date`),
    `foreign_doc_number` = VALUES(`foreign_doc_number`),
    `kind` = VALUES(`kind`),
    `filename` = VALUES(`filename`),
    `updated_date` = VALUES(`updated_date`),
    `version_indicator` = VALUES(`version_indicator`);

INSERT INTO pregrant_publications.further_cpc SELECT * FROM `{{source_database}}`.further_cpc
ON DUPLICATE KEY UPDATE
    `sequence` = VALUES(`sequence`),
    `version` = VALUES(`version`),
    `section` = VALUES(`section`),
    `class` = VALUES(`class`),
    `subclass` = VALUES(`subclass`),
    `main_group` = VALUES(`main_group`),
    `subgroup` = VALUES(`subgroup`),
    `symbol_position` = VALUES(`symbol_position`),
    `value` = VALUES(`value`),
    `action_date` = VALUES(`action_date`),
    `filename` = VALUES(`filename`),
    `updated_date` = VALUES(`updated_date`),
    `version_indicator` = VALUES(`version_indicator`);

INSERT INTO pregrant_publications.ipcr SELECT * FROM `{{source_database}}`.ipcr
ON DUPLICATE KEY UPDATE
    `version` = VALUES(`version`),
    `class_level` = VALUES(`class_level`),
    `section` = VALUES(`section`),
    `class` = VALUES(`class`),
    `subclass` = VALUES(`subclass`),
    `main_group` = VALUES(`main_group`),
    `subgroup` = VALUES(`subgroup`),
    `symbol_position` = VALUES(`symbol_position`),
    `class_value` = VALUES(`class_value`),
    `action_date` = VALUES(`action_date`),
    `class_status` = VALUES(`class_status`),
    `class_data_source` = VALUES(`class_data_source`),
    `filename` = VALUES(`filename`),
    `updated_date` = VALUES(`updated_date`),
    `version_indicator` = VALUES(`version_indicator`);

INSERT INTO pregrant_publications.main_cpc SELECT * FROM `{{source_database}}`.main_cpc
ON DUPLICATE KEY UPDATE
    `sequence` = VALUES(`sequence`),
    `version` = VALUES(`version`),
    `section` = VALUES(`section`),
    `class` = VALUES(`class`),
    `subclass` = VALUES(`subclass`),
    `main_group` = VALUES(`main_group`),
    `subgroup` = VALUES(`subgroup`),
    `symbol_position` = VALUES(`symbol_position`),
    `value` = VALUES(`value`),
    `action_date` = VALUES(`action_date`),
    `filename` = VALUES(`filename`),
    `updated_date` = VALUES(`updated_date`),
    `version_indicator` = VALUES(`version_indicator`);

INSERT INTO pregrant_publications.pct_data SELECT * FROM `{{source_database}}`.pct_data
ON DUPLICATE KEY UPDATE
    `pct_doc_number` = VALUES(`pct_doc_number`),
    `country` = VALUES(`country`),
    `date` = VALUES(`date`),
    `us_371c124_date` = VALUES(`us_371c124_date`),
    `us_371c12_date` = VALUES(`us_371c12_date`),
    `kind` = VALUES(`kind`),
    `doc_type` = VALUES(`doc_type`),
    `filename` = VALUES(`filename`),
    `updated_date` = VALUES(`updated_date`),
    `version_indicator` = VALUES(`version_indicator`);

INSERT INTO pregrant_publications.rawassignee (id, document_number, `sequence`, name_first, name_last, `organization`, type, rawlocation_id, city, state, country, filename, version_indicator) SELECT id, document_number, sequence, name_first, name_last, organization, type, rawlocation_id, city, state, country, filename,version_indicator FROM `{{source_database}}`.rawassignee
ON DUPLICATE KEY UPDATE
    `name_first` = VALUES(`name_first`),
    `name_last` = VALUES(`name_last`),
    `organization` = VALUES(`organization`),
    `type` = VALUES(`type`),
    `rawlocation_id` = VALUES(`rawlocation_id`),
    `city` = VALUES(`city`),
    `state` = VALUES(`state`),
    `country` = VALUES(`country`),
    `filename` = VALUES(`filename`),
    `updated_date` = VALUES(`updated_date`),
    `version_indicator` = VALUES(`version_indicator`);

INSERT INTO pregrant_publications.rawinventor (id, document_number, name_first, name_last, `sequence`, designation, deceased, rawlocation_id, city, state, country, filename, version_indicator) SELECT id, document_number, name_first, name_last, sequence, designation, deceased, rawlocation_id, city, state, country, filename, version_indicator FROM `{{source_database}}`.rawinventor
ON DUPLICATE KEY UPDATE
    `name_first` = VALUES(`name_first`),
    `name_last` = VALUES(`name_last`),
    `designation` = VALUES(`designation`),
    `deceased` = VALUES(`deceased`),
    `rawlocation_id` = VALUES(`rawlocation_id`),
    `city` = VALUES(`city`),
    `state` = VALUES(`state`),
    `country` = VALUES(`country`),
    `filename` = VALUES(`filename`),
    `updated_date` = VALUES(`updated_date`),
    `version_indicator` = VALUES(`version_indicator`);


INSERT INTO pregrant_publications.rawlocation (id, city, state, country, country_transformed, latitude, longitude, location_id, filename, version_indicator) SELECT id, city, state, country, country_transformed, latitude, longitude, location_id, filename,version_indicator FROM `{{source_database}}`.rawlocation
ON DUPLICATE KEY UPDATE
    city = VALUES(city),
    state = VALUES(state),
    country = VALUES(country),
    country_transformed = VALUES(country_transformed),
    latitude = VALUES(latitude),
    longitude = VALUES(longitude),
    location_id = VALUES(location_id),
    filename = VALUES(filename),
    version_indicator = VALUES(version_indicator);

INSERT INTO pregrant_publications.rawuspc SELECT * FROM `{{source_database}}`.rawuspc
ON DUPLICATE KEY UPDATE
    `classification` = VALUES(`classification`),
    `filename` = VALUES(`filename`),
    `created_date` = VALUES(`created_date`),
    `updated_date` = VALUES(`updated_date`),
    `version_indicator` = VALUES(`version_indicator`);

INSERT INTO pregrant_publications.rel_app_text SELECT * FROM `{{source_database}}`.rel_app_text
ON DUPLICATE KEY UPDATE
    `text` = VALUES(`text`),
    `filename` = VALUES(`filename`),
    `created_date` = VALUES(`created_date`),
    `updated_date` = VALUES(`updated_date`),
    `version_indicator` = VALUES(`version_indicator`);


INSERT INTO pregrant_publications.us_parties (id, document_number, name_first, name_last, organization, type, applicant_authority, designation, sequence, rawlocation_id, city, state, country, filename, version_indicator) SELECT id, document_number, name_first, name_last, organization, type, applicant_authority, designation, sequence, rawlocation_id, city, state, country, filename, version_indicator FROM `{{source_database}}`.us_parties
ON DUPLICATE KEY UPDATE
    name_first = VALUES(name_first),
    name_last = VALUES(name_last),
    organization = VALUES(organization),
    type = VALUES(type),
    applicant_authority = VALUES(applicant_authority),
    designation = VALUES(designation),
    rawlocation_id = VALUES(rawlocation_id),
    city = VALUES(city),
    state = VALUES(state),
    country = VALUES(country),
    filename = VALUES(filename),
    version_indicator = VALUES(version_indicator),
    updated_date = VALUES(updated_date);

INSERT INTO pregrant_publications.uspc SELECT * FROM `{{source_database}}`.uspc
ON DUPLICATE KEY UPDATE
    mainclass_id = VALUES(mainclass_id),
    subclass_id = VALUES(subclass_id),
    sequence = VALUES(sequence),
    filename = VALUES(filename),
    created_date = VALUES(created_date),
    updated_date = VALUES(updated_date),
    version_indicator = VALUES(version_indicator);

INSERT INTO pregrant_publications.usreldoc SELECT * FROM `{{source_database}}`.usreldoc
ON DUPLICATE KEY UPDATE
    related_doc_number = VALUES(related_doc_number),
    country = VALUES(country),
    doc_type = VALUES(doc_type),
    relkind = VALUES(relkind),
    date = VALUES(date),
    filename = VALUES(filename),
    created_date = VALUES(created_date),
    updated_date = VALUES(updated_date),
    version_indicator = VALUES(version_indicator);

INSERT INTO pregrant_publications.government_interest (`document_number`,`gi_statement`,`version_indicator`,`created_date`,`updated_date`) 
    SELECT `document_number`,`gi_statement`,`version_indicator`,`created_date`,`updated_date` FROM `{{source_database}}`.government_interest
    ON DUPLICATE KEY UPDATE
    gi_statement = VALUES(gi_statement),
    version_indicator = VALUES(version_indicator),
    updated_date = VALUES(updated_date);

INSERT INTO pregrant_publications.publication_govintorg (`document_number`,`organization_id`,`version_indicator`,`created_date`,`updated_date`) 
    SELECT `document_number`,`organization_id`,`version_indicator`,`created_date`,`updated_date` FROM `{{source_database}}`.publication_govintorg
    ON DUPLICATE KEY UPDATE
    version_indicator = VALUES(version_indicator),
    updated_date = VALUES(updated_date);

INSERT INTO pregrant_publications.publication_contractawardnumber (`document_number`,`contract_award_number`,`version_indicator`,`created_date`,`updated_date`) 
    SELECT `document_number`,`contract_award_number`,`version_indicator`,`created_date`,`updated_date` FROM `{{source_database}}`.publication_contractawardnumber
    ON DUPLICATE KEY UPDATE
        version_indicator = VALUES(version_indicator),
        updated_date = VALUES(updated_date);
