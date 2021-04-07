{% set target_database = params.database %}
    {% set dbdate= execution_date+macros.timedelta(days=7) %}
    {% set year = dbdate.strftime('%Y') %}
    {% if params.add_suffix %}
    {% set target_database = target_database  +  dbdate.strftime('%Y%m%d')|string  %}
    {% endif %}

UPDATE {{ target_database }}.`brf_sum_text_{{ year }}`
SET patent_id = CONCAT(SUBSTR(patent_id, 1, REGEXP_INSTR(`patent_id`, '[0-9]') - 1),
                       TRIM(LEADING '0' FROM SUBSTR(patent_id, REGEXP_INSTR(`patent_id`, '[0-9]'))))
where patent_id REGEXP '^[a-zA-Z]';


UPDATE {{ target_database }}.`claim_{{ year }}`
SET patent_id = CONCAT(SUBSTR(patent_id, 1, REGEXP_INSTR(`patent_id`, '[0-9]') - 1),
                       TRIM(LEADING '0' FROM SUBSTR(patent_id, REGEXP_INSTR(`patent_id`, '[0-9]'))))
where patent_id REGEXP '^[a-zA-Z]';


UPDATE {{ target_database }}.`detail_desc_text_{{ year }}`
SET patent_id = CONCAT(SUBSTR(patent_id, 1, REGEXP_INSTR(`patent_id`, '[0-9]') - 1),
                       TRIM(LEADING '0' FROM SUBSTR(patent_id, REGEXP_INSTR(`patent_id`, '[0-9]'))))
where patent_id REGEXP '^[a-zA-Z]';


UPDATE {{ target_database }}.`draw_desc_text_{{ year }}`
SET patent_id = CONCAT(SUBSTR(patent_id, 1, REGEXP_INSTR(`patent_id`, '[0-9]') - 1),
                       TRIM(LEADING '0' FROM SUBSTR(patent_id, REGEXP_INSTR(`patent_id`, '[0-9]'))))
where patent_id REGEXP '^[a-zA-Z]';

UPDATE {{ target_database }}.`rel_app_text`
SET patent_id = CONCAT(SUBSTR(patent_id, 1, REGEXP_INSTR(`patent_id`, '[0-9]') - 1),
                       TRIM(LEADING '0' FROM SUBSTR(patent_id, REGEXP_INSTR(`patent_id`, '[0-9]'))))
where patent_id REGEXP '^[a-zA-Z]';

UPDATE {{ target_database }}.`government_interest`
SET patent_id = CONCAT(SUBSTR(patent_id, 1, REGEXP_INSTR(`patent_id`, '[0-9]') - 1),
                       TRIM(LEADING '0' FROM SUBSTR(patent_id, REGEXP_INSTR(`patent_id`, '[0-9]'))))
where patent_id REGEXP '^[a-zA-Z]';

UPDATE {{ target_database }}.`main_cpc`
SET patent_id = CONCAT(SUBSTR(patent_id, 1, REGEXP_INSTR(`patent_id`, '[0-9]') - 1),
                       TRIM(LEADING '0' FROM SUBSTR(patent_id, REGEXP_INSTR(`patent_id`, '[0-9]'))))
where patent_id REGEXP '^[a-zA-Z]';

UPDATE {{ target_database }}.`further_cpc`
SET patent_id = CONCAT(SUBSTR(patent_id, 1, REGEXP_INSTR(`patent_id`, '[0-9]') - 1),
                       TRIM(LEADING '0' FROM SUBSTR(patent_id, REGEXP_INSTR(`patent_id`, '[0-9]'))))
where patent_id REGEXP '^[a-zA-Z]';
