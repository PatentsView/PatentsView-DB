{% set target_database = params.database %}
    {% set dbdate= execution_date+macros.timedelta(days=7) %}
    {% set year = dbdate.strftime('%Y') %}
    {% if params.add_suffix %}
    {% set target_database = target_database  +  dbdate.strftime('%Y%m%d')|string  %}
    {% endif %}
CREATE TRIGGER before_insert_main_cpc
    BEFORE INSERT
    ON main_cpc
    FOR EACH ROW SET new.uuid = uuid();

CREATE TRIGGER before_insert_further_cpc
    BEFORE INSERT
    ON further_cpc
    FOR EACH ROW SET new.uuid = uuid();

create TRIGGER before_insert_rel_app_text
    BEFORE INSERT
    ON rel_app_text
    FOR EACH ROW SET new.uiud= uuid();
