{% set target_database = params.database %}
    {% set dbdate= execution_date+macros.timedelta(days=7) %}
    {% set year = dbdate.strftime('%Y') %}
    {% if params.add_suffix %}
    {% set target_database = target_database  +  dbdate.strftime('%Y%m%d')|string  %}
    {% endif %}
DROP TRIGGER if exists `{{target_database}}`.before_insert_main_cpc;
CREATE TRIGGER `{{target_database}}`.before_insert_main_cpc
    BEFORE INSERT
    ON main_cpc
    FOR EACH ROW SET new.uuid = uuid();
DROP TRIGGER if exists `{{target_database}}`.before_insert_further_cpc;
CREATE TRIGGER `{{target_database}}`.before_insert_further_cpc
    BEFORE INSERT
    ON further_cpc
    FOR EACH ROW SET new.uuid = uuid();
DROP TRIGGER if exists `{{target_database}}`.before_insert_rel_app_text;
create TRIGGER `{{target_database}}`.before_insert_rel_app_text
    BEFORE INSERT
    ON rel_app_text
    FOR EACH ROW SET new.uuid= uuid();
