{% set source_database = params.database %}
    {% set dbdate= execution_date+macros.timedelta(days=7) %}
    {% set year = dbdate.strftime('%Y') %}
    {% if params.add_suffix %}
    {% set source_database = source_database  +  dbdate.strftime('%Y%m%d')|string  %}
    {% endif %}
INSERT INTO pregrant_publications.publication SELECT * FROM `{{source_database}}`.publication;

INSERT INTO pregrant_publications.application SELECT * FROM `{{source_database}}`.application;

INSERT INTO pgpubs_text.brf_sum_text_2022 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2022;

INSERT INTO pgpubs_text.brf_sum_text_2021 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2021;

INSERT INTO pgpubs_text.brf_sum_text_2020 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2020;

INSERT INTO pgpubs_text.brf_sum_text_2019 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2019;

INSERT INTO pgpubs_text.brf_sum_text_2018 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2018;

INSERT INTO pgpubs_text.brf_sum_text_2017 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2017;

INSERT INTO pgpubs_text.brf_sum_text_2016 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2016;

INSERT INTO pgpubs_text.brf_sum_text_2015 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2015;

INSERT INTO pgpubs_text.brf_sum_text_2014 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2014;

INSERT INTO pgpubs_text.brf_sum_text_2013 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2013;

INSERT INTO pgpubs_text.brf_sum_text_2012 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2012;

INSERT INTO pgpubs_text.brf_sum_text_2011 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2011;

INSERT INTO pgpubs_text.brf_sum_text_2010 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2010;

INSERT INTO pgpubs_text.brf_sum_text_2009 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2009;

INSERT INTO pgpubs_text.brf_sum_text_2008 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2008;

INSERT INTO pgpubs_text.brf_sum_text_2007 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2007;

INSERT INTO pgpubs_text.brf_sum_text_2006 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2006;

INSERT INTO pgpubs_text.brf_sum_text_2005 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2005;

INSERT INTO pgpubs_text.brf_sum_text_2004 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2004;

INSERT INTO pgpubs_text.brf_sum_text_2003 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2003;

INSERT INTO pgpubs_text.brf_sum_text_2002 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2002;

INSERT INTO pgpubs_text.brf_sum_text_2001 (`id`, `pgpub_id`, `summary_text`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `filename`, `version_indicator` FROM `{{source_database}}`.brf_sum_text_2001;

INSERT INTO pgpubs_text.claims_2022 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2022;

INSERT INTO pgpubs_text.claims_2021 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2021;

INSERT INTO pgpubs_text.claims_2020 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2020;

INSERT INTO pgpubs_text.claims_2019 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2019;

INSERT INTO pgpubs_text.claims_2018 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2018;

INSERT INTO pgpubs_text.claims_2017 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2017;

INSERT INTO pgpubs_text.claims_2016 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2016;

INSERT INTO pgpubs_text.claims_2015 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2015;

INSERT INTO pgpubs_text.claims_2014 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2014;

INSERT INTO pgpubs_text.claims_2013 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2013;

INSERT INTO pgpubs_text.claims_2012 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2012;

INSERT INTO pgpubs_text.claims_2011 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2011;

INSERT INTO pgpubs_text.claims_2010 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2010;

INSERT INTO pgpubs_text.claims_2009 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2009;

INSERT INTO pgpubs_text.claims_2008 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2008;

INSERT INTO pgpubs_text.claims_2007 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2007;

INSERT INTO pgpubs_text.claims_2006 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2006;

INSERT INTO pgpubs_text.claims_2005 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2005;

INSERT INTO pgpubs_text.claims_2004 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2004;

INSERT INTO pgpubs_text.claims_2003 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2003;

INSERT INTO pgpubs_text.claims_2002 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2002;

INSERT INTO pgpubs_text.claims_2001 (`id`, `pgpub_id`, `claim_text`, `claim_sequence`, `dependent`, `filename`, `version_indicator`, `claim_number`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `dependent`, `filename`, `version_indicator`, `num` FROM `{{source_database}}`.claims_2001;

INSERT INTO pregrant_publications.cpc SELECT * FROM `{{source_database}}`.cpc;

INSERT INTO pgpubs_text.detail_desc_text_2022 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2022;

INSERT INTO pgpubs_text.detail_desc_text_2021 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2021;

INSERT INTO pgpubs_text.detail_desc_text_2020 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2020;

INSERT INTO pgpubs_text.detail_desc_text_2019 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2019;

INSERT INTO pgpubs_text.detail_desc_text_2018 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2018;

INSERT INTO pgpubs_text.detail_desc_text_2017 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2017;

INSERT INTO pgpubs_text.detail_desc_text_2016 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2016;

INSERT INTO pgpubs_text.detail_desc_text_2015 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2015;

INSERT INTO pgpubs_text.detail_desc_text_2014 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2014;

INSERT INTO pgpubs_text.detail_desc_text_2013 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2013;

INSERT INTO pgpubs_text.detail_desc_text_2012 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2012;

INSERT INTO pgpubs_text.detail_desc_text_2011 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2011;

INSERT INTO pgpubs_text.detail_desc_text_2010 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2010;

INSERT INTO pgpubs_text.detail_desc_text_2009 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2009;

INSERT INTO pgpubs_text.detail_desc_text_2008 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2008;

INSERT INTO pgpubs_text.detail_desc_text_2007 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2007;

INSERT INTO pgpubs_text.detail_desc_text_2006 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2006;

INSERT INTO pgpubs_text.detail_desc_text_2005 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2005;

INSERT INTO pgpubs_text.detail_desc_text_2004 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2004;

INSERT INTO pgpubs_text.detail_desc_text_2003 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2003;

INSERT INTO pgpubs_text.detail_desc_text_2002 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2002;

INSERT INTO pgpubs_text.detail_desc_text_2001 (`id`, `pgpub_id`, `description_text`, `description_length`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `length`, `filename`, `version_indicator` FROM `{{source_database}}`.detail_desc_text_2001;

INSERT INTO pgpubs_text.draw_desc_text_2022 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2022;

INSERT INTO pgpubs_text.draw_desc_text_2021 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2021;

INSERT INTO pgpubs_text.draw_desc_text_2020 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2020;

INSERT INTO pgpubs_text.draw_desc_text_2019 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2019;

INSERT INTO pgpubs_text.draw_desc_text_2018 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2018;

INSERT INTO pgpubs_text.draw_desc_text_2017 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2017;

INSERT INTO pgpubs_text.draw_desc_text_2016 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2016;

INSERT INTO pgpubs_text.draw_desc_text_2015 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2015;

INSERT INTO pgpubs_text.draw_desc_text_2014 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2014;

INSERT INTO pgpubs_text.draw_desc_text_2013 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2013;

INSERT INTO pgpubs_text.draw_desc_text_2012 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2012;

INSERT INTO pgpubs_text.draw_desc_text_2011 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2011;

INSERT INTO pgpubs_text.draw_desc_text_2010 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2010;

INSERT INTO pgpubs_text.draw_desc_text_2009 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2009;

INSERT INTO pgpubs_text.draw_desc_text_2008 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2008;

INSERT INTO pgpubs_text.draw_desc_text_2007 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2007;

INSERT INTO pgpubs_text.draw_desc_text_2006 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2006;

INSERT INTO pgpubs_text.draw_desc_text_2005 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2005;

INSERT INTO pgpubs_text.draw_desc_text_2004 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2004;

INSERT INTO pgpubs_text.draw_desc_text_2003 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2003;

INSERT INTO pgpubs_text.draw_desc_text_2002 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2002;

INSERT INTO pgpubs_text.draw_desc_text_2001 (`id`, `pgpub_id`, `draw_desc_text`, `draw_desc_sequence`, `filename`, `version_indicator`) 
    SELECT `id`, `pgpub_id`, `text`, `sequence`, `filename`, `version_indicator` FROM `{{source_database}}`.draw_desc_text_2001;

INSERT INTO pregrant_publications.foreign_priority SELECT * FROM `{{source_database}}`.foreign_priority;

INSERT INTO pregrant_publications.further_cpc SELECT * FROM `{{source_database}}`.further_cpc;

INSERT INTO pregrant_publications.ipcr SELECT * FROM `{{source_database}}`.ipcr;

INSERT INTO pregrant_publications.main_cpc SELECT * FROM `{{source_database}}`.main_cpc;

INSERT INTO pregrant_publications.pct_data SELECT * FROM `{{source_database}}`.pct_data;

INSERT INTO pregrant_publications.rawassignee (id, document_number, `sequence`, name_first, name_last, `organization`, type, rawlocation_id, city, state, country, filename, version_indicator) SELECT id, document_number, sequence, name_first, name_last, organization, type, rawlocation_id, city, state, country, filename,version_indicator FROM `{{source_database}}`.rawassignee;

INSERT INTO pregrant_publications.rawinventor (id, document_number, name_first, name_last, `sequence`, designation, deceased, rawlocation_id, city, state, country, filename,version_indicator) SELECT id, document_number, name_first, name_last, sequence, designation, deceased, rawlocation_id, city, state, country, filename,version_indicator FROM `{{source_database}}`.rawinventor;

INSERT INTO pregrant_publications.rawlocation (id, city, state, country, latitude, longitude, filename, version_indicator) SELECT id, city, state, country, latitude, longitude, filename,version_indicator FROM `{{source_database}}`.rawlocation;

INSERT INTO pregrant_publications.rawuspc SELECT * FROM `{{source_database}}`.rawuspc;

INSERT INTO pregrant_publications.rel_app_text SELECT * FROM `{{source_database}}`.rel_app_text;

INSERT INTO pregrant_publications.us_parties (id, document_number, name_first, name_last, organization, type, designation, sequence, rawlocation_id, city, state, country, filename, version_indicator) SELECT id, document_number, name_first, name_last, organization, type, designation, sequence, rawlocation_id, city, state, country, filename, version_indicator FROM `{{source_database}}`.us_parties;

INSERT INTO pregrant_publications.uspc SELECT * FROM `{{source_database}}`.uspc;

INSERT INTO pregrant_publications.usreldoc SELECT * FROM `{{source_database}}`.usreldoc;

INSERT INTO pregrant_publications.government_interest (`document_number`,`gi_statement`,`version_indicator`,`created_date`,`updated_date`) 
    SELECT `document_number`,`gi_statement`,`version_indicator`,`created_date`,`updated_date` FROM `{{source_database}}`.government_interest;

INSERT INTO pregrant_publications.publication_govintorg (`document_number`,`organization_id`,`version_indicator`,`created_date`,`updated_date`) 
    SELECT `document_number`,`organization_id`,`version_indicator`,`created_date`,`updated_date` FROM `{{source_database}}`.publication_govintorg;

INSERT INTO pregrant_publications.publication_contractawardnumber (`document_number`,`contract_award_number`,`version_indicator`,`created_date`,`updated_date`) 
    SELECT `document_number`,`contract_award_number`,`version_indicator`,`created_date`,`updated_date` FROM `{{source_database}}`.publication_contractawardnumber;
