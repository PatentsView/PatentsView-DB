CREATE TRIGGER before_insert_brf
  BEFORE INSERT
  ON temp_brf_sum_text
  FOR EACH row
  SET new.uuid = uuid();
CREATE TRIGGER before_insert_claim
  BEFORE INSERT
  ON temp_claim
  FOR EACH row
  SET new.uuid = uuid();
CREATE TRIGGER before_insert_ddt
  BEFORE INSERT
  ON temp_detail_desc_text
  FOR EACH row
  SET new.uuid = uuid();
CREATE TRIGGER before_insert_drawdt
  BEFORE INSERT
  ON temp_draw_desc_text
  FOR EACH row
  SET new.uuid = uuid();
