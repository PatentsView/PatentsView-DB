from sqlalchemy import create_engine

from lib.configuration import get_connection_string


class TextParserTest:
    def __init__(self, config):
        self.conn_string = get_connection_string(config, database='TEMP_UPLOAD_DB')
        self.text_items = {'brf_sum_text': {"text_field": "text"}, 'claim': {"text_field": "text"},
                           'detail_desc_text': {"text_field": "text"}, 'draw_desc_text': {"text_field": "text"}}

    def runTests(self, update_config):
        self.verify_text_loaded(update_config)

    def verify_text_loaded(self, update_config):
        engine = create_engine(self.conn_string)
        upload_database = update_config["PATENTSVIEW_DATABASES"]["TEMP_UPLOAD_DB"]
        for text_entity in self.text_items:
            fully_qualified_table_name = "`{database}`.`{table_name}``".format(database=upload_database,
                                                                              table_name=text_entity)
            count_cursor = engine.execute("SELECT count(*) from {fqtn}".format(fqtn=fully_qualified_table_name))
            count_data = count_cursor.fetchall()[0][0]
            assert count_data > 0
