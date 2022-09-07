import sqlalchemy as sqla
import re
import json
import os

def update_create_view_sql_file(config):
	host = '{}'.format(config['DATABASE_SETUP']['HOST'])
	user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
	password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
	port = '{}'.format(config['DATABASE_SETUP']['PORT'])
	engine = sqla.create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}?charset=utf8mb4')

	inspector = sqla.inspect(engine)

	target_path = f"{config['FOLDERS']['project_root']}/{config['FOLDERS']['resources_folder']}/create_export_views.sql"

	if os.path.exists(target_path):
		os.remove(target_path)

	for schema in ['patentsview_exports_granted','patentsview_exports_pregrant']:
		for view in inspector.get_view_names(schema):
			create_syntax = inspector.get_view_definition(view,schema)
			remove = 'ALGORITHM=UNDEFINED DEFINER=.* SQL SECURITY DEFINER'
			create_syntax = re.sub(remove, 'OR REPLACE', create_syntax)
			create_syntax = re.sub('[0-9]{4}-[01][0-9]-[0123][0-9]', "{{datestring}}",create_syntax)
			create_syntax = re.sub(' from ', ' \nfrom ', create_syntax, flags=re.I)
			create_syntax = re.sub(' select ', ' \nselect ', create_syntax, flags=re.I)
			create_syntax = re.sub(' where ', ' \nwhere ', create_syntax, flags=re.I)
			create_syntax = re.sub(' group by ', ' \ngroup by ', create_syntax, flags=re.I)
			create_syntax = re.sub('`,','`,\n',create_syntax)
			with open(target_path, 'a') as f:
				f.write(create_syntax)
				f.write(";\n\n")

def update_create_view_json(config):
	host = '{}'.format(config['DATABASE_SETUP']['HOST'])
	user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
	password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
	port = '{}'.format(config['DATABASE_SETUP']['PORT'])
	engine = sqla.create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}?charset=utf8mb4')

	inspector = sqla.inspect(engine)

	create_commands = {}

	for schema in ['patentsview_exports_granted','patentsview_exports_pregrant']:
		for view in inspector.get_view_names(schema):
			create_syntax = inspector.get_view_definition(view,schema)
			remove = 'ALGORITHM=UNDEFINED DEFINER=.* SQL SECURITY DEFINER'
			create_syntax = re.sub(remove, 'OR REPLACE', create_syntax)
			create_syntax = re.sub('[0-9]{4}-[01][0-9]-[0123][0-9]', "{datestring}",create_syntax)
			create_commands[f"{schema}.{view}"] = create_syntax

	with open(f"{config['FOLDERS']['project_root']}/{config['FOLDERS']['resources_folder']}/create_export_views.json",'w') as f:
		json.dump(create_commands, f)

def read_create_view_dictionary(config):
	with open(f"{config['FOLDERS']['project_root']}/{config['FOLDERS']['resources_folder']}/create_export_views.json",'r') as f:
		return(json.load(f))


def update_view_date_ranges(config):
	host = '{}'.format(config['DATABASE_SETUP']['HOST'])
	user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
	password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
	port = '{}'.format(config['DATABASE_SETUP']['PORT'])
	engine = sqla.create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}?charset=utf8mb4')

	view_creations = read_create_view_dictionary(config)

	failed_updates = []
	successful_updates = []
	for view in view_creations:
		try :
			sql = view_creations[view].format(datestring = config['DATES']['END_DATE'])
			print(f'UPDATING VIEW {view}')
			print(sql)
			engine.execute(sql)
			successful_updates.append(view)
		except Exception as e:
			print('update unsuccessful')
			print(e)
			failed_updates.append(view)
	
	if len(failed_updates) > 0:
		raise Exception(f"view creation/update failed for {len(failed_updates)} views:\n{'\n'.join(failed_updates)}")