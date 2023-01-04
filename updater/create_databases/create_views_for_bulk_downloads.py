import sqlalchemy as sqla
import re
import json
import os

def update_create_view_sql(config, output_path, if_exists='replace'):
	assert if_exists in ('replace','fail','append'), "disallowed if_exists value. must be in ('replace','fail','append')"
	host = '{}'.format(config['DATABASE_SETUP']['HOST'])
	user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
	password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
	port = '{}'.format(config['DATABASE_SETUP']['PORT'])
	engine = sqla.create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}?charset=utf8mb4')

	inspector = sqla.inspect(engine)

	if os.path.exists(output_path): 
		if if_exists == 'fail':
			raise Exception("output path already exists")
		if if_exists == 'replace':
			os.remove(output_path)
	
	for schema in ['patentsview_export_granted','patentsview_export_pregrant']:
		for view in inspector.get_view_names(schema):
			create_syntax = inspector.get_view_definition(view,schema)
			remove = 'ALGORITHM=UNDEFINED DEFINER=.* SQL SECURITY (DEFINER|INVOKER)'
			create_syntax = re.sub(remove, 'OR REPLACE SQL SECURITY INVOKER', create_syntax)
			create_syntax = re.sub('[0-9]{4}-[01][0-9]-[0123][0-9]', "{{datestring}}",create_syntax)
			create_syntax = re.sub(' from ', ' \nfrom ', create_syntax, flags=re.I)
			create_syntax = re.sub(' select ', ' \nselect ', create_syntax, flags=re.I)
			create_syntax = re.sub(' where ', ' \nwhere ', create_syntax, flags=re.I)
			create_syntax = re.sub(' group by ', ' \ngroup by ', create_syntax, flags=re.I)
			create_syntax = re.sub('`,','`,\n',create_syntax)
			with open(output_path, 'a') as f:
				f.write(create_syntax)
				f.write(";\n\n")

def update_create_view_json(config, output_path, if_exists='replace'):
	assert if_exists in ('replace','fail','append'), "disallowed if_exists value. must be in ('replace','fail','append')"
	host = '{}'.format(config['DATABASE_SETUP']['HOST'])
	user = '{}'.format(config['DATABASE_SETUP']['USERNAME'])
	password = '{}'.format(config['DATABASE_SETUP']['PASSWORD'])
	port = '{}'.format(config['DATABASE_SETUP']['PORT'])
	engine = sqla.create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}?charset=utf8mb4')

	inspector = sqla.inspect(engine)

	if os.path.exists(output_path): 
		if if_exists == 'fail':
			raise Exception("output path already exists")
		if if_exists == 'replace':
			os.remove(output_path)

	create_commands = {}

	for schema in ['patentsview_export_granted','patentsview_export_pregrant']:
		for view in inspector.get_view_names(schema):
			create_syntax = inspector.get_view_definition(view,schema)
			remove = 'ALGORITHM=UNDEFINED DEFINER=.* SQL SECURITY (DEFINER|INVOKER)'
			create_syntax = re.sub(remove, 'OR REPLACE SQL SECURITY INVOKER', create_syntax)
			create_syntax = re.sub('[0-9]{4}-[01][0-9]-[0123][0-9]', "{datestring}",create_syntax)
			create_commands[f"{schema}.{view}"] = create_syntax

	with open(output_path,'a') as f:
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
		raise Exception("view creation/update failed for {0} views:\n{1}".format(len(failed_updates), '\n'.join(failed_updates)))