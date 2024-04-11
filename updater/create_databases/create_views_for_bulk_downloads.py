import sqlalchemy as sqla
import re
import json
import os
import traceback
from lib.configuration import get_current_config


def update_create_view_sql(config, output_path, if_exists="replace"):
    assert if_exists in (
        "replace",
        "fail",
        "append",
    ), "disallowed if_exists value. must be in ('replace','fail','append')"
    host = "{}".format(config["DATABASE_SETUP"]["HOST"])
    user = "{}".format(config["DATABASE_SETUP"]["USERNAME"])
    password = "{}".format(config["DATABASE_SETUP"]["PASSWORD"])
    port = "{}".format(config["DATABASE_SETUP"]["PORT"])
    engine = sqla.create_engine(
        f"mysql+pymysql://{user}:{password}@{host}:{port}?charset=utf8mb4"
    )

    inspector = sqla.inspect(engine)

    if os.path.exists(output_path):
        if if_exists == "fail":
            raise Exception("output path already exists")
        if if_exists == "replace":
            os.remove(output_path)

    for schema in ["patentsview_export_granted", "patentsview_export_pregrant"]:
        for view in inspector.get_view_names(schema):
            if view.startswith("temp_"):
                continue
            create_syntax = inspector.get_view_definition(view, schema)
            remove = "ALGORITHM=UNDEFINED DEFINER=.* SQL SECURITY (DEFINER|INVOKER)"
            create_syntax = re.sub(
                remove, "OR REPLACE SQL SECURITY INVOKER", create_syntax
            )
            create_syntax = re.sub(
                "(?<=['\"])[0-9]{4}-?[01][0-9]-?[0123][0-9](?=['\"])",
                "{{datestring}}",
                create_syntax,
            )
            create_syntax = re.sub(" from ", " \nfrom ", create_syntax, flags=re.I)
            create_syntax = re.sub(" select ", " \nselect ", create_syntax, flags=re.I)
            create_syntax = re.sub(" where ", " \nwhere ", create_syntax, flags=re.I)
            create_syntax = re.sub(
                " group by ", " \ngroup by ", create_syntax, flags=re.I
            )
            create_syntax = re.sub(
                " left join ", " \nleft join ", create_syntax, flags=re.I
            )
            create_syntax = re.sub(
                "(?<!left) join ", " \njoin ", create_syntax, flags=re.I
            )
            create_syntax = re.sub(" union ", " \nunion ", create_syntax, flags=re.I)
            create_syntax = re.sub("`, ?", "`,\n", create_syntax)
            with open(output_path, "a") as f:
                f.write(create_syntax)
                f.write(";\n\n")


def update_create_view_json(config, output_path, if_exists="replace"):
    assert if_exists in (
        "replace",
        "fail",
        "append",
    ), "disallowed if_exists value. must be in ('replace','fail','append')"
    host = "{}".format(config["DATABASE_SETUP"]["HOST"])
    user = "{}".format(config["DATABASE_SETUP"]["USERNAME"])
    password = "{}".format(config["DATABASE_SETUP"]["PASSWORD"])
    port = "{}".format(config["DATABASE_SETUP"]["PORT"])
    engine = sqla.create_engine(
        f"mysql+pymysql://{user}:{password}@{host}:{port}?charset=utf8mb4"
    )

    inspector = sqla.inspect(engine)

    if os.path.exists(output_path):
        if if_exists == "fail":
            raise Exception("output path already exists")
        if if_exists == "replace":
            os.remove(output_path)

    create_commands = {}

    for schema in ["patentsview_export_granted", "patentsview_export_pregrant"]:
        for view in inspector.get_view_names(schema):
            if view.startswith("temp_"):
                continue
            create_syntax = inspector.get_view_definition(view, schema)
            remove = "ALGORITHM=UNDEFINED DEFINER=.* SQL SECURITY (DEFINER|INVOKER)"
            create_syntax = re.sub(
                remove, "OR REPLACE SQL SECURITY INVOKER", create_syntax
            )
            create_syntax = re.sub(
                "(?<=['\"])[0-9]{4}-?[01][0-9]-?[0123][0-9](?=['\"])",
                "{datestring}",
                create_syntax,
            )
            create_commands[f"{schema}.{view}"] = create_syntax

    with open(output_path, "a") as f:
        json.dump(create_commands, f, indent=4)


def read_create_view_dictionary(config):
    with open(
        f"{config['FOLDERS']['resources_folder']}/create_export_views.json", "r"
    ) as f:
        return json.load(f)


def create_persistent_view_definition(view_name:str, engine:sqla.engine.Engine, end_date:str) -> str:
    """
    generates the view definition for the bulk download view for persistent entity tables.
    :param view_name: The name of the view to be created.
    :param engine (sqlalchemy.engine.base.Engine): SQLAlchemy database engine.
    :param end_date (str): The end date for filtering data.

    :return: SQL syntax for creating the specified view.
    """
    if view_name.startswith("patentsview_export_pregrant"):
        base_doc_id = "document_number"
        exp_doc_id = "pgpub_id"
        base_schema = "pregrant_publications"
    elif view_name.startswith("patentsview_export_granted"):
        base_doc_id = "patent_id"
        exp_doc_id = "patent_id"
        base_schema = "patent"
    else:
        raise Exception(f"invalid persistent entity view name: `{view_name}`. Ensure the provided name includes both the schema and view name")
    if view_name.endswith("assignee"):
        ent_type = "assignee"
    elif view_name.endswith("inventor"):
        ent_type = "inventor"
    else:
        raise Exception(f"invalid persistent entity view name: `{view_name}`. Ensure the provided name includes both the schema and view name.")
    inspector = sqla.inspect(engine)
    fields = [
        col["name"]
        for col in inspector.get_columns(
            table_name=f"persistent_{ent_type}_disambig", schema=base_schema
        )
    ]
    id_fields = [
        fld
        for fld in fields
        if fld.startswith(f"disamb_{ent_type}_id_")
        and (re.search(r"\d{8}", fld).group(0) <= end_date)
    ]
    view_syntax = (
        f"""
CREATE OR REPLACE SQL SECURITY INVOKER VIEW {view_name} AS 
SELECT
    `{base_schema}`.`persistent_{ent_type}_disambig`.`{base_doc_id}` AS `{exp_doc_id}`,
    `{base_schema}`.`persistent_{ent_type}_disambig`.`sequence` AS `{ent_type}_sequence`,
    """ + ",\n    ".join(
            [
                f"`{base_schema}`.`persistent_{ent_type}_disambig`.`{field}` AS `{field}`"
                for field in sorted(id_fields)
            ]
        )
        + f"""
FROM `{base_schema}`.`persistent_{ent_type}_disambig`
WHERE `{base_schema}`.`persistent_{ent_type}_disambig`.`version_indicator` <= '{end_date}'
"""
    )
    return view_syntax


def update_view_date_ranges(**kwargs):
    """
    reads view dictionary json file and executes view create/replace SQL using new date threshold.
    all view replacements are attempted before any errors are raised to streamline simultaneous identification and correction of issues.
    """
    config = get_current_config("granted_patent", schedule="quarterly", **kwargs)
    host = "{}".format(config["DATABASE_SETUP"]["HOST"])
    user = "{}".format(config["DATABASE_SETUP"]["USERNAME"])
    password = "{}".format(config["DATABASE_SETUP"]["PASSWORD"])
    port = "{}".format(config["DATABASE_SETUP"]["PORT"])
    engine = sqla.create_engine(
        f"mysql+pymysql://{user}:{password}@{host}:{port}?charset=utf8mb4"
    )

    view_creations = read_create_view_dictionary(config)

    failed_updates = []
    successful_updates = []
    for view in view_creations:
        try:
            print(f"UPDATING VIEW {view}")
            if "persistent" in view:
                sql = create_persistent_view_definition(
                    view, view_creations[view], engine, config["DATES"]["END_DATE"]
                )
            else:
                sql = view_creations[view].format(
                    datestring=config["DATES"]["END_DATE"]
                )
            print(sql)
            engine.execute(sql)
            successful_updates.append(view)
        except Exception as e:
            print("update unsuccessful")
            print(e)
            print(traceback.format_exc())
            failed_updates.append(view)
        print("\n\n")

    if len(failed_updates) > 0:
        raise Exception(
            "view creation/update failed for {0} views:\n{1}".format(
                len(failed_updates), "\n".join(failed_updates)
            )
        )
