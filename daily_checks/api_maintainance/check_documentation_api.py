import json
import requests


def check_query_result_size(query):
    r = requests.get(query)
    if r.status_code != 200:
        raise Exception("Query Failed")
    json_response = r.json()
    if json_response["count"] == 0:
        raise Exception("Query returned 0 rows")


def test_api_site_with_defaults(api_host, querylist):
    for endpoint, endpoint_queries in querylist.items():
        for query_suffix in endpoint_queries:
            api_query = "{host}/{q}".format(host=api_host, q=query_suffix)
            check_query_result_size(api_query)


def start_query_verification(config):
    api_host = config["DAILY_CHECKS"]["api_host"]
    querylist = json.load(open(config["DAILY_CHECKS"]["querylist_file"]))
    test_api_site_with_defaults(api_host, querylist)
