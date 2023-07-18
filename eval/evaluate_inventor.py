import datetime
import pandas as pd
from sqlalchemy import create_engine
from lib.configuration import get_connection_string, get_current_config, get_disambig_config

from er_evaluation.estimators import pairwise_precision_design_estimate, pairwise_recall_design_estimate
from er_evaluation.summary import cluster_sizes
from pv_evaluation.benchmark import load_binette_2022_inventors_benchmark


def evaluate_inventor_clustering(config, **kwargs):
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    pquery = "select patent_id, patent_date from patentsview_export_granted.g_patent"
    print(pquery)
    patent = pd.read_sql_query(sql=pquery, con=engine)
    patent.to_csv("patent.csv")
    rquery = "select patent_id, sequence, inventor_id from patent.rawinventor "
    print(rquery)
    rawinventor = pd.read_sql_query(sql=rquery, con=engine)
    rawinventor.to_csv("rawinventor.csv")
    patent_date = pd.DatetimeIndex(patent.patent_date)
    patent["patent_date"] = patent_date.year.astype(int)
    joined = rawinventor.merge(patent, on="patent_id", how="left")
    joined["mention_id"] = "US" + joined.patent_id + "-" + str(joined.sequence)
    joined = joined.query('patent_date >= 1975 and patent_date <= 2022')
    breakpoint()
    current_disambiguation = joined.set_index("mention_id")["inventor_id"]

    pairwise_precision_design_estimate(current_disambiguation, load_binette_2022_inventors_benchmark(),
                                       weights=1 / cluster_sizes(load_binette_2022_inventors_benchmark()))


if __name__ == '__main__':
    d = datetime.date(2023, 1, 1)
    config = get_disambig_config(schedule='quarterly', **{
        "execution_date": d
    })
    # config = get_current_config('granted_patent', schedule='quarterly', **kwargs)
    evaluate_inventor_clustering(config)

