import datetime
import pandas as pd
from sqlalchemy import create_engine
from lib.configuration import get_connection_string, get_current_config, get_disambig_config
from importlib import resources

from er_evaluation.estimators import pairwise_precision_estimator, pairwise_recall_estimator
from er_evaluation.summary import cluster_sizes
from pv_evaluation.benchmark import load_pv_2024_assignee_benchmark

def get_data_for_assignee_summary_stats(config):
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    pquery = "select * from patentsview_export_granted.g_persistent_assignee"
    print(pquery)
    # patent = pd.read_sql_query(sql=pquery, con=engine, chunksize=150000)
    # patent.to_csv("patent.csv")
    iquery = "select patent_id, assignee_sequence, raw_assignee_name_first, raw_assignee_name_last from patentsview_export_granted.g_assignee_not_disambiguated "
    print(iquery)
    persistent_assignee = pd.read_sql_query(sql=pquery, con=engine)
    assignee_not_disambiguated = pd.read_sql_query(sql=iquery, con=engine)

    assignee_not_disambiguated["mention_id"] = "US" + assignee_not_disambiguated.patent_id.astype(str) + "-" + assignee_not_disambiguated.assignee_sequence.astype(str)
    assignee_not_disambiguated["name"] = assignee_not_disambiguated.raw_assignee_name_first + " " + assignee_not_disambiguated.raw_assignee_name_last
    assignee_not_disambiguated.set_index("mention_id", inplace=True)
    names = assignee_not_disambiguated["name"]
    return persistent_assignee, names

def get_data_for_assignee_precision_recall(config):
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    pquery = "select patent_id, patent_date from patentsview_export_granted.g_patent"
    print(pquery)
    patent = pd.read_sql_query(sql=pquery, con=engine)
    patent.to_csv("patent.csv")
    rquery = "select patent_id, sequence, assignee_id from patent.rawassignee "
    print(rquery)
    rawassignee = pd.read_sql_query(sql=rquery, con=engine)
    patent_date = pd.DatetimeIndex(patent.patent_date)
    patent["patent_date"] = patent_date.year.astype(int)
    joined = rawassignee.merge(patent, on="patent_id", how="left")
    joined["mention_id"] = "US" + joined.patent_id.astype(str) + "-" + joined.sequence.astype(str)
    joined = joined.query('patent_date >= 1975 and patent_date < 2024')
    current_disambiguation = joined.set_index("mention_id")["assignee_id"]
    return current_disambiguation

# def load_pv_2024_assignee_benchmark():
#     """Quick Hack: consolidated_assignee_samples.csv copied from the PatentsView-Evaluation repo"""
#     # df = pd.read_csv("consolidated_assignee_samples.csv")
#     with resources.open_text("eval", "consolidated_assignee_samples.csv") as f:
#         df = pd.read_csv(f)
#     df = df.drop(columns=['Unnamed: 0'])
#     df.set_index("mention_id", inplace=True)
#     return df["unique_id"]

def evaluate_assignee_precision_recall(config):
    qa_data = {
        "DataMonitor_assignee_precision_recall": []
    }
    end_date = config["DATES"]["END_DATE"]
    current_disambiguation = get_data_for_assignee_precision_recall(config)
    # PRECISION & RECALL
    precision, p_std = pairwise_precision_estimator(current_disambiguation, load_pv_2024_assignee_benchmark(), weights=1 / cluster_sizes(load_pv_2024_assignee_benchmark()))
    recall, r_std =pairwise_recall_estimator(current_disambiguation, load_pv_2024_assignee_benchmark(), weights=1 / cluster_sizes(load_pv_2024_assignee_benchmark()))
    qa_data['DataMonitor_assignee_precision_recall'].append(
        {
            'update_version': end_date,
            'precision': precision,
            "p_std": p_std,
            'recall': recall,
            'r_std': r_std
        })
    qa_connection_string = get_connection_string(config, database='QA_DATABASE', connection='APP_DATABASE_SETUP')
    qa_engine = create_engine(qa_connection_string)
    pd.DataFrame(qa_data['DataMonitor_assignee_precision_recall']).to_sql(name="DataMonitor_assignee_precision_recall", if_exists='append', con=qa_engine, index=False)


if __name__ == '__main__':
    d = datetime.date(2023, 10, 1)
    # config = get_disambig_config(schedule='quarterly', **{
    #     "execution_date": d
    # })
    config = get_current_config('granted_patent', schedule='quarterly', **{
        "execution_date": d
    })
    # sample = load_pv_2024_assignee_benchmark()
    # cluster_sizes(load_pv_2024_assignee_benchmark())
    evaluate_assignee_precision_recall(config)
    # evaluation_assignee_summary_stats(config)

