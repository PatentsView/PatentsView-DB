import datetime
import pandas as pd
from sqlalchemy import create_engine
from lib.configuration import get_connection_string, get_current_config, get_disambig_config

from er_evaluation.estimators import pairwise_precision_design_estimate, pairwise_recall_design_estimate
from er_evaluation.summary import cluster_sizes
from pv_evaluation.benchmark import load_binette_2022_inventors_benchmark

def get_data_for_inventor_summary_stats(config):
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    pquery = "select * from patentsview_export_granted.g_persistent_inventor"
    print(pquery)
    patent = pd.read_sql_query(sql=pquery, con=engine)
    patent.to_csv("patent.csv")
    iquery = "select patent_id, inventor_sequence, raw_inventor_name_first, raw_inventor_name_last from patentsview_export_granted.g_inventor_not_disambiguated "
    print(iquery)
    persistent_inventor = pd.read_sql_query(sql=pquery, con=engine)
    inventor_not_disambiguated = pd.read_sql_query(sql=iquery, con=engine)

    inventor_not_disambiguated["mention_id"] = "US" + inventor_not_disambiguated.patent_id.astype(str) + "-" + inventor_not_disambiguated.inventor_sequence.astype(str)
    inventor_not_disambiguated["name"] = inventor_not_disambiguated.raw_inventor_name_first + " " + inventor_not_disambiguated.raw_inventor_name_last
    inventor_not_disambiguated.set_index("mention_id", inplace=True)
    names = inventor_not_disambiguated["name"]
    return persistent_inventor, names

def evaluation_inventor_summary_stats(config):
    qa_data = {
        "DataMonitor_inventor_summary_stats": []
    }
    end_date = config["DATES"]["END_DATE"]
    persistent_inventor, names = get_data_for_inventor_summary_stats(config)

    from pv_evaluation.benchmark import inventor_summary_trend_plot
    fig = inventor_summary_trend_plot(persistent_inventor, names)
    fig.update_layout(
        width=800,
        height=300,
        title="Summary Statistics"
    )
    fig['layout'].update(margin=dict(l=20, r=20, b=20, t=60))
    fig.write_image("summary_trend.pdf")
    fig.show(renderer="svg")
    breakpoint()

    # qa_data['DataMonitor_inventor_summary_stats'].append(
    #     {
    #         'update_version': end_date,
    #         'precision': precision,
    #         "p_std": p_std,
    #         'recall': recall,
    #         'r_std': r_std
    #     })
    # qa_data.to_sql(name="DataMonitor_inventor_summary_stats", if_exists='append', con=engine, index=False)

def get_data_for_inventor_precision_recall(config):
    engine = create_engine(get_connection_string(config, "RAW_DB"))
    pquery = "select patent_id, patent_date from patentsview_export_granted.g_patent"
    print(pquery)
    patent = pd.read_sql_query(sql=pquery, con=engine)
    patent.to_csv("patent.csv")
    rquery = "select patent_id, sequence, inventor_id from patent.rawinventor "
    print(rquery)
    rawinventor = pd.read_sql_query(sql=rquery, con=engine)
    patent_date = pd.DatetimeIndex(patent.patent_date)
    patent["patent_date"] = patent_date.year.astype(int)
    joined = rawinventor.merge(patent, on="patent_id", how="left")
    joined["mention_id"] = "US" + joined.patent_id.astype(str) + "-" + joined.sequence.astype(str)
    joined = joined.query('patent_date >= 1975 and patent_date <= 2022')
    current_disambiguation = joined.set_index("mention_id")["inventor_id"]
    return current_disambiguation


def evaluate_inventor_precision_recall(config):
    qa_data = {
        "DataMonitor_inventor_precision_recall": []
    }
    end_date = config["DATES"]["END_DATE"]
    current_disambiguation = get_data_for_inventor_precision_recall(config)
    # PRECISION & RECALL
    precision, p_std = pairwise_precision_design_estimate(current_disambiguation, load_binette_2022_inventors_benchmark(), weights=1 / cluster_sizes(load_binette_2022_inventors_benchmark()))
    recall, r_std =pairwise_recall_design_estimate(current_disambiguation, load_binette_2022_inventors_benchmark(), weights=1 / cluster_sizes(load_binette_2022_inventors_benchmark()))
    qa_data['DataMonitor_inventor_precision_recall'].append(
        {
            'update_version': end_date,
            'precision': precision,
            "p_std": p_std,
            'recall': recall,
            'r_std': r_std
        })
    qa_connection_string = get_connection_string(config, database='QA_DATABASE', connection='APP_DATABASE_SETUP')
    qa_engine = create_engine(qa_connection_string)
    pd.DataFrame(qa_data['DataMonitor_inventor_precision_recall']).to_sql(name="DataMonitor_inventor_precision_recall", if_exists='append', con=qa_engine, index=False)


if __name__ == '__main__':
    d = datetime.date(2023, 1, 1)
    config = get_disambig_config(schedule='quarterly', **{
        "execution_date": d
    })
    # config = get_current_config('granted_patent', schedule='quarterly', **kwargs)
    # evaluate_inventor_precision_recall(config)
    evaluation_inventor_summary_stats(config)

