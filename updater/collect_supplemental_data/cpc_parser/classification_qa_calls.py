from QA.collect_supplemental_data.cpc_parser.CPCQA import qa_cpc_current, qa_wipo, save_qa_data


def call_patent_cpc_qa(**kwargs):
    qa_data = qa_cpc_current(**kwargs)
    save_qa_data(qa_data, **kwargs)

def call_pgpubs_cpc_qa(**kwargs):
    qa_data = qa_cpc_current(**kwargs)
    save_qa_data(qa_data, **kwargs)

def call_patent_wipo_qa(**kwargs):
    qa_data = qa_wipo(**kwargs)
    save_qa_data(qa_data, **kwargs)

def call_pgpubs_wipo_qa(**kwargs):
    qa_data = qa_wipo(**kwargs)
    save_qa_data(qa_data, **kwargs)