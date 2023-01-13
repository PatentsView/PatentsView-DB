from QA.collect_supplemental_data.cpc_parser.CPCQA import qa_cpc_current, qa_wipo, save_qa_data


def call_patent_cpc_qa(**kwargs):
    db = 'granted_patent'
    qa_data = qa_cpc_current(db, **kwargs)
    save_qa_data(qa_data, db, **kwargs)

def call_pgpubs_cpc_qa(**kwargs):
    db = 'pgpubs'
    qa_data = qa_cpc_current(db, **kwargs)
    save_qa_data(qa_data, db, **kwargs)


def call_patent_wipo_qa(**kwargs):
    db = 'granted_patent'
    qa_data = qa_wipo(db, **kwargs)
    save_qa_data(qa_data, db, **kwargs)


def call_pgpubs_wip_qa(**kwargs):
    db = 'pgpubs'
    qa_data = qa_wipo(db, **kwargs)
    save_qa_data(qa_data, db, **kwargs)