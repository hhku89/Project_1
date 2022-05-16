from sdw4_mapping.core.domain import Domain
from lib import common
import pandas as pd
import numpy as np
# drop_lst=['projectid', 'studyid', 'Folder','environmentName', 'StudySiteId', 'siteid',
#             'Site','instanceId','InstanceName', 'InstanceRepeatNumber', 'folderid',
#             'FolderSeq', 'TargetDays', 'DataPageId', 'DataPageName',
#             'PageRepeatNumber', 'RecordDate', 'RecordId', 'RecordPosition',
#             'MinCreated', 'MaxUpdated', 'SaveTS', 'StudyEnvSiteNumber' ]
def cook(study):
    dom_raw = study.get_raw_domain("mhyn")
    dom = Domain(study, "MHYN")
    dom.data = dom_raw.data.copy()
    dom.add_column_constant("DOMAIN", "MHYN")
    dom.drop_columns("subjectId")
    
    # [dom.drop_columns(i) for i in drop_lst]
    study.apply_sex_age(dom)
    study.apply_common_transforms(dom)
    dom.drop_columns(common.drop_lst)
    dom.drop_columns(common.cols_drop(dom.data))
    dom.rename_columns({"S_MH_MHYN": "MHYN","LBV_HIV": "LBVHIV", "LBV_HEPB": "LBVHEPB", "LBV_HEPC": "LBVHEPC"})
    common.make_tc(dom, 'LBDAT')
    dom.filter_rows(lambda r: r["RecordActive"] == '1')

    return dom
