from sdw4_mapping.core.domain import Domain
from lib import common
import pandas as pd
import numpy as np

drop_lst = ['projectid', 'studyid', 'Folder', 'environmentName', 'StudySiteId', 'siteid',
            'Site', 'instanceId', 'InstanceName', 'InstanceRepeatNumber', 'folderid',
            'FolderSeq', 'TargetDays', 'DataPageId', 'DataPageName',
            'PageRepeatNumber', 'RecordDate', 'RecordId', 'RecordPosition',
            'MinCreated', 'MaxUpdated', 'SaveTS', 'StudyEnvSiteNumber']


def cook(study):
    dom_raw = study.get_raw_domain("ae_react2")
    dom = Domain(study, "AERCT2")
    dom.data = dom_raw.data.copy()
    dom.add_column_constant("DOMAIN", "AERCT2")
    dom.drop_columns("subjectId")
    [dom.drop_columns(i) for i in drop_lst]
    study.apply_sex_age(dom)
    study.apply_common_transforms(dom)
    # dom.drop_columns(common.drop_lst)
    # dom.drop_columns(common.cols_drop(dom.data))
    common.make_tc(dom, 'AE_REACT_DAT')
    dom.data.columns = dom.data.columns.str.replace("AE_REACT_", "AEREACT")
    dom.rename_columns({'AE_REACT_DAT': 'AEREACTSTDTC'})
    dom.data = dom.data.loc[:, ~dom.data.columns.str.contains('_RAW$|_STD$|_INT$|_YYYY$|_MM$|_DD$')]
    dom.filter_rows(lambda r: r["RecordActive"] == '1')
    return dom