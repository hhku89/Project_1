from sdw4_mapping.core.domain import Domain
from lib import common
import pandas as pd
import numpy as np
drop_lst=['projectid', 'studyid', 'Folder','environmentName',
            'instanceId','InstanceName', 'InstanceRepeatNumber', 'folderid',
            'FolderSeq', 'TargetDays', 'DataPageId', 'DataPageName',
            'PageRepeatNumber', 'RecordDate', 'RecordId', 'RecordPosition',
            'MinCreated', 'MaxUpdated', 'SaveTS', 'StudyEnvSiteNumber']


def cook(study):
    dom_raw = study.get_raw_domain("lab")
    dom = Domain(study, "LB")
    dom.data = dom_raw.data.copy()
    dom.add_column_constant("DOMAIN", "LB")
    dom.rename_columns({'SUBJID':'Subject'})
    # dom.drop_columns("subjectId")
    [dom.drop_columns(i) for i in drop_lst]
    dom.rename_columns({"LBTESTCD": "LABCD","VISIT": "VISITNAME", "LBDATC": "LBSTDTC", "SITEID": "Site"})
    dom.data['LBTESTCD'] = dom.data["LABCD"].map(common.dictlab)
    study.apply_sex_age(dom)
    study.apply_common_transforms(dom)
    dom.rename_columns({"VISITNAME": "VISIT"})
    # dom.drop_columns(common.drop_lst)
    # dom.drop_columns(common.cols_drop(dom.data))
    common.make_tc(dom,"LBSTDTC")
    common.studyDUR(dom.data, "RFSTDTC", "LBSTDTC", "LBSTDY")
    return dom
