from sdw4_mapping.core.domain import Domain
from lib import common
import pandas as pd
import numpy as np


def cook(study):
    parts = [
        cook_dsscrn(study),
        cook_dsscrf(study),
        cook_dsCMRE(study),
        cook_dsCMRNE(study),
        cook_dsfw(study),
        cook_dsAEDTH(study),
        cook_dsSTYC(study),
        cook_dsSTYD(study),
        cook_rand(study)
    ]

    ds = Domain(study, "DS")
    ds.data = pd.concat([p.data for p in parts], ignore_index=True)

    return ds


def cook_dsscrn(study):
    dom = Domain(study, "DS")
    dom.data = study.get_raw_domain("ds_ic").data.copy()
    dom.filter_rows(lambda r: r["RecordActive"] == '1')
    dom.add_column_constant("DOMAIN", "DS")
    dom.drop_columns("subjectId")
    study.apply_common_transforms(dom)

    dom.rename_columns({"S_DS_IC_IFCSTDAT": "DSSTDTC"})
    common.make_tc(dom, "DSSTDTC")

    # dom.filter_rows(lambda r: not pd.isnull(r["domSTDTC"]))
    dom.filter_rows(lambda r: pd.notnull(r["DSSTDTC"]))
    dom.add_column_constant("DSDECOD", "Screened")

    common.studyDUR(dom.data, "RFSTDTC", "DSSTDTC", "DSSTDY")
    dom.data["DSSTDY"] = dom.data["DSSTDY"].apply(lambda x: x if pd.isnull(x) or int(x) != 0 else 1)

    dom.set_columns(["subjectId", "SITEID", "visitId", "DOMAIN",
                     "DSDECOD", "DSSTDTC", "DSSTDY", "RFSTDTC", "DateCreated", "DataPageName","DataPageId"])

    return dom


def cook_rand(study):
    dom = Domain(study, "DS")
    dom.data = study.get_raw_domain("sv").data.copy()
    dom.filter_rows(lambda r: r["RecordActive"] == '1')
    dom.add_column_constant("DOMAIN", "DS")
    dom.drop_columns("subjectId")
    dom.data = dom.data[dom.data['Folder'] == 'VCHECKIN'].copy()
    study.apply_common_transforms(dom)

    dom.filter_rows(lambda r: not pd.isnull(r["S_SV_VISDAT"]))
    dom.add_column_constant("DSDECOD", "Randomised")
    dom.rename_columns({"S_SV_VISDAT": "DSSTDTC"})
    common.make_tc(dom, "DSSTDTC")

    common.studyDUR(dom.data, "RFSTDTC", "DSSTDTC", "DSSTDY")
    dom.data["DSSTDY"] = dom.data["DSSTDY"].apply(lambda x: x if pd.isnull(x) or int(x) != 0 else 1)
    dom.set_columns(["subjectId", "SITEID", "visitId", "DOMAIN",
                     "DSDECOD", "DSSTDTC", "DSSTDY", "RFSTDTC", "DateCreated", "DataPageName", "DataPageId"])
    return dom


def cook_dsscrf(study):
    dom = Domain(study, "DS")
    dom.data = study.get_raw_domain("sf").data.copy()
    dom.filter_rows(lambda r: r["RecordActive"] == '1')
    dom.add_column_constant("DOMAIN", "DS")
    dom.drop_columns("subjectId")
    study.apply_common_transforms(dom)

    dom.rename_columns({"SFDAT": "DSSTDTC"})
    dom.filter_rows(lambda r: pd.notnull(r["DSSTDTC"]))
    dom.add_column_constant("DSDECOD", "Screen Failed")
    common.make_tc(dom, "DSSTDTC")

    common.studyDUR(dom.data, "RFSTDTC", "DSSTDTC", "DSSTDY")
    dom.data["DSSTDY"] = dom.data["DSSTDY"].apply(lambda x: x if pd.isnull(x) or int(x) != 0 else 1)

    dom.set_columns(["subjectId", "SITEID", "visitId", "DOMAIN",
                     "DSDECOD", "DSSTDTC", "DSSTDY", "RFSTDTC", "DateCreated", "DataPageName", "DataPageId"])

    return dom


def cook_dsCMRE(study):
    ds = Domain(study, "DS")
    ds.data = study.get_raw_domain("impprep").data.copy()
    ds.filter_rows(lambda r: r["RecordActive"] == '1')
    ds.add_column_constant("DOMAIN", "DS")
    ds.drop_columns("subjectId")
    study.apply_common_transforms(ds)

    ds.rename_columns({"CMRCDDAT": "DSSTDTC"})
    common.make_tc(ds, "DSSTDTC")

    # ds.filter_rows(lambda r: not pd.isnull(r["DSSTDTC"]))
    ds.filter_rows(lambda r: r["CMRSCAN"] == "Yes")
    ds.add_column_constant("DSDECOD", "Subjects eligible for CMR exam")

    common.studyDUR(ds.data, "RFSTDTC", "DSSTDTC", "DSSTDY")
    ds.data["DSSTDY"] = ds.data["DSSTDY"].apply(lambda x: x if pd.isnull(x) or int(x) != 0 else 1)

    ds.set_columns(["subjectId", "SITEID", "visitId", "DOMAIN",
                    "DSDECOD", "DSSTDTC", "DSSTDY", "RFSTDTC", "DateCreated", "DataPageName", "DataPageId"])

    return ds


def cook_dsCMRNE(study):
    ds = Domain(study, "DS")
    ds.data = study.get_raw_domain("impprep").data.copy()
    ds.filter_rows(lambda r: r["RecordActive"] == '1')
    ds.add_column_constant("DOMAIN", "DS")
    ds.drop_columns("subjectId")
    study.apply_common_transforms(ds)
    ds.rename_columns({"CMRCDDAT": "DSSTDTC"})
    common.make_tc(ds, "DSSTDTC")

    # ds.filter_rows(lambda r: not pd.isnull(r["DSSTDTC"]))
    ds.filter_rows(lambda r: r["CMRSCAN"] == "No")
    ds.add_column_constant("DSDECOD", "Subjects  not eligible for CMR exam")

    common.studyDUR(ds.data, "RFSTDTC", "DSSTDTC", "DSSTDY")
    ds.data["DSSTDY"] = ds.data["DSSTDY"].apply(lambda x: x if pd.isnull(x) or int(x) != 0 else 1)

    ds.set_columns(["subjectId", "SITEID", "visitId", "DOMAIN",
                    "DSDECOD", "DSSTDTC", "DSSTDY", "RFSTDTC", "DateCreated", "DataPageName", "DataPageId"])

    return ds


def cook_dsfw(study):
    ds = Domain(study, "DS")
    ds.data = study.get_raw_domain("flu").data.copy()
    ds.filter_rows(lambda r: r["RecordActive"] == '1')
    ds.add_column_constant("DOMAIN", "DS")
    ds.drop_columns("subjectId")
    study.apply_common_transforms(ds)
    ds.rename_columns({"FLU_DAT": "DSSTDTC"})
    common.make_tc(ds, "DSSTDTC")

    # ds.filter_rows(lambda r: not pd.isnull(r["DSSTDTC"]))
    ds.filter_rows(lambda r: r["FLUDIED"] == "Yes")
    ds.add_column_constant("DSDECOD", "Death")

    common.studyDUR(ds.data, "RFSTDTC", "DSSTDTC", "DSSTDY")
    ds.data["DSSTDY"] = ds.data["DSSTDY"].apply(lambda x: x if pd.isnull(x) or int(x) != 0 else 1)

    ds.set_columns(["subjectId", "SITEID", "visitId", "DOMAIN",
                    "DSDECOD", "DSSTDTC", "DSSTDY", "RFSTDTC", "DateCreated", "DataPageName", "DataPageId"])

    return ds


def cook_dsAEDTH(study):
    ds = Domain(study, "DS")
    ds.data = study.get_raw_domain("sae").data.copy()
    ds.filter_rows(lambda r: r["RecordActive"] == '1')
    ds.add_column_constant("DOMAIN", "DS")
    ds.drop_columns("subjectId")
    study.apply_common_transforms(ds)
    ds.rename_columns({"S_AE_AESTDAT": "DSSTDTC"})
    common.make_tc(ds, "DSSTDTC")

    # ds.filter_rows(lambda r: not pd.isnull(r["DSSTDTC"]))
    ds.filter_rows(lambda r: r["S_AE_AESDTH"] == "Yes")
    ds.add_column_constant("DSDECOD", "Death")

    common.studyDUR(ds.data, "RFSTDTC", "DSSTDTC", "DSSTDY")
    ds.data["DSSTDY"] = ds.data["DSSTDY"].apply(lambda x: x if pd.isnull(x) or int(x) != 0 else 1)

    ds.set_columns(["subjectId", "SITEID", "visitId", "DOMAIN",
                    "DSDECOD", "DSSTDTC", "DSSTDY", "RFSTDTC", "DateCreated", "DataPageName", "DataPageId"])

    return ds


def cook_dsSTYC(study):
    ds = Domain(study, "DS")
    ds.data = study.get_raw_domain("ds_eos").data.copy()
    ds.filter_rows(lambda r: r["RecordActive"] == '1')
    ds.add_column_constant("DOMAIN", "DS")
    ds.drop_columns("subjectId")
    ds.data.columns = ds.data.columns.str.replace("DS_", "DS")
    study.apply_common_transforms(ds)
    ds.rename_columns({"DSEOSSTDAT": "DSSTDTC", "DSEOSDECOD": "DSSCAT"})
    common.make_tc(ds, "DSSTDTC")

    ds.filter_rows(lambda r: r["DSEOSNY"] == "Yes")
    ds.add_column_constant("DSDECOD", "Study Completed")

    common.studyDUR(ds.data, "RFSTDTC", "DSSTDTC", "DSSTDY")
    ds.data["DSSTDY"] = ds.data["DSSTDY"].apply(lambda x: x if pd.isnull(x) or int(x) != 0 else 1)
    ds.rename_columns({})
    ds.set_columns(["subjectId", "SITEID", "visitId", "DOMAIN",
                    "DSSCAT", "DSDECOD", "RFSTDTC",
                    'DSEOSPV','DSEOSTEC','DSEOSSF','DSAESPID3','DSEOSPAR','DSEOSOTH','DSAESPID1','DSEOSNY','DSAESPID2','DSEOSPHY',
                    "DateCreated", "DataPageName", "DataPageId",])

    return ds


def cook_dsSTYD(study):
    ds = Domain(study, "DS")
    ds.data = study.get_raw_domain("ds_eos").data.copy()
    ds.filter_rows(lambda r: r["RecordActive"] == '1')
    ds.add_column_constant("DOMAIN", "DS")
    ds.drop_columns("subjectId")
    study.apply_common_transforms(ds)
    ds.rename_columns({"DS_EOSSTDAT": "DSSTDTC"})
    common.make_tc(ds, "DSSTDTC")

    ds.rename_columns({"DS_EOSDECOD": "DSSCAT"})

    ds.filter_rows(lambda r: r["DS_EOSNY"] == "No")
    ds.add_column_constant("DSDECOD", "Study Discontinued")

    common.studyDUR(ds.data, "RFSTDTC", "DSSTDTC", "DSSTDY")
    ds.data["DSSTDY"] = ds.data["DSSTDY"].apply(lambda x: x if pd.isnull(x) or int(x) != 0 else 1)

    ds.set_columns(["subjectId", "SITEID", "visitId", "DOMAIN",
                    "DSSCAT", "DSDECOD", "RFSTDTC", "DateCreated", "DataPageName", "DataPageId"])

    return ds
