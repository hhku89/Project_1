#!/usr/bin/env python

import os
import shutil
import sys
import settings
import Rapiscan
import sdw4_mapping
import patches
import patch_date_created
# import patch_read_data

outf = settings.output_folder

study = Rapiscan.Rapiscan()

# This is a fail-safe.
# If we are *not* in production, then we probably *should not* modify
# the shared drives.  It is a bit of a kludge.
if not settings.is_production:
    if ("dfwpnfps14" in study.input_folder.lower()
            or "sdw_share" in study.input_folder.lower()
            or "dfwpnfps14" in study.archive_folder.lower()
            or "sdw_share" in study.archive_folder.lower()):
        raise Exception(
            "Alter data on shared drives from non-prod???????")
# ---end fail-safe---

patch_date_created.do_created_dates_procedure(
    sas_folder=study.raw_input_folder,
    output_folder=study.input_folder,
    archive_folder_base=study.archive_folder,
    cache_data=False)

study.cook_ae().to_csv(outf)
study.cook_aereact().to_csv(outf)
study.cook_aereact2().to_csv(outf)
study.cook_cm().to_csv(outf)
study.cook_cmr().to_csv(outf)
study.cook_cmrev().to_csv(outf)
study.cook_conmeth().to_csv(outf)
study.cook_dm().to_csv(outf)
study.cook_ds().to_csv(outf)
study.cook_dseos().to_csv(outf)
study.cook_ecgd().to_csv(outf)
study.cook_eg().to_csv(outf)
study.cook_egfr().to_csv(outf)
study.cook_ex().to_csv(outf)
study.cook_exg().to_csv(outf)
study.cook_flu().to_csv(outf)
study.cook_fpe().to_csv(outf)
study.cook_ie().to_csv(outf)
study.cook_lb().to_csv(outf)
study.cook_mh().to_csv(outf)
study.cook_mhyn().to_csv(outf)
study.cook_pe().to_csv(outf)
study.cook_pevcg().to_csv(outf)
study.cook_poim().to_csv(outf)
study.cook_povcg().to_csv(outf)
study.cook_povcgjoint().to_csv(outf)
study.cook_sae().to_csv(outf)
study.cook_su().to_csv(outf)
study.cook_sub().to_csv(outf)
study.cook_sus().to_csv(outf)
study.cook_sv().to_csv(outf)
study.cook_vs().to_csv(outf)
# study.cook_lbs().to_csv(outf)


if settings.should_update_datamart:
    print("Updating datamart.")
    sys.stdout.flush()  # don't mix output with subprocess
    sdw4_mapping.cmr_loader(
        folder=outf,
        study_ref=settings.study_ref)

processed_folder = os.path.join(
    study.raw_input_folder,
    "processed")
print(f"caching {outf} to {processed_folder}")
try:
    shutil.rmtree(processed_folder)
except Exception as e:
    print(f"while deleting old {processed_folder}: {e}")
shutil.copytree(outf, processed_folder)
