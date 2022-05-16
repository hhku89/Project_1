#!/usr/bin/env python

# proposal 2021-09-23-B

import datetime
import os
import shutil
from collections import namedtuple
import tempfile
import zipfile

from sdw4_mapping.common.utils import read_data
from sdw4_mapping.common.utils import save_to_csv



FileToArchive = namedtuple("FileToArchive", [
        "file", # e.g., /sdw_share/..../MyStudy/xyz.csv
        "archive" # e.g., "xyz.csv")
    ])



def get_files_to_archive(sas_path):
    files_to_archive = []
    for name in os.listdir(sas_path):
        filepath = os.path.join(sas_path, name)
        if (name.lower().endswith(".csv") and
                os.path.isfile(filepath)):
            files_to_archive.append(
                FileToArchive(file=filepath, archive=name))
    return files_to_archive
    


def create_zip(files_to_archive, filename=None):
    def do_zip(zipf):
        for f in files_to_archive:
            zipf.write(f.file, f.archive)

    if filename is not None:
        with zipfile.ZipFile(filename, "w") as zipf:
            do_zip(zipf)
        return filename
    else:
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            with zipfile.ZipFile(tmp, "w") as zipf:
                do_zip(zipf)
            return tmp.name
                    
                
def create_archive(files_to_archive, archive_base_folder, 
                            now=None, zip_file_name="dataset.zip"):
    if now is None:
        now = datetime.datetime.now(datetime.timezone.utc)
    subfolder = os.path.join(archive_base_folder,
        f"{now.year:04d}-{now.month:02d}-{now.day:02d}"
        + f"T{now.hour:02d}.{now.minute:02d}"
        + f".{now.second:02d}.{now.microsecond:06d}Z")
    os.makedirs(subfolder)
    tmp = create_zip(files_to_archive)
    shutil.move(tmp, os.path.join(subfolder, zip_file_name))
    

def add_date_created(df_new, df_old=None, 
                        domain_name="**UNKNOWN**",
                        date_created=None, keys=["RecordId"],
                        column_name="DateCreated"):
    if date_created is None:
        # Default: created today
        date_created = datetime.datetime.now().isoformat(
                            timespec="seconds")

    if df_old is None or any(k not in df_old for k in keys):
        # Really don't have old data?
        return df_new.assign(**{column_name: date_created})

    # Just in case record ids aren't quite unique
    df_old_len0 = len(df_old)
    # The ...min(column_name) sometimes works?
    # df_old = df_old.groupby(keys).min(column_name).reset_index()
    # The next works...
    # df_old = df_old.groupby(keys)[[column_name]].min().reset_index()
    # but I think this is clearer... (max or min doesn't matter)
    df_old = df_old.groupby(keys).agg({column_name: "max"}).reset_index()
    df_old_len1 = len(df_old)
    if df_old_len0 != df_old_len1:
        print(f"ALERT: for domain {domain_name} key not unique; "
            + f" old files has {df_old_len1} uniques "
            + f" in {df_old_len0} rows with key {keys}")


    df = df_new.merge(df_old[keys + [column_name]],
                        how="left",
                        on=keys)

    df = df.assign(**{
                column_name: df[column_name].fillna(date_created)})

    return df


# This follows the logic in the javascript code.
# But is this useful?
def get_non_maching_keys(df1, df2, keys=["RecordId"],
                            column_name="ColumnsUpdated"):
    A = df1.set_index(keys,drop=False)
    B = df2.set_index(keys,drop=False)

    df = A.eq(B) | (A.isnull() & B.isnull())

    columns = df.columns
    A = A.assign(**{ column_name:
            df.apply(lambda row: 
                    "|".join(columns[i] for i,v in enumerate(row) 
                                                            if not v),
                     axis=1)})

    A = A.reset_index(drop=True)

    A = A[keys + [column_name]]

    df1 = df1.merge(A,
                how="left",
                on=keys)

    return df1
                


def columns_for_unique_key(domain, df, df_old, 
                                key_info_by_domain_name,
                                recordIdName="RecordId"):
    common_columns = set(df.columns).intersection(df_old.columns)
    requested_columns = set(key_info_by_domain_name.get(domain, set()))

    key_columns = None

    if recordIdName in common_columns:
        key_columns = requested_columns.union(set([recordIdName]))
    else:
        key_columns = requested_columns or common_columns

    assert key_columns, f"Can not determine keys for {domain}."

    return list(key_columns)


# For a details of how the "record-id key" is determined
# see columns_for_unique_key(), above.
#
# The key_info_by_domain_name is a dictionary whose keys are
# domain names and whose values are a list of columns intended
# to be used as the unique key, or part of the unique key.
#      - If you don't specify a key and there is a "RecordId",
#        then "RecordId" is the key.
#      - If you don't specify a key and there is no "RecordId",
#        all columns are used as the "key".
#      - If you specify a list of columns as the key and 
#        there is no "RecordId", then the list you specify is used
#        as the key.
#      - If specifiy a list of columns for the key and there is 
#        a "RecordId", then "RecordId" is added to the list of columns
#        you have specified.
#      - Given these rules, if there is a "RecordId" in the data, 
#        there is currently no way to avoid using it.  This was a
#        stated user requirement. If (when?) we find that that 
#        doesn't work, one possibility is to add a parameter 
#        such as key_determination_strategy() whose default 
#        value is the columns_for_unique_key() function.

def do_created_dates_procedure(
        sas_folder, # the "new" files
        archive_folder_base = None, # for zip files
        output_folder = None, # with created date
        cache_data = True,
        key_info_by_domain_name = {},
        audit_column_changes = False,
        date_created = None,  # will default to "today"
        date_created_column_name = "DateCreated",
        audit_column_changes_column_name = "ColumnsUpdated",
        domain_name_from_path = 
                lambda p: os.path.splitext(       # xyz/Foo.csv->foo
                               os.path.basename(p))[0].lower(),
        logger=print
    ):

    
    if archive_folder_base is None:
        archive_folder_base = os.path.join(sas_folder, "archive")
    if output_folder is None:
        output_folder = os.path.join(sas_folder, "output")
    if date_created is None:
        date_created = datetime.datetime.now().isoformat(
                                timespec="seconds")
    
    files_to_archive = get_files_to_archive(sas_folder)
    create_archive(files_to_archive, archive_folder_base)


    sas_files_by_domain = { domain_name_from_path(p):p for p in 
                            (f.file for f in files_to_archive) }

    os.makedirs(output_folder, exist_ok=True)
    old_output = { domain_name_from_path(p):p for p in 
        (os.path.join(output_folder, f) 
                for f in os.listdir(output_folder)
                        if f.lower().endswith(".csv"))
            if os.path.isfile(p) }

    all_domains = set(sas_files_by_domain.keys()).union(
                                            old_output.keys())

    cache = {} if cache_data else None

    for domain in all_domains:
        if domain in sas_files_by_domain and domain in old_output:

            df = read_data(sas_files_by_domain[domain], usecols=None)
            df_old = read_data(old_output[domain],usecols=None)
            keys = columns_for_unique_key(domain, df, df_old, 
                                key_info_by_domain_name)
            if date_created_column_name not in df_old.columns:
                if len(df_old) != 0:
                    logger(f"ALERT: old {domain} missing "
                            + f"{date_created_column_name}")
                df_old[date_created_column_name] = date_created
            if audit_column_changes:
                df = get_non_maching_keys(df, 
                        df_old = df_old.drop(
                            columns=[audit_column_changes_column_name],
                            errors="ignore"),
                        keys=keys,
                        column_name=audit_column_changes_column_name)
            df = add_date_created(df,
                            df_old,
                            domain_name=domain,
                            date_created=date_created,
                            keys=keys,
                            column_name=date_created_column_name)
            if cache_data:
                cache[domain] = df
            tmp = f"temp-updating-{domain}"
            save_to_csv(df, tmp, output_folder)
            os.remove(old_output[domain])
            os.rename(os.path.join(output_folder, f"{tmp}.csv"),
                        old_output[domain])
        elif domain in sas_files_by_domain: # and not in old_output
            df = read_data(sas_files_by_domain[domain], usecols=None)
            df = add_date_created(df,
                        domain_name=domain,
                        date_created=date_created,
                       column_name=date_created_column_name)
            if cache_data:
                cache[domain] = df
            save_to_csv(df, domain, output_folder)
        else: # only in old output
            os.remove(old_output[domain])

    return cache
