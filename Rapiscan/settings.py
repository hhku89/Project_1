import os
import pathlib
import json
import datetime
import platform
import production_settings

# default for development on Mohan's laptop
# _base intended to be private to this file
_base = r"C:\Users\hkumar3\work\XMR\Rapiscan_Prod"
input_folder = os.path.join(_base, "input")
output_folder = os.path.join(_base, "output")
study_ref = None
should_update_datamart = False

os.environ['SDW4_DATA_INPUT_DIR'] = input_folder
os.environ['SDW4_DATA_OUTPUT_DIR'] = output_folder
os.environ['GEN_LABELS_PATH'] = ''
os.environ['CMR_ENDPOINT'] = ''
os.environ['CMR_LOADER_PATH'] = ''
os.environ['CMR_PG_USERNAME'] = ''
os.environ['POSTGRES_PW'] = ''
os.environ['CMR_PG_HOST'] = ''
os.environ['CMR_PG_PORT'] = ''
os.environ['CMR_PG_DB'] = ''

is_production = False

nodename = platform.node()
print(f"Executing on {nodename}.");
# this is the study name as seen in the "scripts" folder and the UI
# when we have the files set up as per the "SDW4 server" conventions
study_name = os.path.basename(os.path.dirname(os.getcwd()))
print(f"study_name={study_name}")


def get_output_folder(base_path=None, now=None):
    base_path = base_path
    now = datetime.datetime.now()
    # implements 'basepath/YYYY/mm/YYYY-mm-dd-HHMMSS-millis'
    year = now.strftime("%Y")
    month = now.strftime("%m")
    parent = os.path.join(base_path, year, month)
    pathlib.Path(parent).mkdir(parents=True, exist_ok=True)
    folder = os.path.join(parent, now.strftime("%Y%m%d-%H%M%S-%f"))
    os.mkdir(folder)  # fails if already exists, which is desired behavior
    # since this has a millisecond time stamp
    return folder


# This is not the best way to do this....
def get_datamart_info(filename=None):
    try:
        with open(filename) as fh:
            info = json.load(fh)
        keys = list(info.keys())
        for key in keys:
            if "pass" in key.lower():
                del info[key]
        return info
    except:
        return {}


if nodename == production_settings.nodename:
    is_production = True
    # "raw" input folder
    input_folder = production_settings.input_folder
    # 'output_folder' is staging for upload to the database
    # (*not* where the "date created" is put)
    # it is the output of the mapping code
    output_folder = get_output_folder(
        production_settings.datamart_staging_base)
    study_ref = production_settings.study_ref
    should_update_datamart = True

    # This is the "output" of "date created",
    # the "input" of the mapping code proper
    os.environ['SDW4_DATA_INPUT_DIR'] = os.path.join(
        input_folder, "output")
    os.environ['SDW4_DATA_OUTPUT_DIR'] = output_folder

    datamart_info_filename = os.path.expanduser(
        production_settings.datamart_credentials_filename)
    info = get_datamart_info(datamart_info_filename)

    if not info:
        print("ALERT: want_datamart_update set but no info!!!!")
        print("ALERT: the datamart will not be updated!!!!")
    else:
        os.environ['CMR_LOADER_PATH'] = ('/data/sdw4/current-sdw4/'
                                         'sdw4/cmr-datamart/build/lib/index.js')
        os.environ['CMR_PG_USERNAME'] = info.get('username')
        os.environ['CMR_PG_HOST'] = info.get('host')
        os.environ['CMR_PG_PORT'] = str(info.get('port'))
        os.environ['CMR_PG_DB'] = info.get('database')
        os.environ['POSTGRES_PW'] = (
                'use-credentials-from-file=' + datamart_info_filename)
elif nodename == "ULW5CG0128KV0":  # Mike (Tex) Albert
    input_folder = "c:/Users/malbert2/covance-code/SDWetc/new-anons/anji900D3501/20210909/InputData"
    output_folder = "c:/Users/malbert2/covance-code/SDWetc/new-anons/anji900D3501/output"
elif nodename == "dfwtl0app078.ent.covance.com":  # informal test/tinker
    if study_name == "ANJ900D3501-DEV":
        input_folder = '/data/important-data/inputs/ANJ900D3501/ANJ900D3501-20210622-dev-A/'
        output_folder = get_output_folder('/data/important-data/outputs/ANJ900D3501-dev/')
        study_ref = -102
    elif study_name == "ANJ900D3501":
        input_folder = '/data/important-data/inputs/ANJ900D3501/ANJ900D3501-20210718-preprod-A/'
        output_folder = get_output_folder('/data/important-data/outputs/ANJ900D3501-preprod/')
        study_ref = -107
    else:
        raise ValueError(f"Don't know how to handle study_name={study_name}!")

    should_update_datamart = True

    # This is the "output" of "date created",
    # the "input" of the mapping code proper
    os.environ['SDW4_DATA_INPUT_DIR'] = os.path.join(
        input_folder, "output")
    os.environ['SDW4_DATA_OUTPUT_DIR'] = output_folder

    datamart_info_filename = os.path.expanduser(
        '~/credentials/tinker-datamart0.json')
    info = get_datamart_info(datamart_info_filename)

    if not info:
        print("ALERT: want_datamart_update set but no info!!!!")
        print("ALERT: the datamart will not be updated!!!!")
    else:
        os.environ['CMR_LOADER_PATH'] = ('/data/sdw4/current-sdw4/'
                                         'sdw4/cmr-datamart/build/lib/index.js')
        os.environ['CMR_PG_USERNAME'] = info.get('username')
        os.environ['CMR_PG_HOST'] = info.get('host')
        os.environ['CMR_PG_PORT'] = str(info.get('port'))
        os.environ['CMR_PG_DB'] = info.get('database')
        os.environ['POSTGRES_PW'] = (
                'use-credentials-from-file=' + datamart_info_filename)
