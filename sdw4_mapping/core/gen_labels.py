import subprocess

from sdw4_mapping.core.envvars import envVars

def gen_labels(input_folder: str,
               output_file_path: str,
               odm_meta_path: str = None,
               overrides_csv_path: str = None) -> None:
    args = ['node', envVars.gen_labels_script_path, '--input-folder', str(input_folder), '--output-file-path', str(output_file_path)]
    if (odm_meta_path is not None):
        args.extend(['--odm-meta-path', str(odm_meta_path)])
    if (overrides_csv_path is not None):
        args.extend(['--overrides-csv-path', str(overrides_csv_path)])
    completed = subprocess.run(args)
    if (completed.returncode != 0):
        raise Exception('Failed subprocess ' + ' '.join(args))
