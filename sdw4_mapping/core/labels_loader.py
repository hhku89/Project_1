import subprocess

from sdw4_mapping.core.envvars import envVars

def labels_loader(study_ref: str, output_file_path: str) -> None:
    endpoint = 'http://' + envVars.cmr_endpoint + '/' + str(study_ref) + '/uploadAttribute'
    filePath = 'file=@' + output_file_path
    args = ['curl', '-S', '-X', 'POST', str(endpoint), '-F', str(filePath)]
    completed = subprocess.run(args)
    if (completed.returncode != 0):
        raise Exception('Failed subprocess ' + ' '.join(args))
