import subprocess
from typing import List

from sdw4_mapping.core.envvars import envVars

def cmr_loader(folder: str,
               study_ref: str = None,
               domains: List[str] = None
) -> None:
    username = envVars.cmr_pg_username
    host = envVars.cmr_pg_host
    port = envVars.cmr_pg_port
    database = envVars.cmr_pg_db
    args = ['node', envVars.cmr_loader_script_path, '--username', str(username), '--host', str(host), '--port', str(port),
            '--database', str(database), '--folder', str(folder)]
    if study_ref is not None:
        args.extend(['--studyRef', str(study_ref)])
    if domains is not None:
        domainsArgs = ['--domains']
        domainsArgs.extend(domains)
        args.extend(domainsArgs)
    completed = subprocess.run(args)
    if (completed.returncode != 0):
        raise Exception('Failed subprocess ' + ' '.join(args))
