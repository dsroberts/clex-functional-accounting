import subprocess

from typing import List

from . import config

def run_remote_cmd(cmd: List[str]) -> List[str]:
    
    p = subprocess.run(["ssh",config.settings['remote_cmd_host']] + cmd,check=True,capture_output=True)

    return p.stdout.decode().split('\n')[:-1]
