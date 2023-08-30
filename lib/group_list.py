from . import remote_command

from typing import List

def get_group_list() -> List[str]:
    ### Replace me with 'official' group list
    return remote_command.run_remote_cmd(["id","-Gn"])[0].split()