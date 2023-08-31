#!/usr/bin/env python3
import uuid
from datetime import datetime

from typing import Dict, List, Union

from ..lib import cosmosdb, remote_command, config, group_list

def construct_compute_entry(user: str, val: str, ts: str, proj: str) -> Dict[str,Union[str,float]]:
    return {
        'id': str(uuid.uuid4()),
        'ts': ts,
        'project': proj,
        'system': config.settings['remote_cmd_host'],
        'user': user,
        'usage': float(val),
    }

def parse_block(block: List[str],ts: str) -> List[Dict[str,Union[str,float]]]:
    ### First, get the project, this will always be in the first line
    out=[]
    
    try:
        proj=block[0].split('=')[1].split(' ')[0]
    except IndexError:
        ### Looks like we've been fed some garbage
        return out

    in_user_block=False
    user_start_block_seen=False

    for line in block:
        linelist = line.split()
        if not linelist: continue
        if in_user_block:
            
            if linelist[0] == "-------------------------------------------------------------":
                if user_start_block_seen:    
                    in_user_block=False
                else:
                    user_start_block_seen=True
            else:                
                out.append(construct_compute_entry(linelist[0],linelist[1],ts,proj))
        else:
            if linelist[0] == "Grant:":
                out.append(construct_compute_entry('grant',linelist[1],ts,proj))
            elif linelist[0] == "Used:":
                out.append(construct_compute_entry('total',linelist[1],ts,proj))
            elif linelist[0] == "User":
                in_user_block=True
            elif linelist[0] == "massdata":
                ### Found some massdata usage, add it to the storage container
                out.append({
                    'id':  str(uuid.uuid4()),
                    'ts': ts,
                    'system': config.settings['remote_cmd_host'],
                    'project': proj,
                    'fs': 'massdata',
                    'usage': int(linelist[1]),
                    'quota': 0,
                    'limit': 0,
                    'iusage': int(linelist[2]),
                    'iquota': 0,
                    'ilimit': 0
                })
    
    return out
        

def main():

    ### Placeholder for grabbing list of groups
    ts = str(datetime.now())
    my_groups = group_list.get_group_list()

    nci_account_out = remote_command.run_remote_cmd([f'for i in {" ".join(my_groups)}; do nci_account -P $i -vvv --no-pretty-print; sleep 0.1; done'])

    block_start = 1
    block_end = 0
    entry_list=[]
    for i,line in enumerate(nci_account_out):
        if line.startswith("Usage Report"):
            block_end=i-1
            entry_list.extend(parse_block(nci_account_out[block_start:block_end],ts))
            block_start=i
    
    ### Get the last entry
    entry_list.extend(parse_block(nci_account_out[block_start:],ts))
    
    writer = cosmosdb.CosmosDBWriter()
    _ = writer.get_container('compute',"Accounting","project",quarterly=True)
    _ = writer.get_container('storage',"Accounting","project",quarterly=True)

    for entry in entry_list:
        ### In this case we can have both 'storage' and 'compute' entries
        ### Figure out which and create the item in the right database
            if 'fs' in entry:
                writer.create_item('storage',entry)
            else:
                writer.create_item('compute',entry)

if __name__ == "__main__":
    main()