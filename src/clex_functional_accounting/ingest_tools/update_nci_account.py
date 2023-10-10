#!/usr/bin/env python3
import uuid
import asyncio
from datetime import datetime

from typing import Dict, List, Union

from ..lib import remote_command, config, group_list
from ..lib.cosmosdb import aio as cosmosdb

RATE_LIMIT=100

def construct_compute_entry(user: str, val: float, ts: str, proj: str) -> Dict[str,Union[str,float]]:
    return {
        'id': str(uuid.uuid4()),
        'ts': ts,
        'project': proj,
        'system': config.settings['remote_cmd_host'],
        'user': user,
        'usage': val,
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

    total_usage=0

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
                ### Actual usage equals reported usage minus reserved.
                out.append(construct_compute_entry(linelist[0],float(linelist[1])-float(linelist[3]),ts,proj))
        else:
            if linelist[0] == "Grant:":
                out.append(construct_compute_entry('grant',float(linelist[1]),ts,proj))
            elif linelist[0] == "Used:":
                ### Whereas in the summary actual usage equals usage.
                out.append(construct_compute_entry('total',float(linelist[1]),ts,proj))
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
        

async def main():

    writer = cosmosdb.CosmosDBWriter()
    compute_future = writer.get_container('compute',cosmosdb.DATABASE_ID,quarterly=True)
    compute_latest_future = writer.get_container('compute_latest',cosmosdb.DATABASE_ID)
    storage_latest_future = writer.get_container('storage_latest',cosmosdb.DATABASE_ID)
    storage_future = writer.get_container('storage',cosmosdb.DATABASE_ID,quarterly=True)
    ### Placeholder for grabbing list of groups
    ts = datetime.now().isoformat() + "Z"
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
    
    await asyncio.wait([compute_future,storage_future,compute_latest_future,storage_latest_future])

    futures = []
    counter = 0
    for entry in entry_list:
        ### In this case we can have both 'storage' and 'compute' entries
        ### Figure out which and create the item in the right database
            if 'fs' in entry:
                futures.append(writer.create_item('storage',entry))
            else:
                futures.append(writer.create_item('compute',entry))

            counter += 1
            if counter >= RATE_LIMIT:
                await asyncio.wait(futures)
                counter=0
                futures=[]

    if futures:
        await asyncio.wait(futures)

    futures = []
    counter = 0
    for entry in entry_list:
        if 'fs' in entry:
            entry['id'] = f"{entry['system']}_massdata_{entry['project']}"
            futures.append(writer.upsert_item('storage_latest',entry))
        else:
            entry['id'] = f"{entry['system']}_{entry['user']}_{entry['project']}"
            futures.append(writer.upsert_item('compute_latest',entry))

        counter += 1
        if counter >= RATE_LIMIT:
            await asyncio.wait(futures)
            counter=0
            futures=[]

    if futures:
        await asyncio.wait(futures)
    futures = []

    ### Finally, handle stale entries 'compute_latest' database
    for item in await writer.query('compute_latest',where=f'ts != "{ts}"'):
        futures.append(writer.delete_item('compute_latest',item))

    ### And any stale massdata entries
    for item in await writer.query('storage_latest',where=[f'ts != "{ts}"','fs = "massdata"']):
        futures.append(writer.delete_item('storage_latest',item))

    if futures:
        await asyncio.wait(futures)
    await writer.close()

def async_main():
    asyncio.run(main())

if __name__ == "__main__":
    async_main()
