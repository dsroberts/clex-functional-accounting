#!/usr/bin/env python3
from ..lib import remote_command, config, group_list
from ..lib.cosmosdb import aio as cosmosdb

import asyncio
import uuid
from datetime import datetime

async def main():
    
    writer = cosmosdb.CosmosDBWriter()
    get_future = writer.get_container("storage",cosmosdb.DATABASE_ID,quarterly=True)
    latest_future = writer.get_container("storage_latest",cosmosdb.DATABASE_ID)
    lquota_out=remote_command.run_remote_cmd(["lquota","-q","--no-pretty-print"])
    
    ts = datetime.now().isoformat() + "Z"
    field_names=[ 'project','fs','usage','quota','limit','iusage','iquota','ilimit' ]

    my_groups = group_list.get_group_list()
    futures=[]
    entries=[]

    await asyncio.wait([get_future,latest_future])

    for line in lquota_out:
        fields=line.split(maxsplit=len(field_names))
        for i in range(2,len(fields)):
            try:
                fields[i]=int(fields[i])
            except ValueError:
                ### Attempt to do int('Over ... quota')...
                pass
        ### Can't really use id for anything here but it is required
        ### Put in some uuid
        #entry={ 'id': str(uuid.uuid4()),
        #        'project': fields[0],
        #        'fs': fields[1],
        #        'usage': fields[2],
        #       }
        entry=dict(zip(field_names,fields))
        entry['id'] = str(uuid.uuid4())
        entry['ts'] = ts
        entry['system'] = config.settings['remote_cmd_host']

        if entry['project'] in my_groups:
            entries.append(entry)
            futures.append(writer.create_item("storage",entry))

    await asyncio.wait(futures)
    print("Making latest entries")

    futures = []
    for entry in entries:
        entry['id'] = f"{entry['system']}_{entry['fs']}_{entry['project']}"
        if entry['project'] in my_groups:
            futures.append(writer.upsert_item("storage_latest",entry))

    await asyncio.wait(futures)
    futures = []

    ### Finally, handle stale entries in 'storage_latest' database
    for item in await writer.query('storage_latest',f'c.ts != "{ts}"'):
        futures.append(writer.delete_item(item))

    await writer.close()

def async_main():
    asyncio.run(main())

if __name__ == "__main__":
    async_main()