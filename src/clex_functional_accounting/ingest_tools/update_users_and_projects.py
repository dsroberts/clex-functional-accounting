#!/usr/bin/env python3
import asyncio
from ..lib import cosmosdb, remote_command, config, group_list

from typing import Dict, Any

#logging.basicConfig(level=logging.INFO)

async def maybe_update_entry(container_name: str, writer: cosmosdb.CosmosDBWriter, entry:Dict[str,Any]) -> None:

    try:
        out = await writer.read_items(container_name,entry['id'])
        out = out[0]
    except IndexError:
        out = None
    if not out:
        await writer.create_item(container_name,entry)
        print(f"{container_name} entry for {entry['id']} created")
        return

    for k in entry:
        if k not in out:
            await writer.upsert_item(container_name,entry)
            print(f"{container_name} entry for {entry['id']} updated")
            return
        if type(out[k]) == list:
            if sorted(out[k]) != sorted(entry[k]):
                await writer.upsert_item(container_name,entry)
                print(f"{container_name} entry for {entry['id']} updated")
                return
        else:
            if out[k] != entry[k]:
                await writer.upsert_item(container_name,entry)
                print(f"{container_name} entry for {entry['id']} updated")
                return
    
    return

async def main():

    writer = cosmosdb.CosmosDBWriter()
    groups_future = writer.get_container("groups","Accounting")
    users_future = writer.get_container("users","Accounting")
    ### First, return a list of all of the groups we're a part of, and create our entry
    ### In the user database
    my_groups = group_list.get_group_list()

    all_seen_users=set()
    ### Get all data from these groups
    all_group_data=remote_command.run_remote_cmd([f'for i in {" ".join(my_groups)}; do getent group $i; done'])

    await asyncio.gather(groups_future,users_future)

    futures=[]
    for line in all_group_data:
        split_line=line.split(':')
        group_users=split_line[3].split(',')
        all_seen_users|=set(group_users)
        
        group_entry={ 'id': split_line[0],
                      'gid': int(split_line[2]),
                      'PartitionKey': config.settings['remote_cmd_host'],
                      'users': group_users
                    }
        futures.append(maybe_update_entry("groups",writer,group_entry))
    await asyncio.gather(*futures)

    ### Now get all users in all of these groups - batch the command to avoid hitting rate limits at either end
    user_list=list(all_seen_users)
    batch_size=100
    for batch in range(len(user_list)//batch_size):
        futures=[]
        all_user_data=remote_command.run_remote_cmd([f'for i in {" ".join(user_list[batch*batch_size:(batch+1)*batch_size])}; do getent passwd $i; id -Gn $i; sleep 0.01; done'])
        for passwd,idgn in zip(all_user_data[0::2],all_user_data[1::2]):
            pw=passwd.split(':')
            groups=idgn.split()
            user_entry={ 'id': pw[0],
                         'uid': int(pw[2]),
                         'gid': int(pw[3]),
                         'pw_name': pw[4],
                         'home': pw[5],
                         'PartitionKey': config.settings['remote_cmd_host'],
                         'groups': groups
                        }
            futures.append(maybe_update_entry("users",writer,user_entry))
        await asyncio.gather(*futures)
    
    await writer.close()

def async_main():
    asyncio.run(main())

if __name__ == "__main__":
    async_main()