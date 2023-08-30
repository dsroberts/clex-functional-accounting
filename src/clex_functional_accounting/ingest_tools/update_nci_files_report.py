#!/usr/bin/env python3
import json
import uuid
from datetime import datetime

from ..lib import cosmosdb, remote_command, config, group_list

def main():

    ### Grab the list of groups we care about out of the database
    writer = cosmosdb.CosmosDBWriter()
    _ = writer.get_container("groups","Accounting")
    _ = writer.get_container("users","Accounting")
    all_groups = dict([ (i['gid'],i['id'] ) for i in writer.read_all_items("groups") ])
    ### Use this to key off of username rather than uid on db insert
    all_users = dict([(i['uid'],i['id']) for i in writer.read_all_items("users") ])

    _ = writer.get_container("files_report","Accounting",quarterly=True)

    my_groups = group_list.get_group_list()

    ts=str(datetime.now())

    defer_entry = False
    unknown_users=set()
    unknown_groups=set()
    deferred_entries=[]

    for i,fs in enumerate(config.settings['remote_fs_keys']):
        fs_path=config.settings['remote_fs_paths'][i]

        ### Figure out if their {{filesystem}} directories a) exist and b) use group or
        ### project quotas
        ### Figure out some way to capture this in the db
        quota_types = remote_command.run_remote_cmd( [ f'for i in {" ".join([ i[1] + "+" + str(i[0]) for i in all_groups.items() if i[1] in my_groups])}; do if [[ -d {fs_path}/${{i%+*}} ]]; then lfs quota -p ${{i#*+}} {fs_path}/${{i%+*}} > /dev/null 2>&1 && echo ${{i%+*}} --project || echo "${{i%+*}}" "--group"; fi; done' ] )
        ### Organise into quota types dict
        quota_types_d={}
        for i in quota_types:
            proj, kind = i.split()
            try:
                quota_types_d[kind].append(proj)
            except KeyError:
                quota_types_d[kind] = [ proj, ]
        
        for k,v in quota_types_d.items():
            if v:
                nci_files_report_out = json.loads(remote_command.run_remote_cmd(['nci-files-report',k,' '.join(v),'--filesystem',fs,'--json'])[0])
                for entry in nci_files_report_out:
                    defer_entry = False
                    unknown_users=set()
                    unknown_groups=set()
                    deferred_entries=[]

                    try:
                        user = all_users[entry['uid']]
                    except:
                        user = entry['uid']
                        defer_entry = True
                        unknown_users.add(entry['uid'])
                    
                    try:
                        ownership = all_groups[entry['gid']]
                    except KeyError:
                        ownership = entry['gid']
                        defer_entry = True
                        unknown_groups.add(entry['gid'])

                    
                    location = entry['project']
                    ### In theory this should never happen as we can't access
                    ### quota info about projects we're not a member of
                    if location not in list(all_groups.values()):
                        defer_entry = True
                        unknown_groups.add(entry['project'])

                    size = 512 * int(entry['blocks']['single'] + entry['blocks']['multiple'])
                    inodes = int(entry['count']['single'] + entry['count']['multiple'])

                    db_entry= { 'ts':ts,
                                'id':str(uuid.uuid4()),
                                'fs':fs,
                                'user':user,
                                'ownership':ownership,
                                'location':location,
                                'size':size,
                                'inodes':inodes,
                                'system':config.settings['remote_cmd_host'],
                                'PartitionKey':f'{user}_{ownership}_{location}'
                              }

                    if defer_entry:
                        deferred_entries.append(db_entry)
                    else:
                        writer.create_item("files_report",db_entry)
        
        if unknown_users:
            missing_user_data=remote_command.run_remote_cmd([f'for i in {" ".join([ str(i) for i in unknown_users ])}; do getent passwd $i; id -Gn $i; sleep 0.01; done'])
            for passwd,idgn in zip(missing_user_data[0::2],missing_user_data[1::2]):
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
                writer.create_item('users',user_entry)
                print(f"User entry for {user_entry['id']} created")
                all_users[user_entry['uid']]=user_entry['id']

        if unknown_groups:
            missing_group_data=remote_command.run_remote_cmd([f'for i in {" ".join([ str(i) for i in unknown_groups ])}; do getent group $i; done'])
            print(missing_group_data)
            for line in missing_group_data:
                print(unknown_groups)
                split_line=line.split(':')
                if split_line[0] in unknown_groups or int(split_line[2]) in unknown_groups:
                    group_users=split_line[3].split(',')
                    group_entry={ 'id': split_line[0],
                                  'gid': int(split_line[2]),
                                  'PartitionKey': config.settings['remote_cmd_host'],
                                  'users': group_users
                                }
                    writer.create_item('groups',group_entry)
                    ### unknown_groups can contain either gids or group names
                    ### Fortunately, gids are always first, so we can discard the
                    ### group name if it exists to prevent multiple entries
                    ### being created for the same group name.
                    unknown_groups.discard(split_line[0])
                    unknown_groups.discard(int(split_line[2]))
                    print(f"Group entry for {group_entry['id']} created")
                    all_groups[group_entry['gid']]=group_entry['id']
        
        for entry in deferred_entries:
            try:
                entry['user']=all_users[entry['user']]
            except KeyError:
                ### OK to pass, means its already a valid group
                pass
            
            try:
                entry['ownership']=all_groups[entry['ownership']]
            except KeyError:
                pass

            try:
                entry['location']=all_groups[entry['location']]
            except KeyError:
                pass

            entry['PartitionKey'] = f'{entry["user"]}_{entry["ownership"]}_{entry["location"]}'
            writer.create_item("files_report",db_entry)

if __name__ == "__main__":
    main()