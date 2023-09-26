#!/usr/bin/env python3
from ..lib import remote_command, group_list, blob

def main():

    writer = blob.BlobWriter()
    ### First, return a list of all of the groups we're a part of, and create our entry
    ### In the user database
    my_groups = group_list.get_group_list()

    all_seen_users=set()
    ### Get all data from these groups
    all_group_data=remote_command.run_remote_cmd([f'for i in {" ".join(my_groups)}; do getent group $i; done'])

    group_d=writer.read_item(blob.CONTAINER,'groups')
    for line in all_group_data:
        split_line=line.split(':')
        group_users=split_line[3].split(',')
        all_seen_users|=set(group_users)
        
        group_d[split_line[0]]= { 'gid': int(split_line[2]),
                      'users': group_users
                    }
    writer.write_item(group_d,blob.CONTAINER,'groups')

    ### Now get all users in all of these groups - batch the command to avoid hitting sssd on Gadi too hard
    user_list=list(all_seen_users)
    batch_size=100
    user_d=writer.read_item(blob.CONTAINER,'users')
    for batch in range(len(user_list)//batch_size+1):
        all_user_data=remote_command.run_remote_cmd([f'for i in {" ".join(user_list[batch*batch_size:(batch+1)*batch_size])}; do getent passwd $i; id -Gn $i; sleep 0.01; done'])
        for passwd,idgn in zip(all_user_data[0::2],all_user_data[1::2]):
            pw=passwd.split(':')
            groups=idgn.split()
            user_d[pw[0]]={ 'uid': int(pw[2]),
                         'gid': int(pw[3]),
                         'pw_name': pw[4],
                         'home': pw[5],
                         'groups': groups
                        }
    writer.write_item(user_d,blob.CONTAINER,'users')

if __name__ == "__main__":
    main()