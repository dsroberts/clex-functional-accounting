#!/usr/bin/env python3
from lib import cosmosdb, remote_command, config

import logging
import uuid
from datetime import datetime

def main():
    
    lquota_out=remote_command.run_remote_cmd(["lquota","-q","--no-pretty-print"])
    writer = cosmosdb.CosmosDBWriter()
    _ = writer.get_container("lquota","Accounting","project")
    
    ts = str(datetime.now())
    field_names=[ 'project','fs','usage','quota','limit','iusage','iquota','ilimit' ]

    for line in lquota_out:
        fields=line.split()
        for i in range(2,len(fields)):
            fields[i]=int(fields[i])
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

        writer.create_item("lquota",entry)

if __name__ == "__main__":
    main()