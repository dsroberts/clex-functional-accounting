import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.database as cosmos_database
import azure.cosmos.container as cosmos_container
import azure.cosmos.exceptions as cosmos_exceptions
from azure.cosmos.partition_key import PartitionKey
from datetime import datetime

from typing import Dict, Any, Optional, List

from . import config

HOST: str = config.settings['cosmos_host']
WRITER_KEY: str = config.settings['key']
DATABASE_ID:str = config.settings['database_id']
DRY_RUN: bool = config.settings['dry_run']

class CosmosDBWriter():
    def __init__(self):
        self.client = cosmos_client.CosmosClient(HOST, {'masterKey':WRITER_KEY}, user_agent="CosmosDBPythonQuickstart", user_agent_overwrite=True)
        self.db_clients = {}
        self.container_clients = {}
        self.partition_keys = {}
        self.client_caches = {}

    def get_db(self,db_id: str) -> cosmos_database.DatabaseProxy:
        if db_id not in self.db_clients:
            try:
                self.db_clients[db_id] = self.client.create_database(id=db_id)
            except cosmos_exceptions.CosmosResourceExistsError:
                self.db_clients[db_id] = self.client.get_database_client(db_id)
        return self.db_clients[db_id]
        
    def get_container(self,container_id: str,db_id: Optional[str]=None,partition_key: str="PartitionKey",quarterly: Optional[bool]=False) -> cosmos_container.ContainerProxy:
        if container_id not in self.container_clients:
            if not db_id:
                raise KeyError(f"If {container_id} does not already have a client, {db_id} must be provided")
            db = self.get_db(db_id)
            if quarterly:
                d = datetime.now()
                container_name=f"{container_id}_{d.year}.q{(d.month-1)//3+1}"
            else:
                container_name=container_id
            try:
                self.container_clients[container_id] = db.create_container(id=container_name,partition_key=PartitionKey(path='/'+partition_key))
                self.partition_keys[container_id] = partition_key
            except cosmos_exceptions.CosmosResourceExistsError:
                self.container_clients[container_id] = db.get_container_client(container_name)
                self.partition_keys[container_id] = self.container_clients[container_id].read()['partitionKey']['paths'][0].lstrip('/')
            
        return self.container_clients[container_id]
    
    def create_item(self, container: str,d: Dict[str,Any]) -> None:

        if DRY_RUN:
            print(f"Would have created: {d}")
            return
        
        if container not in self.container_clients:
            raise NotImplementedError("Container client does not exist")
        
        container_client=self.get_container(container)
        pk=self.partition_keys[container]

        if pk not in d:
            raise KeyError(f"Partition Key ({pk}) not present in object to create")

        container_client.create_item(body=d)

    def read_items(self, container: str, item: Any, field: Optional[str] =  None) -> List[Dict[str,Any]]:

        ### 999 times out of 1000 its going to be faster to just grab everything and do the search within
        ### python itself. These databases are so small it just doesn't matter
        all_items=self.read_all_items(container)

        if not field:
            return [ k for k in all_items if k['id'] == item ]
        
        if field:
            if field not in all_items[0]:
                raise KeyError(f"{container} entries do not contain {field}")
        
            return [ k for k in all_items if k[field] == item ]

        ### This code is for the case where the above assertion turns out
        ### to be false
#        if container not in self.container_clients:
#            raise NotImplementedError("Container client does not exist")
#        
#        if not partition_key_val:
#            raise KeyError("Partition Key value must be provided")
#        
#        container_client=self.get_container(container)
#        partition_key=self.partition_keys[container]
#
#        try:
#            if not field or field == "id":
#                response=[ container_client.read_item(item=item,partition_key=partition_key_val), ]
#            else:
#                response = list(container_client.query_items(query=f"SELECT * FROM r WHERE r.{partition_key}='{partition_key_val}' AND r.{field}=@val",parameters=[{"name":"@val","value":item}]))
#        except cosmos_exceptions.CosmosResourceNotFoundError:
#            ### Return an empty list if we didn't find anything
#            return []
#
#        return response

    def read_all_items(self,container: str) -> List[Dict[str,Any]]:

        if container not in self.client_caches:

            if container not in self.container_clients:
                raise NotImplementedError("Container client does not exist")
        
            self.client_caches[container] = list(self.container_clients[container].read_all_items())
        
        return self.client_caches[container]
        
    
    def upsert_item(self,container: str, d: Dict[str,Any]) -> None:
        
        if DRY_RUN:
            print(f"Would have created: {d}")
            return

        if container not in self.container_clients:
            raise NotImplementedError("Container client does not exist")

        container_client=self.get_container(container)
        pk=self.partition_keys[container]

        if pk not in d:
            raise KeyError(f"Partition Key ({pk}) not present in object to create")

        container_client.upsert_item(body=d)
        

    

        

        
        







        
        
        

        
