import azure.cosmos.aio as cosmos_aio
import azure.cosmos.exceptions as cosmos_exceptions
from azure.cosmos.partition_key import PartitionKey
from datetime import datetime
import asyncio

from typing import Dict, Any, Optional, List, Union

from .. import config

HOST: str = config.settings['cosmos_host']
WRITER_KEY: str = config.settings['key']
DATABASE_ID:str = config.settings['database_id']
DRY_RUN: bool = config.settings['dry_run']

class CosmosDBWriter():
    def __init__(self):
        self.client = cosmos_aio.CosmosClient(HOST, {'masterKey':WRITER_KEY}, user_agent="CosmosDBPythonQuickstart", user_agent_overwrite=True)
        self.db_clients = {}
        self.container_clients = {}
        self.quarterly = {}
        self.client_caches = {}
    
    async def close(self):
        await self.client.close()

    async def get_db(self,db_id: str) -> cosmos_aio.DatabaseProxy:
        if db_id not in self.db_clients:
            try:
                self.db_clients[db_id] = await self.client.create_database(id=db_id)
            except (cosmos_exceptions.CosmosResourceExistsError,cosmos_exceptions.CosmosHttpResponseError):
                self.db_clients[db_id] = self.client.get_database_client(db_id)
        return self.db_clients[db_id]
        
    async def get_container(self,container_id: str,db_id: Optional[str]=None,partition_key: str="PartitionKey",quarterly: Optional[bool]=False) -> cosmos_aio.ContainerProxy:
        if container_id not in self.container_clients:
            if not db_id:
                raise KeyError(f"If {container_id} does not already have a client, {db_id} must be provided")
            db = await self.get_db(db_id)
            self.quarterly[container_id] = quarterly
            try:
                self.container_clients[container_id] = await db.create_container(id=container_id,partition_key=PartitionKey(path='/PartitionKey'))
            except (cosmos_exceptions.CosmosResourceExistsError,cosmos_exceptions.CosmosHttpResponseError):
                self.container_clients[container_id] = db.get_container_client(container_id)
            
        return self.container_clients[container_id]
    
    async def create_item(self, container: str,d: Dict[str,Any]) -> None:

        if DRY_RUN:
            print(f"Would have created: {d}")
            return
        
        if container not in self.container_clients:
            raise NotImplementedError("Container client does not exist")
        
        container_client = await self.get_container(container)
        d['PartitionKey'] = self._get_partition_key_val(container)
        await container_client.create_item(body=d)

    async def delete_item(self, container: str, d: Union[Dict[str,Any],str] ) -> None:

        if DRY_RUN:
            print(f"Would have deleted: {d}")
            return

        if container not in self.container_clients:
            raise NotImplementedError("Container client does not exist")

        container_client=self.get_container(container)
        pk = None
        if isinstance(d,dict):
            pk=d.get('PartitionKey',None)
        if not pk:
            pk = self._get_partition_key_val(container)
        await container_client.delete_item(d,pk)

    async def read_items(self, container: str, item: Any, field: Optional[str] =  None, partition_key_val: Optional[str] = None, once_off: bool = False) -> List[Dict[str,Any]]:

        ### 999 times out of 1000 its going to be faster to just grab everything and do the search within
        ### python itself. These databases are so small it just doesn't matter
        if container not in self.container_clients:
                raise NotImplementedError("Container client does not exist")
        if once_off:
            if field is not None:
                raise KeyError("When 'once_off' is true the field can only be id")
        
            container_client = await self.get_container(container)
            partition_key_val = self._get_partition_key_val(container)

            try:
                return [ await container_client.read_item(item=item,partition_key=partition_key_val), ]
            except cosmos_exceptions.CosmosResourceNotFoundError:
                return []

        else:

            all_items=await self.read_all_items(container)
        
            if field:
                if field not in all_items[0]:
                   raise KeyError(f"{container} entries do not contain {field}")

                return [ k for k in all_items if k[field] == item ]

            return [ k for k in all_items if k['id'] == item ]

        ### This code is for the case where the above assertion turns out
        ### to be false


    async def read_all_items(self,container: str) -> List[Dict[str,Any]]:

        if container not in self.client_caches:

            if container not in self.container_clients:
                raise NotImplementedError("Container client does not exist")
        
            self.client_caches[container] = [ item async for item in self.container_clients[container].read_all_items() ]
        
        return self.client_caches[container]
        
    
    async def upsert_item(self,container: str, d: Dict[str,Any]) -> None:
        
        if DRY_RUN:
            print(f"Would have created: {d}")
            return

        if container not in self.container_clients:
            raise NotImplementedError("Container client does not exist")

        container_client = await self.get_container(container)
        d['PartitionKey'] = self._get_partition_key_val(container)
        await container_client.upsert_item(body=d)

    async def query(self, container: str, fields: Optional[Union[str,List[str]]]=None,where: Optional[List[str]] = None,order: Optional[str] = None,offset: Optional[int] = None,limit: Optional[int] = None):

        if container not in self.container_clients:
            raise NotImplementedError("Container client does not exist")

        q = "SELECT"
        if not fields:
            q+=" * "
        elif isinstance(fields,list):
            q+=f" VALUE {{ {', '.join([ i+':  c.'+i for i in fields ])} }}"
        elif isinstance(fields,str):
            q+= f" VALUE {{ {fields}: c.{fields} }}"
        q += " FROM c"

        if where:
            if isinstance(where,List):
                q+=f" WHERE c.{' AND c.'.join(i for i in where)}"
            elif isinstance(where,str):
                q+=f" WHERE c.{where}"

        if order:
            q+=f" ORDER BY c.{order}"

        if offset:
            q+=f" OFFSET {offset}"
        elif limit:
            q+=f" OFFSET 0"

        if limit:
            q+=f" LIMIT {limit}"
        elif offset:
            q+=f" LIMIT 10000"

        container_client = await self.get_container(container)
        try:
            return [ item async for item in container_client.query_items(q)]
        except cosmos_exceptions.CosmosHttpResponseError:
            return []

    def _get_partition_key_val(self,container: str, quarter: Optional[str] = None):
        if self.quarterly[container]:
            if quarter:
                return quarter
            else:
                date = datetime.now()
                return f"{date.year}.q{(date.month-1)//3+1}"
                
        else:
            return "1"