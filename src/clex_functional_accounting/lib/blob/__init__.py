from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import azure.core.exceptions as az_exceptions
import json

from typing import Dict, Any, Optional, List, Union


from .. import config

HOST: str = config.settings['blob_host']
WRITER_KEY: str = config.settings['blob_key']
CONTAINER: str = config.settings['blob_container']
DRY_RUN: bool = config.settings['dry_run']

class BlobWriter():
    def __init__(self):
        self.client = BlobServiceClient(HOST, WRITER_KEY)
        self.container_clients = {}
        self.client_caches = {}

        if CONTAINER:
            _ = self.get_container(CONTAINER)

    def get_container(self,name: str) -> ContainerClient:

        if name not in self.container_clients:
        
            try:
                self.container_clients[name] = self.client.create_container(name)
            except az_exceptions.ResourceExistsError:
                self.container_clients[name] = self.client.get_container_client(name)
        
        return self.container_clients[name]
    
    def write_item(self,data:Dict[Any,Any],container: str, item: str) -> None:
        
        if DRY_RUN:
            print(f"Would have created new blob {item}")
            return

        self.client_caches[f"{container}_{item}"] = data

        with self.client.get_blob_client(container=container,blob=item) as blob_client:
            to_write = json.dumps(data)
            blob_client.upload_blob(to_write,overwrite=True)

    def read_item(self, container: str, item: str) -> Dict[Any,Any]:

        k = f"{container}_{item}"
        if k not in self.client_caches:
            with self.client.get_blob_client(container=container,blob=item) as blob_client:
                to_read = blob_client.download_blob().readall()
                self.client_caches[k] = json.loads(to_read)
        return self.client_caches[k]
        
    def finalise(self):
        for c in self.container_clients.values():
            c.close()
        self.client.close()
