from . import blob

from typing import List

def get_group_list() -> List[str]:
    ### Replace me with 'official' group list
    writer = blob.BlobWriter()
    return writer.read_item(blob.CONTAINER,'projectlist')