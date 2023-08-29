import os

settings = {
    'cosmos_host': os.environ.get('COSMOS_ACCOUNT_HOST'),
    'key': os.environ.get('COSMOS_ACCOUNT_KEY'),
    'database_id': os.environ.get('COSMOS_DATABASE', 'Accounting'),
    'remote_cmd_host': os.environ.get('REMOTE_CMD_HOST'),
    'remote_fs_keys': os.environ.get('REMOTE_FS_KEYS').split(','),
    'remote_fs_paths': os.environ.get('REMOTE_FS_PATHS').split(',')
}
