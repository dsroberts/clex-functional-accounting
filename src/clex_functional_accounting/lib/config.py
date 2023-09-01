import os

try:
    remote_fs_keys = os.environ.get('REMOTE_FS_KEYS').split(',')
except AttributeError:
    remote_fs_keys = []

try:
    remote_fs_paths = os.environ.get('REMOTE_FS_PATHS').split(',')
except AttributeError:
    remote_fs_paths =[]

settings = {
    'cosmos_host': os.environ.get('COSMOS_ACCOUNT_HOST'),
    'key': os.environ.get('COSMOS_ACCOUNT_KEY'),
    'database_id': os.environ.get('COSMOS_DATABASE', 'Accounting'),
    'remote_cmd_host': os.environ.get('REMOTE_CMD_HOST'),
    'remote_fs_keys': remote_fs_keys,
    'remote_fs_paths': remote_fs_paths,
    'dry_run': os.environ.get('CLEXFA_DRY_RUN',False)
}
