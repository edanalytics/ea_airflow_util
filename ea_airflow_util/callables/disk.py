import os
import shutil

def delete_from_disk(local_path: str):
    """
    Delete a file or directory from the local filesystem.
    """
    if os.path.isfile(local_path):
        os.remove(local_path)
    elif os.path.isdir(local_path):
        shutil.rmtree(local_path)