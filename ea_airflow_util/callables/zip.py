import os
import zipfile
from collections.abc import Callable
from typing import Any


def extract_zips(
    local_dir: str,
    extract_dir: str | None = None,
    filter_lambda: Callable[[Any], bool] | None = None,
    remove_zips: bool = False,
):
    """
    Extract zip files from a local_dir to an extract_dir, optionally filtering on filepath
    """
    # Ensure extract directory exists before unzipping to it.
    if extract_dir is None:
        extract_dir = local_dir
    os.makedirs(extract_dir, exist_ok=True)

    files_to_extract = [file for file in os.listdir(local_dir) if file.endswith("zip")]
    files_to_extract = list(
        filter(filter_lambda, files_to_extract)
    )  # Optional extra filter

    for file in files_to_extract:
        zip_path = os.path.join(local_dir, file)
        unzip_path = os.path.join(extract_dir, file.replace(".zip", ""))

        os.makedirs(unzip_path, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(unzip_path)

        if remove_zips:
            os.remove(zip_path)
