import os
import pandas as pd


def txt_to_csv(
    file_in,
    file_out=None,
    delimiter=',',
    has_header=True,
    column_names=None,
    delete_txt=False
):
    """Convert a txt file to a csv.
    
    Args:
    - file_in (str): A path to a txt file.
    - file_out (str): A path to a csv file. If 'None', then the input file path
        is used.
    - delimiter (str): A txt file delimiter.
    - has_header (bool): If True, use the first row of the txt file as a column
        header. If False, insert a column header using the column_names arg.
        Default is True.
    - column_names (list[str]): An ordered list of column names to use in the
        output csv. If 'None' and has_header is False, insert an ordered,
        integer column header (e.g. 1, 2, ..., n where n is the number of
        columns).
    - delete_txt (bool): If True, delete the input txt file.

    Returns:
    - file_out (str): A csv file path.
    """
    
    if has_header == True:
        # Force str dtype, otherwise pandas will do things like cast int's to
        # floats
        df = pd.read_csv(file_in, delimiter=delimiter, dtype=str)

    elif has_header == False and column_names != None:
        df = pd.read_csv(file_in, delimiter=delimiter, dtype=str, header=None)
        df.columns = column_names

    elif has_header == False and column_names == None:
        df = pd.read_csv(file_in, delimiter=delimiter, dtype=str, header=None)
        # 1-indexed column labels can simplify downstream processing
        df.columns = df.columns + 1

    if file_out == None:
        file_out = file_in[:-4] + '.csv'

    df.to_csv(file_out, index=False)

    if delete_txt:
        os.remove(file_in)

    return file_out


def txt_files_to_csv(
    path_in,
    path_out=None,
    delimiter=',',
    has_header=False,
    column_names=None,
    delete_txt=False,
    include_subdirs=False
):
    """Convert all txt files in a directory to csv files. Also works with a 
    single txt file path and can be optionally configured to process files in
    all subdirectories.
    
    Args:
    - path_in (str): A file or directory path containing zero or more txt files.
    - path_out (str): A file or directory path to write csv file(s) to. If
        'None', then the input path is used. Note that a file will retain its
        original names except with a .csv extension.
    - delimiter (str): A txt file delimiter. Note that this function assumes
        that all txt files in an input directory use the same delimiter.
    - has_header (bool): If True, use the first row of the txt file(s) as a 
        column header. If False, insert a column header using the column_names
        arg. Default is True.
    - column_names (list[str]): An ordered list of column names to use in the
        output csv(s). If 'None' and has_header is False, insert an ordered,
        integer column header (e.g. 1, 2, ..., n where n is the number of
        columns).
    - delete_txt (bool): If True, delete all of the input txt files.
    - include_subdirs (bool): If True, process all files in all subdirectories.
        If False, only process files in the top level of the specified
        directory. Default is False.

    Returns:
    - path_out (str): A file or directory path containing the output csv
        file(s).
    """

    for root, _, files in os.walk(path_in):
        
        for file in files:

            # Only process txt files
            if file[-4:] != '.txt':
                continue
            
            filepath_in = os.path.join(root, file)

            if path_out == None:
                dir_out = root
            else:
                dir_out = path_out

            filename_out = file[:-4] + '.csv'
            filepath_out = os.path.join(dir_out, filename_out)

            txt_to_csv(
                file_in=filepath_in,
                file_out=filepath_out,
                delimiter=delimiter,
                has_header=has_header,
                column_names=column_names,
                delete_txt=delete_txt
            )

        if include_subdirs == False:
            break
        
    if path_out == None:
        path_out = path_in

    return path_out
